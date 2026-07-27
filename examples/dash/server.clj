(ns dash.server
  "The dashboard server. Bare Ring + Jetty + Datastar, no zodiac.

  Deliberately not a zodiac app: `examples/chat` already exercises the extension's
  integrant wiring, and this app exists to test fan-out under a rate spread. Fewer
  moving parts means a surprise here is about `watch`, not about wiring.

      clojure -M:dev:dash -m dash.server     # port 3004"
  (:require [charred.api :as charred]
            [clojure.java.io :as io]
            [dash.jobs :as jobs]
            [dash.view :as view]
            [dev.onionpancakes.chassis.core :as h]
            [remuda.watch-engine :as engine]
            [ring.adapter.jetty :as jetty]
            [starfederation.datastar.clojure.adapter.ring :as d*ring]
            [starfederation.datastar.clojure.api :as d*]))

;;; ==========================================================================
;;; Subscriptions
;;; ==========================================================================
;;; A local topic -> connection index, rather than remuda.pubsub, because that
;;; namespace's coalescing flush loop is the thing under test elsewhere. Here the
;;; question is whether fragment selection holds up at 10 hints a second, so the
;;; subscription side is kept as simple as possible.

(defonce ^{:doc "topic -> #{conn-id}"} topic-subs (atom {}))

(defn- subscribe! [id topics]
  (swap! topic-subs
         (fn [m]
           ;; Replace this connection's set, not merge into it: a dependency set
           ;; shrinks as well as grows — collapsing a log unsubscribes its topic — and
           ;; only adding would leave the connection woken by data it no longer shows.
           (let [m (reduce-kv (fn [acc t ids]
                                (let [ids (disj ids id)]
                                  (if (seq ids) (assoc acc t ids) acc)))
                              {} m)]
             (reduce (fn [acc t] (update acc t (fnil conj #{}) id)) m topics)))))

(defn- unsubscribe-all! [id]
  (swap! topic-subs
         (fn [m] (reduce-kv (fn [acc t ids]
                              (let [ids (disj ids id)]
                                (if (seq ids) (assoc acc t ids) acc)))
                            {} m))))

(declare eng)

(defonce ^{:doc "Counts what the fan-out actually did, for the rate-spread report."}
  stats
  (atom {:hints 0 :refreshes 0 :patches 0 :pruned 0}))

(defn publish!
  "Refreshes every connection subscribed to `topic` and pushes what changed.

  ## This is the throughput bottleneck, measured

  Fan-out is SERIAL and runs on whichever thread called `publish!` — which is the
  simulation's single-threaded scheduler. So every render and every socket write for
  every subscriber happens on one thread, and the server saturates at roughly one
  core:

  | connections | events in 45s | server CPU | all alive? |
  |---|---|---|---|
  | 500 | ~360,000 | 48% | yes, silent=0 |
  | 1,000 | ~380,000 | 102% | yes, silent=0 |
  | 2,000 | ~238,000 | 102% | yes, silent=0 |

  Throughput *falls* between 1,000 and 2,000 while connection count keeps working, so
  the limit is per-hint work rather than connection capacity. Nothing dropped and no
  connection went silent at any level.

  Parallelising this is the obvious fix and is deliberately not done: the point of the
  measurement was to find where the ceiling is, and a `pmap` here would hide it behind
  8 cores rather than explain it. A real deployment would hand each subscriber's
  refresh to an executor, or coalesce hints per connection the way
  `remuda.pubsub`'s flush loop does — this app skips that loop on purpose so that
  fragment selection is what is under test."
  [topic]
  (swap! stats update :hints inc)
  (doseq [id (get @topic-subs topic)]
    (try
      (let [{:keys [patches topics pruned]} (engine/refresh! eng id topic)]
        (swap! stats (fn [s] (-> s
                                 (update :refreshes inc)
                                 (update :patches + (count patches))
                                 (cond-> pruned (update :pruned inc)))))
        ;; Re-subscribe on every refresh: expanding a log adds `[:log id]`, collapsing
        ;; it removes it, and neither is known until the render has happened.
        (when (seq topics) (subscribe! id topics)))
      (catch Throwable e
        (println "refresh failed" id (pr-str topic) (ex-message e))))))

(def eng
  (engine/engine {:components {:dash #'view/component}
                  :render-fn h/html}))

;;; ==========================================================================
;;; HTTP
;;; ==========================================================================

(def ^:private css "
:root{--bg:#0f1116;--panel:#171a21;--line:#262b36;--text:#e7e9ee;--muted:#8a92a6;
      --run:#4c8dff;--done:#3ddc84;--fail:#ff5c5c;--sp:12px;--r:8px}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);padding:calc(var(--sp)*2);
     font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace}
h1{font-size:18px;margin:0 0 var(--sp)}
.summary{margin-bottom:calc(var(--sp)*1.5)}
.stats{display:flex;gap:var(--sp);flex-wrap:wrap}
.stat{background:var(--panel);border:1px solid var(--line);border-radius:var(--r);
      padding:4px 10px;color:var(--muted)}
.stat b{color:var(--text)}
.stat--running b{color:var(--run)} .stat--done b{color:var(--done)}
.stat--failed b{color:var(--fail)}
.jobs{list-style:none;margin:0;padding:0;display:grid;gap:6px;max-width:760px}
.job{background:var(--panel);border:1px solid var(--line);border-radius:var(--r)}
.job__head{display:grid;grid-template-columns:110px 76px 1fr 48px;align-items:center;
           gap:var(--sp);padding:8px 10px;cursor:pointer}
.job__name{font-weight:600}
.job__status{color:var(--muted);font-size:12px}
.job__bar{height:6px;background:#00000055;border-radius:99px;overflow:hidden}
.job__fill{height:100%;background:var(--run);transition:width .12s linear}
.job--done .job__fill{background:var(--done)}
.job--failed .job__fill{background:var(--fail)}
.job__pct{text-align:right;color:var(--muted);font-size:12px}
.log{margin:0;padding:10px 12px;border-top:1px solid var(--line);color:var(--muted);
     font-size:12px;max-height:220px;overflow:auto;white-space:pre-wrap}
")

(defn- page []
  (h/html
   [h/doctype-html5
    [:html
     [:head
      [:meta {:charset "utf-8"}]
      [:title "build queue"]
      [:script {:type "module" :src "/datastar.js"}]
      [:style css]]
     [:body
      [:div {:id "dash"} "connecting…"]
      [:div {:data-init "@get('/d/live', {requestCancellation: 'cleanup'})"}]]]]))

(defn- live [request]
  (let [id* (promise)]
    (d*ring/->sse-response
     request
     {d*ring/on-open
      (fn [sse-gen]
        (let [id (engine/connect!
                  eng :dash
                  {:send! (fn [patches]
                            (doseq [{:keys [selector mode html]} patches]
                              (d*/patch-elements! sse-gen (or html "")
                                                  {d*/selector selector
                                                   d*/patch-mode (if (= :inner mode)
                                                                   d*/pm-inner
                                                                   d*/pm-outer)})))})]
          (deliver id* id)
          (swap! (:registry eng) update id assoc-in [:params :conn-id] id)
          (let [{:keys [html topics]} (engine/mount! eng id)]
            (subscribe! id topics)
            (d*/patch-elements! sse-gen html
                                {d*/selector "#dash" d*/patch-mode d*/pm-outer}))
          (d*/execute-script! sse-gen (str "window.__id=" (pr-str id) ";"))
          nil))
      d*ring/on-close
      (fn [& _]
        (when-let [id (deref id* 1000 nil)]
          (unsubscribe-all! id)
          (view/forget-connection! id)
          (engine/disconnect! eng id)))})))

(defn- toggle
  "Expands or collapses one job's log for one connection.

  Reads the datastar payload, which arrives as the JSON request body. The connection
  id comes from the client because a POST is a separate request with no association to
  the SSE stream — `window.__id` is set by the mount script."
  [request]
  ;; ## Two traps here, both measured
  ;;
  ;; 1. `d*/get-signals` returns the request body as a STREAM — a jetty `HttpInput` —
  ;;    not a parsed map and not a string. A version that tested `map?` then `string?`
  ;;    matched neither and produced nil, which surfaced as a 409 that read like a
  ;;    missing connection id rather than a parsing mistake.
  ;;
  ;; 2. `wrap-params` DRAINS that stream. It reads the body looking for form params on
  ;;    a POST, so by the time this runs there is nothing left to read — and the
  ;;    identical request sent by `curl` worked, because curl was hitting a handler
  ;;    reached before the middleware had a reason to look. The chat app hit the same
  ;;    wall with muuntaja and solved it by reading `:body-params`; here the fix is to
  ;;    keep `wrap-params` away from the body entirely (see `-main`).
  (let [payload (try (some-> (d*/get-signals request)
                             slurp
                             (charred/read-json :key-fn keyword))
                     (catch Exception e
                       (println "could not read toggle payload:" (ex-message e))
                       nil))
        id (:id payload)
        job (:job payload)]
    (if-let [conn-id id]
      (do (doseq [topic (engine/dispatch! eng conn-id :toggle {:job job})]
            (publish! topic))
          {:status 204})
      {:status 409 :body "no connection id"})))

(defn handler [{:keys [uri] :as request}]
  (case uri
    "/" {:status 200 :headers {"content-type" "text/html; charset=utf-8"} :body (page)}
    "/datastar.js" {:status 200 :headers {"content-type" "text/javascript"}
                    :body (slurp (io/resource "datastar.js"))}
    "/d/live" (live request)
    "/d/toggle" (toggle request)
    "/d/stats" {:status 200 :headers {"content-type" "text/plain"}
                :body (str (pr-str @stats) "\n"
                           "connections=" (count @(:registry eng)) "\n"
                           "topics=" (count @topic-subs) "\n")}
    {:status 404 :body "nope"}))

(defn async-handler [request respond _raise] (respond (handler request)))

(defn -main [& _]
  (jobs/reset-all!)
  (dotimes [_ 3] (jobs/enqueue! (rand-nth ["web" "api" "worker"])))
  (jobs/start! publish!)
  ;; `:async? true` with `:async-timeout 0` is load-bearing for SSE, not tuning — see
  ;; examples/watchspike.
  ;; `wrap-params` is applied to NOTHING here: no route reads query or form params, and
  ;; wrapping the whole app made it drain the JSON body before `/d/toggle` could read
  ;; it. That failure is invisible — the body is simply empty — and it does not
  ;; reproduce with `curl`.
  (jetty/run-jetty #'async-handler
                   {:port 3004 :join? false :async? true :jetty {:async-timeout 0}})
  (println "dashboard on http://localhost:3004/  (stats at /d/stats)")
  @(promise))
