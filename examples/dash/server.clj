(ns dash.server
  "The dashboard server. Bare Ring + Jetty + Datastar, no zodiac.

  Deliberately not a zodiac app: `examples/chat` already exercises the extension's
  integrant wiring, and this app exists to test fan-out under a rate spread. Fewer
  moving parts means a surprise here is about `watch`, not about wiring.

      clojure -M:dev:dash -m dash.server     # port 3004"
  (:import [java.util.concurrent ConcurrentHashMap Executors]
           [java.util.concurrent.atomic LongAdder])
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

(defonce ^{:doc "topic -> ConcurrentHashMap-backed set of conn-ids."}
  topic-subs
  (ConcurrentHashMap.))

(defonce ^{:doc "conn-id -> the topic set it is currently subscribed to."}
  conn-topics
  (ConcurrentHashMap.))

(defn- subscribe!
  "Sets `id`'s subscriptions to exactly `topics`.

  ## Why this is not one atom

  It was: a single `topic -> #{conn-id}` atom, updated with a `swap!` that rebuilt the
  whole map to remove this connection before re-adding it. That is O(topics) work per
  refresh, serialized by CAS retries, and under parallel fan-out it became the
  bottleneck rather than a fix for it — every worker thread contending on one atom,
  each retry redoing the whole rebuild.

  Now: a per-connection record of what it holds, so a change touches only the topics
  that actually changed, and two `ConcurrentHashMap`s so unrelated topics never
  contend. The common case — a refresh whose topic set is unchanged — does no work at
  all."
  [id topics]
  (let [wanted (set topics)
        current (or (.get conn-topics id) #{})]
    (when-not (= wanted current)
      (doseq [t current :when (not (contains? wanted t))]
        (when-let [ids (.get topic-subs t)]
          (.remove ^java.util.Set ids id)
          (when (.isEmpty ^java.util.Set ids) (.remove topic-subs t))))
      (doseq [t wanted :when (not (contains? current t))]
        (-> (.computeIfAbsent topic-subs t
                              (reify java.util.function.Function
                                (apply [_ _] (ConcurrentHashMap/newKeySet))))
            ^java.util.Set (.add id)))
      (.put conn-topics id wanted))))

(defn- subscribers
  "Connection ids subscribed to `topic`, as a snapshot safe to iterate."
  [topic]
  (if-let [ids (.get topic-subs topic)] (vec ids) []))

(defn- unsubscribe-all! [id]
  (doseq [t (or (.get conn-topics id) #{})]
    (when-let [ids (.get topic-subs t)]
      (.remove ^java.util.Set ids id)
      (when (.isEmpty ^java.util.Set ids) (.remove topic-subs t))))
  (.remove conn-topics id))

(declare eng)

(defonce ^{:doc "Work-stealing pool for fan-out. Sized to the machine, since the work
  is render-then-write per connection and mostly CPU."}
  fanout-pool
  (Executors/newWorkStealingPool))

(defonce ^{:doc "Counts what the fan-out actually did, for the rate-spread report.

  `LongAdder` rather than an atom: every worker thread increments these on every
  refresh, and a shared `swap!` here would reintroduce exactly the contention removed
  from `subscribe!` — measuring the instrumentation instead of the fan-out."}
  stats
  {:hints (LongAdder.) :refreshes (LongAdder.)
   :patches (LongAdder.) :pruned (LongAdder.)})

(defn- bump! [k ^long n] (.add ^LongAdder (get stats k) n))

(defn- stats-snapshot []
  (into {} (map (fn [[k ^LongAdder a]] [k (.sum a)])) stats))

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

  ## What was changed to lift it

  Two things, and the second mattered more than the first:

  1. **The fan-out runs on a work-stealing pool** rather than the caller's thread, so
     renders and socket writes for different connections proceed in parallel.
  2. **The subscription index is per connection** (see `subscribe!`). It used to be one
     atom holding `topic -> #{conn-id}`, rebuilt wholesale on every refresh. Adding
     threads to that made it *worse*, not better — every worker contending on one atom
     with each CAS retry redoing an O(topics) rebuild. Parallelism exposed it; it was
     already the wrong shape serially."
  [topic]
  (bump! :hints 1)
  (let [ids (subscribers topic)]
    (when (seq ids)
      ;; `invokeAll` blocks until every subscriber is done, which keeps back-pressure:
      ;; the simulation cannot outrun the fan-out and queue unbounded work. Firing and
      ;; forgetting would produce better numbers and an ever-growing queue.
      (.invokeAll
       ^java.util.concurrent.ExecutorService fanout-pool
       ^java.util.Collection
       (mapv (fn [id]
               (reify java.util.concurrent.Callable
                 (call [_]
                   (try
                     (let [{:keys [patches topics pruned]} (engine/refresh! eng id topic)]
                       (bump! :refreshes 1)
                       (bump! :patches (count patches))
                       (when pruned (bump! :pruned 1))
                       ;; Re-subscribe on every refresh: expanding a log adds
                       ;; `[:log id]`, collapsing it removes it, and neither is known
                       ;; until the render has happened.
                       (when (seq topics) (subscribe! id topics)))
                     (catch Throwable e
                       (println "refresh failed" id (pr-str topic) (ex-message e))))
                   ;; `Callable` must return something; the value is unused.
                   nil)))
             ids)))))

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
                :body (str (pr-str (stats-snapshot)) "\n"
                           "connections=" (count @(:registry eng)) "\n"
                           "topics=" (.size topic-subs) "\n")}
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
