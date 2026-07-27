(ns snapspike.server
  "Does the recovery snapshot do work nothing else can? A browser test.

  ## The question

  `:recoverable` and `remuda.snapshot` are ~834 lines that no example app has ever
  exercised. `examples/chat` declares `:draft` recoverable and then never transmits a
  snapshot — it reconnects by re-requesting `/live` with identity in a signed URL
  token, exactly as `examples/watchspike` does. So the machinery might be redundant.

  An earlier test appeared to show it was: a half-typed value survived a JVM restart
  with no server state at all. But that proved less than it looked. The value survived
  because the DOM held it, and the session rebuilt because the URL carried the
  identity. Neither is the snapshot's job.

  So this app removes both crutches:

  - **No identity in the URL.** The page is served at `/` with no channel, no user,
    no token. A reload cannot tell the server who it is.
  - **State the client cannot re-derive and no Source holds.** A server-assigned
    ticket number and the position in a queue. The browser never computed these and
    cannot recompute them; there is no database to re-read them from.

  ## What happened

  It works, and it needed no crutch: after killing the JVM, a fresh page load sent the
  stored snapshot and the server logged `RESTORED from snapshot: {:ticket 101,
  :queue-pos 19}` — state the client could not compute and no store held.

  But the first attempt did NOT restore, and that is the more useful result. Datastar's
  automatic SSE reconnect re-requests the URL it originally connected with, so the
  snapshot appended by `window.__connect` at first connect never rode along. Only a
  full page load re-evaluated `data-init` and picked up the stored value.

  So the snapshot recovers a **reload or a returning visitor**, not a dropped
  connection. A dropped connection is already handled by the reconnect re-issuing the
  original request — which is exactly what `examples/chat` and `examples/watchspike`
  rely on, and why neither ever needed a snapshot. Anything wanting snapshot recovery
  on transport reconnect has to rebuild the URL itself.

      clojure -M:dev:snapspike -m snapspike.server    # port 3003"
  (:require [clojure.java.io :as io]
            [dev.onionpancakes.chassis.core :as h]
            [remuda.snapshot :as snap]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :refer [wrap-params]]
            [starfederation.datastar.clojure.adapter.ring :as d*ring]
            [starfederation.datastar.clojure.api :as d*]))

(def secret "snapspike-example-secret-not-for-production")

;;; ==========================================================================
;;; State that is genuinely unrecoverable
;;; ==========================================================================
;;; Deliberately in a plain atom with no persistence. This is the crux of the test:
;;; if it were in SQLite the snapshot would be pointless, because `:mount` could
;;; re-read it. A ticket number assigned by a server that has since restarted exists
;;; nowhere else — not in the DOM as a value the user typed, not in any store.

(defonce ^{:doc "Next ticket to hand out. Reset by a restart, which is the point."}
  next-ticket
  (atom 100))

(defn issue-ticket! [] (swap! next-ticket inc))

(defn- page []
  (h/html
   [h/doctype-html5
    [:html
     [:head
      [:meta {:charset "utf-8"}]
      [:title "snapshot spike"]
      [:script {:type "module" :src "/datastar.js"}]
      [:style "
:root{--bg:#12141a;--panel:#1b1e27;--line:#2b3040;--text:#e6e8ee;--muted:#8b93a7;--ok:#3ddc84}
body{margin:0;font:15px/1.6 system-ui,sans-serif;background:var(--bg);color:var(--text);padding:24px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px;max-width:460px}
.big{font-size:32px;font-weight:600;color:var(--ok)}
.muted{color:var(--muted);font-size:13px}
code{background:#00000040;padding:2px 6px;border-radius:4px}"]]
     [:body
      [:p {:class "muted"}
       "URL carries no identity: " [:code "/"] " — nothing to rebuild from but the snapshot."]
      [:div {:id "live-root" :class "card"} "connecting…"]
      ;; The snapshot rides from localStorage into the connect request. This is the
      ;; whole mechanism: the client holds signed state it cannot read or forge, and
      ;; hands it back on reconnect.
      ;; A CLASSIC script, not a module: modules are deferred, so `window.__connect`
      ;; would not exist yet when Datastar evaluates `data-init` — the connect
      ;; simply never fired.
      ;;
      ;; It reads localStorage at call time rather than page load, because Datastar
      ;; reconnects the stream without re-running this script; a value captured now
      ;; would be the one from before the server issued a fresh snapshot.
      [:script
       "window.__connect = function () {
          var s = localStorage.getItem('snap');
          return '/live' + (s ? ('?snap=' + encodeURIComponent(s)) : '');
        };"]
      [:div {:data-init "@get(window.__connect(), {requestCancellation: 'cleanup'})"}]]]]))

(defn- view [{:keys [ticket queue-pos]}]
  [:div {:id "live-root" :class "card"}
   [:p {:class "muted"} "Your ticket"]
   [:p {:class "big"} "#" ticket]
   [:p "Position in queue: " [:strong queue-pos]]
   [:p {:class "muted"}
    "Issued by the server. Not in the DOM, not in any store, not derivable by the "
    "client. Kill the JVM and reload: if this number is the same, the snapshot "
    "rebuilt it."]])

(defn- live
  "Opens the stream. Rebuilds from the snapshot if one arrived, otherwise issues new.

  The branch here is the experiment. `snap/verify` is the only path by which a
  reconnecting client can be recognised, because nothing else identifies it."
  [{:keys [params] :as request}]
  (let [incoming (get params "snap")
        verified (when (seq incoming)
                   (snap/verify {:secret secret} incoming))
        restored (when (:ok verified) (get-in verified [:snapshot :state]))
        state (or restored
                  {:ticket (issue-ticket!) :queue-pos (rand-int 20)})]
    (println (cond
               restored (str "RESTORED from snapshot: " (pr-str state))
               (seq incoming) (str "REJECTED snapshot (" (:reason verified) "), issued new: "
                                   (pr-str state))
               :else (str "NEW session: " (pr-str state))))
    (d*ring/->sse-response
     request
     {d*ring/on-open
      (fn [sse-gen]
        (d*/patch-elements! sse-gen (h/html (view state))
                            {d*/selector "#live-root" d*/patch-mode d*/pm-outer})
        ;; Hand the client a fresh signed snapshot to hold for next time.
        (let [signed (snap/create {:secret secret}
                                  {:component :ticket
                                   :params {}
                                   :recoverable state})]
          (d*/execute-script!
           sse-gen (str "localStorage.setItem('snap'," (pr-str signed) ");")))
        nil)
      d*ring/on-close (fn [& _] nil)})))

(defn handler [{:keys [uri] :as request}]
  (case uri
    "/" {:status 200 :headers {"content-type" "text/html; charset=utf-8"} :body (page)}
    "/datastar.js" {:status 200 :headers {"content-type" "text/javascript"}
                    :body (slurp (io/resource "datastar.js"))}
    "/live" (live request)
    {:status 404 :body "nope"}))

(defn async-handler [request respond _raise] (respond (handler request)))

(defn -main [& _]
  ;; See watchspike: async mode is load-bearing for SSE, not tuning.
  (jetty/run-jetty (wrap-params #'async-handler)
                   {:port 3003 :join? false :async? true :jetty {:async-timeout 0}})
  (println "snapshot spike on http://localhost:3003/")
  @(promise))
