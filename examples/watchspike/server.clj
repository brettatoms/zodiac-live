(ns watchspike.server
  "A browser test for `remuda.watch` and `remuda.watch-engine`. Roster only.

  ## Why this exists rather than a change to the chat app

  Every idea in this project that read correctly still broke once a DOM was
  involved, so `watch` does not get adopted on the strength of a headless test. But
  wiring it into `examples/chat` would mean changing `:mount`, `:subscribe` and the
  zodiac extension at the same time, and a failure would not say which part was
  wrong.

  So: the smallest thing that is still a *real* browser test. Real SSE, real
  Datastar patching, real DOM, and the same heartbeat/TTL presence shape as the chat
  app. No zodiac, no extension, no diffing, no tiers.

  ## What a pass looks like

  1. Two tabs open as different users; each sees both members in the roster.
  2. Closing one tab turns that member's dot grey **in the other tab**, pushed —
     without a refresh. That is the exact bug that took nine fixes in the framework
     version.
  3. A third user joining appears in both existing tabs immediately.
  4. Only the changed element is patched: a presence change must patch one row, not
     the whole list.

  Point 4 is the one a headless test cannot check, because it is about what the
  browser receives, not what the server computed.

      clojure -M:watchspike -m watchspike.server    # port 3002"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [dev.onionpancakes.chassis.core :as h]
            [remuda.watch :as w]
            [remuda.watch-engine :as we]
            [ring.adapter.jetty :as jetty]
            [ring.middleware.params :refer [wrap-params]]
            [starfederation.datastar.clojure.adapter.ring :as d*ring]
            [starfederation.datastar.clojure.api :as d*])
  (:import [java.util.concurrent ConcurrentHashMap Executors TimeUnit]))

;;; ==========================================================================
;;; Sources — plain server state, read at render time
;;; ==========================================================================
;;; Same shape as chat.presence: keyed by the [channel-id username] PAIR, because a
;;; username identifies a person only within a channel. And liveness is a TTL over a
;;; heartbeat rather than a disconnect event, because there is no reliable disconnect
;;; signal — verified against bare Jetty in an earlier session.

(def ttl-ms 6000)

(defonce ^{:doc "[channel-id username] -> last-heartbeat-ms"} presence
  (ConcurrentHashMap.))

(defonce ^{:doc "channel-id -> #{username}. Roster membership, which outlives presence."}
  members
  (atom {}))

(defn join! [channel-id username]
  (swap! members update channel-id (fnil conj #{}) username))

(defn roster [channel-id] (vec (sort (get @members channel-id))))

(defn online? [channel-id username]
  (when-let [t (.get presence [channel-id username])]
    (> ^long t (- (System/currentTimeMillis) ^long ttl-ms))))

(defn touch! [channel-id username]
  (let [was (.put presence [channel-id username] (System/currentTimeMillis))]
    (or (nil? was) (< ^long was (- (System/currentTimeMillis) ^long ttl-ms)))))

;;; ==========================================================================
;;; Hints — no payload, so they are safe to lose, reorder or duplicate
;;; ==========================================================================
;;; A topic -> the connections subscribed to it. `watch` decides what those topics
;;; are; nothing here writes a topic by hand except as a publish target, and those
;;; are the same values `watch` recorded.

(defonce ^{:doc "topic -> #{conn-id}"} topic-subs (atom {}))

(defn subscribe! [conn-id topics]
  (swap! topic-subs (fn [m] (reduce (fn [m t] (update m t (fnil conj #{}) conn-id)) m topics))))

(defn unsubscribe! [conn-id topics]
  (swap! topic-subs
         (fn [m] (reduce (fn [m t]
                           (let [ids (disj (get m t) conn-id)]
                             (if (seq ids) (assoc m t ids) (dissoc m t))))
                         m topics))))

(defn unsubscribe-all! [conn-id]
  (swap! topic-subs #(into {} (keep (fn [[t ids]]
                                      (let [ids (disj ids conn-id)]
                                        (when (seq ids) [t ids])))
                                    %))))

(declare engine)

(defn- read-fns
  "Readers for every topic this app publishes, keyed the same way the component reads
  them. This is what lets `refresh!` answer \"did anything change?\" without rendering.

  The duplication with the component is the cost of the optimisation, and it is worth
  naming: the component reads through `watch`, and this repeats those reads for the
  engine. Getting one wrong makes a fragment re-render when it need not — wasteful,
  never wrong — so it fails safe."
  [channel-id]
  (into {[:members channel-id] #(roster channel-id)}
        (for [u (roster channel-id)]
          [[:presence channel-id u] #(online? channel-id u)])))

(defn publish!
  "Re-renders every connection subscribed to `topic`, pushes what changed, and
  updates that connection's subscriptions.

  The re-subscription is not housekeeping. A dependency set is data: a roster that
  reads one presence topic per member depends on a different set of topics the moment
  the membership changes. Subscribing only at mount produced a bug that looked fine —
  a member who joined *after* a viewer connected appeared in that viewer's roster and
  then never went grey, because the viewer had never subscribed to a topic that did
  not exist when it mounted. Found in a browser, not in a test."
  [topic]
  ;; Every topic here is [kind channel-id ...], so the readers for it are derivable
  ;; from the topic itself. No need to look up the connection.
  (let [readers (read-fns (second topic))]
    (doseq [id (get @topic-subs topic)]
      (try
        (let [{:keys [patches added removed pruned]}
              (we/refresh! engine id topic {:read-fns readers})]
          (when (seq added) (subscribe! id added))
          (when (seq removed) (unsubscribe! id removed))
          (println (if pruned "PRUNED" "PUSH") id (pr-str topic)
                   "->" (pr-str (mapv :selector patches))
                   (when (seq added) (str "+sub " (pr-str added)))
                   (when (seq removed) (str "-sub " (pr-str removed)))))
        (catch Throwable e
          (println "refresh FAILED" id (pr-str topic) (ex-message e)))))))

;;; ==========================================================================
;;; The component — one function, dependencies at the point of use
;;; ==========================================================================

(defn- member-id [username]
  (str "member-" (str/replace username #"[^A-Za-z0-9_-]" "_")))

(defn member-row
  "One roster row.

  The dependency on this member's presence is declared by *reading* it. There is no
  `:subscribe` to keep in step and no boundary path to resolve into an id — the
  fragment id and the element id are the same expression."
  [channel-id username]
  (w/fragment (member-id username)
              (fn []
                (let [on? (w/watch [:presence channel-id username]
                                   #(online? channel-id username))]
                  [:li {:id (member-id username)
                        :class (str "member " (if on? "online" "offline"))}
                   [:span {:class "dot"}]
                   [:span {:class "name"} username]
                   [:span {:class "state"} (if on? "online" "offline")]]))))

(defn roster-view
  "The whole roster.

  `mapv`, not `for`. A lazy seq escapes the recording binding, the rows' topics are
  never recorded, and the first render still looks perfect — which is why
  `render-recording` throws on one rather than leaving a silently stale screen."
  [{:keys [channel-id]}]
  (w/fragment "roster"
              (fn []
                (let [ms (w/watch [:members channel-id] #(roster channel-id))]
                  [:div {:id "roster"}
                   [:h3 "Members " [:span {:class "count"} (count ms)]]
                   [:ul {:class "roster__list"}
                    (mapv #(member-row channel-id %) ms)]]))))

(def engine
  (we/engine {:components {:roster roster-view}
              :render-fn h/html}))

;;; ==========================================================================
;;; HTTP
;;; ==========================================================================

(defonce ^{:doc "conn-id -> {:channel-id :username}, for cleanup and heartbeats."}
  conn-info
  (atom {}))

(def ^:private css "
:root { --bg:#12141a; --panel:#1b1e27; --line:#2b3040; --text:#e6e8ee;
        --muted:#8b93a7; --on:#3ddc84; --off:#575e72; --space:12px; --radius:10px; }
* { box-sizing:border-box; }
body { margin:0; font:15px/1.5 system-ui,sans-serif; background:var(--bg);
       color:var(--text); padding:calc(var(--space) * 2); }
#roster { background:var(--panel); border:1px solid var(--line);
          border-radius:var(--radius); padding:var(--space); max-width:320px; }
h3 { margin:0 0 var(--space); font-size:14px; text-transform:uppercase;
     letter-spacing:.06em; color:var(--muted); }
.count { color:var(--text); background:var(--line); border-radius:999px;
         padding:1px 8px; margin-left:6px; }
.roster__list { list-style:none; margin:0; padding:0; display:grid; gap:6px; }
.member { display:flex; align-items:center; gap:8px; padding:6px 8px;
          border-radius:8px; background:#00000030; }
.dot { width:9px; height:9px; border-radius:50%; background:var(--off); }
.member.online .dot { background:var(--on); box-shadow:0 0 0 3px #3ddc8422; }
.name { font-weight:500; }
.state { margin-left:auto; font-size:12px; color:var(--muted); }
")

(defn- page [channel-id username]
  (h/html
   [h/doctype-html5
    [:html
     [:head
      [:title (str "watch spike — " username)]
      [:meta {:charset "utf-8"}]
      [:script {:type "module" :src "/datastar.js"}]
      [:style css]]
     [:body
      [:p {:style "color:#8b93a7"} "channel " channel-id " · you are " [:b username]]
      [:div {:id "live-root"} "connecting…"]
      [:div {:data-init (str "@get('/live?c=" channel-id "&u=" username
                             "', {requestCancellation: 'cleanup'})")}]]]]))

(defn- live
  "Opens the SSE stream, mounts the roster, and subscribes to what it read.

  The order matters and is the fix for a race found in the framework version: the
  connection must be subscribed and mounted *before* anything is published, or its
  own arrival hint reaches everyone except itself."
  [{:keys [params] :as request}]
  (let [channel-id (get params "c" "c1")
        username (get params "u" "anon")
        id* (promise)]
    (join! channel-id username)
    (touch! channel-id username)
    (d*ring/->sse-response
     request
     {d*ring/on-open
      (fn [sse-gen]
        (let [id (we/connect! engine :roster
                              {:params {:channel-id channel-id}
                               :send! (fn [patches]
                                        (doseq [{:keys [selector mode html]} patches]
                                          (d*/patch-elements!
                                           sse-gen html
                                           {d*/selector selector
                                            d*/patch-mode (if (= :outer mode)
                                                            d*/pm-outer d*/pm-inner)})))})
              {:keys [html topics]} (we/mount! engine id)]
          (deliver id* id)
          (swap! conn-info assoc id {:channel-id channel-id :username username})
          ;; Subscribe to exactly what the render read. Nothing declared this list.
          (subscribe! id topics)
          (println "MOUNT" id username "subscribed to" (pr-str topics))
          (d*/patch-elements! sse-gen html
                              {d*/selector "#live-root" d*/patch-mode d*/pm-outer})
          (d*/execute-script! sse-gen
                              (str "window.__id=" (pr-str id) ";"
                                   "window.__hb=setInterval(()=>fetch('/hb?id='+window.__id,"
                                   "{method:'POST'}),2000);"))
          ;; Now that this connection can hear it, tell the channel someone joined.
          (publish! [:members channel-id])
          (publish! [:presence channel-id username])))
      d*ring/on-close
      (fn [& _]
        (when-let [id (deref id* 1000 nil)]
          (let [{:keys [username]} (get @conn-info id)]
            ;; No hint published here. Presence is a TTL over a heartbeat, so the
            ;; sweeper is what turns this member grey for everyone else — and it has
            ;; to be, because `on-close` does not fire when a client vanishes.
            (println "CLOSE" id username)
            (unsubscribe-all! id)
            (we/disconnect! engine id)
            (swap! conn-info dissoc id))))})))

(defn- heartbeat [{:keys [params]}]
  (let [id (get params "id")
        {:keys [channel-id username]} (get @conn-info id)]
    (when (and channel-id (touch! channel-id username))
      (publish! [:presence channel-id username]))
    {:status 204}))

;;; A sweeper, because a lapsed heartbeat is not an event. Nothing tells the server
;;; that a TTL expired, so someone has to look.
(defonce ^{:doc "Turns lapsed presence into hints."} sweeper
  (doto (Executors/newSingleThreadScheduledExecutor)
    (.scheduleAtFixedRate
     (fn []
       (try
         (doseq [[[channel-id username] t] (into {} presence)
                 :when (< ^long t (- (System/currentTimeMillis) ^long ttl-ms))]
           ;; Publish once per lapse, not every tick: remove the entry so the next
           ;; sweep does not see it again. The roster keeps the member.
           (.remove presence [channel-id username])
           (println "LAPSED" channel-id username)
           (publish! [:presence channel-id username]))
         (catch Throwable e (println "sweep failed" (ex-message e)))))
     1 1 TimeUnit/SECONDS)))

(defn handler [{:keys [uri params] :as request}]
  (case uri
    "/" {:status 200 :headers {"content-type" "text/html; charset=utf-8"}
         :body (page (get params "c" "c1") (get params "u" "anon"))}
    "/datastar.js" {:status 200
                    :headers {"content-type" "text/javascript"}
                    :body (slurp (io/resource "datastar.js"))}
    "/live" (live request)
    "/hb" (heartbeat request)
    {:status 404 :body "nope"}))

(defn async-handler
  "Adapts the synchronous handler to jetty's 3-arity async contract.

  Everything here is synchronous; async mode is on only so an SSE response outlives
  the request that opened it. See `-main`."
  [request respond _raise]
  (respond (handler request)))

(defn -main [& _]
  ;; `:async? true` with `:async-timeout 0` is load-bearing, not tuning. In Jetty's
  ;; default synchronous mode the servlet request completes as soon as the handler
  ;; returns, so an SSE response is torn down the moment `on-open` finishes — and a
  ;; later write from another thread reports success while the client receives
  ;; nothing and `on-close` has already fired. Verified in isolation with no remuda
  ;; involved: a 30-line jetty+datastar server loses every cross-connection patch
  ;; the same way. `examples/chat` sets both for the same reason.
  (jetty/run-jetty (wrap-params #'async-handler)
                   {:port 3002 :join? false
                    :async? true
                    :jetty {:async-timeout 0}})
  (println "watch spike on http://localhost:3002/?c=c1&u=alice")
  @(promise))
