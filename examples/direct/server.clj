(ns direct.server
  "The same encrypted chat app, on the same stack, with Datastar used directly.

  A control experiment for the framework version in `examples/chat`. **Same zodiac,
  same zodiac-sql, same routing, same middleware, same CSS, same crypto.** The only
  variable removed is the remuda engine — no view state, no diffing, no boundaries,
  no tiers, no live children.

  Run:
    clojure -M:direct -m direct.server
  then open http://localhost:3001

  ## The shape of every update

  A handler mutates, then names what changed and where it goes:

      (let [id (db/add-message! db channel-id author ct iv)]
        (push-to-channel! channel-id
          (fn [_] {:selector \"#messages\" :mode :append
                   :html (chassis/html (views/message …))})))

  The selector is built by the same function the view used, so the target and the
  element are the same string by construction. Nothing derives an id, so the whole
  class of \"the patch addressed an element that does not exist\" cannot occur — which
  is what four of the framework version's bugs were.

  ## What this gives up, stated plainly

  - **A forgotten push is a silently stale screen.** The engine re-derives from a
    hint and therefore cannot forget. Here the discipline is the author's, and
    nothing checks it.
  - **No recovery of client-held state.** A reconnect re-renders from the database.
    A half-typed draft survives only because the input owns its own value — which,
    notably, is also why the composer never clobbers itself here.
  - **Fan-out renders per viewer.** Fine at this scale; would need thought at
    thousands of connections."
  (:require [charred.api :as charred]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [dev.onionpancakes.chassis.core :as chassis]
            [starfederation.datastar.clojure.api :as d*]
            [starfederation.datastar.clojure.adapter.ring :as d*ring]
            [zodiac.core :as z]
            [zodiac.ext.sql :as z.sql]
            [direct.db :as db]
            [direct.live :as live]
            [direct.token :as token]
            [direct.views :as views]))

(def secret (or (System/getenv "CHAT_LIVE_SECRET") "direct-example-secret"))

(def ttl-ms
  "How long a heartbeat counts as connected.

  Same reasoning as the framework version, and the same finding behind it: there is
  no reliable disconnect signal from this transport, so liveness is a timing
  property rather than an event."
  15000)

(defonce ^{:doc "channel-id -> {username -> last-keystroke-ms}"}
  typing
  (atom {}))

(defn- typists
  [channel-id self]
  (let [cutoff (- (System/currentTimeMillis) 3000)]
    (->> (get @typing channel-id)
         (keep (fn [[u t]] (when (and (>= t cutoff) (not= u self)) u)))
         sort vec)))

(defn- db-of [request] (get-in request [::z/context ::z.sql/db]))

;;; ==========================================================================
;;; Pushes — one function per thing that can change
;;; ==========================================================================
;;; This is the whole difference from the framework version. There, a change
;;; published a hint and every subscriber re-derived its view so a diff could
;;; discover what moved. Here the handler already knows, and says so.

(defn- push-message!
  [channel-id msg]
  (live/push-to-channel!
   channel-id
   (fn [_] {:selector (str "#" (views/messages-id))
            :mode :append
            :html (chassis/html (views/message msg))})))

(defn- push-typing!
  [channel-id]
  (live/push-to-channel!
   channel-id
   ;; Rendered per viewer, so everyone sees "who is typing except me". The framework
   ;; version got this from `:mount` receiving each context's own params.
   (fn [{:keys [username]}]
     {:selector (str "#" (views/typing-id))
      :html (chassis/html (views/typing-line (typists channel-id username)))})))

(defn- push-member!
  "One roster row.

  This is the update that took the framework version several sessions to get right,
  because the row was a live child whose boundary carried no id, so the patch
  targeted a derived id that existed nowhere in the DOM. Here the selector is
  `(views/member-id username)` in both the view and the push."
  [channel-id username online?]
  (live/push-to-channel!
   channel-id
   (fn [_] {:selector (str "#" (views/member-id username))
            :html (chassis/html (views/member username online?))})))

(defn- push-roster!
  "The whole roster and its count.

  Used when membership *changes*: a new member has no row to patch, so the list is
  the only thing to target. Rows change status individually via `push-member!`."
  [db channel-id]
  (let [members (db/members db channel-id)
        online (live/online-users channel-id ttl-ms)]
    (live/push-to-channel!
     channel-id
     (fn [_] {:selector (str "#" (views/roster-id))
              :html (chassis/html (views/roster members online))}))
    (live/push-to-channel!
     channel-id
     (fn [_] {:selector (str "#" (views/roster-count-id))
              :html (chassis/html (views/roster-count (count members)))}))))

;;; ==========================================================================
;;; SSE
;;; ==========================================================================

(defn sse-handler
  [request respond _raise]
  (let [db (db-of request)
        {:keys [channel-id username]}
        (token/read-session secret (get (:params request) "t"))]
    (respond
     (if-not channel-id
       {:status 403 :body "invalid session token"}
       (d*ring/->sse-response
        request
        {d*ring/on-open
         (fn [gen]
           (let [id (live/open! gen channel-id username)
                 msgs (db/page db channel-id nil)]
             (db/add-member! db channel-id username)
             ;; The whole panel, once. Everything after this is a fragment.
             (d*/patch-elements!
              gen
              (chassis/html
               (views/chat {:channel-name (:name (db/channel db channel-id))
                            :messages msgs
                            :members (db/members db channel-id)
                            :online (live/online-users channel-id ttl-ms)
                            :typists (typists channel-id username)
                            :more-count (db/count-before db channel-id
                                                         (db/oldest-id msgs))}))
              {d*/selector "#live-root" d*/patch-mode d*/pm-outer})
             (d*/execute-script! gen (str "window.__connId=" (pr-str id) ";"))
             ;; Announce AFTER this connection has its own render — same ordering
             ;; requirement the framework version had, for the same reason.
             (push-roster! db channel-id)
             nil))

         d*ring/on-close
         (fn [& _] nil)})))))

;;; ==========================================================================
;;; Actions
;;; ==========================================================================

(defn- signals
  "Datastar's signals. Reads `:body-params` first because zodiac runs muuntaja,
  which has already consumed the body — reading the stream throws EOFException."
  [request]
  (or (:body-params request)
      (try (some-> (d*/get-signals request) (charred/read-json :key-fn keyword))
           (catch Exception _ nil))))

(defn- with-session
  "Runs `f` with the token's identity, or 403s. Every action needs this, and the
  identity must come from the signed token rather than the request body — otherwise
  anyone can post as anyone."
  [request f]
  (let [{:keys [t] :as sig} (signals request)
        {:keys [channel-id username]} (token/read-session secret t)]
    (if-not channel-id
      {:status 403 :body "invalid session token"}
      (f (db-of request) channel-id username sig))))

(defn send-message [request]
  (with-session
    request
    (fn [db channel-id username {:keys [ct iv]}]
      (let [id (db/add-message! db channel-id username ct iv)]
        (swap! typing update channel-id dissoc username)
        (push-message! channel-id {:id id :author username :body ct :iv iv})
        (push-typing! channel-id)
        (push-roster! db channel-id)
        {:status 204}))))

(defn typing-ping [request]
  (with-session
    request
    (fn [_db channel-id username _sig]
      (swap! typing assoc-in [channel-id username] (System/currentTimeMillis))
      (push-typing! channel-id)
      {:status 204})))

(defn heartbeat [request]
  (with-session
    request
    (fn [_db channel-id username {:keys [connId]}]
      (when (live/touch! connId ttl-ms)
        ;; Was lapsed, now back: their dot changed.
        (push-member! channel-id username true))
      {:status 204})))

(defn older
  "Scroll-up pagination. Prepends a page, so the reader keeps their position."
  [request]
  (with-session
    request
    (fn [db channel-id _username {:keys [connId before]}]
      (let [msgs (db/page db channel-id (some-> before str parse-long))]
        (when connId
          (live/push! connId
                      {:selector (str "#" (views/messages-id))
                       :mode :prepend
                       :html (chassis/html (for [m msgs] (views/message m)))}))
        {:status 204}))))

(defn create-channel [request]
  (let [{:keys [name]} (signals request)
        id (str (random-uuid))]
    (db/create-channel! (db-of request) id (or name "untitled"))
    {:status 200 :headers {"content-type" "application/json"}
     :body (charred/write-json-str {:invite (token/invite secret id)})}))

(defn join
  "Claims a username in a channel and mints a session token.

  A name is claimed exactly once per channel, enforced by the primary key rather
  than a read-then-write check."
  [request]
  (let [{:keys [invite username]} (signals request)
        channel-id (token/read-invite secret invite)
        name' (let [n (str/trim (or username ""))]
                (if (str/blank? n) "anon" (subs n 0 (min 24 (count n)))))
        json (fn [status m] {:status status
                             :headers {"content-type" "application/json"}
                             :body (charred/write-json-str m)})]
    (cond
      (not channel-id) (json 403 {:error "invalid-invite"})
      (not (db/claim-member! (db-of request) channel-id name'))
      (json 409 {:error "username-taken" :username name'})
      :else (json 200 {:token (token/session secret channel-id name')}))))

;;; ==========================================================================
;;; Pages
;;; ==========================================================================

(defn- layout [& body]
  [chassis/doctype-html5
   [:html
    [:head
     [:meta {:charset "utf-8"}]
     [:meta {:name "viewport" :content "width=device-width, initial-scale=1"}]
     [:title "direct chat"]
     [:script {:type "module" :src "/datastar.js"}]
     [:script {:src "/direct.js"}]
     [:link {:rel "stylesheet" :href "/chat.css"}]]
    (into [:body] body)]])

(defn home [_request]
  (layout
   [:h1 "Create a channel"]
   [:p "The key is generated in your browser and kept in the URL "
    [:em "fragment"] ", which is never sent to the server."]
   [:p [:input {:id "channel-name" :placeholder "Channel name"}]]
   [:p [:input {:id "username" :placeholder "Your name"}]]
   [:p [:button {:class "btn btn--primary" :onclick "window.dCreate()"}
        "Create channel"]]
   [:script (chassis/raw "window.dBindKeys()")]))

(defn invite-page [{:keys [path-params]}]
  (let [t (:token path-params)]
    (if-not (token/read-invite secret t)
      (layout [:p {:class "banner banner--error"} "This invite link is not valid."])
      (layout
       [:h1 "Join the channel"]
       [:p "Pick a name. It identifies you in this channel only."]
       [:p [:input {:id "username" :placeholder "Your name"}]]
       [:p [:button {:class "btn btn--primary"
                     :onclick (str "window.dJoin(" (pr-str t) ")")} "Join"]]
       [:script (chassis/raw (format "window.dBindKeys(%s)" (pr-str t)))]))))

(defn channel-page [{:keys [path-params]}]
  (let [t (:token path-params)
        {:keys [channel-id username]} (token/read-session secret t)]
    (if-not channel-id
      (layout [:p {:class "banner banner--error"} "This link is not valid."])
      (layout
       [:div {:class "invite"}
        [:span {:class "invite__hint"}
         "Signed in as " [:strong username]
         ". Anyone with the invite link can join and read this channel."]
        [:button {:class "btn btn--ghost"
                  :data-invite (str "/i/" (token/invite secret channel-id))
                  :onclick "window.dCopyInvite(this)"}
         "Copy invite link"]
        [:input {:id "invite-fallback" :class "invite__fallback"
                 :hidden true :readonly true}]]
       [:div {:id "live-root"} "connecting…"]
       ;; requestCancellation cleanup: without it, navigating between channels leaves
       ;; the old SSE stream open and one tab accumulates a stream per channel
       ;; visited. That cost the framework version a long debugging session, and it
       ;; is a Datastar fact rather than an engine one — so it applies here too.
       [:div {:data-init (str "@get('/live?t=" t
                              "', {requestCancellation: 'cleanup'})")}]
       [:script (chassis/raw (format "window.dBoot(%s)" (pr-str t)))]))))

;;; ==========================================================================
;;; Wiring
;;; ==========================================================================

(defn- static [resource content-type]
  (fn [_request]
    {:status 200 :headers {"content-type" content-type}
     :body (slurp (io/resource resource))}))

(defn routes []
  [["/" {:get {:handler home}}]
   ["/live" {:get {:handler sse-handler}
             :zodiac/async? true
             :zodiac/skip-csrf true}]
   ["/c/:token" {:get {:handler channel-page}}]
   ["/i/:token" {:get {:handler invite-page}}]
   ["/create" {:post {:handler create-channel} :zodiac/skip-csrf true}]
   ["/join" {:post {:handler join} :zodiac/skip-csrf true}]
   ["/send" {:post {:handler send-message} :zodiac/skip-csrf true}]
   ["/typing" {:post {:handler typing-ping} :zodiac/skip-csrf true}]
   ["/heartbeat" {:post {:handler heartbeat} :zodiac/skip-csrf true}]
   ["/older" {:post {:handler older} :zodiac/skip-csrf true}]
   ["/direct.js" {:get {:handler (static "direct.js" "text/javascript")}}]
   ["/chat.css" {:get {:handler (static "chat.css" "text/css")}}]
   ["/datastar.js" {:get {:handler (static "datastar.js" "text/javascript")}}]])

(defn- start-expiry!
  "Turns a lapsed heartbeat into a grey dot, and keeps typing lines fresh.

  Expiry is only observable if something pushes — the same lesson the framework
  version learned, where it needed a published hint."
  [db]
  (doto (Thread.
         ^Runnable
         (fn []
           (while true
             (try
               (Thread/sleep 3000)
               (doseq [id (live/lapsed ttl-ms)]
                 (when-let [c (get live/conns id)]
                   (live/close! id)
                   (push-member! (:channel-id c) (:username c) false)
                   (push-roster! db (:channel-id c))))
               (doseq [[ch users] @typing]
                 (when (seq users) (push-typing! ch)))
               (catch InterruptedException _ (throw (InterruptedException.)))
               (catch Exception e (println "expiry error" (.getMessage e))))))
         "direct-expiry")
    (.setDaemon true)
    (.start)))

(defn -main [& _]
  (let [db-path "/tmp/direct-chat.db"
        sql-ext (z.sql/init {:spec {:jdbcUrl (str "jdbc:sqlite:" db-path)}})
        sys (z/start {:routes #'routes
                      :extensions [sql-ext]
                      :cookie-secret "0123456789abcdef"
                      :async? true
                      :jetty {:port 3001 :async-timeout 0}})
        db (get-in sys [::z.sql/db])]
    (db/migrate! db)
    (start-expiry! db)
    (println "direct chat on http://localhost:3001")
    sys))
