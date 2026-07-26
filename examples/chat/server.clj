(ns chat.server
  "The chat example, as a zodiac app.

  Two extensions compose here without knowing about each other: `zodiac-sql`
  provides the db in the request context, and `zodiac.ext.live` provides the live
  engine. Both are just functions transforming the same integrant config, which is
  the property that makes extensions compose.

  Run:
    clojure -M:example -m chat.server
  then open http://localhost:3000"
  (:require [charred.api :as charred]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [dev.onionpancakes.chassis.core :as chassis]
            [starfederation.datastar.clojure.api :as d*]
            [starfederation.datastar.clojure.adapter.ring :as d*ring]
            [zodiac.core :as z]
            [zodiac.ext.live :as z.live]
            [zodiac.ext.sql :as z.sql]
            [chat.component :as component]
            [chat.db :as db]
            [chat.member :as member]
            [chat.presence :as presence]
            [chat.token :as token]
            [chat.typing :as typing]
            [remuda.engine :as engine]
            [remuda.source :as source]))

;;; These are committed on purpose: this is a demo, and `clojure -M:example -m
;;; chat.server` should just work. Both are overridable by environment variable,
;;; and neither should be used anywhere real.

(def secret
  "HMAC key for recovery snapshots."
  (or (System/getenv "CHAT_LIVE_SECRET") "chat-example-secret-not-for-production"))

(def cookie-secret
  "Session cookie key. Zodiac requires exactly 16 bytes."
  (or (System/getenv "CHAT_COOKIE_SECRET") "0123456789abcdef"))

(defonce ^{:doc "live id -> {:channel-id :username}, for cleanup on disconnect."}
  connection-info
  (atom {}))

;;; ==========================================================================
;;; Transport: engine instructions -> datastar
;;; ==========================================================================

(def ^:private mode->datastar
  {:outer d*/pm-outer :inner d*/pm-inner :remove d*/pm-remove
   :append d*/pm-append :prepend d*/pm-prepend
   :before d*/pm-before :after d*/pm-after :replace d*/pm-replace})

(defn- send-instructions!
  [sse-gen instructions]
  (doseq [{:keys [mode selector html move]} instructions]
    (cond
      (= :remove mode)
      (d*/patch-elements! sse-gen "" {d*/selector selector
                                      d*/patch-mode d*/pm-remove})
      ;; A move is remove-then-insert: datastar has no move primitive, and a
      ;; positional insert alone duplicates the element rather than relocating it.
      move
      (do (d*/patch-elements! sse-gen "" {d*/selector move
                                          d*/patch-mode d*/pm-remove})
          (d*/patch-elements! sse-gen html {d*/selector selector
                                            d*/patch-mode (mode->datastar mode)}))
      html
      (d*/patch-elements! sse-gen html {d*/selector selector
                                        d*/patch-mode (mode->datastar mode)}))))

(defn- signals
  "Datastar signals for this request.

  **Reads `:body-params` first.** Zodiac runs muuntaja, which has already consumed
  and parsed the JSON body by the time a handler sees it, so `d*/get-signals`
  returns a drained stream and reading it throws `EOFException`. That exception was
  being swallowed, so signals came back nil and every action failed with 409 \"no
  live context\" — a confusing symptom for a body that had in fact arrived intact.

  Falls back to `get-signals` for a request muuntaja did not touch, and logs rather
  than silently returning nil, because a swallowed failure here is invisible at the
  call site."
  [request]
  (or (:body-params request)
      (try (some-> (d*/get-signals request) (charred/read-json :key-fn keyword))
           (catch Exception e
             (log/debug e "could not read datastar signals")
             nil))))

;;; ==========================================================================
;;; SSE
;;; ==========================================================================

(defn- sse-fn
  "Opens a live connection. Supplied to the extension so the transport stays the
  application's choice — the extension names no server.

  ## on-open must not block

  An earlier version parked here on a `CountDownLatch` that nothing ever counted
  down. The connection stayed open, but the adapter calls `on-close` only after
  `on-open` returns — so cleanup never ran. Presence, typing state and the live
  context leaked on every disconnect, and a member who closed their tab stayed in
  every other user\u2019s roster forever. `presence/connected` still listed them.

  So `on-open` returns immediately and `on-close` does the cleanup."
  [{:keys [request engine source]}]
  ;; Identity comes from the signed session token, never from a query param. A
  ;; connection with a bad token gets no live context at all rather than a default
  ;; one, so a tampered URL fails closed.
  (let [{:keys [channel-id username]}
        (token/read-session secret (get (:params request) "t"))]
    (if-not channel-id
      {:status 403 :body "invalid or missing session token"}
      ;; on-open and on-close are separate closures, so the live id has to be
      ;; shared. A promise rather than an atom: written once, and on-close must not
      ;; proceed before on-open has produced it.
      (let [id* (promise)
            live (z.live/live-ctx request)]
        (d*ring/->sse-response
         request
         {d*ring/on-open
          (fn [sse-gen]
            (let [id (engine/connect! engine :chat
                                      {:send! #(send-instructions! sse-gen %)
                                       :close! #(d*/close-sse! sse-gen)
                                       :params {:channel-id channel-id
                                                :username username}})]
              (deliver id* id)
              (swap! connection-info assoc id {:channel-id channel-id
                                               :username username})
              ;; Idempotent: the name was claimed at join time, so this covers a
              ;; reconnect or a second tab for someone who already holds it.
              (db/add-member! (get-in request [::z/context ::z.sql/db])
                              channel-id username)
              (let [newly-online? (presence/touch! channel-id username)]
                ;; Subscribe and mount BEFORE announcing.
                ;;
                ;; Publishing first marks every subscriber dirty — and this context
                ;; then subscribes and mounts with the *new* roster, so the flush
                ;; diffs the new view against itself and emits nothing. Whichever
                ;; context happened to flush after its own hint lost its update,
                ;; which is why one tab would show a joiner and another would not.
                ;;
                ;; Mounting first also means this connection's own view is correct
                ;; without needing a hint at all.
                (z.live/subscribe! live id
                                   [[:channel channel-id] [:typing channel-id]])
                (d*/patch-elements! sse-gen
                                    (engine/mount! engine id {:source source})
                                    {d*/selector "#live-root"
                                     d*/patch-mode d*/pm-outer})
                ;; Now tell everyone else. A hint only when the status actually
                ;; changed: a second tab changes the connection count but not the
                ;; status, and hinting on it would push an identical render.
                (when newly-online?
                  (z.live/publish! live [:presence channel-id username])
                  ;; The roster gained a member, so other viewers re-derive
                  ;; :members. That is what makes a joiner visible immediately,
                  ;; without waiting for them to post.
                  (z.live/publish! live [:channel channel-id])))
              (d*/patch-signals! sse-gen (charred/write-json-str {:liveId id}))
              ;; Also expose it to plain JS: chat.js needs it for fetch() calls,
              ;; and datastar signals live in datastar's own store, not on window.
              (d*/execute-script! sse-gen (str "window.__liveId=" (pr-str id) ";"))
              nil))

          d*ring/on-close
          (fn [& _]
            (when-let [id (deref id* 5000 nil)]
              (typing/clear! channel-id username)
              ;; Two hints, and both are needed. [:presence ...] wakes this
              ;; member's own row; [:channel ...] wakes the PARENT so it re-derives
              ;; :members. Without the second, a departing member is never removed
              ;; from anyone else's roster, because nothing announced it shrank.
              ;; Fires on an orderly close. Not relied on — see `presence/touch!`
              ;; for why the TTL is what actually guarantees the dot turns grey.
              (presence/forget! channel-id username)
              (z.live/publish! live [:presence channel-id username])
              (engine/disconnect! engine id)
              (swap! connection-info dissoc id)))})))))

;;; ==========================================================================
;;; Pages
;;; ==========================================================================

(defn- layout
  [& body]
  [chassis/doctype-html5
   [:html
    [:head
     [:meta {:charset "utf-8"}]
     ;; Required for the responsive layout to apply on a phone at all.
     [:meta {:name "viewport" :content "width=device-width, initial-scale=1"}]
     [:title "zodiac-live chat"]
     [:script {:type "module" :src "/datastar.js"}]
     [:script {:src "/chat.js"}]
     [:link {:rel "stylesheet" :href "/chat.css"}]]
    (into [:body] body)]])

(defn home
  [_request]
  (layout
   [:h1 "Create a channel"]
   [:p "The encryption key is generated in your browser and put in the URL "
    [:em "fragment"] ", which is never sent to the server. Share the whole link."]
   [:p [:input {:id "channel-name" :placeholder "Channel name"}]]
   [:p [:input {:id "username" :placeholder "Your name"}]]
   [:p [:button {:class "btn btn--primary"
                 :onclick "window.chatCreate()"} "Create channel"]]
   [:script (chassis/raw "window.chatBindKeys()")]))

(defn channel-page
  "The live channel, addressed by a signed session token.

  No query params: the username is inside the token, so there is nothing in the URL
  a reader can edit to become someone else."
  [{:keys [path-params]}]
  (let [t (:token path-params)
        {:keys [channel-id username]} (token/read-session secret t)]
    (if-not channel-id
      {:status 403 :headers {"content-type" "text/html"}
       :body (chassis/html
              (layout [:p {:class "banner banner--error"}
                       "This link is not valid. Ask for a fresh invite."]))}
      (layout
       ;; The invite bar copies an INVITE token, which names the channel and
       ;; nothing else. Copying the current URL would share this user's session and
       ;; make every recipient them — the bug this replaced.
       [:div {:class "invite"}
        [:span {:class "invite__hint"}
         "Signed in as " [:strong username]
         ". Anyone with the invite link can join and read this channel."]
        [:button {:class "btn btn--ghost"
                  :data-invite (str "/i/" (token/invite secret channel-id))
                  :onclick "window.chatCopyInvite(this)"}
         "Copy invite link"]
        [:input {:id "invite-fallback" :class "invite__fallback"
                 :hidden true :readonly true}]]
       [:div {:id "live-root"} "connecting…"]
       ;; The live connection carries the same session token.
       [:div {:data-init (str "@get('/live?t=" t "')")}]
       [:script (chassis/raw (format "window.chatBoot(%s, %s, %s)"
                                     (pr-str channel-id) (pr-str username)
                                     (pr-str t)))]))))

(defn invite-page
  "Landing page for an invite link: asks for a name, then mints a session token.

  The key stays in the fragment across the redirect, which the client does — the
  server never sees it and could not forward it."
  [{:keys [path-params]}]
  (let [t (:token path-params)
        channel-id (token/read-invite secret t)]
    (if-not channel-id
      {:status 403 :headers {"content-type" "text/html"}
       :body (chassis/html
              (layout [:p {:class "banner banner--error"}
                       "This invite link is not valid."]))}
      (layout
       [:h1 "Join the channel"]
       [:p "Pick a name. It identifies you in this channel only."]
       [:p [:input {:id "username" :placeholder "Your name"}]]
       [:p [:button {:class "btn btn--primary"
                     :onclick (str "window.chatJoin(" (pr-str t) ")")}
            "Join"]]
       [:script (chassis/raw (format "window.chatBindKeys(%s)" (pr-str t)))]))))

(defn join
  "Claims a username in a channel and mints a session token.

  **A name is claimed exactly once per channel.** Refusing a duplicate is the point:
  a username is the only identity in this model, so letting a second person enter an
  existing name would make them that person. The claim is enforced by a primary key
  rather than a read-then-write, so two simultaneous joins cannot both succeed.

  Returns 409 with `{:error \"username-taken\"}` so the client can show the message
  beside the field the user typed into."
  [{:keys [body-params] :as request}]
  (let [db (get-in request [::z/context ::z.sql/db])
        {:keys [invite username]} body-params
        channel-id (token/read-invite secret invite)
        name' (let [n (str/trim (or username ""))]
                (if (str/blank? n) "anon" (subs n 0 (min 24 (count n)))))]
    (cond
      (not channel-id)
      {:status 403
       :headers {"content-type" "application/json"}
       :body (charred/write-json-str {:error "invalid-invite"})}

      (not (db/claim-member! db channel-id name'))
      {:status 409
       :headers {"content-type" "application/json"}
       :body (charred/write-json-str {:error "username-taken" :username name'})}

      :else
      {:status 200
       :headers {"content-type" "application/json"}
       :body (charred/write-json-str
              {:token (token/session secret channel-id name')})})))

(defn- json-handler
  "Wraps a handler so an exception is logged and returned, rather than becoming an
  opaque 500. Zodiac has an error-handler hook, but during development a printed
  stack trace is what actually shortens the loop."
  [f]
  (fn [request]
    (try (f request)
         (catch Throwable e
           (println "handler error:" (type e) (.getMessage e))
           (.printStackTrace e)
           {:status 500 :body (str (type e) ": " (.getMessage e))}))))

(defn create-channel
  [{:keys [body-params] :as request}]
  ;; :body-params, not (slurp :body) — zodiac runs muuntaja, which has already
  ;; consumed and parsed the JSON body. Slurping the stream yields EOFException.
  (let [db (get-in request [::z/context ::z.sql/db])
        name (or (:name body-params) "untitled")
        id (str (random-uuid))]
    (db/create-channel! db id name)
    {:status 200
     :headers {"content-type" "application/json"}
     ;; An invite token, not a bare id: the creator joins through the same path
     ;; everyone else does, so there is one code path to get wrong instead of two.
     :body (charred/write-json-str {:invite (token/invite secret id)})}))

(defn send-message
  "Accepts ciphertext and publishes a hint. The server stores bytes it cannot read.

  The author comes from the signed session token, not from the request body. Taking
  it from the body let anyone post as anyone by editing one JSON field."
  [{:keys [body-params] :as request}]
  (let [db (get-in request [::z/context ::z.sql/db])
        live (z.live/live-ctx request)
        {:keys [ct iv t]} body-params
        {:keys [channel-id username]} (token/read-session secret t)]
    (if-not channel-id
      {:status 403 :body "invalid session token"}
      (do
        (db/add-message! db channel-id username ct iv)
        (typing/clear! channel-id username)
        ;; Hints only: "this channel changed", no payload.
        (z.live/publish! live [:channel channel-id])
        (z.live/publish! live [:typing channel-id])
        ;; The roster may have gained a first-time poster.
        (z.live/publish! live [:presence channel-id username])
        {:status 204}))))

(defn heartbeat
  "Keeps a member's presence alive.

  The client pings this while its page is open. There is no reliable disconnect
  signal from the transport, so being connected is expressed as a recent heartbeat
  rather than as an event — see `chat.presence/touch!`."
  [{:keys [body-params] :as request}]
  (let [live (z.live/live-ctx request)
        {:keys [channel-id username]} (token/read-session secret (:t body-params))]
    (if-not channel-id
      {:status 403 :body "invalid session token"}
      (do
        ;; Publish only when the status actually changed, so a routine ping does
        ;; not push a patch that renders identically.
        (when (presence/touch! channel-id username)
          (z.live/publish! live [:presence channel-id username])
          (z.live/publish! live [:channel channel-id]))
        {:status 204}))))

(defn typing-ping
  "Records ephemeral typing state and hints. Separate from the live action route
  because it touches server state rather than a view.

  Reads `liveId` from `:body-params`, not from datastar signals: this is a plain
  `fetch`, not a datastar action, so `get-signals` finds nothing. Using the signals
  path here meant `liveId` was nil and the hint was silently never published."
  [{:keys [body-params] :as request}]
  (let [live (z.live/live-ctx request)
        live-id (:liveId body-params)
        {:keys [channel-id username]} (get @connection-info live-id)]
    (if channel-id
      (do (typing/touch! channel-id username)
          (z.live/publish! live [:typing channel-id])
          {:status 204})
      ;; Loud rather than silent: a 204 for an unknown live id looked like success
      ;; while doing nothing at all.
      {:status 409 :body "unknown live id"})))

(defn- static
  [resource content-type]
  (fn [_request]
    {:status 200
     :headers {"content-type" content-type}
     :body (slurp (io/resource resource))}))

(defn routes []
  ["" (z.live/routes)
   ["/" {:handler home}]
   ["/c/:token" {:handler channel-page}]
   ["/i/:token" {:handler invite-page}]
   ["/join" {:post {:handler (json-handler join)} :zodiac/skip-csrf true}]
   ["/create" {:post {:handler (json-handler create-channel)} :zodiac/skip-csrf true}]
   ["/send" {:post {:handler (json-handler send-message)} :zodiac/skip-csrf true}]
   ["/typing" {:post {:handler (json-handler typing-ping)} :zodiac/skip-csrf true}]
   ["/heartbeat" {:post {:handler (json-handler heartbeat)} :zodiac/skip-csrf true}]
   ["/chat.css" {:handler (static "chat.css" "text/css")}]
   ["/chat.js" {:handler (static "chat.js" "text/javascript")}]
   ["/datastar.js" {:handler (static "datastar.js" "text/javascript")}]])

;;; ==========================================================================
;;; Start
;;; ==========================================================================

(defn- start-presence-expiry!
  "Turns a lapsed heartbeat into a grey dot.

  Entries expire by TTL, but expiry is only *observable* if something re-derives the
  row — a hint is what causes that read. Without this ticker a member who closed
  their tab would stay green forever, because nothing announced the change.

  Publishes the member's own topic so exactly their row repatches, plus the channel
  topic so any view deriving from the roster re-reads."
  [live]
  (let [running? (atom true)
        t (Thread.
           (fn []
             (while @running?
               (try
                 (Thread/sleep 5000)
                 (doseq [[channel-id username] (presence/expired)]
                   (presence/forget! channel-id username)
                   (z.live/publish! live [:presence channel-id username])
                   (z.live/publish! live [:channel channel-id]))
                 (catch InterruptedException _ (reset! running? false))
                 (catch Exception e (println "presence expiry error" (.getMessage e))))))
           "chat-presence-expiry")]
    (.setDaemon t true)
    (.start t)
    running?))

(defn- start-typing-expiry!
  "Publishes a typing hint for every channel with typing state, on a timer.

  Typing entries expire when read, but a hint is what causes a read — so without
  this a \"bob is typing...\" persists after bob stops. Ticking at half the TTL
  means the indicator clears within one TTL of the last keystroke.

  Cheap: it publishes only for channels that currently have typists, and the hint
  coalesces with any others in the same flush window."
  [live]
  (let [running? (atom true)
        t (Thread. (fn []
                     (while @running?
                       (try
                         (Thread/sleep (long (quot typing/ttl-ms 2)))
                         ;; Expire first, then hint: the hint causes subscribers to
                         ;; re-read, and they must see the post-expiry state.
                         (doseq [ch (typing/active-channels)]
                           (typing/expire! ch)
                           (z.live/publish! live [:typing ch]))
                         (catch InterruptedException _ (reset! running? false))
                         (catch Exception _ nil))))
                   "chat-typing-expiry")]
    (.setDaemon t true)
    (.start t)
    running?))

(defonce ^:dynamic *system* nil)

(defn -main
  [& _]
  (let [db-path "/tmp/zodiac-live-chat.db"
        sql-ext (z.sql/init {:spec {:jdbcUrl (str "jdbc:sqlite:" db-path)}})
        ;; The Source needs the db, which the sql extension owns. Resolved lazily
        ;; from the running system rather than threaded at construction time.
        source-holder (atom nil)
        live-ext (z.live/init
                  {;; A VAR, so redefining the component at a REPL reaches
                   ;; already-connected contexts.
                   :components {:chat #'component/chat
                                ;; The sidebar rows. Registered like any component;
                                ;; the parent's render mounts them.
                                :member #'member/member}
                   :render-fn chassis/html
                   ;; Indirection through an atom because the Source needs the db,
                   ;; which the sql extension only creates when the system starts.
                   :source (reify source/Source
                             (fetch [_ q] (source/fetch @source-holder q))
                             (fetch [_ q b] (source/fetch @source-holder q b))
                             (basis [_] (source/basis @source-holder)))
                   :secret secret
                   :sse-fn sse-fn
                   :signals-fn signals})
        sys (z/start {:routes #'routes
                      :extensions [sql-ext live-ext]
                      :cookie-secret cookie-secret
                      ;; Async, so a parked SSE connection does not hold a request
                      ;; worker thread. :async-timeout 0 keeps it open.
                      :async? true
                      :jetty {:async-timeout 0}})
        db (get-in sys [::z.sql/db])]
    (db/migrate! db)
    (reset! source-holder (db/->source db))
    (start-typing-expiry! (get-in sys [::z.live/context]))
    (start-presence-expiry! (get-in sys [::z.live/context]))
    (alter-var-root #'*system* (constantly sys))
    (println "chat on http://localhost:3000")
    sys))
