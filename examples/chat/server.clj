(ns chat.server
  "The chat example, as a zodiac app.

  Two extensions compose here without knowing about each other: `zodiac-sql`
  provides the db in the request context, and `zodiac.ext.live` provides the live
  engine. Both are just functions transforming the same integrant config, which is
  the property  was betting on.

  Run:
    clojure -M:example -m chat.server
  then open http://localhost:3000"
  (:require [charred.api :as charred]
            [clojure.java.io :as io]
            [dev.onionpancakes.chassis.core :as chassis]
            [starfederation.datastar.clojure.api :as d*]
            [starfederation.datastar.clojure.adapter.ring :as d*ring]
            [zodiac.core :as z]
            [zodiac.ext.live :as z.live]
            [zodiac.ext.sql :as z.sql]
            [chat.component :as component]
            [chat.db :as db]
            [chat.typing :as typing]
            [remuda.engine :as engine]
            [remuda.source :as source])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(def secret "chat-example-secret-not-for-production")

(defonce ^{:doc "live id -> latch, keeping each async SSE connection alive."}
  latches
  (atom {}))

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
  [request]
  (try (some-> (d*/get-signals request) (charred/read-json :key-fn keyword))
       (catch Exception _ nil)))

;;; ==========================================================================
;;; SSE
;;; ==========================================================================

(defn- sse-fn
  "Opens a live connection. Supplied to the extension so the transport stays the
  application's choice — the extension names no server."
  [{:keys [request engine source]}]
  (let [params (:params request)
        channel-id (get params "channel")
        username (or (get params "user") "anon")]
    (d*ring/->sse-response
     request
     {d*ring/on-open
      (fn [sse-gen]
        (let [id (engine/connect! engine :chat
                                  {:send! #(send-instructions! sse-gen %)
                                   :close! #(d*/close-sse! sse-gen)
                                   :params {:channel-id channel-id
                                            :username username}})
              latch (CountDownLatch. 1)]
          (swap! latches assoc id latch)
          (swap! connection-info assoc id {:channel-id channel-id
                                           :username username})
          (z.live/subscribe! (z.live/live-ctx request)
                             id
                             [[:channel channel-id] [:typing channel-id]])
          (d*/patch-elements! sse-gen
                              (engine/mount! engine id {:source source})
                              {d*/selector "#live-root"
                               d*/patch-mode d*/pm-outer})
          (d*/patch-signals! sse-gen (charred/write-json-str {:liveId id}))
          ;; Also expose it to plain JS. chat.js needs it for fetch() calls, and
          ;; datastar signals live in datastar's own store rather than on window.
          (d*/execute-script! sse-gen (str "window.__liveId=" (pr-str id) ";"))
          ;; Park. The route is async, so this does not hold a request worker.
          (.await latch 8 TimeUnit/HOURS)
          (typing/clear! channel-id username)
          (engine/disconnect! engine id)
          (swap! latches dissoc id)
          (swap! connection-info dissoc id)))})))

;;; ==========================================================================
;;; Pages
;;; ==========================================================================

(defn- layout
  [& body]
  [chassis/doctype-html5
   [:html
    [:head
     [:title "zodiac-live chat"]
     [:script {:type "module" :src "/datastar.js"}]
     [:script {:src "/chat.js"}]
     [:style (chassis/raw "
body{font-family:system-ui;max-width:44rem;margin:2rem auto;padding:0 1rem}
.chat{display:flex;flex-direction:column;height:80vh}
.messages{flex:1;overflow-y:auto;list-style:none;padding:0;margin:0}
.messages li{padding:.25rem 0;border-bottom:1px solid #eee}
.author{font-weight:600}
.typing{height:1.5rem;color:#666;font-size:.9rem}
form{display:flex;gap:.5rem}
input[name=draft]{flex:1;padding:.5rem}
")]]
    (into [:body] body)]])

(defn home
  [_request]
  (layout
   [:h1 "Create a channel"]
   [:p "The encryption key is generated in your browser and put in the URL "
    [:em "fragment"] ", which is never sent to the server. Share the whole link."]
   [:p [:input {:id "channel-name" :placeholder "Channel name"}]]
   [:p [:input {:id "username" :placeholder "Your name"}]]
   [:p [:button {:onclick "window.chatCreate()"} "Create channel"]]))

(defn channel-page
  [{:keys [path-params params]}]
  (let [channel-id (:id path-params)
        username (or (get params "user") "anon")]
    (layout
     [:div {:id "live-root"} "connecting..."]
     ;; data-init, not data-on-load: v1's run-once hook is the `init` plugin.
     [:div {:data-init (str "@get('/live?channel=" channel-id
                            "&user=" username "')")}]
     [:script (chassis/raw (format "window.chatBoot(%s, %s)"
                                   (pr-str channel-id) (pr-str username)))])))

;;; ==========================================================================
;;; Non-live routes
;;; ==========================================================================

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
     :body (charred/write-json-str {:id id})}))

(defn send-message
  "Accepts ciphertext and publishes a hint. The server stores bytes it cannot read."
  [{:keys [body-params] :as request}]
  (let [db (get-in request [::z/context ::z.sql/db])
        live (z.live/live-ctx request)
        {:keys [channel author ct iv]} body-params]
    (db/add-message! db channel author ct iv)
    (typing/clear! channel author)
    ;; Hints only: "this channel changed", no payload.
    (z.live/publish! live [:channel channel])
    (z.live/publish! live [:typing channel])
    {:status 204}))

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
   ["/c/:id" {:handler channel-page}]
   ["/create" {:post {:handler (json-handler create-channel)} :zodiac/skip-csrf true}]
   ["/send" {:post {:handler (json-handler send-message)} :zodiac/skip-csrf true}]
   ["/typing" {:post {:handler (json-handler typing-ping)} :zodiac/skip-csrf true}]
   ["/chat.js" {:handler (static "chat.js" "text/javascript")}]
   ["/datastar.js" {:handler (static "datastar.js" "text/javascript")}]])

;;; ==========================================================================
;;; Start
;;; ==========================================================================

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
                   :components {:chat #'component/chat}
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
                      :cookie-secret "0123456789abcdef"
                      ;; Async, so a parked SSE connection does not hold a request
                      ;; worker thread. :async-timeout 0 keeps it open.
                      :async? true
                      :jetty {:async-timeout 0}})
        db (get-in sys [::z.sql/db])]
    (db/migrate! db)
    (reset! source-holder (db/->source db))
    (start-typing-expiry! (get-in sys [::z.live/context]))
    (alter-var-root #'*system* (constantly sys))
    (println "chat on http://localhost:3000")
    sys))
