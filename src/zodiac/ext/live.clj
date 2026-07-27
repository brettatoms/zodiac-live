(ns zodiac.ext.live
  "Zodiac extension for remuda + darkstar.

  The core is plain Ring and knows nothing about zodiac; this
  namespace is the wiring, following the shape of `zodiac-hot-reload`: integrant
  keys, route registration, and a config transformer. **No domain logic.** If this
  file grows past wiring, 's map-of-pieces design has failed.

  Usage:

      (z/start {:extensions [(z.live/init {:components {:chat #'app/chat}
                                           :render-fn chassis/html
                                           :source my-source
                                           :secret \"...\"})]})

  What it wires:

  - the engine, the pubsub bus and registry, and the fragment cache as integrant
    components, so they participate in zodiac's lifecycle;
  - an SSE route and an action route;
  - the flush loop that turns coalesced hints into pushes.

  ## Components are registered as vars, deliberately

  Redefining `:render` at a REPL must reach already-connected
  contexts. The engine resolves a component by name on every render and derefs it
  if it is deref-able, so **an adapter should register vars** — `#'app/chat`, not
  `app/chat`. A plain map still works but only whole-map replacement is visible.

  ## Registries survive reload

  The live-context registry, subscription registry and cache are held in
  `defonce`d atoms rather than created per `init-key`, so they
  to survive `tools.namespace/refresh`. Recreating them on reload would drop every
  connection — the same failure as a server restart, triggered by saving a file."
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [remuda.pubsub :as pubsub]
            [remuda.watch-engine :as engine]))

(create-ns 'zodiac.core)
(alias 'z 'zodiac.core)

;;; ==========================================================================
;;; Reload-surviving state
;;; ==========================================================================

(defonce ^{:doc "id -> live context. Survives namespace reload."}
  registry
  (atom {}))

(defonce ^{:doc "Subscription registry. Survives namespace reload."}
  subscriptions
  (pubsub/registry))

;;; ==========================================================================
;;; Integrant components
;;; ==========================================================================

(defmethod ig/init-key ::engine [_ {:keys [components render-fn]}]
  (log/debug "Starting zodiac-live engine")
  ;; No `:on-children` hook: nested fragments are ordinary function calls inside one
  ;; render, so there are no child components whose subscriptions need establishing
  ;; separately. And no `start!`/`stop!` — the watch engine holds a registry and
  ;; nothing else, so there is no lifecycle to run.
  (engine/engine {:components components
                  :render-fn render-fn
                  :registry registry}))

(defmethod ig/init-key ::bus [_ {:keys [bus]}]
  (or bus (pubsub/local-pubsub)))

(defmethod ig/init-key ::flusher
  [_ {:keys [engine interval-ms bus]}]
  ;; The flush loop is what makes coalescing real: hints arriving inside one
  ;; interval collapse to a single rebuild per context. Flushing per hint
  ;; would defeat it.
  (let [running? (atom true)
        thread (Thread.
                (fn []
                  (while @running?
                    (try
                      (Thread/sleep (long interval-ms))
                      (let [{:keys [topics]} (pubsub/flush-dirty! subscriptions)]
                        (when (seq topics)
                          (log/debug "flush" {:topics topics})
                          ;; Per TOPIC, not per context. `refresh!` needs to know
                          ;; which topic fired so it can pick the narrowest fragment
                          ;; that read it — a context alone does not say where to
                          ;; patch.
                          (doseq [topic topics
                                  id (pubsub/contexts-for subscriptions #{topic})]
                            (try
                              ;; `current` rather than `topics`: the outer `topics`
                              ;; is the dirty set being iterated, and shadowing it
                              ;; here would be a trap for the next reader.
                              (let [{current :topics} (engine/refresh! engine id topic)]
                                ;; A dependency set is data, so it changes: a roster
                                ;; that reads one presence topic per member depends on
                                ;; a different set the moment its membership changes.
                                ;; Subscribing only at mount left a later joiner
                                ;; permanently stale.
                                ;;
                                ;; The whole current set is handed over rather than
                                ;; the :added/:removed diff, because
                                ;; `subscribe-context!` already diffs against what is
                                ;; registered — and doing it there keeps one place
                                ;; that decides what a subscription change means.
                                (when (seq current)
                                  (pubsub/subscribe-context! subscriptions bus id current)))
                              (catch Exception e
                                ;; One failing context must not stop the others.
                                (log/warn e "refresh failed"
                                          {:id id :topic topic}))))))
                      (catch InterruptedException _ (reset! running? false))
                      (catch Exception e (log/error e "flush loop error")))))
                "zodiac-live-flusher")]
    (.setDaemon thread true)
    (.start thread)
    {:running? running? :thread thread}))

(defmethod ig/init-key ::context
  [_ {:keys [engine bus source secret sse-fn signals-fn]}]
  ;; What handlers see in the request context. Deliberately a plain map: the
  ;; flusher is depended on so integrant starts it, but nothing here calls it.
  {:engine engine
   :bus bus
   :source source
   :secret secret
   :sse-fn sse-fn
   :signals-fn signals-fn
   :subscriptions subscriptions})

(defmethod ig/halt-key! ::flusher [_ {:keys [running? thread]}]
  (reset! running? false)
  (.interrupt ^Thread thread))

;;; ==========================================================================
;;; Handlers
;;; ==========================================================================

(def context-key
  "Where this extension puts itself in zodiac's request context.

  A var rather than a literal keyword in each caller, because a mismatched
  namespaced keyword fails *silently* — the lookup returns nil and the failure
  surfaces somewhere unrelated. That already cost a debugging session once in this
  project (a `::cache` that resolved to the wrong namespace), so the key is named
  once and referenced."
  ::live)

(defn live-ctx
  "The extension's components, from zodiac's injected request context."
  [request]
  (or (get-in request [:zodiac.core/context context-key])
      (throw (ex-info "zodiac-live context missing from the request"
                      {:looked-for context-key
                       :available (keys (:zodiac.core/context request))}))))

(defn sse-handler
  "The connection route, in zodiac's async 3-arity form.

  Delegates everything to the caller's `sse-fn`, which owns the transport, because
  The server must stay replaceable — this namespace must not name
  Jetty, http-kit, or a datastar adapter."
  [request respond _raise]
  (let [{:keys [engine sse-fn source secret]} (live-ctx request)]
    (respond
     (sse-fn {:request request
              :engine engine
              :source source
              :secret secret
              :subscriptions subscriptions}))))

(defn action-handler
  "The interaction route. Looks up the live context and dispatches an event.

  Args arrive as a JSON payload and are passed through **uncoerced**,
  because their types already survived the wire. That is the whole point of the
  payload form: the previous query-string encoding forced every receiver to guess a
  type, and `dev/slice` guessed with a hardcoded `parse-long` that silently turned
  string args into nil.

  `liveId` and `event` fall back to query params so a plain form post still works,
  but *args* are read only from the payload — a query-string arg cannot carry a
  type, and admitting one would reopen defect 3 through the back door."
  [{:keys [params] :as request}]
  (let [{:keys [engine signals-fn]} (live-ctx request)
        signals (when signals-fn (signals-fn request))
        id (or (:liveId signals) (get params "liveId"))
        ;; some-> so a missing event stays nil and produces a 409, rather than
        ;; (keyword nil) succeeding and failing later inside dispatch!.
        event (some-> (or (:event signals) (get params "event")) name keyword)]
    (if (and id event (get @(:registry engine) id))
      ;; A handler mutates the application's own state and RETURNS the topics it
      ;; invalidated; it does not push. Publishing them here is what keeps one
      ;; invalidation path — a hint from the acting connection and a hint from
      ;; another user's action travel identically, through the bus and the flusher.
      ;; Pushing from the handler would give the actor a second path that could
      ;; disagree with everyone else's.
      (let [{:keys [bus]} (live-ctx request)
            topics (engine/dispatch! engine id event
                                     (dissoc signals :liveId :event))]
        (doseq [topic topics]
          (pubsub/publish! bus topic))
        {:status 204})
      {:status 409 :body "no live context"})))

;;; ==========================================================================
;;; Public API
;;; ==========================================================================

(defn publish!
  "Announces that `topic` changed.

  `live` is the extension's context map, from `live-ctx`."
  [live topic]
  (when-not (:bus live)
    (throw (ex-info "no bus in the zodiac-live context" {:got (keys live)})))
  (pubsub/publish! (:bus live) topic))

(defn subscribe!
  "Subscribes live context `id` to `topics`.

  Throws on a missing bus rather than silently doing nothing: this runs inside an
  SSE `on-open` callback where a swallowed exception looks like a connection that
  opens and then sends nothing at all."
  [live id topics]
  (when-not (:bus live)
    (throw (ex-info "no bus in the zodiac-live context" {:got (keys live)})))
  (pubsub/subscribe-context! subscriptions (:bus live) id topics))

(defn routes
  "The routes zodiac-live needs, for the application to splice into its own route
  vector.

  Returned rather than injected because zodiac's `:routes` is typically a var the
  application owns, and an extension reaching in to `conj` onto it would fight that
  ownership — and would break if the application passed a function rather than a
  vector. The application writes:

      (defn routes []
        [\"\" (z.live/routes)
         [\"/\" {:handler home}]])

  The handlers read their dependencies from the request context, which is how they
  avoid needing an integrant ref at route-definition time."
  ([] (routes {}))
  ([{:keys [sse-path action-path]}]
   [[(or sse-path "/live")
     {:get {:handler sse-handler}
      ;; Async, per zodiac's own SSE example: the handler responds immediately with
      ;; a streaming body and the connection is held open without occupying a
      ;; request worker thread. That is a materially better concurrency profile
      ;; than blocking a thread per connection, and it is the reason the
      ;; app must start with :async? true and :jetty {:async-timeout 0}.
      :zodiac/async? true
      ;; An SSE stream is not a form post; CSRF does not apply and would reject
      ;; the connection.
      :zodiac/skip-csrf true}]
    [(or action-path "/live/act")
     {:post {:handler action-handler}
      :zodiac/skip-csrf true}]]))

(defn init
  "Creates a zodiac extension for zodiac-live.

  Wires the engine, bus, flusher and handlers as integrant components and injects
  them into zodiac's request context, so a handler reaches them via
  `[::z/context ::live]`.

  Options:
  - `:components`  map of name -> component (register **vars**, )
  - `:render-fn`   hiccup -> string
  - `:source`      a `remuda.source/Source`
  - `:secret`      HMAC key for recovery snapshots
  - `:sse-fn`      (fn [ctx] -> ring response) owning the transport
  - `:signals-fn`  (fn [request] -> map) reading client signals, optional
  - `:interval-ms` flush interval, default 100
  - `:bus`         a `PubSub`; an in-process one is created if absent"
  [{:keys [components render-fn source secret sse-fn signals-fn interval-ms bus]}]
  {:pre [(map? components) (ifn? render-fn) (ifn? sse-fn)]}
  (fn [config]
    (-> config
        (assoc ::engine {:components components :render-fn render-fn
                         :bus (ig/ref ::bus)}
               ::bus {:bus bus}
               ::flusher {:engine (ig/ref ::engine)
                          :bus (ig/ref ::bus)
                          :source source
                          :interval-ms (or interval-ms 100)}
               ::context {:engine (ig/ref ::engine)
                          :bus (ig/ref ::bus)
                          ;; The flusher is referenced so integrant starts it,
                          ;; even though handlers never call it.
                          :flusher (ig/ref ::flusher)
                          :source source
                          :secret secret
                          :sse-fn sse-fn
                          :signals-fn signals-fn})
        ;; Injected into the request context, which is how zodiac extensions
        ;; expose themselves to handlers.
        (assoc-in [::z/middleware :context context-key] (ig/ref ::context)))))
