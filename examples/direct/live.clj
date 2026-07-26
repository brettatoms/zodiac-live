(ns direct.live
  "Connections and targeted pushes. The whole runtime, in one namespace.

  This is the control experiment for the framework version in `examples/chat`. Same
  features, no engine: no view state, no diffing, no boundaries, no tiers, no child
  reconciliation.

  ## The model

  A connection is a generator plus the facts needed to render for it — which
  channel, which user. When something changes, a handler names the *target* and
  pushes a fragment. That is the whole idea:

      (push-to-channel! channel-id
        (fn [conn] {:selector \"#messages\" :mode :append :html (render-msg …)}))

  The author already knows what changed and where it goes. Nothing has to rediscover
  it, so nothing can rediscover it wrongly — which is where most of the framework
  version's bugs lived.

  ## What is deliberately absent

  - **No diff.** The server does not compare old and new state to find what changed.
    A handler knows.
  - **No view state.** There is no per-connection projection to keep in sync,
    therefore nothing to go stale, and no reconnect problem for it.
  - **No boundaries.** A selector is written where it is used, so it cannot drift
    from the element it targets.
  - **No child components.** A member row is a function returning hiccup.

  The cost is real and should be stated: the author must remember to push after every
  mutation, and a forgotten push is a silently stale screen. The framework version
  makes that structurally impossible by re-deriving. That is the trade."
  (:require [starfederation.datastar.clojure.api :as d*])
  (:import [java.util.concurrent ConcurrentHashMap]))

;;; ==========================================================================
;;; Connections
;;; ==========================================================================
;;; One entry per open SSE stream. Held in a ConcurrentHashMap rather than an atom
;;; because entries are added and removed from request threads while the fan-out
;;; loop iterates, and a snapshot iterator is exactly what that wants.

(defonce ^{:doc "conn-id -> {:gen :channel-id :username :last-seen}"}
  conns
  (ConcurrentHashMap.))

(defn- new-id [] (str "d" (subs (str (random-uuid)) 0 8)))

(defn open!
  "Registers a connection and returns its id."
  [gen channel-id username]
  (let [id (new-id)]
    (.put conns id {:gen gen :channel-id channel-id :username username
                    :last-seen (System/currentTimeMillis)})
    id))

(defn close! [id] (.remove conns id) nil)

(defn touch!
  "Marks a connection alive. Returns true if it had lapsed, so a caller can decide
  whether the change is worth pushing."
  [id ttl-ms]
  (when-let [c (.get conns id)]
    (let [was (:last-seen c)]
      (.put conns id (assoc c :last-seen (System/currentTimeMillis)))
      (< ^long was (- (System/currentTimeMillis) ^long ttl-ms)))))

(defn in-channel
  "Live connections for `channel-id`."
  [channel-id]
  (->> (into {} conns)
       (filter (fn [[_ c]] (= channel-id (:channel-id c))))
       (into {})))

(defn online-users
  "Usernames with a connection in `channel-id` whose heartbeat has not lapsed.

  Sorted, so callers get a stable value."
  [channel-id ttl-ms]
  (let [cutoff (- (System/currentTimeMillis) ^long ttl-ms)]
    (->> (in-channel channel-id)
         (keep (fn [[_ c]] (when (>= ^long (:last-seen c) cutoff) (:username c))))
         distinct sort vec)))

(defn lapsed
  "Connection ids whose heartbeat has lapsed."
  [ttl-ms]
  (let [cutoff (- (System/currentTimeMillis) ^long ttl-ms)]
    (->> (into {} conns)
         (keep (fn [[id c]] (when (< ^long (:last-seen c) cutoff) id)))
         vec)))

;;; ==========================================================================
;;; Pushing
;;; ==========================================================================

(def ^:private mode->datastar
  {:outer d*/pm-outer :inner d*/pm-inner :remove d*/pm-remove
   :append d*/pm-append :prepend d*/pm-prepend
   :before d*/pm-before :after d*/pm-after :replace d*/pm-replace})

(defn- send!
  "Applies one patch to one connection. Drops the connection if the write fails.

  Writing to a dead socket is the only reliable way to discover one here — the
  adapter's `on-close` does not fire when a client simply disappears."
  [id {:keys [gen]} {:keys [selector mode html]}]
  (try
    (d*/patch-elements! gen (or html "")
                        {d*/selector selector
                         d*/patch-mode (mode->datastar (or mode :outer))})
    true
    (catch Throwable _
      (close! id)
      false)))

(defn push!
  "Sends one patch to one connection id."
  [id patch]
  (when-let [c (.get conns id)]
    (send! id c patch)))

(defn push-to-channel!
  "Sends a patch to every connection in `channel-id`.

  `patch-fn` takes the connection map and returns a patch, or nil to skip it. Taking
  the connection is what lets a fan-out render differently per viewer — the typing
  indicator excludes the typist, and a member row is the same for everyone.

  One failed write does not stop the others: a dropped viewer must not prevent the
  rest from updating."
  [channel-id patch-fn]
  (doseq [[id c] (in-channel channel-id)]
    (when-let [patch (patch-fn c)]
      (send! id c patch))))
