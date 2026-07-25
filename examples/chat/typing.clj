(ns chat.typing
  "Ephemeral typing state.

  A deliberate resolution of a tension with `DESIGN.md` §7.2. \"Who is typing\" *is*
  data, but a PubSub hint carries none — the whole point of hints is that a dropped,
  duplicated or reordered one cannot corrupt a view. Putting the username in the
  hint would break exactly that: a dropped \"stopped typing\" would leave
  \"alice typing...\" on screen forever.

  So typing state lives here, in memory, and the hint stays empty. A keystroke
  updates this map and publishes a hint; subscribers re-read the map on refresh.
  The hint still carries nothing, and this map is the source of truth for a fact
  that simply happens not to be durable.

  It is **not** a state tier (§5.1). Tiers describe how a field of a *view*
  survives a reconnect; this is server-side state that several views derive from,
  which makes it a `Source` in every respect except durability.

  Entries expire, because \"stopped typing\" is not an event anyone can rely on
  receiving — a closed tab sends nothing. A TTL turns liveness into a timing
  property rather than a correctness one."
  (:import [java.util.concurrent ConcurrentHashMap]))

(def ttl-ms 3000)

(defonce ^{:doc "channel-id -> {username -> last-keystroke-ms}. Survives reload."}
  state
  (ConcurrentHashMap.))

(defn touch!
  "Records that `username` is typing in `channel-id`, now."
  [channel-id username]
  (let [m (.computeIfAbsent state channel-id
                            (reify java.util.function.Function
                              (apply [_ _] (ConcurrentHashMap.))))]
    (.put ^ConcurrentHashMap m username (System/currentTimeMillis))
    nil))

(defn clear!
  "Records that `username` has stopped — on send, or on disconnect.

  Best-effort: the TTL is what actually guarantees the indicator disappears."
  [channel-id username]
  (when-let [^ConcurrentHashMap m (.get state channel-id)]
    (.remove m username))
  nil)

(defn typists
  "Usernames typing in `channel-id` right now, excluding `self`, sorted.

  **Read-only.** An earlier version expired entries here, which turned out to be a
  real bug: the expiry ticker also calls this, so the ticker's read could remove an
  entry before the subscriber's refresh had seen it — the indicator then never
  appeared at all. Reads must not mutate when several readers race.

  Sorted so the derived view is a stable value: an unordered set would produce a
  different vector on each read and make every refresh look like a change, which
  would defeat the diff."
  [channel-id self]
  (if-let [^ConcurrentHashMap m (.get state channel-id)]
    (let [cutoff (- (System/currentTimeMillis) ttl-ms)]
      (->> (into {} m)
           (keep (fn [[user t]] (when (and (>= t cutoff) (not= user self)) user)))
           sort
           vec))
    []))

(defn expire!
  "Drops entries older than the TTL. Called by the expiry ticker, not by readers."
  [channel-id]
  (when-let [^ConcurrentHashMap m (.get state channel-id)]
    (let [cutoff (- (System/currentTimeMillis) ttl-ms)]
      (doseq [[user t] (into {} m)]
        (when (< t cutoff) (.remove m user)))
      ;; Drop the channel entirely once empty, so `active-channels` stops
      ;; publishing hints for a channel nobody is typing in.
      (when (empty? (into {} m))
        (.remove state channel-id))))
  nil)

(defn active-channels
  "Channels with any typing entry, expired or not.

  Used to drive expiry: entries expire on read, but nothing re-reads once hints
  stop arriving — so a \"bob is typing...\" would stick forever after bob went
  quiet. A caller publishes a hint for these channels on a timer, which is what
  turns the TTL into an observable effect rather than a fact nobody looks at."
  []
  (vec (keys (into {} state))))

(defn forget-channel!
  [channel-id]
  (.remove state channel-id)
  nil)
