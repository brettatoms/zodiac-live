(ns chat.presence
  "Who is connected, per channel.

  ## A username identifies a person only within one channel

  This is the load-bearing rule. `alice` in `#general` and `alice` in `#random` are
  unrelated people, and nothing here may join them. So every key is the **pair**
  `[channel-id username]`, and there is deliberately no function that takes a bare
  username — a signature like `(online? username)` would be unanswerable, and the
  plausible-looking answer would be wrong.

  That also decides the PubSub topic shape. A member's presence topic is
  `[:presence channel-id username]`, so `alice` appearing in `#random` cannot
  invalidate the `alice` row in `#general`.

  ## A heartbeat, not connect/disconnect events

  There is no reliable disconnect signal from the transport (see `touch!`), so
  liveness is a TTL over a client heartbeat. Several tabs are handled for free: any
  one of them refreshing the timestamp keeps the member online, and closing one
  while another remains changes nothing.

  Like `chat.typing`, this is server-side state several views derive from: a
  `Source` in every respect except durability. It is not a state tier, which
  describes how a *view* field survives a reconnect."
  (:import [java.util.concurrent ConcurrentHashMap]))

(def ttl-ms
  "How long a heartbeat counts as \"connected\".

  Generous relative to the client's ping interval, so one dropped request does not
  flicker someone offline."
  15000)

(defonce ^{:doc "[channel-id username] -> last-heartbeat-ms. Survives reload."}
  state
  (ConcurrentHashMap.))

(defn touch!
  "Records that `username` is connected in `channel-id`, now.

  Returns true if this made them newly online, which is what a caller needs to
  decide whether the change is worth publishing a hint about. A routine heartbeat
  from someone already online changes nothing observable, and hinting on it would
  push a patch that renders identically.

  ## Why a heartbeat rather than connect/disconnect bookkeeping

  There is no reliable disconnect signal to hang cleanup on. Verified against a
  bare Jetty + Ring datastar adapter, with none of this application in the way:
  `on-close` fires on an orderly shutdown but **not** when a client simply
  disappears, and writing to the dead socket afterwards neither triggers it nor
  throws — Jetty buffers the write and reports nothing.

  So liveness is a timing property, not an event. That is the same conclusion
  `chat.typing` reached for the same reason, and the same shape of fix."
  [channel-id username]
  (let [k [channel-id username]
        was (.put state k (System/currentTimeMillis))]
    (or (nil? was) (< ^long was (- (System/currentTimeMillis) ttl-ms)))))

(defn online?
  "Is `username` connected in `channel-id`?

  Takes the channel deliberately. There is no way to ask whether a username is
  online globally, because that question has no answer in this model."
  [channel-id username]
  (if-let [^Long t (.get state [channel-id username])]
    (>= t (- (System/currentTimeMillis) ttl-ms))
    false))

(defn connected
  "Usernames currently connected in `channel-id`, sorted.

  Sorted so the value is stable across reads: an unordered result would make every
  refresh look like a change and defeat the diff.

  Read-only — expiry is `expire!`'s job. A read that mutated raced with the expiry
  ticker in `chat.typing` and made the indicator never appear at all."
  [channel-id]
  (let [cutoff (- (System/currentTimeMillis) ttl-ms)]
    (->> (into {} state)
         (keep (fn [[[ch user] t]] (when (and (= ch channel-id) (>= t cutoff)) user)))
         sort
         vec)))

(defn expired
  "`[channel-id username]` pairs whose heartbeat has lapsed.

  Returned rather than removed, so a caller can publish a hint for each before the
  entry disappears — otherwise the dot never turns grey, because nothing tells the
  row to re-derive."
  []
  (let [cutoff (- (System/currentTimeMillis) ttl-ms)]
    (->> (into {} state)
         (keep (fn [[k t]] (when (< t cutoff) k)))
         vec)))

(defn forget!
  "Drops one entry."
  [channel-id username]
  (.remove state [channel-id username])
  nil)

(defn forget-channel!
  "Drops every entry for `channel-id`."
  [channel-id]
  (doseq [k (keys (into {} state))
          :when (= (first k) channel-id)]
    (.remove state k))
  nil)
