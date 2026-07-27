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

  Retested across transports, because an earlier version of this docstring got the
  reason wrong. It claimed `on-close` fires on an orderly shutdown but not when a
  client vanishes. What actually happens, measured with bare adapters and no
  application in the way:

  | case | Jetty | http-kit |
  |---|---|---|
  | polite close (FIN), server not writing | no signal | no signal |
  | polite close, server writing every second | **fires** | no signal |
  | client SIGKILLed (RST), server writing | **fires** | no signal |
  | black hole: socket open, peer never reads | no signal | no signal |

  Two conclusions, and only the second justifies a heartbeat:

  1. The signal is **write-triggered and transport-dependent**. Jetty does report a
     dead peer, but only once something writes to it; http-kit never did in any case
     tested. So an app that wants to be transport-agnostic cannot rely on it.
  2. The **black hole is undetectable** by any close callback — a closed laptop lid or
     a dropped network sends nothing, and 30 seconds of writes disappeared silently.
     Liveness there is a timing property, not an event.

  Phoenix is not exempt from (2), which is worth knowing before treating this as a JVM
  shortcoming: `phoenix/socket.js` heartbeats every 30s and Bandit reaps an idle
  websocket on a server-side timeout — verified, 2 connections to 0 after 75s of
  silence. The difference is that theirs lives in the transport and this lives here.

  ## Orderly cleanup on the JVM: `ServerConnector.setIdleTimeout` plus a write

  There IS a fix for the leaked-connection half of this, and it is not the obvious one.

  What does not work, all measured against a black-holed socket (peer holding the
  connection open, never reading):

  - `:async-timeout`, at 0 or 20000, on platform threads or virtual threads. All four
    combinations held the dead connection indefinitely. The async context is not idle
    from Jetty's point of view, so the servlet-level timeout never fires.
  - `SO_KEEPALIVE`. macOS defaults `net.inet.tcp.keepidle` to 7200000 — two hours
    before the first probe, then 8 probes at 75s. Correct eventually, useless in
    practice, and not exposed by `ServerConnector` anyway.

  What does work: **`ServerConnector.setIdleTimeout` combined with a periodic
  server->client write.**

      :configurator (fn [server]
                      (doseq [c (.getConnectors server)]
                        (when (instance? ServerConnector c)
                          (.setIdleTimeout c 15000))))

  Measured with a 15s connector idle timeout:

  | server behaviour | dead connection after 35s |
  |---|---|
  | writes once a second | **reaped**, `on-close` fired |
  | silent | still held (also still held at 65s) |

  So the write is what discovers the peer, and the idle timeout is what acts on it.
  Neither alone is enough. That makes a server-side keepalive tick a *transport*
  concern rather than an application one — see darkstar."
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
