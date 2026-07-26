# Zodiac Live

[Zodiac](https://github.com/brettatoms/zodiac) extension for
[Remuda](https://github.com/brettatoms/remuda) +
[Darkstar](https://github.com/brettatoms/darkstar).

Wiring only: integrant keys, two routes, and request-context injection. No domain
logic. Remuda holds the view state, Darkstar translates changes into Datastar
patches, and this connects both to a Zodiac app.

## Usage

```clojure
(require '[zodiac.core :as z]
         '[zodiac.ext.live :as z.live])

(defn routes []
  ["" (z.live/routes)
   ["/" {:get {:handler home}}]])

(z/start {:routes  #'routes
          :async?  true
          :jetty   {:async-timeout 0}
          :extensions
          [(z.live/init {:components {:chat #'app/chat}   ; vars, see below
                         :render-fn  chassis/html
                         :source     my-source
                         :secret     (env "LIVE_SECRET")
                         :sse-fn     my-sse-fn})]})
```

`:async? true` and `:async-timeout 0` are required. The SSE route holds a
connection open, so it must not occupy a request worker or time out.

Register components as **vars** (`#'app/chat`). The engine resolves a component by
name on every render and derefs it, so redefining `:render` at a REPL reaches
connections that are already open.

## What it wires

- the engine, PubSub bus, subscription registry and fragment cache as integrant
  components, so they participate in Zodiac's lifecycle
- an SSE route (`/live`) and an action route (`/live/act`), both CSRF-exempt
- a flush loop that turns coalesced invalidation hints into pushes

The live-context registry, subscriptions and cache are held in `defonce`d atoms, so
they survive `tools.namespace/refresh` with connections intact.

## Example app

`examples/chat/` is an end-to-end encrypted chat app: channels, message history
with pagination, and live typing indicators.

```
clojure -M:example -m chat.server
```

The encryption key lives in the URL fragment, which browsers do not transmit, so
the server stores ciphertext it cannot read. Messages are held in SQLite behind
Remuda's `Source` protocol; typing indicators are ephemeral server state published
as hints.

The signing keys are committed so the demo runs with no setup. Override them with
`CHAT_LIVE_SECRET` and `CHAT_COOKIE_SECRET`.

## Related

- [Remuda](https://github.com/brettatoms/remuda) — the engine: view state,
  diffing, tiers, reconnect
- [Darkstar](https://github.com/brettatoms/darkstar) — Datastar binding, and notes
  on what this model suits

## Status

Working, not released.

## License

MIT. See [LICENSE](LICENSE).
