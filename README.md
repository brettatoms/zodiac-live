# Zodiac Live

[Zodiac](https://github.com/brettatoms/zodiac) extension for
[Darkstar](https://github.com/brettatoms/darkstar).

Wiring only: integrant keys, two routes, and request-context injection. No domain logic.
Darkstar holds the per-connection fragment state and translates changes into Datastar
patches; this connects it to a Zodiac app.

## Install

```clojure
com.github.brettatoms/zodiac-live {:mvn/version "0.1.27"}
```

Brings in Darkstar transitively. The patch number is `git rev-list --count HEAD` at
release time, so it moves with every commit — check the
[Clojars page](https://clojars.org/com.github.brettatoms/zodiac-live) rather than
trusting this line.

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
          [(z.live/init {:components {:chat #'app/component}   ; vars, see below
                         :render-fn  chassis/html
                         :source     my-source
                         :secret     (env "LIVE_SECRET")
                         :sse-fn     my-sse-fn})]})
```

`:async? true` and `:async-timeout 0` are required. The SSE route holds a connection
open, so it must not occupy a request worker or be timed out.

Register components as **vars** (`#'app/component`). The engine resolves a component by
name on every render and derefs it, so redefining a render at a REPL reaches connections
that are already open.

## What it wires

- the engine, PubSub bus and subscription registry as integrant components, so they
  participate in Zodiac's lifecycle
- an SSE route (`/live`) and an action route (`/live/act`), both CSRF-exempt
- a flush loop that turns coalesced invalidation hints into pushes

The registry and subscriptions are held in `defonce`d atoms, so they survive
`tools.namespace/refresh` with connections intact.

Two details in the flush loop worth knowing, because both were bugs first:

- It iterates dirty **topics**, not dirty connections. `refresh!` needs to know which
  topic fired in order to pick the narrowest fragment that read it; a connection id
  alone does not say where to patch.
- After each refresh it hands the connection's whole current topic set back to the
  subscription registry. A dependency set is data and changes as the rendered data
  changes — subscribing once at mount leaves a later joiner permanently stale.

## Examples

Four applications, kept because each answers a different question. They run against the
released Darkstar; add `:dev` (`-M:dev:example`) to resolve it from a sibling checkout
instead.

### `examples/chat` — the feature-complete one

End-to-end encrypted chat: channels behind signed invite links, per-channel-unique
usernames, message history with pagination, typing indicators, and a member roster with
presence dots.

```
clojure -M:example -m chat.server            # http://localhost:3000
```

The encryption key lives in the URL fragment, which browsers do not transmit, so the
server stores ciphertext it cannot read. Messages are in SQLite behind Darkstar's
`Source` protocol; typing state is ephemeral server state published as hints.

Signing keys are committed so the demo runs with no setup. Override with
`CHAT_LIVE_SECRET` and `CHAT_COOKIE_SECRET`.

### `examples/direct` and `examples/ported` — the control experiment

The same app twice: `direct` uses Datastar with no engine at all, naming a selector and
pushing to it; `ported` is that app with only the engine swapped for `watch`.

```
clojure -M:direct -m direct.server           # http://localhost:3001
```

This pair exists because comparing `chat` against `direct` compared two apps written
days apart with different ideas in mind. Like-for-like:

| | lines |
|---|---|
| `direct`: `views.clj` + `live.clj` | 177 |
| `ported`: `view.clj` | 142 |

The port also removes 4 push functions, their 7 call sites, and all of `live.clj` — a
101-line connection registry doing open, close, touch, fan-out and drop-on-failed-write.
That was infrastructure rather than application, and it is the clearest thing the library
supplies.

### `examples/dash` — the one the numbers come from

A build dashboard: one writer, many readers, with sibling fragments updating at rates
spanning 100× (a summary every few seconds, a job row a few times a second, a log tail
ten times a second).

```
clojure -M:dash -m dash.server               # http://localhost:3004
examples/dash/soak.py 1000 60                # fan-out soak
```

Chat cannot test what the concurrency goal is about: it is many-to-many with a small N,
and every update follows a user action. This is one-to-many with no user input at all.
Under soak it delivered 978,430 patches to 2,000 connections with none going silent, and
about a third of published hints never reached a given connection because it had not
expanded that job's log — the dependency set being data rather than a declaration.

### `examples/watchspike` — the minimal reproduction

A roster only, on bare Jetty with no Zodiac and no extension.

```
clojure -M:watchspike -m watchspike.server   # http://localhost:3002
```

Kept because when something breaks, it is useful to have a version where a failure
cannot be the wiring. Its docstring records two findings that cost real time: Jetty must
run in async mode or an SSE response is torn down when `on-open` returns, and a
`<script type="module">` is deferred, so a function it defines does not exist when
Datastar evaluates `data-init`.

## Related

[Darkstar](https://github.com/brettatoms/darkstar) — the library: `watch`, connections,
diagnostics, and notes on what this model suits and does not.

## Status

New and unproven in production. Browser-verified end to end.

## License

MIT. See [LICENSE](LICENSE).
