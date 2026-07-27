(ns dash.jobs
  "A fake build queue: the state a dashboard watches.

  ## Why a build dashboard rather than another chat

  Chat is N-to-N with a small N per channel, and every update is triggered by a user
  action. A dashboard is **1 writer, N readers** — one build fanning out to every
  watcher — which is the shape that actually stresses fan-out, and the shape the
  concurrency goal is about. It also has no user input at all, so it cannot
  accidentally lean on the DOM owning state the way a chat composer does.

  The property that makes it a good test of `watch` specifically is that sibling
  fragments update at rates differing by ~100x:

  | fragment | changes |
  |---|---|
  | queue summary | when a job starts or finishes — every few seconds |
  | one job's row  | on status and progress — a few times a second |
  | one job's log  | on every line — ~10 a second |

  A design that re-renders too widely wastes most of its work on the fast fragment;
  one that prunes wrongly makes the slow fragments stale. Neither shows up in a chat
  app, where everything changes at roughly the speed someone types.

  ## What it actually showed

  Verified in a browser with one connection watching, over ~30 seconds:

      hints=1713  refreshes=1087  patches=1007  topics-subscribed=19

  So roughly a third of the hints the server published never reached the connection at
  all: it was not subscribed to the log topics of jobs it had not expanded. That is the
  dependency set being *data* rather than a declaration — the fastest-moving state on
  the server costs a viewer nothing until they ask for it.

  With a log expanded, its tail changed on 11 of 11 samples over 6 seconds while the
  job list and summary stayed put. The rate spread is real and the narrow patching
  holds.

  ## Under fan-out

  Soaked with hundreds of real SSE readers, counting bytes per connection so that a
  connection which opened but received nothing could not be mistaken for a working one:

  | connections | events delivered | MB | server CPU | silent |
  |---|---|---|---|---|
  | 500 (60s) | 478,000 | 219 | 48% | 0 |
  | 1,000 (60s) | 505,992 | 227 | 102% | 0 |
  | 2,000 (45s) | 237,883 | 102 | 102% | 0 |

  Every connection stayed alive and receiving at every level, and the per-sample
  progression was linear with no degradation. But throughput did not improve past
  ~1,000 connections and fell at 2,000, because fan-out ran serially on the
  simulation's single scheduler thread.

  ## After parallelising the fan-out

  | connections | serial | parallel | server CPU |
  |---|---|---|---|
  | 1,000 (60s) | 505,992 events | **837,952** events | 102% -> 32% |
  | 2,000 | 237,883 events | **978,430** patches delivered | 102% -> ~26% |

  At 2,000 the serial version had gone *backwards*; the parallel one delivers about
  four times as much. Every connection stayed alive and none went silent at either
  level.

  One cost showed up only at 2,000: opening all of them took **305 seconds**, because
  each mount competes with fan-out already in flight. Steady-state throughput was
  unaffected, but a thundering herd of reconnects after a deploy would be slow — which
  is a real operational property this app is now the only place to observe.

  1.66x the throughput at a third of the CPU. Two changes, and the second mattered
  more:

  1. Fan-out moved to a work-stealing pool, so renders and socket writes for different
     connections proceed in parallel.
  2. The subscription index became per connection. It had been a single atom holding
     `topic -> #{conn-id}`, rebuilt wholesale on every refresh — O(topics) work per
     connection, serialized by CAS retries. Adding threads to that made it *worse*, and
     the CPU drop from 102% to 32% is mostly this rather than the pool: the serial
     version was burning a core on map rebuilds and lock contention, not on rendering.

  The instrumentation had the same problem and needed `LongAdder` instead of an atom,
  or it would have measured itself.

  This is the first fan-out measurement in the project. Every earlier concurrency
  number (44,836 connections at 3.6 KB each) was IDLE connections: they mounted once
  and then nothing was pushed.

  ## Everything here is a plain atom

  No SQLite, deliberately. The chat example already proves the `Source` path, and a
  database here would add a variable without testing anything new — build state is
  ephemeral and genuinely server-owned, which is the point."
  (:import [java.util.concurrent Executors TimeUnit]))

(def ^:private log-cap
  "Lines kept per job. A tail, not a transcript: an unbounded log would make this a
  memory test rather than a fan-out test."
  40)

(defonce ^{:doc "job-id -> {:id :name :status :progress :started-at :finished-at}"}
  jobs
  (atom {}))

(defonce ^{:doc "job-id -> vector of log lines, newest last."}
  logs
  (atom {}))

(defonce ^{:doc "Set by `start!`, so the sweeper can publish without importing the
  extension. Called with one topic."}
  publish-fn
  (atom (fn [_topic] nil)))

(defn- publish! [topic] (@publish-fn topic))

;;; ==========================================================================
;;; Reads — what fragments watch
;;; ==========================================================================
;;; **A read must return an `identical?` value when nothing changed, or pruning cannot
;;; work.** `watch` compares reads with `identical?` first and falls back to `=` only
;;; for scalars, so a reader that rebuilds a collection on every call always compares
;;; as changed. That is safe but wasteful — and it silently removes the optimisation
;;; that makes a speculative hint cheap.
;;;
;;; Measured here before it was fixed: `all-jobs` sorted into a fresh vector and
;;; `summary` built a fresh map per call, so both compared as changed on every tick and
;;; the summary re-rendered ten times a second instead of every few seconds. Only
;;; `job-log`, which returns the stored vector directly, pruned correctly.
;;;
;;; So derived values are computed ONCE at write time and cached. The cost is a little
;;; bookkeeping in the writers; the benefit is that the slow fragments are actually
;;; slow.

(defonce ^{:doc "Cached derived reads, recomputed only when their inputs change."}
  derived
  (atom {:all-jobs [] :summary {:total 0 :running 0 :done 0 :failed 0}}))

(defn- recompute-derived!
  "Rebuilds the cached views of `jobs`. Called only from writers that changed them."
  []
  (let [js (vals @jobs)]
    (reset! derived
            {:all-jobs (vec (sort-by :id > js))
             :summary {:total (count js)
                       :running (count (filter (comp #{:running} :status) js))
                       :done (count (filter (comp #{:done} :status) js))
                       :failed (count (filter (comp #{:failed} :status) js))}})))

(defn all-jobs
  "Every job, newest first. The cached vector, so an unchanged read is `identical?`."
  []
  (:all-jobs @derived))

(defn job [id] (get @jobs id))

(defn job-log [id] (get @logs id []))

(defn summary
  "Counts by status. Cached for the same reason as `all-jobs`."
  []
  (:summary @derived))

;;; ==========================================================================
;;; Writes — the simulated build
;;; ==========================================================================

(def ^:private steps
  ["resolving dependencies" "compiling core" "compiling ui" "running unit tests"
   "running integration tests" "building image" "pushing artifact"])

(defn- next-id [] (inc (reduce max 0 (keys @jobs))))

(defn enqueue!
  "Adds a queued job and returns its id."
  [name]
  (let [id (next-id)]
    (swap! jobs assoc id {:id id :name name :status :queued :progress 0})
    (swap! logs assoc id [])
    (recompute-derived!)
    ;; Two topics, because two different fragments care: the list gained a row, and
    ;; the summary counts changed. Publishing one and expecting the other to follow is
    ;; the mistake that made a roster stale in the chat app.
    (publish! [:jobs])
    (publish! [:summary])
    id))

(defn- retire-finished!
  "Drops all but the newest `keep` finished jobs, so the list has a bounded size."
  [keep]
  (let [finished (->> @jobs vals
                      (filter (comp #{:done :failed} :status))
                      (sort-by :id >))
        doomed (map :id (drop keep finished))]
    (when (seq doomed)
      (swap! jobs #(apply dissoc % doomed))
      (swap! logs #(apply dissoc % doomed))
      (recompute-derived!)
      (publish! [:jobs])
      (publish! [:summary]))))

(defn- log-line! [id line]
  (swap! logs update id
         (fn [ls] (vec (take-last log-cap (conj (or ls []) line)))))
  (publish! [:log id]))

(defn- advance!
  "One tick of one job. Returns true while the job is still running."
  [id]
  (when-let [j (get @jobs id)]
    (case (:status j)
      :queued
      (do (swap! jobs assoc-in [id :status] :running)
          (swap! jobs assoc-in [id :started-at] (System/currentTimeMillis))
          (recompute-derived!)
          (log-line! id (str "starting " (:name j)))
          (publish! [:job id])
          (publish! [:summary])
          true)

      :running
      (let [p (min 100 (+ (:progress j) 1 (rand-int 3)))]
        (swap! jobs assoc-in [id :progress] p)
        ;; NOT recomputed here. Progress does not appear in `all-jobs`'s ordering or in
        ;; `summary`'s counts, so refreshing the cache on every tick would make both
        ;; compare as changed and defeat the point.
        ;; A log line per tick: this is the fast fragment, ~10/sec.
        (log-line! id (str "[" p "%] " (rand-nth steps)))
        ;; And the row, which changes at the same rate but renders far less.
        (publish! [:job id])
        (if (< p 100)
          true
          (let [failed? (< (rand) 0.15)]
            (swap! jobs update id merge
                   {:status (if failed? :failed :done)
                    :finished-at (System/currentTimeMillis)})
            (recompute-derived!)
            (log-line! id (if failed? "FAILED" "done"))
            (publish! [:job id])
            (publish! [:summary])
            false)))

      false)))

(defonce ^:private ticker (atom nil))

(defn start!
  "Starts the simulation. `publish` is called with one topic.

  Ticks every 100ms, so a running job emits ~10 log lines a second — the fast
  fragment. Jobs are enqueued far less often, which is what produces the rate spread
  this app exists to exercise."
  [publish]
  (reset! publish-fn publish)
  (when-not @ticker
    (let [ex (Executors/newSingleThreadScheduledExecutor)]
      (reset! ticker ex)
      (.scheduleAtFixedRate
       ex
       (fn []
         (try
           ;; Advance every running or queued job.
           (doseq [id (keys @jobs)] (advance! id))
           ;; And occasionally add one, so the list and summary change too.
           ;; Keep one or two jobs live at all times, and retire the oldest finished
           ;; ones so the list stays a fixed size.
           ;;
           ;; An earlier version capped on TOTAL jobs, which starved the simulation
           ;; permanently: once twelve had accumulated the condition could never be
           ;; true again, so the dashboard sat with nothing running and every log
           ;; frozen. It looked exactly like a fan-out bug.
           (let [live (count (filter (comp #{:queued :running} :status) (vals @jobs)))]
             (when (and (< live 2) (< (rand) 0.05))
               (retire-finished! 8)
               (enqueue! (rand-nth ["web" "api" "worker" "scheduler" "docs"]))))
           (catch Throwable e (println "tick failed" (ex-message e)))))
       200 100 TimeUnit/MILLISECONDS))))

(defn stop! []
  (when-let [ex @ticker]
    (.shutdownNow ex)
    (reset! ticker nil)))

(defn reset-all! []
  (reset! jobs {})
  (reset! logs {})
  (recompute-derived!))
