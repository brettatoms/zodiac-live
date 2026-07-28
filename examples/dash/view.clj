(ns dash.view
  "The dashboard, in `darkstar.watch`.

  ## What this is testing

  Sibling fragments whose update rates differ by ~100x. The summary changes every few
  seconds, a job row a few times a second, a job's log ten times a second. If pruning
  or fragment selection is wrong, the symptom is either wasted renders on the fast
  fragment or staleness on the slow ones — and neither shows up in a chat app, where
  everything moves at typing speed.

  It also has **no user input**. Every update is server-originated, so nothing here can
  quietly depend on the DOM owning state the way a chat composer does. That makes it a
  cleaner test of \"state lives on the server\" than the chat example, which had to be
  argued into leaving `:draft` in the browser.

  ## One expanded job at a time, per connection

  Clicking a job expands its log. Which job is expanded is **per connection** — it has
  no meaning to another viewer and no row in any table — so it lives in an atom keyed
  by connection id and is read through `watch` like anything else. That is the same
  shape as pagination in the chat app, and it is how this design holds UI state without
  a view map."
  (:require [clojure.string :as str]
            [dash.jobs :as jobs]
            [darkstar.watch :refer [fragment watch]]))

;;; ==========================================================================
;;; Ids
;;; ==========================================================================

(defn summary-id [] "summary")
(defn job-list-id [] "job-list")
(defn job-id [id] (str "job-" id))
(defn log-id [id] (str "log-" id))

;;; ==========================================================================
;;; Per-connection UI state
;;; ==========================================================================

(defonce ^{:doc "conn-id -> the job id whose log is expanded, or nil."}
  expanded
  (atom {}))

(defn expanded-job [conn-id] (get @expanded conn-id))

(defn toggle-expanded!
  "Expands `id`, or collapses it if it was already expanded. Returns the topic to
  publish — this connection's, and nobody else's."
  [conn-id id]
  (swap! expanded update conn-id #(when (not= % id) id))
  [[:expanded conn-id]])

(defn forget-connection! [conn-id] (swap! expanded dissoc conn-id))

;;; ==========================================================================
;;; Fragments
;;; ==========================================================================

(defn summary
  "Counts by status. The SLOW fragment: changes only when a job starts or finishes."
  []
  (fragment
   (summary-id)
   (fn []
     (let [{:keys [total running done failed]} (watch [:summary] jobs/summary)]
       [:header {:id (summary-id) :class "summary"}
        [:h1 "Build queue"]
        [:div {:class "stats"}
         [:span {:class "stat"} [:b total] " total"]
         [:span {:class "stat stat--running"} [:b running] " running"]
         [:span {:class "stat stat--done"} [:b done] " done"]
         [:span {:class "stat stat--failed"} [:b failed] " failed"]]]))))

(defn job-log
  "One job's log tail. The FAST fragment: a new line ~10 times a second.

  Only rendered when expanded, which is what makes the rate spread observable: a
  connection with nothing expanded does not subscribe to any `[:log id]` topic at all,
  so the fastest-moving data on the server costs it nothing. That is the dependency set
  being *data* rather than a declaration."
  [id]
  (fragment
   (log-id id)
   (fn []
     (let [lines (watch [:log id] #(jobs/job-log id))]
       [:pre {:id (log-id id) :class "log"}
        (str/join "\n" lines)]))))

(defn job-row
  "One job. Changes a few times a second while running.

  Takes only the **id**, and reads the job itself through `watch`. Passing the job map
  in from the list would have been simpler and wrong: the row would subscribe to
  nothing, and progress — which changes ten times a second — would be frozen at
  whatever the list's cached snapshot held. The list deliberately does not recompute on
  progress, so the row has to own that dependency.

  Two topics with very different rates in one fragment: `[:job id]` moves constantly,
  `[:expanded conn-id]` only on a click."
  [conn-id id]
  (fragment
   (job-id id)
   (fn []
     (let [{:keys [name status progress]} (watch [:job id] #(jobs/job id))
           open? (= id (watch [:expanded conn-id] #(expanded-job conn-id)))]
       [:li {:id (job-id id) :class (str "job job--" (clojure.core/name status))}
        [:div {:class "job__head"
               ;; The only interaction in the app. `evt` carries nothing useful, so the
               ;; job id travels in the payload.
               ;; `{payload: {...}}`, not a bare map. The second argument to `@post`
               ;; is an OPTIONS map, so `{job: 1}` is silently ignored and datastar
               ;; sends its signal set instead — which here is `{}`. That produced a
               ;; 409 with no parse error and no clue, and `curl` could not reproduce
               ;; it because curl sent the body the server expected.
               ;;
               ;; This is exactly what `darkstar.action/post` exists to get right; it
               ;; is spelled out by hand here only because this app deliberately has no
               ;; darkstar dependency.
               :data-on:click (str "@post('/d/toggle', {payload: {job: " id
                                   ", id: window.__id}})")}
         [:span {:class "job__name"} name]
         [:span {:class "job__status"} (clojure.core/name status)]
         [:div {:class "job__bar"}
          [:div {:class "job__fill" :style (str "width:" progress "%")}]]
         [:span {:class "job__pct"} progress "%"]]
        (when open? (job-log id))]))))

(defn job-list
  "Every job. Changes when one is enqueued or finishes.

  `mapv`, not `for`: a lazy seq escapes the recording binding and the rows' topics are
  never recorded — the first render looks right and nothing ever updates."
  [conn-id]
  (fragment
   (job-list-id)
   (fn []
     ;; Only the IDS are read here. The list changes when a job is added or finishes;
     ;; each row watches its own job for everything else.
     (let [js (watch [:jobs] jobs/all-jobs)]
       [:ul {:id (job-list-id) :class "jobs"}
        (mapv #(job-row conn-id (:id %)) js)]))))

;;; ==========================================================================
;;; The component
;;; ==========================================================================

(defn render
  [{:keys [conn-id]}]
  (fragment
   "dash"
   (fn []
     [:div {:id "dash" :class "dash"}
      (summary)
      (job-list conn-id)])))

(def component
  {:render render
   :on {:toggle (fn [{:keys [id]} {:keys [job]}]
                  (toggle-expanded! id (if (string? job) (parse-long job) job)))}})
