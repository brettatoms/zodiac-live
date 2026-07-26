(ns chat.member-watch
  "The member sidebar row, rewritten with `remuda.watch`.

  Kept beside `chat.member` rather than replacing it, so the two authoring styles can
  be compared on the page.

  ## What changed

  `chat.member` says the same thing three times and keeps them consistent by hand:

      :mount     (fn [{:keys [params]}]            ; depends on presence…
                   {:online? (presence/online? channel-id username)})
      :subscribe (fn [{:keys [params]}]            ; …so watch this topic…
                   [[:presence channel-id username]])
      :render    (fn [view] (render/boundary [] …)) ; …and patch this region

  Here there is one function, and the dependency appears once — at the point of use:

      (watch/watch [:presence channel-id username]
                   #(presence/online? channel-id username))

  The subscription set is now whatever the render read, so it cannot disagree with
  the render. Every live-children bug in this project was one of those three
  declarations drifting from the others, and there is now only one site to change.

  ## What is still the author's job

  The region id and the element's `:id` must match. `watch/region` cannot check that,
  because only the author knows the markup — so both come from `member-id` below,
  which is the whole discipline. That is the same rule the direct (no-engine) version
  follows, and for the same reason: a target you *name* cannot drift from the element,
  while a target you *derive* can."
  (:require [remuda.watch :as watch]
            [chat.presence :as presence]))

(defn member-id
  "The row's DOM id, used by both the render and the region.

  One function, two call sites — which is what makes the patch target and the
  rendered element the same string by construction."
  [username]
  (str "member-" username))

(defn row
  "One roster row. A plain function: no `:mount`, no `:subscribe`, no view map.

  Callable outside an engine, because `watch` outside a recording is just a read —
  so this is testable at a REPL with nothing else running."
  [channel-id username]
  (watch/region
   (member-id username)
   (fn []
     (let [online? (watch/watch [:presence channel-id username]
                                #(presence/online? channel-id username))]
       [:li {:id (member-id username)
             :class (str "member " (if online? "online" "offline"))}
        [:span {:class "dot"} (if online? "●" "○")]
        [:span {:class "name"} username]]))))

(defn roster
  "The whole sidebar.

  `mapv`, not `for`: a lazy seq escapes the recording binding, so the rows' topics
  would never be recorded and nothing would ever re-render them — while the first
  render looked correct. `watch/render-recording` throws on an unrealised seq rather
  than letting that pass, but writing it correctly is better than relying on the
  guard."
  [channel-id members]
  (watch/region
   "roster"
   (fn []
     [:aside {:class "roster"}
      [:h3 {:class "roster__title"}
       "Members"
       (watch/region "roster-count"
                     (fn [] [:span {:id "roster-count" :class "roster__count"}
                             (count members)]))]
      [:ul {:id "roster" :class "roster__list"}
       (mapv #(row channel-id %) members)]])))
