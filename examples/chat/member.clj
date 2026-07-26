(ns chat.member
  "One sidebar row: a member of a channel and whether they are connected.

  This exists to be a **live child** — a component with its own boundary, state,
  lifecycle and subscription, mounted by the chat component rather than by a route.
  It is the smallest thing that genuinely needs to be one:

  - **own subscription.** A row subscribes to `[:presence channel-id username]`, so
    one member connecting patches one row. Subscribing the whole sidebar to
    `[:presence channel-id]` would re-render every row on every connect, which is
    what a plain boundary would give.
  - **own lifecycle.** Rows appear and disappear as members join and leave, so the
    keyed mount/unmount path is exercised rather than theoretical.

  Everything else in the chat app is deliberately *not* a child: the message list,
  typing indicator and composer are plain boundaries, because they share one
  subscription set and one lifecycle. That is the intended default.

  ## Identity is `[channel-id username]`

  A username identifies a person only within a channel. The params carry both, and
  nothing here accepts a bare username — see `chat.presence`."
  (:require [remuda.engine :as engine]
            [remuda.render :as render]
            [chat.presence :as presence]))

(def member
  {;; No :state declaration: every field is :sourced, so a rebuild re-reads
   ;; presence. There is nothing here a client could hold that the server cannot
   ;; recompute, which is what makes recovery for this child trivial.
   :mount
   (fn [{:keys [params]}]
     (let [{:keys [channel-id username]} params]
       {:username username
        :online? (presence/online? channel-id username)}))

   ;; The narrow topic is the whole reason this is a child.
   :subscribe
   (fn [{:keys [params]}]
     [[:presence (:channel-id params) (:username params)]])

   :render
   (fn [{:keys [username online?] ::engine/keys [id]}]
     (render/boundary
      []
      [:li {:id id :class (str "member " (if online? "online" "offline"))}
       [:span {:class "dot"} (if online? "●" "○")]
       [:span {:class "name"} username]]))})
