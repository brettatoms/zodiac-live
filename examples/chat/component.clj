(ns chat.component
  "The live chat component.

  Exercises most of the design at once: keyed collections (messages), tiers
  (`:recoverable` draft, `:derived` pagination state), PubSub (new messages and
  typing), boundaries (message list vs typing line vs composer), and the recovery
  snapshot.

  ## The server cannot read the messages

  Bodies are ciphertext and the key lives in the URL fragment, which browsers never
  transmit. So `:render` emits message *envelopes* — id, author, timestamp, and the
  ciphertext in data attributes — and a small client hook decrypts and fills in the
  text. Keyed diffing still works normally, because it operates on envelopes; only
  the visible characters are client-side.

  That is honest about the boundary rather than pretending the server knows more
  than it does. It also means the id scheme of  keeps working: the element the
  client writes into is the element the server addresses."
  (:require [clojure.string :as str]
            [chat.db :as db]
            [chat.typing :as typing]
            [remuda.engine :as engine]
            [darkstar.action :as action]
            [remuda.render :as render]
            [remuda.source :as source]))

(defn- msg-id [id n] (str id "-m-" n))

;;; ==========================================================================
;;; Render
;;; ==========================================================================

(defn- message-item
  [id {:keys [author body iv] :as m}]
  (render/boundary
   [:messages (:id m)]
   [:li {:id (msg-id id (:id m))
         :class "msg"
         ;; Ciphertext and IV ride as data attributes for the client to decrypt.
         ;; The server never holds the plaintext, so it cannot render it.
         :data-ct body
         :data-iv iv}
    [:span {:class "author"} author ": "]
    ;; Filled in by the client. Deliberately not a placeholder string: an empty
    ;; node avoids a flash of "decrypting..." on every patch.
    [:span {:class "body"}]]))

(defn- render-chat
  [{:keys [messages typists draft channel-name more-count]
    ::engine/keys [id]}]
  (render/boundary
   []
   [:div {:id id :class "chat"}
    [:h2 channel-name]

    (render/boundary
     [:more-count]
     [:div {:id (str id "-more")}
      (when (pos? (or more-count 0))
        [:button {:data-on:click (action/post "/live/act" :older)}
         (str "load " (min 50 more-count) " older")])])

    (render/boundary
     [:messages]
     [:ul {:id (str id "-messages") :class "messages"}
      (for [m messages] (message-item id m))])

    ;; Typing indicator. A separate boundary so a keystroke elsewhere patches only
    ;; this line rather than the message list.
    (render/boundary
     [:typists]
     [:div {:id (str id "-typing") :class "typing"}
      (when (seq typists)
        (str (str/join ", " typists)
             (if (= 1 (count typists)) " is" " are")
             " typing..."))])

    (render/boundary
     [:draft]
     [:form {:id (str id "-composer")
             :data-on:submit "evt.preventDefault(); window.chatSend()"}
      [:input {:id (str id "-input")
               :name "draft"
               :autocomplete "off"
               :placeholder "Message"
               :value draft
               ;; No data-bind: it would overwrite a server-restored value with the
               ;; signal's empty one on load, which is how a recovered draft gets
               ;; clobbered. The handler reads evt.target.value instead.
               :data-on:input "window.chatTyping(evt.target.value)"}]
      [:button {:type "submit"} "Send"]])]))

;;; ==========================================================================
;;; The component
;;; ==========================================================================

(def chat
  {;; Only exceptions are declared; everything else is :sourced by default.
   :state
   {;; A half-typed message has no row to rebuild from, so it rides the recovery
    ;; snapshot and survives a deploy.
    :draft {:tier :recoverable}
    ;; Pagination cursor: a function of the loaded messages, so recomputing it is
    ;; correct and storing it would let it drift. Also keeps it out of the diff.
    :oldest {:tier :derived
             :from (fn [{:keys [messages]}] (db/oldest-id messages))}}

   :mount
   (fn [{:keys [source params before]}]
     (let [{:keys [channel-id username]} params
           ch (source/fetch source [:channel channel-id])
           msgs (source/fetch source [:messages channel-id before])
           oldest (db/oldest-id msgs)]
       {:channel-name (or (:name ch) "unknown channel")
        :messages (with-meta (vec msgs) {:live/key :id})
        :typists (typing/typists channel-id username)
        :more-count (source/fetch source [:count-before channel-id oldest])
        :draft ""}))

   :on
   {;; Typing: update ephemeral state and let the flush loop push to everyone.
    ;; The draft itself is kept in the view so it survives a reconnect.
    :typing
    (fn [view _ctx {:keys [text]}]
      (assoc view :draft (or text "")))

    ;; Scroll-up pagination. Prepends the previous page rather than replacing, so
    ;; the keyed diff sees inserts and the reader's scroll position is not lost.
    :older
    (fn [{:keys [messages] :as view} {:keys [source params]} _args]
      (let [before (db/oldest-id messages)
            older (source/fetch source [:messages (:channel-id params) before])
            combined (into (vec older) messages)
            oldest (db/oldest-id combined)]
        (assoc view
               :messages (with-meta combined {:live/key :id})
               :more-count (source/fetch source
                                         [:count-before (:channel-id params) oldest]))))}

   ;; Two topics: one for the channel's messages, one for its typing state. Narrow
   ;; topics matter — a keystroke must not invalidate the message list.
   :subscribe
   (fn [{:keys [params]}]
     [[:channel (:channel-id params)]
      [:typing (:channel-id params)]])

   :render render-chat})
