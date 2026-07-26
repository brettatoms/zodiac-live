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
  than it does. It also keeps the id scheme working: the element the
  client writes into is the element the server addresses.

  ## The member sidebar is the only live child here

  Everything else — message list, typing line, composer — is a plain boundary,
  because they share this component's subscriptions and lifecycle. The sidebar rows
  are different: each subscribes to `[:presence channel-id username]` on its own, so
  one member connecting patches one row rather than the whole list. See
  `chat.member`."
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
  [{:keys [messages typists draft channel-name more-count members channel-id
           member-count]
    ::engine/keys [id]}]
  (render/boundary
   []
   [:div {:id id :class "chat"}
    [:header {:class "chat__header"}
     [:h2 {:class "chat__title"} channel-name]]

    [:div {:class "chat__body"}
     [:main {:class "chat__main"}
      (render/boundary
       [:more-count]
       [:div {:id (str id "-more") :class "chat__older"}
        (when (pos? (or more-count 0))
          [:button {:class "btn btn--ghost"
                    :data-on:click (action/post "/live/act" :older)}
           (str "Load " (min 50 more-count) " older")])])

      (render/boundary
       [:messages]
       [:ul {:id (str id "-messages") :class "messages"}
        (for [m messages] (message-item id m))])

      ;; Typing indicator. A separate boundary so a keystroke elsewhere patches
      ;; only this line rather than the message list.
      (render/boundary
       [:typists]
       [:div {:id (str id "-typing") :class "typing"}
        (when (seq typists)
          (str (str/join ", " typists)
               (if (= 1 (count typists)) " is" " are")
               " typing\u2026"))])

      (render/boundary
       [:draft]
       [:form {:id (str id "-composer") :class "composer"
               :data-on:submit "evt.preventDefault(); window.chatSend()"}
        [:input {:id (str id "-input")
                 :class "composer__input"
                 :name "draft"
                 :autocomplete "off"
                 :placeholder "Message"
                 :value draft
                 ;; No data-bind: it would overwrite a server-restored value with
                 ;; the signal's empty one on load, which is how a recovered draft
                 ;; gets clobbered. The handler reads evt.target.value instead.
                 :data-on:input
                 ;; Two calls, for two kinds of state.
                 ;;
                 ;; The darkstar action updates this component's :draft, which is
                 ;; :recoverable and so survives a deploy. It must go through
                 ;; datastar rather than a hand-rolled fetch: datastar sends the
                 ;; liveId signal with its own actions, and a plain fetch does not
                 ;; — which is why doing this by hand returned 409 with "no live
                 ;; context".
                 ;;
                 ;; window.chatTyping then pokes ephemeral server state so OTHER
                 ;; users see the indicator.
                 (str (action/post "/live/act" :typing
                                   {:text (action/raw "evt.target.value")})
                      "; window.chatTyping(evt.target.value)")}]
        [:button {:type "submit" :class "btn btn--primary"} "Send"]])]

     ;; The sidebar: one live child per member, keyed by username *within this
     ;; channel*. A username identifies a person only per channel, so the key
     ;; carries both and nothing joins them across channels.
     (render/boundary
      [:members]
      [:aside {:id (str id "-members") :class "roster"}
       [:h3 {:class "roster__title"}
        "Members"
        ;; Its own boundary. A keyed insert patches only the new `<li>`, so it never
        ;; re-renders this `<aside>` — the count would sit stale while the list beside
        ;; it grew. A separate boundary means the count is patched on its own whenever
        ;; the roster's size changes.
        (render/boundary
         [:member-count]
         [:span {:id (str id "-member-count") :class "roster__count"}
          member-count])]
       [:ul {:class "roster__list"}
        (for [{:keys [username]} members]
          ;; Each row is marked as its own boundary too, so the keyed insert has an
          ;; element to anchor against.
          (render/boundary
           [:members username]
           (render/child [:member username] :member
                         {:channel-id channel-id :username username})))]])]]))

;;; ==========================================================================
;;; The component
;;; ==========================================================================

(def chat
  {;; Only exceptions are declared; everything else is :sourced by default.
   :state
   {;; A half-typed message has no row to rebuild from, so it rides the recovery
    ;; snapshot and survives a deploy.
    ;; :diff? false is load-bearing, not an optimisation. The draft must be in the
    ;; view to ride the recovery snapshot, but diffing it re-rendered the composer
    ;; on every keystroke and patched the input with a value one keystroke stale —
    ;; the field visibly clobbered what was being typed into it. The client owns
    ;; this while connected; the view only remembers it for a rebuild.
    :draft {:tier :recoverable :diff? false}
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
       {:channel-id channel-id
        :channel-name (or (:name ch) "unknown channel")
        ;; The roster is DURABLE: everyone who has ever joined, whether connected
        ;; or not. Only the dot beside a name changes as they come and go, which is
        ;; handled by each row's own child subscribing to
        ;; `[:presence channel-id username]`.
        ;;
        ;; Deriving this from `presence/connected` instead made members vanish the
        ;; moment they disconnected — a roster that only lists who is online is a
        ;; presence list, not a roster.
        ;;
        ;; A KEYED collection, and that matters: as a plain vector, one person
        ;; joining changed the value and the diff reported `[:members]`, so the
        ;; whole `<aside>` re-rendered and every row was replaced. Keyed by
        ;; username, a join is an *insert* — one new `<li>`, siblings untouched.
        :members (->> (source/fetch source [:members channel-id])
                      (mapv (fn [u] {:username u}))
                      (#(with-meta % {:live/key :username})))
        ;; Plain state, NOT :derived. Derived fields are stripped before diffing —
        ;; that is what keeps bookkeeping like :next-id from widening patches — so a
        ;; value that is actually *displayed* must be diffable. It needs its own
        ;; field at all because a keyed insert patches one `<li>` and never
        ;; re-renders the wrapper the count sits in.
        :member-count (count (source/fetch source [:members channel-id]))
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
