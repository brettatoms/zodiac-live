(ns chat.view
  "The chat channel, written with `darkstar.watch`.

  Replaces `chat.component`, which used the `:mount`/`:subscribe`/`:render` triple.
  The whole component is functions returning hiccup; a dependency is declared by
  reading it, once, at the point of use.

  ## What went away

  - **`:mount`.** Every fetch it did is now a `watch` inside the fragment that needs
    it. Nothing assembles a view map, so nothing can hold a field the render does not
    use, and nothing needs a tier to say how that field is rebuilt.
  - **`:subscribe`.** The topic list is whatever the render read. It cannot disagree
    with the render, which is what the old triple could not guarantee.
  - **`render/boundary` and view paths.** A fragment names its own id, and that id is
    the patch target. There is no path to resolve, so there is no path-to-id
    translation to drift — the defect behind five of seven live-children bugs.
  - **`:state` tiers.** No view means no fields to classify. `:draft` is gone
    entirely: the input owns its value (see below).
  - **live children.** A member row is `(member-row channel-id username)`, a function
    call. No child registry, no keyed reconciliation, no `:on-children` hook.

  ## Two things to keep in mind while reading

  **`mapv`, never `for`.** A lazy seq escapes the recording binding, so its `watch`
  calls are never seen and its fragments never update — while the first render looks
  perfect. `watch/render-recording` throws on an unrealised seq, but writing it right
  is better than relying on the guard.

  **Per-connection UI state is a topic keyed by connection id.** Pagination is the
  case: `[:pages conn-id]`. That is how state with no row in the database lives here,
  and it is why `:older` needs no view to write into.

  ## The draft is not here at all

  The old version kept `:draft` in the view so it could ride a recovery snapshot, and
  then needed `:diff? false` to stop the server echoing a one-keystroke-stale value
  back into the field being typed in. The input simply owns its value now. Verified
  in a browser: a half-typed value survives a full JVM restart, because the DOM held
  it and the server never touched it."
  (:require [clojure.string :as str]
            [chat.db :as db]
            [chat.presence :as presence]
            [chat.typing :as typing]
            [darkstar.action :as action]
            [darkstar.source :as source]
            [darkstar.watch :as w :refer [fragment watch]]))

;;; ==========================================================================
;;; Ids — the only naming discipline this component needs
;;; ==========================================================================
;;; A fragment id IS the patch selector, so these are called from both the fragment
;;; and the element. `watch/fragment` verifies the two match rather than deriving one
;;; from the other: a derived target is what drifted before.

(defn member-id [username] (str "member-" username))
(defn message-id [id] (str "m-" id))

;;; ==========================================================================
;;; Per-connection UI state
;;; ==========================================================================
;;; State with no row in any table and no meaning to another viewer. It lives in a
;;; plain atom keyed by connection id, and is read through `watch` like anything else
;;; — so the topic carries the identity and a publish reaches exactly one connection.

(defonce ^{:doc "conn-id -> pages of older messages this viewer has opened."}
  pages
  (atom {}))

(defn page-count [conn-id] (get @pages conn-id 1))

(defn load-older! [conn-id] (swap! pages update conn-id (fnil inc 1)))

(defn forget-connection! [conn-id] (swap! pages dissoc conn-id))

;;; ==========================================================================
;;; Fragments
;;; ==========================================================================

(defn member-row
  "One roster row: a member and whether they are connected.

  The dependency on this member's presence is the `watch` call. There is nothing else
  to keep in step with it."
  [channel-id username]
  (fragment
   (member-id username)
   (fn []
     (let [online? (watch [:presence channel-id username]
                          #(presence/online? channel-id username))]
       [:li {:id (member-id username)
             :class (str "member " (if online? "online" "offline"))}
        [:span {:class "dot"}]
        [:span {:class "name"} username]]))))

(defn roster
  "The member sidebar.

  Reads the membership list *and*, through `member-row`, one presence topic per
  member. So this connection's subscription set grows and shrinks with the channel —
  which is why `refresh!` reports `:added`/`:removed` and the caller must act on them.
  Subscribing once at mount left a later joiner permanently green."
  [source channel-id]
  (fragment
   "roster"
   (fn []
     ;; `[:channel …]`, not `[:members …]`: that is the topic the server publishes
     ;; when the roster changes. A topic name invented here is a fragment that never
     ;; updates, and it fails silently — the first render is correct.
     (let [members (watch [:channel channel-id]
                          #(source/fetch source [:members channel-id]))]
       [:aside {:id "roster" :class "roster"}
        [:h3 {:class "roster__title"}
         "Members"
         [:span {:class "roster__count"} (count members)]]
        [:ul {:class "roster__list"}
         ;; mapv, not for. See the namespace docstring.
         (mapv #(member-row channel-id %) members)]]))))

(defn message
  "One message envelope.

  The body is deliberately empty: the server holds ciphertext it cannot read, and a
  client hook decrypts into this span."
  [{:keys [id author body iv]}]
  [:li {:id (message-id id) :class "msg"}
   [:span {:class "author"} author ": "]
   [:span {:class "body" :data-ct body :data-iv iv}]])

(defn messages
  "The message list, paginated by this connection's own page count."
  [source channel-id conn-id]
  (fragment
   "messages"
   (fn []
     (let [n (watch [:pages conn-id] #(page-count conn-id))
           msgs (watch [:channel channel-id]
                       #(source/fetch source [:messages channel-id nil]))
           shown (take-last (* 50 n) msgs)
           oldest (db/oldest-id shown)
           more (watch [:channel channel-id]
                       #(source/fetch source [:count-before channel-id oldest]))]
       [:div {:id "messages" :class "chat__main"}
        [:div {:class "chat__older"}
         (when (pos? (or more 0))
           [:button {:class "btn btn--ghost"
                     :data-on:click (action/post "/live/act" :older)}
            (str "Load " (min 50 more) " older")])]
        [:ul {:class "messages"} (mapv message shown)]]))))

(defn typing-line
  "Who is typing, excluding this viewer."
  [channel-id username]
  (fragment
   "typing"
   (fn []
     (let [typists (watch [:typing channel-id]
                          #(typing/typists channel-id username))]
       [:div {:id "typing" :class "typing"}
        (when (seq typists)
          (str (str/join ", " typists)
               (if (= 1 (count typists)) " is" " are") " typing…"))]))))

(defn composer
  "The message input.

  No `watch` and no server-held value: the input owns what is typed into it. A
  `data-on:input` reports *whether* someone is typing, never the text."
  []
  (fragment
   "composer"
   (fn []
     [:form {:id "composer" :class "composer"
             :data-on:submit "evt.preventDefault(); window.chatSend()"}
      [:input {:id "draft" :class "composer__input" :name "draft"
               :autocomplete "off" :placeholder "Message"
               :data-on:input (str (action/post "/live/act" :typing
                                                {:typing (action/raw "evt.target.value.length > 0")})
                                   "; window.chatTyping(evt.target.value)")}]
      [:button {:type "submit" :class "btn btn--primary"} "Send"]])))

;;; ==========================================================================
;;; The component
;;; ==========================================================================

(defn render
  "The whole channel. One function of params, and that is the component."
  [{:keys [source channel-id username conn-id channel-name]}]
  (fragment
   "live-root"
   (fn []
     [:div {:id "live-root" :class "chat"}
      [:header {:class "chat__header"}
       [:h2 {:class "chat__title"} channel-name]
       [:button {:class "btn btn--ghost"
                 :data-on:click "window.chatCopyInvite()"}
        "Copy invite link"]]
      [:div {:class "chat__body"}
       (messages source channel-id conn-id)
       (typing-line channel-id username)
       (composer)
       (roster source channel-id)]])))

(def component
  {:render render

   :on
   {;; Ephemeral, and interesting to everyone else in the channel. The handler
    ;; mutates the typing registry and names the topic; the caller publishes it.
    :typing
    (fn [{:keys [params]} {:keys [typing]}]
      (let [{:keys [channel-id username]} params]
        (if typing
          (typing/touch! channel-id username)
          (typing/clear! channel-id username))
        [[:typing channel-id]]))

    ;; Pagination: per-connection, so only this viewer's topic is published and only
    ;; this viewer's `#messages` is patched.
    :older
    (fn [{:keys [id]} _args]
      (load-older! id)
      [[:pages id]])}})
