(ns ported.view
  "`examples/direct` ported to `darkstar.watch`, changing nothing but the engine.

  This exists to make one comparison honest. `examples/chat` and `examples/direct`
  were written days apart with different ideas in mind, so measuring them against each
  other measured the authors as much as the designs. Here the app is the same app:
  same `db.clj`, same `token.clj`, same features, same fake-free stack. Only the way
  updates reach the browser differs.

  ## What the direct version does that this does not

  `examples/direct` names a target and pushes to it. Five functions do that:

      push-message!   -> #messages, :append
      push-typing!    -> #typing, rendered per viewer
      push-member!    -> #member-<name>
      push-roster!    -> #roster-list AND #roster-count
      push-older!     -> #messages for one connection

  Seven call sites invoke them. Its own docstring names the cost: \"the author must
  remember to push after every mutation, and a forgotten push is a silently stale
  screen.\"

  Here nothing pushes. A fragment declares what it read, and the engine re-renders the
  ones whose reads changed. The five functions and their seven call sites become
  `publish!` calls naming a topic — and a topic is cheap to get right because it
  carries no target and no HTML.

  ## What it costs

  Two things, both real:

  - A topic name invented at the read site has no counterpart at the publish site to
    disagree with. Porting `examples/chat` I wrote `[:members id]` where the server
    published `[:channel id]`, and the fragment simply never updated — the first
    render was correct, and nothing errored. The direct version cannot have that bug,
    because a selector that matches nothing is visible in the browser console.
  - `mapv`, never `for`. A lazy seq escapes the recording binding.

  ## `live.clj` is gone

  The direct version's 101-line connection registry — open, close, touch, fan-out by
  channel, drop on failed write — is what `darkstar.live` plus the zodiac
  extension provide. That is the clearest win: it was infrastructure, not application."
  (:require [clojure.string :as str]
            [direct.db :as db]
            [darkstar.watch :as w :refer [fragment watch]]))

;;; ==========================================================================
;;; Ids
;;; ==========================================================================
;;; Kept as functions for the same reason `examples/direct` keeps them: a fragment id
;;; is a patch target, and calling one function from both the fragment and the element
;;; is what makes them the same string. `watch/fragment` then verifies it.

(defn messages-id [] "messages")
(defn message-id [n] (str "m-" n))
(defn typing-id [] "typing")
(defn roster-id [] "roster-list")
(defn member-id [username] (str "member-" (str/replace username #"[^A-Za-z0-9_-]" "_")))

;;; ==========================================================================
;;; Per-connection UI state
;;; ==========================================================================
;;; Pagination has no row in any table and no meaning to another viewer, so it lives
;;; in an atom keyed by connection id and is read through `watch` like anything else.
;;; The direct version held the same thing in its connection map.

(defonce ^{:doc "conn-id -> pages of older messages opened."} pages (atom {}))

(defn page-count [conn-id] (get @pages conn-id 1))
(defn load-older! [conn-id] (swap! pages update conn-id (fnil inc 1)))
(defn forget-connection! [conn-id] (swap! pages dissoc conn-id))

;;; ==========================================================================
;;; Fragments
;;; ==========================================================================

(defn message
  "One message envelope. The body is ciphertext a client hook decrypts."
  [{:keys [id author body iv]}]
  [:li {:id (message-id id) :class "msg"}
   [:span {:class "author"} author ": "]
   [:span {:class "body" :data-ct body :data-iv iv}]])

(defn member
  "One roster row.

  The direct version pushed this from `push-member!` after touching presence. Here the
  presence read *is* the subscription, so a member going offline patches this row and
  nothing else."
  [username online?]
  (fragment
   (member-id username)
   (fn []
     [:li {:id (member-id username)
           :class (str "member " (if online? "online" "offline"))}
      [:span {:class "dot"} (if online? "●" "○")]
      [:span {:class "name"} username]])))

(defn roster
  "The member sidebar.

  One fragment, where the direct version needed two pushes — `#roster-list` and
  `#roster-count` — because a keyed insert patched one `<li>` and never re-rendered
  the wrapper the count sat in. Re-rendering the whole fragment makes the count
  correct for free, and datastar's morph moves the surviving rows by id rather than
  recreating them."
  [{:keys [channel-id members-fn online-fn]}]
  (fragment
   (roster-id)
   (fn []
     (let [members (watch [:members channel-id] members-fn)
           online (set (watch [:presence channel-id] online-fn))]
       [:aside {:id (roster-id) :class "roster"}
        [:h3 {:class "roster__title"}
         "Members"
         [:span {:class "roster__count"} (count members)]]
        [:ul {:class "roster__list"}
         (mapv #(member % (contains? online %)) members)]]))))

(defn typing-line
  "Who is typing, excluding this viewer.

  Rendered per connection, which the direct version achieved by passing the connection
  map into its push function. Here each connection renders its own component, so
  `username` is simply a param."
  [{:keys [channel-id username typists-fn]}]
  (fragment
   (typing-id)
   (fn []
     (let [typists (watch [:typing channel-id] typists-fn)
           others (remove #{username} typists)]
       [:div {:id (typing-id) :class "typing"}
        (when (seq others)
          (str (str/join ", " others)
               (if (= 1 (count others)) " is" " are") " typing…"))]))))

(defn messages
  "The message list, paginated per connection."
  [{:keys [channel-id conn-id messages-fn count-before-fn]}]
  (fragment
   (messages-id)
   (fn []
     (let [n (watch [:pages conn-id] #(page-count conn-id))
           msgs (watch [:messages channel-id] messages-fn)
           shown (take-last (* 50 n) msgs)
           more (watch [:messages channel-id]
                       #(count-before-fn (db/oldest-id shown)))]
       [:div {:id (messages-id) :class "chat__main"}
        [:div {:class "chat__older"}
         (when (pos? (or more 0))
           [:button {:class "btn btn--ghost"
                     :data-on:click "@post('/p/older')"}
            (str "Load " (min 50 more) " older")])]
        [:ul {:class "messages"} (mapv message shown)]]))))

;;; ==========================================================================
;;; The component
;;; ==========================================================================

(defn render
  "The whole panel.

  Params carry *reader functions* rather than a db handle. That keeps this namespace
  free of `next.jdbc` and the sql extension, and it means the component is callable in
  a test with plain functions — the same property the direct version's `views.clj` has
  by being pure hiccup."
  [{:keys [channel-name] :as params}]
  (fragment
   "chat"
   (fn []
     [:div {:id "chat" :class "chat"}
      [:header {:class "chat__header"}
       [:h2 {:class "chat__title"} channel-name]]
      [:div {:class "chat__body"}
       (messages params)
       (typing-line params)
       [:form {:id "composer" :class "composer"
               :data-on:submit "evt.preventDefault(); window.dSend()"}
        [:input {:id "draft" :class "composer__input" :name "draft"
                 :autocomplete "off" :placeholder "Message"
                 :data-on:input "window.dTyping(evt.target.value)"}]
        [:button {:type "submit" :class "btn btn--primary"} "Send"]]
       (roster params)]])))

(def component
  {:render render
   :on {:older (fn [{:keys [id]} _] (load-older! id) [[:pages id]])}})
