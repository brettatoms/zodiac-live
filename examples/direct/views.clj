(ns direct.views
  "Hiccup. Ordinary functions, composed by calling them.

  Every id used as a patch target is built by one of the `*-id` functions below, so
  the selector a handler pushes to and the element a view renders are the same
  string by construction. In the framework version this correspondence was the job
  of boundary extraction, and it was where most of the bugs lived — a boundary
  wrapping a live child had no id, so the patcher derived one that existed nowhere
  in the DOM.

  Here there is nothing to derive. A target is `(member-id \"alice\")` in both
  places."
  (:require [clojure.string :as str]))

;;; ==========================================================================
;;; Ids — the only naming discipline this app needs
;;; ==========================================================================

(defn messages-id [] "messages")
(defn message-id [n] (str "m-" n))
(defn typing-id [] "typing")
(defn roster-id [] "roster-list")
(defn roster-count-id [] "roster-count")
(defn member-id [username] (str "member-" (str/replace username #"[^A-Za-z0-9_-]" "_")))

;;; ==========================================================================
;;; Fragments
;;; ==========================================================================

(defn message
  "One message envelope.

  The body is empty: the server holds ciphertext it cannot read, so a client hook
  decrypts and fills it in. Same design as the framework version — that part was
  never the problem."
  [{:keys [id author body iv]}]
  [:li {:id (message-id id) :class "msg"}
   [:span {:class "author"} author ": "]
   [:span {:class "body" :data-ct body :data-iv iv}]])

(defn member
  "One roster row. `online?` decides the dot."
  [username online?]
  [:li {:id (member-id username)
        :class (str "member " (if online? "online" "offline"))}
   [:span {:class "dot"} (if online? "●" "○")]
   [:span {:class "name"} username]])

(defn roster-count
  "The badge. Its own function because it is pushed on its own: a roster insert
  patches the list, and the count sits outside it."
  [n]
  [:span {:id (roster-count-id) :class "roster__count"} n])

(defn roster
  [members online]
  [:ul {:id (roster-id) :class "roster__list"}
   (for [m members] (member m (contains? (set online) m)))])

(defn typing-line
  "Who is typing, excluding `self`."
  [typists]
  [:div {:id (typing-id) :class "typing"}
   (when (seq typists)
     (str (str/join ", " typists)
          (if (= 1 (count typists)) " is" " are") " typing…"))])

(defn chat
  "The whole panel. Rendered once at connect; after that only fragments are pushed."
  [{:keys [channel-name messages members online typists more-count]}]
  [:div {:id "chat" :class "chat"}
   [:header {:class "chat__header"}
    [:h2 {:class "chat__title"} channel-name]]
   [:div {:class "chat__body"}
    [:main {:class "chat__main"}
     [:div {:id "older" :class "chat__older"}
      (when (pos? (or more-count 0))
        [:button {:class "btn btn--ghost"
                  :data-on:click "@post('/d/older')"}
         (str "Load " (min 50 more-count) " older")])]
     [:ul {:id (messages-id) :class "messages"}
      (for [m messages] (message m))]
     (typing-line typists)
     [:form {:id "composer" :class "composer"
             :data-on:submit "evt.preventDefault(); window.dSend()"}
      [:input {:id "draft" :class "composer__input" :name "draft"
               :autocomplete "off" :placeholder "Message"
               ;; No server round-trip for the draft itself. The framework version
               ;; kept it in the view so it could survive a deploy, and that is what
               ;; made the composer clobber itself on every keystroke until the field
               ;; was excluded from diffing. Here the input simply owns its value.
               :data-on:input "window.dTyping(evt.target.value)"}]
      [:button {:type "submit" :class "btn btn--primary"} "Send"]]]
    [:aside {:class "roster"}
     [:h3 {:class "roster__title"}
      "Members"
      (roster-count (count members))]
     (roster members online)]]])
