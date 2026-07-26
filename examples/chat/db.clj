(ns chat.db
  "Storage and the `Source` implementation for the chat example.

  Uses `zodiac-sql` (HoneySQL over next.jdbc) rather than hand-rolled SQL, since
  that is the idiomatic zodiac choice and it composes with the live extension —
  both are just extensions transforming the same integrant config.

  Two tables. Note what the server **cannot** see: message bodies are ciphertext
  and the key never reaches the server, since it lives in the URL fragment which
  browsers do not transmit. So `messages.body` is opaque to every query here.

  Usernames are stored in the clear, which the brief allows."
  (:require [zodiac.ext.sql :as z.sql]
            [remuda.source :as source]))

(def page-size 50)

;;; ==========================================================================
;;; Schema
;;; ==========================================================================

(defn migrate!
  [db]
  (z.sql/execute! db ["create table if not exists channels (
                         id text primary key,
                         name text not null,
                         created_at integer not null)"])
  (z.sql/execute! db ["create table if not exists messages (
                         id integer primary key autoincrement,
                         channel_id text not null,
                         author text not null,
                         body text not null,
                         iv text not null,
                         created_at integer not null)"])
  (z.sql/execute! db ["create index if not exists idx_msg_channel
                         on messages(channel_id, id)"])
  nil)

;;; ==========================================================================
;;; Writes
;;; ==========================================================================

(defn create-channel!
  "`id` is a client-generated random identifier. It is **not** the encryption key
  and not derived from it — the key never leaves the browser. Its only job is to
  say which channel a row belongs to, so a joiner with the link can find history."
  [db id name]
  (z.sql/execute-one! db {:insert-into :channels
                          :values [{:id id
                                    :name name
                                    :created_at (System/currentTimeMillis)}]})
  id)

(defn channel
  "Normalises zodiac-sql's namespaced result keys to plain ones.

  zodiac-sql returns `#:channels{:id ... :name ...}`, so a plain `(:name row)`
  silently yields nil — the symptom was a channel titled \"unknown channel\".
  Normalising here keeps the namespacing from leaking into the component."
  [db id]
  (when-let [row (z.sql/execute-one! db {:select [:id :name]
                                         :from :channels
                                         :where [:= :id id]})]
    {:id (or (:id row) (:channels/id row))
     :name (or (:name row) (:channels/name row))}))

(defn add-message!
  "Stores a ciphertext body and its IV. The server can read neither."
  [db channel-id author ciphertext iv]
  (z.sql/execute-one! db {:insert-into :messages
                          :values [{:channel_id channel-id
                                    :author author
                                    :body ciphertext
                                    :iv iv
                                    :created_at (System/currentTimeMillis)}]}))

(defn count-before
  "How many messages exist older than `before` in this channel.

  Asked explicitly rather than inferred from a short page: a page can be exactly
  `page-size` with nothing behind it, and inferring would leave a \"load more\"
  control that never resolves."
  [db channel-id before]
  (if before
    (or (:c (z.sql/execute-one! db {:select [[[:count :*] :c]]
                                    :from :messages
                                    :where [:and
                                            [:= :channel_id channel-id]
                                            [:< :id before]]}))
        0)
    0))

;;; ==========================================================================
;;; Source
;;; ==========================================================================

(defn- page
  "Up to `page-size` messages, oldest-first.

  Selected *descending* to get the newest page, then reversed, because \"the latest
  50\" and \"the first 50\" are different queries and only the former is what a chat
  window opens on."
  [db channel-id before]
  (->> (z.sql/execute! db (cond-> {:select [:id :author :body :iv :created_at]
                                   :from :messages
                                   :where [:= :channel_id channel-id]
                                   :order-by [[:id :desc]]
                                   :limit page-size}
                            before (assoc :where [:and
                                                  [:= :channel_id channel-id]
                                                  [:< :id before]])))
       reverse
       ;; Same normalisation as `channel`: accept either plain or namespaced keys
       ;; so the component never sees zodiac-sql's shape.
       (mapv (fn [r] {:id (or (:id r) (:messages/id r))
                      :author (or (:author r) (:messages/author r))
                      :body (or (:body r) (:messages/body r))
                      :iv (or (:iv r) (:messages/iv r))
                      :created-at (or (:created-at r) (:created_at r)
                                      (:messages/created-at r))}))))

(defn ->source
  "A `Source` over the zodiac-sql db.

  Queries:
  - `[:channel id]`         -> the channel row
  - `[:messages id before]` -> up to 50 messages older than `before`, oldest
                               first; `before` of `nil` means the newest page
  - `[:count-before id b]`  -> how many older messages remain

  `basis` returns `nil`: SQLite keeps no history, so it cannot honour a basis
  token, and such a store should say so rather than pretend."
  [db]
  (reify source/Source
    (fetch [this query] (source/fetch this query nil))
    (fetch [_ [kind id before] _basis]
      (case kind
        :channel (channel db id)
        :messages (page db id before)
        :count-before (count-before db id before)
        nil))
    (basis [_] nil)))

(defn oldest-id
  "Lowest message id in a loaded page, for requesting the next one.

  `nil` when empty, which the caller reads as \"nothing older to ask for\"."
  [messages]
  (when (seq messages) (apply min (map :id messages))))
