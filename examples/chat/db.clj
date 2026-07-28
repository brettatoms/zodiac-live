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
            [darkstar.source :as source]))

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
  ;; Membership is DURABLE, unlike presence. Someone who joins stays in the roster
  ;; forever; only the connected indicator changes when they come and go. Deriving
  ;; the roster from presence alone made members vanish on disconnect, which is not
  ;; what a roster is for.
  ;;
  ;; Keyed on the pair, because a username identifies a person only within one
  ;; channel — `alice` in #general and `alice` in #random are unrelated people.
  (z.sql/execute! db ["create table if not exists members (
                         channel_id text not null,
                         username text not null,
                         joined_at integer not null,
                         primary key (channel_id, username))"])
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

(defn claim-member!
  "Claims `username` in `channel-id`. Returns true if the claim succeeded.

  **A username is claimed exactly once per channel.** Returning false means
  somebody already has it, so the caller must refuse rather than issue a token —
  otherwise a second person entering an existing name simply becomes that person,
  and since a username is the only identity in this model, that is impersonation.

  The uniqueness is enforced by the `(channel_id, username)` primary key rather
  than by a read-then-write check, so two simultaneous joins cannot both win. That
  matters: a check-then-insert has a window between the two, and this is exactly
  the kind of race an attacker would retry into.

  `insert or ignore` would be wrong here — it reports success on a duplicate, which
  is the whole failure being prevented. So the insert is allowed to violate the
  constraint and the exception is the answer."
  [db channel-id username]
  (try
    (z.sql/execute! db ["insert into members (channel_id, username, joined_at)
                         values (?, ?, ?)"
                        channel-id username (System/currentTimeMillis)])
    true
    (catch Exception e
      ;; Only a uniqueness violation means "taken". Anything else is a real error
      ;; and must not be reported as a name collision.
      (if (re-find #"(?i)unique|constraint" (str (ex-message e)))
        false
        (throw e)))))

(defn member?
  "Is `username` already claimed in `channel-id`?"
  [db channel-id username]
  (boolean (seq (z.sql/execute! db {:select [:username]
                                    :from :members
                                    :where [:and
                                            [:= :channel_id channel-id]
                                            [:= :username username]]
                                    :limit 1}))))

(defn add-member!
  "Records that `username` belongs to `channel-id`. Idempotent.

  For a member who already holds a claim — a reconnect, or a second tab. Use
  `claim-member!` when granting a *new* identity."
  [db channel-id username]
  (z.sql/execute! db ["insert or ignore into members (channel_id, username, joined_at)
                       values (?, ?, ?)"
                      channel-id username (System/currentTimeMillis)])
  nil)

(defn members
  "Every username ever in `channel-id`, sorted.

  Sorted so the value is stable across reads: an unordered result would make every
  refresh look like a change and defeat the diff."
  [db channel-id]
  (->> (z.sql/execute! db {:select [:username]
                           :from :members
                           :where [:= :channel_id channel-id]
                           :order-by [[:username :asc]]})
       (mapv (fn [r] (or (:username r) (:members/username r))))
       (filterv some?)))

(defn authors
  "Distinct message authors in `channel-id`, sorted.

  Half of the member roster; the other half is who is currently connected. Needs no
  schema change, and means a member who has posted stays in the sidebar after they
  disconnect.

  Sorted so the value is stable across reads, for the same reason
  `chat.presence/connected` sorts: an unordered result makes every refresh look
  like a change."
  [db channel-id]
  (->> (z.sql/execute! db {:select-distinct [:author]
                           :from :messages
                           :where [:= :channel_id channel-id]
                           :order-by [[:author :asc]]})
       (mapv (fn [r] (or (:author r) (:messages/author r))))
       (filterv some?)))

(defn ->source
  "A `Source` over the zodiac-sql db.

  Queries:
  - `[:channel id]`         -> the channel row
  - `[:messages id before]` -> up to 50 messages older than `before`, oldest
                               first; `before` of `nil` means the newest page
  - `[:count-before id b]`  -> how many older messages remain
  - `[:authors id]`         -> distinct message authors, sorted
  - `[:members id]`         -> every username ever in the channel, sorted

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
        :authors (authors db id)
        :members (members db id)
        nil))
    (basis [_] nil)))

(defn oldest-id
  "Lowest message id in a loaded page, for requesting the next one.

  `nil` when empty, which the caller reads as \"nothing older to ask for\"."
  [messages]
  (when (seq messages) (apply min (map :id messages))))
