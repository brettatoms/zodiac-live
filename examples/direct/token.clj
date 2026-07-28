(ns direct.token
  "Signed URL tokens, so identity is not editable in the address bar.

  Two kinds, and the distinction is the point:

  - an **invite** token signs the channel id alone. It is shareable, carries no
    identity, and is what the copy button hands out.
  - a **session** token signs `[channel-id username]`. It is personal, and it is
    what the URL holds after joining.

  ## Why sign at all

  The username used to ride in `?user=`, which meant anyone could impersonate
  anyone by editing the address bar — and since a username identifies a person only
  within a channel (`chat.presence`), that is a one-character attack. Signing moves
  the name inside a payload whose HMAC the client cannot forge, and removes the
  query param entirely, so there is nothing left to edit.

  ## What this does and does not prove

  A valid session token proves **the server issued this name for this channel**. It
  does *not* prove the bearer is the person who joined: forward the URL and the
  recipient becomes that user. Real authentication needs a login, which a demo does
  not have. What this buys is that identity cannot be *changed* by editing a URL,
  which was the actual hole.

  Same construction as `darkstar.snapshot`: base64url payload, `.`, HMAC-SHA256, with
  a constant-time comparison on the way back in."
  (:require [clojure.edn :as edn])
  (:import [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util Base64]
           [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]))

(defn- b64 ^String [^bytes bs]
  (.encodeToString (.withoutPadding (Base64/getUrlEncoder)) bs))

(defn- hmac ^String [^String secret ^String msg]
  (let [mac (Mac/getInstance "HmacSHA256")]
    (.init mac (SecretKeySpec. (.getBytes secret StandardCharsets/UTF_8) "HmacSHA256"))
    (b64 (.doFinal mac (.getBytes msg StandardCharsets/UTF_8)))))

(defn- constant-time=
  "Compares two strings without leaking where they differ through timing."
  [^String a ^String b]
  (and a b
       (MessageDigest/isEqual (.getBytes a StandardCharsets/UTF_8)
                              (.getBytes b StandardCharsets/UTF_8))))

(defn- encode [secret payload]
  (let [body (b64 (.getBytes (pr-str payload) StandardCharsets/UTF_8))]
    (str body "." (hmac secret body))))

(defn- decode
  "Verifies and reads a token, or returns nil.

  Returns nil rather than throwing for every failure mode — a bad token is an
  expected condition here, not an error, since anything can arrive in a URL.

  `edn/read-string` rather than `clojure.core/read-string`: the latter honours
  `*read-eval*` and would evaluate `#=(...)` in a payload we are about to decide is
  untrusted. The signature is checked first regardless, so this is defence in
  depth."
  [secret ^String token]
  (when (string? token)
    (let [idx (.lastIndexOf token ".")]
      (when (pos? idx)
        (let [body (subs token 0 idx)
              sig (subs token (inc idx))]
          (when (constant-time= sig (hmac secret body))
            (try
              (let [payload (edn/read-string
                             (String. (.decode (Base64/getUrlDecoder) body)
                                      StandardCharsets/UTF_8))]
                (when (map? payload) payload))
              (catch Exception _ nil))))))))

;;; ==========================================================================
;;; Invite: channel only, shareable
;;; ==========================================================================

(defn invite
  "A token naming `channel-id` and nothing else."
  [secret channel-id]
  (encode secret {:k :invite :c channel-id}))

(defn read-invite
  "The channel id from an invite token, or nil."
  [secret token]
  (let [{:keys [k c]} (decode secret token)]
    (when (and (= :invite k) (string? c)) c)))

;;; ==========================================================================
;;; Session: channel + username, personal
;;; ==========================================================================

(defn session
  "A token naming `channel-id` and `username` together.

  The pair is signed as one payload, so neither can be swapped independently — and
  since a username is only meaningful within a channel, signing them apart would
  admit exactly the cross-channel confusion `chat.presence` exists to prevent."
  [secret channel-id username]
  (encode secret {:k :session :c channel-id :u username}))

(defn read-session
  "`{:channel-id :username}` from a session token, or nil."
  [secret token]
  (let [{:keys [k c u]} (decode secret token)]
    (when (and (= :session k) (string? c) (string? u))
      {:channel-id c :username u})))
