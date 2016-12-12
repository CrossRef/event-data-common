(ns event-data-common.jwt
  "Middleware for loading verifying JWTs by multiple secrets."
  (:import [com.auth0.jwt JWTSigner JWTVerifier]))

(defn get-token
  "Middleware to retrieve a token from a request. Return nil if missing or malformed."
  [request]
  (let [auth-header (get-in request [:headers "authorization"])]
    (when (and auth-header (.startsWith auth-header "Bearer "))
      (.substring auth-header 7))))

(defprotocol MultiJwtVerifier
  "Verifier for JWTs"
  (secrets [this] "Return seq of secrets.")
  (verifiers [this] "Return seq of verifies in same order as secrets.")
  (signers [this] "Return seq of signers in the same order as secrets.")
  (try-verify [this token] "Attempt to verify a token with one of the verifiers. Return claims or nil.")
  (try-verify-with-verifier [this verifier token] "Attempt to verify a token with this verifier. Return claims or nil.")
  (sign-with-signer [this signer claims] "Create a token.")
  (sign [this claims] "Create a token."))

(defrecord MultiJwtVerifierImpl
  [secrets verifiers signers]
  
  MultiJwtVerifier
  (secrets [this]
    secrets)

  (verifiers [this]
    verifiers)

  (signers [this]
    signers)

  (try-verify-with-verifier [this verifier token]
    (try
      (.verify verifier token)
      ; Can be IllegalStateException, JsonParseException, SignatureException.
      (catch Exception e nil)))

  (try-verify [this token]
    (first (keep #(try-verify-with-verifier this % token) verifiers)))

  (sign-with-signer [this signer claims]
    (.sign signer claims))

  (sign [this claims] "Create a token."
    (when-let [signer (first signers)]
      (sign-with-signer this signer claims))))

(defn build
  "Build a multi-secret JWT verifier."
  [secrets-str]
  (let [secrets (clojure.string/split secrets-str #",")
        verifiers (map #(new JWTVerifier %) secrets)
        signers (map #(new JWTSigner %) secrets)]
    (MultiJwtVerifierImpl. secrets verifiers signers)))

(defn wrap-jwt
  "Return a middleware handler that verifies JWT claims using one of the comma-separated secrets."
  [handler secrets-str]
  (let [^MultiJwtVerifier verifier (build secrets-str)]
    (fn [request]
      (let [token (get-token request)
            matched-token (try-verify verifier token)]
        (if matched-token
          (handler (assoc request :jwt-claims matched-token))
          (handler request))))))
