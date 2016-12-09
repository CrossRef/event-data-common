(ns event-data-common.jwt
  "Middleware for loading verifying JWTs by multiple secrets."
  (:import [com.auth0.jwt JWTSigner JWTVerifier]))

(defn get-token
  "Middleware to retrieve a token from a request. Return nil if missing or malformed."
  [request]
  (let [auth-header (get-in request [:headers "authorization"])]
    (when (and auth-header (.startsWith auth-header "Bearer "))
      (.substring auth-header 7))))

(defn try-verify-token
  "Verify a JWT token using a supplied JWT verifier. Return the claims on success or nil."
  [verifier token]
  (try
    (.verify verifier token)
    ; Can be IllegalStateException, JsonParseException, SignatureException.
    (catch Exception e nil)))

(defn wrap-jwt
  "Return a middleware handler that verifies JWT claims using one of the comma-separated secrets."
  [handler secrets-str]
  (let [secrets (clojure.string/split secrets-str #",")
        verifiers (map #(new JWTVerifier %) secrets)]
    (fn [request]
      (let [token (get-token request)
            matched-token (first (keep #(try-verify-token % token) verifiers))]
        (if matched-token
          (handler (assoc request :jwt-claims matched-token))
          (handler request))))))
