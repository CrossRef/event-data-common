(ns event-data-common.jwt-test
  (:require [clojure.test :refer :all]
            [event-data-common.jwt :as jwt]))

(deftest ^:unit wrap-jwt
  (let [jwt-secret1 "TEST"
        jwt-secret2 "TEST2"
        secrets-str "TEST,TEST2"
        verifier (jwt/build secrets-str)
        middleware (jwt/wrap-jwt identity secrets-str)
        claims {"test-key" "test-value"}
        token1 (jwt/sign-with-signer verifier (first (jwt/signers verifier)) claims)
        token2 (jwt/sign-with-signer verifier (second (jwt/signers verifier)) claims)]

  (testing "wrap-jwt should decode more than one token"
    (let [wrapped1 (middleware
                      {:remote-addr "172.17.0.1"
                       :headers {
                         "accept" "*/*"
                         "authorization" (str "Bearer " token1)
                         "host" "localhost:9990"
                         "user-agent" "curl/7.49.1"}
                       :uri "/heartbeat"
                       :server-name "localhost"
                       :body nil
                       :scheme :http
                       :request-method :get})

          wrapped2 (middleware
                      {:remote-addr "172.17.0.1"
                       :headers {
                         "accept" "*/*"
                         "authorization" (str "Bearer " token2)
                         "host" "localhost:9990"
                         "user-agent" "curl/7.49.1"}
                       :uri "/heartbeat"
                       :server-name "localhost"
                       :body nil
                       :scheme :http
                       :request-method :get})

          wrapped-bad (middleware
                      {:remote-addr "172.17.0.1"
                       :headers {
                         "accept" "*/*"
                         "authorization" (str "Bearer BAD TOKEN")
                         "host" "localhost:9990"
                         "user-agent" "curl/7.49.1"}
                       :uri "/heartbeat"
                       :server-name "localhost"
                       :body nil
                       :scheme :http
                       :request-method :get})]

    (is
      (= claims (:jwt-claims wrapped1))
      "Token can be decoded using one of the secrets, associated to request.")

    (is
      (= claims (:jwt-claims wrapped2))
      "Token can be decoded using the other of the secrets, associated to request.")

    (is
      (= nil (:jwt-claims wrapped-bad))
      "Invalid token ignored.")))))

(deftest ^:unit get-token
  (testing "Get-token should retrieve the token from a request."
    (is (= "abcdefgh" (jwt/get-token
                        {:remote-addr "172.17.0.1",
                         :headers {
                           "accept" "*/*",
                           "authorization" "Bearer abcdefgh",
                           "host" "localhost:9990",
                           "user-agent" "curl/7.49.1"},
                         :uri "/heartbeat",
                         :server-name "localhost",
                         :body nil,
                         :scheme :http,
                         :request-method :get}))))

  (testing "Get-token should return nil if not present."
    (is (= nil (jwt/get-token
                {:remote-addr "172.17.0.1",
                 :headers {
                   "accept" "*/*",
                   "authorization" "",
                   "host" "localhost:9990",
                   "user-agent" "curl/7.49.1"},
                 :uri "/heartbeat",
                 :server-name "localhost",
                 :body nil,
                 :scheme :http,
               :request-method :get}))))

    (testing "Get-token should return nil if malformed."
      (is (= nil (jwt/get-token 
                  {:remote-addr "172.17.0.1",
                   :headers {
                     "accept" "*/*",
                     "authorization" "Token abcdefgh",
                     "host" "localhost:9990",
                     "user-agent" "curl/7.49.1"},
                   :uri "/heartbeat",
                   :server-name "localhost",
                   :body nil,
                   :scheme :http,
                   :request-method :get})))))

(deftest ^:unit create-token
  (testing "create-token should create a valid token using the first secret"
    (let [verifier (jwt/build "TEST,TEST1,TEST3,TEST3")
          claim {"my-claim-key" "my-claim-value"}
          token (jwt/sign verifier claim)
          ; Get the list of verifiers that it will use.
          verifiers (jwt/verifiers verifier)]

      (is (= claim (jwt/try-verify-with-verifier verifier (first verifiers) token)) "Verifier using first secret should verify claim")
      (is (nil? (jwt/try-verify-with-verifier verifier (second verifiers) token)) "Verifier using any other secret should not verify claim"))))

