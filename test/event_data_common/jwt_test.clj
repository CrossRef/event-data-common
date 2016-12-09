(ns event-data-common.core-test
  (:require [clojure.test :refer :all]
            [event-data-common.jwt :as jwt]))

(deftest ^:unit wrap-jwt
  (let [jwt-secret1 "TEST"
        jwt-secret2 "TEST2"
        middleware (jwt/wrap-jwt identity "TEST,TEST2")
        signer1 (new JWTSigner jwt-secret1)
        signer2 (new JWTSigner jwt-secret2)
        token-content {"test-key" "test-value"}
        token1 (.sign signer1 token-content)
        token2 (.sign signer2 token-content)]

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
      (= token-content (:jwt-claims wrapped1))
      "Token can be decoded using one of the secrets, associated to request.")

    (is
      (= token-content (:jwt-claims wrapped2))
      "Token can be decoded using the other of the secrets, associated to request.")

    (is
      (= nil (:jwt-claims wrapped-bad))
      "Invalid token ignored.")))))