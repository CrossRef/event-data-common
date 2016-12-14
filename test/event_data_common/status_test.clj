(ns event-data-common.status-test
  (:require [clojure.test :refer :all]
            [event-data-common.status :as status]
            [org.httpkit.fake :as fake]))

(deftest ^:unit can-send-status
  (testing "send! can send a status to the endpoint, retrying on failure."
    ; A series of HTTP response codes.
    ; The client should stop when it gets a 201 (i.e. response-i == 3).
    (let [responses [500 500 500 201 999 999]
          ; Increment index.
          response-i (atom -1)]

      ; A mock endpoint that returns each of these in sequence.
      ; Hostname set in configuration in docker-compose.yml
      (fake/with-fake-http ["http://localhost:8003/status/my-service/my-component/my-facet"
                            (fn [orig-fn opts callback] {:status (get responses (swap! response-i inc))})]

    (let [result (status/send! "my-service" "my-component" "my-facet" 55)]
      (is (= @response-i 3) "Should have attempted multiple times and stopped the first time a 201 was returned."))))))

