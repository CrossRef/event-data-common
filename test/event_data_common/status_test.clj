(ns event-data-common.status-test
  (:require [clojure.test :refer :all]
            [event-data-common.status :as status]
            [org.httpkit.fake :as fake]))

(deftest ^:unit can-send-status
  (testing "send! can send a status to the endpoint, retrying on failure. NB this will deliberately log errors!"
    ; We don't really need to wait for 10 seconds each time.
    (reset! status/wait-delay 1)

    ; A series of HTTP response codes.
    ; The client should stop when it gets a 201 (i.e. response-i == 3).
    
    (let [responses [500 500 201 999 999]
          ; Increment index.
          response-i (atom -1)
          ; Signal for test, because send! uses backoff, which uses a threadpool.
          wait (promise)]
      ; A mock endpoint that returns each of these in sequence.
      ; Hostname set in configuration in docker-compose.yml
      (fake/with-fake-http ["http://localhost:8003/status/my-service/my-component/my-facet"
                            (fn [orig-fn opts callback]
                              
                              (swap! response-i inc)
                              (when (= 2 @response-i) (deliver wait 1))
                              {:status (get responses @response-i)})]
    ; Wait for the fake-http handler to let us proceed.
    (status/send! "my-service" "my-component" "my-facet" 55)
    (deref wait)
    (is (= @response-i 2) "Should have attempted three times and stopped the first time a 201 was returned.")))))

