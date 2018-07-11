(ns event-data-common.event-bus-test
  (:require [clojure.test :refer :all]
            [event-data-common.event-bus :as event-bus]
            [slingshot.slingshot :refer [throw+ try+]]))


(deftest ^:unit send-event
  (testing "send-event should construct URL from Event ID, include JWT, and retry"
    (let [counter (atom 0)
          retrieved-url (atom nil)
          retrieved-params (atom nil)]
      
      (with-redefs [; No need to hang around in tests.
                    event-data-common.event-bus/retry-delay 1

                    ; Mock out unreliable connection.  
                    clj-http.client/put
                    (fn [url params]
                      (swap! counter inc)

                      ; Throw first two times.
                      (when (< @counter 3)
                        (throw (Exception. "Bang")))

                      (reset! retrieved-url url)
                      (reset! retrieved-params params))]

        (event-bus/send-event
          {:id "1234" :this "that"}
          "this-is-the-jwt")
        
        (is (= @retrieved-url "https://bus.eventdata.crossref.org/events/1234"))
        (is (= @retrieved-params
               {:body "{\"id\":\"1234\",\"this\":\"that\"}"
                :headers {"Authorization" "Bearer this-is-the-jwt"}}))))))


(deftest ^:unit post-event
  ; No need to hang around in tests.
  (with-redefs [event-data-common.event-bus/retry-delay 1]

    (testing "post-event should ignore HTTP 409 conflicts, as this simply means we tried the same thing twice."
      (let [counter (atom 0)
            retrieved-url (atom nil)
            retrieved-params (atom nil)]
        
            ; Mock out unreliable connection.  
            (with-redefs [clj-http.client/post
                          (fn [url params]
                            (reset! retrieved-params params)
                            (swap! counter inc)
                            (throw+ {:status 409}))]

          (event-bus/post-event
            {:id "1234" :this "that" :source_id "mysource"})
          
          (is (= (:body @retrieved-params) "{\"id\":\"1234\",\"this\":\"that\",\"source_id\":\"mysource\"}")
                                           
              "Event should be sent in body.")

          (is (= (get-in @retrieved-params [:headers "Authorization"])
                 "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJteXNvdXJjZSJ9.8KmHd13CdzRiPHtkBB9SigImLCyM7o_dly24YROK7j4")
              "Token should be derived from source_id using the configured secret 'TEST'.")

          (is (= @counter 1) "Request should have been made only once, not retried."))))


    (testing "post-event should retry on HTTP error codes."
      (let [counter (atom 0)
            retrieved-url (atom nil)
            retrieved-params (atom nil)]
        
            ; Mock out unreliable connection.  
            (with-redefs [clj-http.client/post
                          (fn [url params]
                            (reset! retrieved-params params)
                            (swap! counter inc)

                            (when (< @counter 3)
                              (throw+ {:status 500})))]

          (event-bus/post-event
            {:id "1234" :this "that" :source_id "mysource"})
          
          (is (= (:body @retrieved-params) "{\"id\":\"1234\",\"this\":\"that\",\"source_id\":\"mysource\"}")
              "Event should be sent in body.")

          (is (= (get-in @retrieved-params [:headers "Authorization"])
                 "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJteXNvdXJjZSJ9.8KmHd13CdzRiPHtkBB9SigImLCyM7o_dly24YROK7j4")
              "Token should be derived from source_id using the configured secret 'TEST'.")

          (is (= @counter 3) "Request should have been retried as long as there are errors (up to the limit)."))))))



