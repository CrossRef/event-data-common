(ns event-data-common.event-bus-test
  (:require [clojure.test :refer :all]
            [event-data-common.event-bus :as event-bus]))


(deftest ^:unit send-event
  (testing "send-event should construct URL from Event ID, include JWT, and retry"
    (let [counter (atom 0)
          retrieved-url (atom nil)
          retrieved-params (atom nil)]
      ; Mock out unreliable connection.
      (with-redefs [clj-http.client/put
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

