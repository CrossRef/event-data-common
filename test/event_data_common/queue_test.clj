(ns event-data-common.queue-test
  (:require [clojure.test :refer :all]
            [event-data-common.queue :as queue]
            [config.core :refer [env]]
            [clojure.tools.logging :as log])
  (:import [org.apache.activemq.broker BrokerService]))

; thread local

; username and password and uri from config

(deftest ^:component roundtrip
  (testing "Messages can make roundtrip"
    (let [^BrokerService broker (BrokerService.)
          ; We will send this.
          to-send [{:name "one" :value "frogs"} {:name "two" :value "hatstands"}]
          
          ; Collect data out the other end here.
          consumed (atom [])]

      (.addConnector broker (:activemq-url env))
      (.start broker)

      (doseq [x to-send]
        (queue/enqueue x "my-queue"))

     (try 
       (queue/process-queue "my-queue"
         (fn [item]
           (swap! consumed conj item)
           ; When we've consumed all we want, throw a harmless exception to kill the thread.
           (when (= (count @consumed) (count to-send))
              (throw (Exception. "Done!")))))
       (catch Exception _ nil))

      (is (= (set @consumed) (set to-send)) "All messages should have been been recieved.")
     
      (.stop broker))))

(deftest ^:component thread-separation
  (testing "Different threads should get different connection objects, but with same config"
    (let [num-threads 10
          ; Result will be a mapping of {thread-id connection-objects}
          objects (atom {})
          ; Do this twice per thread.
          objects-again (atom {})

          ; Broker service must be running in order to construct connection objects.
          ^BrokerService broker (BrokerService.)]

      (.addConnector broker (:activemq-url env))
      (.start broker)

      ; Spin up a few threads, let each one get connection objects and save in objects and objects-again
      (let [threads (doall (map (fn [thread-number]
                                  (Thread. (fn []
                                    (log/info "Enqueue from " thread-number)
                                    (swap! objects assoc (.getId (Thread/currentThread))
                                                         (queue/get-queue-producer-connection "a-test-queue"))
                                    (swap! objects-again assoc (.getId (Thread/currentThread))
                                                               (queue/get-queue-producer-connection "a-test-queue")))))
                         (range num-threads)))]

        (doseq [thread threads] (.start thread))
        (doseq [thread threads] (.join thread)))

      (is (= (count @objects) num-threads) "Each Thread should report a unique Thread ID and should have created an object.")
      (is (= (count @objects-again) num-threads) "Each Thread should report a unique Thread ID and should have created an object.")
      
      (doseq [object (vals @objects)]
        (is (= #{:connection :session :destination :producer} (set (keys object))) "Every object should have the stipulated keys."))

      (doseq [field [:connection :session :destination :producer]]
        (is (= (count (distinct (map field (vals @objects))))) "The given object in each set should be distinct from that acquired in other Threads."))

      (doseq [thread-id (keys @objects)]
        (is (identical? (@objects thread-id) (@objects-again thread-id)) "Exactly the same objects should be returned in subsequent calls within each Thread."))

    (.stop broker))))

