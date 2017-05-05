(ns event-data-common.queue-test
  (:require [clojure.test :refer :all]
            [event-data-common.queue :as queue]
            [config.core :refer [env]]
            [clojure.tools.logging :as log])
  (:import [org.apache.activemq.broker BrokerService]))

(def queue-config {:username "my-username" :password "my-password" :queue-name "my-queue" :url "vm://my-broker"})

(deftest ^:component roundtrip
  (testing "Messages can make roundtrip"
    (let [^BrokerService broker (BrokerService.)
          ; We will send this.
          to-send [{:name "one" :value "frogs"} {:name "two" :value "hatstands"}]
          
          ; Collect data out the other end here.
          consumed (atom [])]

      (.addConnector broker (:url queue-config))
      (.start broker)

      (doseq [x to-send]
        (queue/enqueue x queue-config))

     (try 
       (queue/process-queue queue-config
         (fn [item]
           (swap! consumed conj item)
           ; When we've consumed all we want, throw a harmless exception to kill the thread.
           (when (= (count @consumed) (count to-send))
              (throw (Exception. "Done!")))))
       (catch Exception _ nil))

      (is (= (set @consumed) (set to-send)) "All messages should have been been recieved.")
     
      (.stop broker))))

(defn soak
  "Helper function for soak testing. Not a test but may come in handy."
  []
  (let [^BrokerService broker (BrokerService.)
        ; We will send this.
        to-send [{:name "one" :value "frogs"} {:name "two" :value "hatstands"}]
        
        ; Collect data out the other end here.
        consumed (atom [])]

    (.addConnector broker (:url queue-config))
    (.start broker)

    (dotimes [i 10000]
      (log/info i)
      (doseq [x to-send]
        (queue/enqueue x queue-config)))

   (try 
     (queue/process-queue queue-config
       (fn [item]
         (swap! consumed conj item)
         ; When we've consumed all we want, throw a harmless exception to kill the thread.
         (when (= (count @consumed) (count to-send))
            (throw (Exception. "Done!")))))
     (catch Exception _ nil))

    (is (= (set @consumed) (set to-send)) "All messages should have been been recieved.")
   
    (.stop broker)))