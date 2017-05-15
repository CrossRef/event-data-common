(ns event-data-common.queue-test
  (:require [clojure.test :refer :all]
            [event-data-common.queue :as queue]
            [config.core :refer [env]]
            [clojure.tools.logging :as log])
  (:import [org.apache.activemq.broker BrokerService]))

(def queue-config {:username "my-username" :password "my-password" :queue-name "my-queue" :url "vm://my-broker"})
(def topic-config {:username "my-username" :password "my-password" :topic-name "my-topic" :url "vm://my-broker"})

(deftest ^:component queue-roundtrip
  (testing "Messages can make roundtrip via a queue"
    (let [^BrokerService broker (BrokerService.)
          ; We will send this.
          to-send [{:name "one" :value "frogs"} {:name "two" :value "hatstands"}]
          
          ; Collect data out the other end here.
          consumed (atom [])]

      (.setPersistent broker false)
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


(deftest ^:component topic-roundtrip
  (testing "Messages can make roundtrip via a topic, and are recieved by all consumers."
    (let [^BrokerService broker (BrokerService.)
          ; We will send this.
          to-send [{:name "one" :value "frogs"} {:name "two" :value "hatstands"}]
          
          ; Collect data out the other end here.
          consumed-1 (atom [])
          consumed-2 (atom [])]

      (.setPersistent broker false)
      (.addConnector broker (:url topic-config))
      (.start broker)

      ; We need to register our topic listeners before we send anything, and keep them going in the background otherwise they won't get the mesages.
      ; Unlinke Queues, Topics don't wait for consumers.
      (let [process-thread-1 (new Thread #(try 
       (queue/process-topic topic-config
         (fn [item]
           (swap! consumed-1 conj item)
           ; When we've consumed all we want, throw a harmless exception to kill the thread.
           (when (= (count @consumed-1) (count to-send))
              (throw (Exception. "Done!")))))
       (catch Exception _ nil)))

          process-thread-2 (new Thread #(try 
       (queue/process-topic topic-config
         (fn [item]
           (swap! consumed-2 conj item)
           ; When we've consumed all we want, throw a harmless exception to kill the thread.
           (when (= (count @consumed-2) (count to-send))
              (throw (Exception. "Done!")))))
       (catch Exception _ nil)))]

      (.start process-thread-1)
      (.start process-thread-2)

      (doseq [x to-send]
        (queue/entopic x topic-config))

      (.join process-thread-1)
      (.join process-thread-2))

      (is (= (set @consumed-1) (set to-send)) "All messages should have been been recieved by first topic listener.")
      (is (= (set @consumed-2) (set to-send)) "All messages should have been been recieved by second topic listener.")
     
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