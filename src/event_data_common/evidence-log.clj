(ns event-data-common.evidence-log
  "Report to Evidence Log."
  (:require [event-data-common.jwt :as jwt]
            [event-data-common.backoff :as backoff]
            [robert.bruce :refer [try-try-again]]
            [clojure.tools.logging :as log]
            [clj-http.client :as client]
            [config.core :refer [env]]
            [clojure.data.json :as json])
  (:import [java.util UUID]
           [org.apache.kafka.clients.producer KafkaProducer Producer ProducerRecord]
           [org.apache.kafka.clients.consumer KafkaConsumer Consumer ConsumerRecords]))

(def kafka-producer
  (delay
    (let [properties (java.util.Properties.)]
      (.put properties "bootstrap.servers" (:global-kafka-bootstrap-servers env))
      (.put properties "acks", "all")
      (.put properties "retries", (int 5))
      (.put properties "key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      (.put properties "value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      (KafkaProducer. properties))))

(def available-fields
  "Known set of fields that we can log.
  If these are modified, the event-data-evidence-log-snapshot should be updated."
  #{:s :c :f :p :v :r :d :n :u :e})

(defn log!
  "Log message as a map of known fields to values to Evidence Log. All fields optional.
  The :t timestamp field is automatically added. Fields default to null.
  Available fields:
   - :s - service (or Agent)
   - :c - component
   - :f - facet
   - :p - partition
   - :v - value
   - :r - Evidence Record ID
   - :d - DOI
   - :n - Event ID
   - :u - URL
   - :e - extra data"
  [message]
  {:pre [(every? available-fields (keys message))]}
  ; Skip connecting to Kafka if not configured. Used for testing.
  (if-not (:global-kafka-bootstrap-servers env)
    (log/debug "Evidence Log" message)
    (.send @kafka-producer
           (ProducerRecord. (:global-status-topic env)
                            (str (UUID/randomUUID))
                            (json/write-str (assoc message
                                              :t (System/currentTimeMillis)))))))
