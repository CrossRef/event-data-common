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
           [org.apache.kafka.clients.producer KafkaProducer Producer ProducerRecord Callback RecordMetadata]
           [org.apache.kafka.clients.consumer KafkaConsumer Consumer ConsumerRecords]))

(def kafka-producer
  (delay
    (KafkaProducer. {
      "bootstrap.servers" (:global-kafka-bootstrap-servers env)
      ; Make sure it's flushed on the broker, but don't wait for all replicas.
      ; This is more robust if one replica stops working.
      "acks" "1"
      "retries" (int 5)
      "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
      "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})))

(def available-fields
  "Known set of fields that we can log.
  If these are modified, the event-data-evidence-log-snapshot should be updated."
  #{:s :c :f :p :a :v :d :n :u :r :e :o :i})


(def error-callback (reify Callback
  (^void onCompletion [this ^RecordMetadata metadata ^Exception exception]
    (when exception
      (log/error "Failed to send Evidence Log message" (.getMessage exception))))))

(defn log!
  "Log message as a map of known fields to values to Evidence Log. All fields optional.
  The :t timestamp field is automatically added. Fields default to null.
  Available fields:
   - :s - service (or Agent)
   - :c - component
   - :f - facet
   - :p - partition
   - :a - Activity ID
   - :v - value
   - :d - DOI
   - :n - Event ID
   - :u - URL
   - :r - Evidence Record ID
   - :e - Result status
   - :o - Origin
   - :i - Log Message type ID"
  [message]
  {:pre [(every? available-fields (keys message))]}

  ; Skip connecting to Kafka if not configured. Used for testing.
  (if-not (:global-kafka-bootstrap-servers env)
    (log/debug "Evidence Log" message)

    ; As this is called with high frequency, allow Kafka's producer to do its asycnronous batching.
    ; Don't block on send. Instead, handle error and log with callback. 
    (.send @kafka-producer
           (ProducerRecord. (:global-status-topic env)
                            (str (UUID/randomUUID))
                            (json/write-str (assoc message
                                              :t (System/currentTimeMillis))))
           error-callback)))


