(ns event-data-common.status
  "Report to status service. Signs requests using secret."
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

(def jwt-auth (delay (jwt/build (:global-jwt-secrets env))))

; Get a token. Make an empty one using the secret.
; Specific claims not required to send to status.
(def jwt-token (delay (jwt/sign @jwt-auth {})))

; This can be reset in unit tests.
(def wait-delay (atom 10000))

(def kafka-producer
  (delay
    (let [properties (java.util.Properties.)]
      (.put properties "bootstrap.servers" (:global-kafka-bootstrap-servers env))
      (.put properties "acks", "all")
      (.put properties "retries", (int 5))
      (.put properties "key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      (.put properties "value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      (KafkaProducer. properties))))

(defn send!
  "Send an update for a component/facet/heartbeat.
   Partition number can be nil.
   typ is one of:
    - :event - single shots that should be aggregated to a rate (e.g. retrieval per second)
    - :snapshot - values that change over time (e.g. current queue length)
    - :sample - scalar representative values that should all be min/max/averaged (e.g. message size)"
  ; For recording single-shot events.
  ([service component facet]
    (send! service component facet -1 1))
  ; When there's no partition use default of -1
  ([service component facet value]
    (send! service component facet -1 value))
  ; Include partition.
  ([service component facet partition-number value]
    (send! service component facet partition-number value nil))
  ; All values
  ([service component facet partition-number value extra]
    ; Skip connecting to Kafka if not configured. Used for testing.
    (if-not (:global-kafka-bootstrap-servers env)
      (log/debug "Status" service component facet partition-number value extra)
      (.send @kafka-producer
             (ProducerRecord. (:global-status-topic env)
                              (str (UUID/randomUUID))
                              (json/write-str {:t (System/currentTimeMillis)
                                               :s service
                                               :c component
                                               :f facet
                                               :p (or partition-number -1)
                                               :v value
                                               :e extra}))))))
