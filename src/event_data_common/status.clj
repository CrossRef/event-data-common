(ns event-data-common.status
  "Report to status service. Signs requests using secret."
  (:require [event-data-common.jwt :as jwt]
            [event-data-common.backoff :as backoff]
            [robert.bruce :refer [try-try-again]]
            [clojure.tools.logging :as log]
            [clj-http.client :as client]
            [config.core :refer [env]])
  (:import [java.util UUID]))

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

(defn add!
  "Send an update for a component/fragment/heartbeat"
  [service component fragment heartbeat-count]
  (.send @kafka-producer
         (ProducerRecord. (:global-status-topic env)
                          (str (UUID/randomUUID))
                          (json/write-str {:service service
                                           :component component
                                           :fragment fragment
                                           :aggregation-type "cumulative"}))))

(defn replace!
  "Send an update for a component/fragment/heartbeat"
  [service component fragment heartbeat-count]
  (.send @kafka-producer
         (ProducerRecord. (:global-status-topic env)
                          (str (UUID/randomUUID))
                          (json/write-str {:service service
                                           :component component
                                           :fragment fragment
                                           :aggregation-type "sampled"}))))
