(ns event-data-common.queue
  "ActiveMQ functions. An ActiveMQ queue is specified as a config hash-map of {:username :password :queue-name :url}.
   Separate pools and connection functions for 'queues' and 'topics'
   This is used to construct the connection, and later to refer to it when sending messages.
   This structure is useful in the places this library is used. No configuration is taken directly from the environment."
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [config.core :refer [env]]
            [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.coerce :as clj-time-coerce]
            [event-data-common.storage.redis :as redis]
            [overtone.at-at :as at-at]
            [robert.bruce :refer [try-try-again]]
            [event-data-common.status :as status])
  (:import [java.net URL MalformedURLException InetAddress]
           [java.io StringWriter PrintWriter]
           [redis.clients.jedis.exceptions JedisConnectionException]
           [javax.jms Session]
           [org.apache.activemq.jms.pool PooledConnectionFactory])
  (:gen-class))


(defn build-factory [config]
  "Build a hashmap of connection objects. This object can be cached. See http://activemq.apache.org/how-do-i-use-jms-efficiently.html "
  (let [factory (org.apache.activemq.ActiveMQConnectionFactory.
                 (:username config)
                 (:password config)
                 (:url config))
        pooled-connection-factory (PooledConnectionFactory.)]
  (.setConnectionFactory pooled-connection-factory factory)
  (.start pooled-connection-factory)
  pooled-connection-factory))

(def queue-connections (atom {}))

(def topic-connections (atom {}))

(defn get-queue-producer-connection
  "Get a PooledConnectionFactory by config object. Cache by Thread."
  [config]
  (if-let [connection (get @queue-connections config)]
    connection
    (let [new-connection (build-factory config)]
      (log/info "Created new queue connection for config" config)
      (swap! queue-connections assoc config new-connection)
      new-connection)))

(defn get-topic-producer-connection
  "Get a PooledConnectionFactory by config object. Cache by Thread."
  [config]
  (if-let [connection (get @topic-connections config)]
    connection
    (let [new-connection (build-factory config)]
      (log/info "Created new topic connection for config" config)
      (swap! topic-connections assoc config new-connection)
      new-connection)))

(defn process-queue
  "Process a queue, a callback is called for every item with structure deserialized from data, assumed to be JSON."
  [config process-f]
  (log/info "Starting to process queue" config)
  ; This runs and blocks in a single thread, so no cache needed.
  (let [factory (get-queue-producer-connection config)]
    (with-open [connection (.createConnection factory)]
      (let [session (.createSession connection false, Session/AUTO_ACKNOWLEDGE)
            destination (.createQueue session (:queue-name config))
            consumer (.createConsumer session destination)]
        (.start connection)
        (loop [message (.receive consumer)]
          (process-f (json/read-str (.getText ^org.apache.activemq.command.ActiveMQTextMessage message) :key-fn keyword))
          (recur (.receive consumer)))))))

(defn process-topic
  "Process a topic, a callback is called for every item with structure deserialized from data, assumed to be JSON."
  [config process-f]
  (log/info "Starting to process topic" config)
  ; This runs and blocks in a single thread, so no cache needed.
  (let [factory (get-topic-producer-connection config)]
    (with-open [connection (.createConnection factory)]
      (let [session (.createSession connection false, Session/AUTO_ACKNOWLEDGE)
            destination (.createTopic session (:topic-name config))
            consumer (.createConsumer session destination)]
        (.start connection)
        (loop [message (.receive consumer)]
          (process-f (json/read-str (.getText ^org.apache.activemq.command.ActiveMQTextMessage message) :key-fn keyword))
          (recur (.receive consumer)))))))

(defn enqueue
  "Send data to a queue. Data serialized to JSON."
  [data config]
  ; This is called repeatedly from any context, so we use the cached objects.
  (let [factory (get-queue-producer-connection config)]
    (with-open [connection (.createConnection factory)
                session (.createSession connection false, Session/AUTO_ACKNOWLEDGE)]
        (let [message (.createTextMessage session (json/write-str data))
              destination (.createQueue session (:queue-name config))
              producer (.createProducer session destination)]
    (.send producer message)))))


(defn entopic
  "Send data to a topic. Data serialized to JSON."
  [data config]
  ; This is called repeatedly from any context, so we use the cached objects.
  (let [factory (get-topic-producer-connection config)]
    (with-open [connection (.createConnection factory)
                session (.createSession connection false, Session/AUTO_ACKNOWLEDGE)]
        (let [message (.createTextMessage session (json/write-str data))
              destination (.createTopic session (:topic-name config))
              producer (.createProducer session destination)]
    (.send producer message)))))

