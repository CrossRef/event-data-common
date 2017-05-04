(ns event-data-common.queue
  "ActiveMQ functions. An ActiveMQ queue is specified as a config hash-map of {:username :password :queue-name :url}.
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
           [javax.jms Session])
  (:gen-class))


(defn build-connection-objects [config]
  "Build a hashmap of connection objects. This object can be cached. See http://activemq.apache.org/how-do-i-use-jms-efficiently.html "
  (let [factory (org.apache.activemq.ActiveMQConnectionFactory.
                 (:username config)
                 (:password config)
                 (:url config))
        connection (.createConnection factory)
        session (.createSession connection false, Session/AUTO_ACKNOWLEDGE)
        destination (.createQueue session (:queue-name config))
        producer (.createProducer session destination)]
  {:connection connection :session session :destination destination :producer producer}))

(def queue-connections (ThreadLocal.))

(defn get-queue-producer-connection
  "Get a hashmap of connection objects ({:connection :session :destination :producer}) by queue config object. Cache by Thread."
  [config]
  ; Use an atom to avoid race conditions within threads. Possible in theory with core.async.
  (locking queue-connections
    (when (nil? (.get queue-connections))
      (.set queue-connections (atom {}))))

  (if-let [connection (get @(.get queue-connections) config)]
    connection
    (let [new-connection (build-connection-objects config)]
      (swap! (.get queue-connections) assoc config new-connection)
      new-connection)))

(defn process-queue
  "Process a queue, a callback is called for every item with structure deserialized from data, assumed to be JSON."
  [config process-f]
  (log/info "Starting to process queue" config)
  ; This runs and blocks in a single thread, so no cache needed.
  (let [factory (org.apache.activemq.ActiveMQConnectionFactory.
                 (:username config)
                 (:password config)
                 (:url config))]
    (with-open [connection (.createConnection factory)]
      (let [session (.createSession connection false, Session/AUTO_ACKNOWLEDGE)
            destination (.createQueue session (:queue-name config))
            consumer (.createConsumer session destination)]
        (.start connection)
        (loop [message (.receive consumer)]
          (process-f (json/read-str (.getText ^org.apache.activemq.command.ActiveMQTextMessage message) :key-fn keyword))
          (recur (.receive consumer)))))))

(defn enqueue
  "Send data to a queue. Data serialized to JSON."
  [data config]
  ; This is called repeatedly from any context, so we use the cached objects.
  (let [connection (get-queue-producer-connection config)
        message (.createTextMessage (:session connection) (json/write-str data))]
    (.send (:producer connection) message)))
