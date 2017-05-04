(ns event-data-common.queue
  "ActiveMQ functions."
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

(def amq-connection-factory
  (delay (org.apache.activemq.ActiveMQConnectionFactory.
           (:activemq-username env)
           (:activemq-password env)
           (:activemq-url env))))

(defn build-connection-objects [queue-name]
  "Build a hashmap of connection objects. This object can be cached. See http://activemq.apache.org/how-do-i-use-jms-efficiently.html "
  (let [connection (.createConnection @amq-connection-factory)
        session (.createSession connection false, Session/AUTO_ACKNOWLEDGE)
        destination (.createQueue session queue-name)
        producer (.createProducer session destination)]
  {:connection connection :session session :destination destination :producer producer}))

(def queue-connections (ThreadLocal.))

(defn get-queue-producer-connection
  "Get a hashmap of connection objects ({:connection :session :destination :producer}) by queue name. Cache by Thread."
  [queue-name]
  ; Use an atom to avoid race conditions within threads. Possible in theory with core.async.
  (when (nil? (.get queue-connections))
    (.set queue-connections (atom {})))

  (if-let [connection (get @(.get queue-connections) queue-name)]
    connection
    (let [new-connection (build-connection-objects queue-name)]
      (swap! (.get queue-connections) assoc queue-name new-connection)
      new-connection)))

(defn process-queue
  "Process a queue, a callback is called for every item with structure deserialized from data, assumed to be JSON."
  [queue-name process-f]
  (log/info "Starting to process queue" queue-name)
  ; This runs and blocks in a single thread, so no cache needed.
  (with-open [connection (.createConnection @amq-connection-factory)]
    (let [session (.createSession connection false, Session/AUTO_ACKNOWLEDGE)
          destination (.createQueue session queue-name)
          consumer (.createConsumer session destination)]
      (.start connection)
      (loop [message (.receive consumer)]
        (process-f (json/read-str (.getText ^org.apache.activemq.command.ActiveMQTextMessage message) :key-fn keyword))
        (recur (.receive consumer))))))

(defn enqueue
  "Send data to a queue. Data serialized to JSON."
  [data queue-name]
  ; This is called repeatedly from any context, so we use the cached objects.
  (let [connection (get-queue-producer-connection queue-name)
        message (.createTextMessage (:session connection) (json/write-str data))]
    (.send (:producer connection) message)))
