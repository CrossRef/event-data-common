(ns event-data-common.storage.redis
  "Storage interface for redis. Provides two interfaces: RedisStore which conforms to Store, and Redis, which contains Redis-specific methods.
   RedisStore satisfies the event-data-common.storage.storage.Store protocol.
   All keys are stored in Redis with the given prefix."
  (:require 
            [event-data-common.storage.store :refer [Store]])
  (:import [redis.clients.jedis Jedis JedisPool JedisPoolConfig ScanResult ScanParams JedisPubSub]))


(defn remove-prefix
  [prefix-length ^String k]
  (when k (.substring k prefix-length)))

(defn add-prefix
  [^String prefix ^String k]
  (str prefix k))

(defn scan-match-cursor
  "Lazy sequence of scan results matching pattern."
  [connection pattern cursor]
  (let [scan-params (.match (new ScanParams) pattern)
        result (.scan connection cursor scan-params)
        items (.getResult result)
        next-cursor (.getCursor result)]
    ; Zero signals end of iteration.
    (if (zero? next-cursor)
      items
      (lazy-cat items (scan-match-cursor connection pattern next-cursor)))))

(defn sscan-cursor
  "Lazy sequence of sscan (set scan) results."
  [connection k cursor]
  (let [;scan-params (.match (new ScanParams) pattern)
        result (.sscan connection k (str cursor))
        items (.getResult result)
        next-cursor (.getCursor result)]
    ; Zero signals end of iteration.
    (if (zero? next-cursor)
      items
      (lazy-cat items (sscan-cursor connection k next-cursor)))))

(defn zscan-cursor
  "Return a realized map of {set-member score}"
  ([connection k] (zscan-cursor connection k 0 {}))
  ([connection k cursor prev-items]
    (let [;scan-params (.match (new ScanParams) pattern)
          result (.zscan connection k (str cursor))
          ; items is a seq of Tuple 
          items (into {} (map #(vector (.getElement %) (.getScore %)) (.getResult result)))

          new-items (conj prev-items items)
          next-cursor (.getCursor result)]
      ; Zero signals end of iteration.
      (if (zero? next-cursor)
        new-items
        (zscan-cursor connection k next-cursor new-items)))))

(defn make-jedis-pool
  [host port]
  (let [pool-config (new org.apache.commons.pool2.impl.GenericObjectPoolConfig)]
    (.setMaxTotal pool-config 100)
    (.setTestOnBorrow pool-config true)

  (new JedisPool pool-config host port)))

(defn ^Jedis get-connection
  "Get a Redis connection from the pool. Must be closed."
  [^JedisPool pool db-number]
  (let [^Jedis resource (.getResource pool)]
    (.select resource db-number)
    resource))

(defprotocol Redis
  "Redis-specific interface."
  (set-string-and-expiry [this k v milliseconds] "Set string value with expiry in milliseconds.")

  (set-string-and-expiry-seconds [this k v seconds] "Set string value with expiry in seconds.")

  (expire-seconds! [this k seconds] "Set expiry in seconds.")

  (expiring-mutex!? [this k milliseconds] "Check and set expiring mutex atomically, returning true if didn't exist.")

  (incr-key-by!? [this k value] "Set and return incremented Long value.")

  (publish-pubsub [this channel value] "Broadcast over PubSub")

  (subscribe-pubsub [this channel callback] "Register callback for PubSub channel and block thread.")

  (set-add [this k value] "Add a value to a set.")

  (set-members [this k] "Return all members from a set.")

  (sorted-set-increment [this k kk value] "Increment the field of a sorted set by the value.")

  (sorted-set-put [this k member value] "Set the field of the sorted set to have the value.")

  (sorted-set-members [this k] "Return all members of the sorted set with their values."))

; An object that implements a Store (see `event-data-common.storage.store` namespace).
; Not all methods are recommended for use in production, some are for component tests.
(defrecord RedisConnection
  [^JedisPool pool db-number prefix prefix-length]
  
  Store
  (get-string [this k]
    (with-open [conn (get-connection pool db-number)]
      (.get conn (add-prefix prefix k))))

  (set-string [this k v]
    (with-open [conn (get-connection pool db-number)]
      (.set conn (add-prefix prefix k) v)))

  (keys-matching-prefix [this the-prefix]
    ; Because we add a prefix to everything in Redis, we need to add that first.
    (with-open [conn (get-connection pool db-number)]
      (let [match (str (add-prefix prefix the-prefix) "*")
            found-keys (scan-match-cursor conn match 0)
            ; remove prefix added here, not the one being searched for.
            all-keys (map #(remove-prefix prefix-length %) found-keys)

            ; Redis may return the same key twice: https://redis.io/commands/scan
            all-unique-keys (set all-keys)]
          all-unique-keys)))

  (delete [this k]
    (with-open [conn (get-connection pool db-number)]
      (.del conn (add-prefix prefix k))))

  Redis
  (set-string-and-expiry [this k milliseconds v]
    (with-open [conn (get-connection pool db-number)]
      (.psetex conn (add-prefix prefix k) milliseconds v)))

  (set-string-and-expiry-seconds [this k seconds v]
    (with-open [conn (get-connection pool db-number)]
      (.setex conn (add-prefix prefix k) seconds v)))

  (expire-seconds! [this k seconds]
    (with-open [conn (get-connection pool db-number)]
      (.expire conn (add-prefix prefix k) seconds)))

  (expiring-mutex!? [this k milliseconds]
    (with-open [conn (get-connection pool db-number)]
      ; Set a token value. SETNX returns true if it wasn't set before.
      (let [success (= 1 (.setnx conn (add-prefix prefix k) "."))]
        (.pexpire conn (add-prefix prefix k) milliseconds)
        success)))

  (incr-key-by!? [this k value]
    (with-open [conn (get-connection pool db-number)]
      (let [result (.incrBy conn (add-prefix prefix k) (long value))]
        result)))

  (publish-pubsub [this channel value]
    (with-open [conn (get-connection pool db-number)]
      (.publish conn channel value)))

  (subscribe-pubsub [this channel-name callback]
    (with-open [conn (get-connection pool db-number)]
      (.subscribe conn
        (proxy [JedisPubSub] []
          (onMessage [^String channel ^String message]
            (callback message)))
        (into-array String [channel-name]))))

  (set-add [this k value]
    (with-open [conn (get-connection pool db-number)]
      (.sadd conn (add-prefix prefix k) (into-array String [value]))))

  (set-members [this k]
    (with-open [conn (get-connection pool db-number)]
      ; Not guaranteed to get no duplicates, so put into a set. 
      ; This also forces evaluation of lazy sequence before the connection is closed.
      (set (sscan-cursor conn (add-prefix prefix k) 0))))

  (sorted-set-increment [this k member value]
    (with-open [conn (get-connection pool db-number)]
      (.zincrby conn (add-prefix prefix k) (double value) member)))
    
  (sorted-set-put [this k member value] "Set the member of the sorted set to have the value."
    (with-open [conn (get-connection pool db-number)]
      (.zadd conn (add-prefix prefix k) (double value) (str member))))

  (sorted-set-members [this k] "Return all members of the sorted set with their values."
    (with-open [conn (get-connection pool db-number)]
      (zscan-cursor conn (add-prefix prefix k))))) 

(defn build
  "Build a RedisConnection object."
  [prefix host port db-number]
  (let [pool (make-jedis-pool host port)
        prefix-length (.length prefix)]
    (RedisConnection. pool db-number prefix prefix-length)))
