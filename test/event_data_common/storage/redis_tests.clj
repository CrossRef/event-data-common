(ns event-data-common.storage.redis-tests
  "Component tests for the storage.redis namespace."
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [config.core :refer [env]]
            [event-data-common.storage.redis :as redis]
            [event-data-common.storage.store :as store]))

; Store interface

(def the-prefix "the-prefix")
(def default-db-str "0")

(defn build
  "Build Redis storage with config"
  []
  (redis/build the-prefix (:redis-host env) (Integer/parseInt (:redis-port env)) (Integer/parseInt (get env :redis-db default-db-str))))

(deftest ^:component incr-key-by
  (testing "Key be incremented and retrieved."
    (let [k "key-to-increment"
          ; build a redis connection with the present configuration.
          conn (build)]
      (is (= nil (store/get-string conn k)) "Nil returned for non-existent key")

      (is (= 7 (redis/incr-key-by!? conn k 7)) "incr-key-by!? from nil returns that number")

      (is (= "7" (store/get-string conn k)) "Number can be retrieved")

      (is (= 18 (redis/incr-key-by!? conn k 11)) "incr-key-by!? adds that number")

      (is (= "18" (store/get-string conn k)) "New number can be retrieved"))))

(deftest ^:component set-and-get
  (testing "Key can be set and retrieved."
    (let [k "this is my key"
          v "this is my value"
          ; build a redis connection with the present configuration.
          conn (build)]
      (store/set-string conn k v)
      (is (= v (store/get-string conn k)) "Correct value returned"))))

(deftest ^:component set-delete-and-get
  (testing "Key can be set and deleted retrieved."
    (let [k "this is my key2"
          v "this is my value2"
          ; build a redis connection with the present configuration.
          conn (build)]

      (is (nil? (store/get-string conn k)) "Key should not exist before.")

      (store/set-string conn k v)

      (is (= v (store/get-string conn k)) "Key exists after setting")

      (store/delete conn k)

      (is (nil? (store/get-string conn k)) "Key should not exist after deletion."))))

(deftest ^:component setex-and-get
  (testing "Key can be set with expiry and retrieved."
    (let [ki "this is my immediately expiring key"
          kl "this is my long expiring key"
          vi "this is my immediately expiring value"
          vl "this is my long expiring value"

          ; build a redis connection with the present configuration.
          conn (build)]
      ; Set immediately expiring key and one that expires after 100 seconds.
      (redis/set-string-and-expiry conn ki 1 vi)
      (redis/set-string-and-expiry conn kl 100000 vl)

      ; A brief nap should be OK.
      (Thread/sleep 2)

      (is (= nil (store/get-string conn ki)) "Expired value should not be returned")
      (is (= vl (store/get-string conn kl)) "Long expiring value should be returned"))))

(deftest ^:component keys-matching-prefix
  (testing "All keys matching prefix should be returned."
    ; Insert 10,000 keys that we do want to match and 10,000 that we don't.
    (let [conn (build)
          num-keys 10000
          included-keys (map #(str "included-" %) (range num-keys))
          not-included-keys (map #(str "not-included-" %) (range num-keys))]

      ; Clear all keys first.
      (doseq [k (store/keys-matching-prefix conn "")]
        (store/delete conn k))

      (store/set-string conn "single-test-object" "some data")

      (doseq [k included-keys]
        (store/set-string conn k "some data"))
      
      (doseq [k not-included-keys]
        (store/set-string conn k "some data"))
      
      (let [keys-matching (store/keys-matching-prefix conn "included-")]
        (is (= (count keys-matching) num-keys) "The right number of keys should be returned.")

        ; Every key we get should start with the right prefix.
        (is (every? true? (map #(.startsWith % "included-") keys-matching)))))))


; Redis interface

(deftest ^:component expiring-mutex
  (testing "Expiring mutex can only be set once in expiry time.")
  (let [conn (build)
        k "my key"]
    ; First set should be OK.
    (is (true? (redis/expiring-mutex!? conn k 2000)) "First set to mutex for key should be true.")

    ; Second should be false. Also reset timing of mutex.
    (is (false? (redis/expiring-mutex!? conn k 1)) "Second set to mutex for key should be false.")

    ; Let it expire for a couple of milliseconds.
    (Thread/sleep 2)

    (is (true? (redis/expiring-mutex!? conn k 2000)) "Access to mutex should be true after expiry.")))

(defn seq!!
  "Returns a (blocking!) lazy sequence read from a channel."
  [c]
  (lazy-seq
   (when-let [v (async/<!! c)]
     (cons v (seq!! c)))))

(deftest ^:component pub-sub
  (testing "Published events are broadcast to all listeners."
    (let [conn1 (build)
          conn2 (build)
          pubsub-channel-1 "Channel One"
          pubsub-channel-2 "Channel Two"

          message-1 "Hello"
          message-2 "Goodbye"

          ; three test core.async channels
          chan-1 (async/chan)
          chan-2 (async/chan)
          chan-both (async/chan)]

      ; Subscriptions block the thread.

      ; Subscriptions to put messages from pubsub channel 1 into channel 1.
      (async/thread (redis/subscribe-pubsub conn1 pubsub-channel-1 #(async/>!! chan-1 %)))

      ; Ditto channel 2
      (async/thread (redis/subscribe-pubsub conn1 pubsub-channel-2 #(async/>!! chan-2 %)))

      ; And two to put them both into channel 3.
      ; The client allows only one subscriber per channel, so these must be on a new client. This also demonstrates cross-client pubsub.
      (async/thread (redis/subscribe-pubsub conn2 pubsub-channel-1 #(async/>!! chan-both %)))
      (async/thread (redis/subscribe-pubsub conn2 pubsub-channel-2 #(async/>!! chan-both %)))

      ; Sleep for a second to allow the subscribers to register in their threads.
      ; This is not ideal, but otherwise we have a race condition in the test code where pubsub events are sent before the listeners have registered.
      (Thread/sleep 1000)

      ; Now send something from each connection.
      (redis/publish-pubsub conn1 pubsub-channel-1 message-1)
      (redis/publish-pubsub conn2 pubsub-channel-2 message-2)

      ; We expected to get two messages on channel 1.
      (is (= message-1 (async/<!! chan-1)) "Channel 1 should have message 1 twice")
      (is (= message-2 (async/<!! chan-2)) "Channel 1 should have message 2 twice")
      (is (= #{message-1 message-2} (set (take 2 (seq!! chan-both)))) "Channel 3 should have both messages"))))

; Internals

(def the-prefix-length (.length the-prefix))

(deftest ^:unit add-remove-prefix
  (testing "Prefix can be added and removed"
    (let [original "one two three"
          prefixed (redis/add-prefix the-prefix original)
          unprefixed (redis/remove-prefix the-prefix-length prefixed)]
      (is (not= original prefixed) "Prefix is added")
      (is (not= prefixed unprefixed) "Prefix is removed")
      (is (= original unprefixed) "Correct prefix is removed"))))


