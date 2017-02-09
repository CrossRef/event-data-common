(ns event-data-common.storage.memory-tests
  "Unit tests for the storage.memory namespace."
  (:require [clojure.test :refer :all]
            [event-data-common.storage.memory :as memory]
            [event-data-common.storage.store :as store]))

(deftest ^:unit set-and-get
  (testing "Key can be set and retrieved."
    (let [k "this is my key"
          v "this is my value"
          conn (memory/build)]
      (store/set-string conn k v)
      (is (= v (store/get-string conn k)) "Correct value returned"))))

(deftest ^:unit set-delete-and-get
  (testing "Key can be set and deleted retrieved."
    (let [k "this is my key2"
          v "this is my value2"
          conn (memory/build)]

      (is (nil? (store/get-string conn k)) "Key should not exist before.")

      (store/set-string conn k v)

      (is (= v (store/get-string conn k)) "Key exists after setting")

      (store/delete conn k)

      (is (nil? (store/get-string conn k)) "Key should not exist after deletion."))))

(deftest ^:unit keys-matching-prefix
  (testing "All keys matching prefix should be returned."
    ; Insert 10,000 keys that we do want to match and 10,000 that we don't.
    (let [conn (memory/build)
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
