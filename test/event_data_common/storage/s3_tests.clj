(ns event-data-common.storage.s3-tests
  "Component tests for the storage.s3 namespace."
  (:require [clojure.test :refer :all]
            [config.core :refer [env]]
            [event-data-common.storage.s3 :as s3]
            [event-data-common.storage.store :as store]))

(defn build
  "Build S3 storage with config"
  []
  (s3/build (:s3-key env) (:s3-secret env) (:s3-region-name env) (:s3-bucket-name env)))

(deftest ^:integration set-and-get
  (testing "Key can be set and retrieved."
    (let [k "this is my key"
          v "this is my value"
          ; build a s3 connection with the present configuration.
          conn (build)]
      (store/set-string conn k v)
      (is (= v (store/get-string conn k)) "Correct value returned")

      ; Then delete
      (store/delete conn k)
      (is (nil? (store/get-string conn k)) "Key correctly deleted"))))


(defn parallel-doseq
  "Apply f to every element of coll as quickly as possible in as many threads as possible."
  [coll f]
  (let [futures (map #(future (f %)) coll)]
    (doseq [futur futures]
      (deref futur))))

; Test both the storage/keys-matching-prefix and the S3-only s3/list-with-delimiter
(deftest ^:integration keys-matching-prefix
    ; Insert 1,500 keys that we do want to match and 1,500 that we don't.
    (let [conn (build)
          num-keys 1500
          included-keys (map #(str "included/item-" %) (range num-keys))
          not-included-keys (map #(str "not-included-" %) (range num-keys))

          c (atom 0)]
      
      ; Clear the bucket.
      ; It's best to do this outside the tests before they are run, e.g. with the AWS command line tools.
      ; Because S3 doesn't have strict deleted-after-delete semantics, this may occasionally fail.
      (parallel-doseq (store/keys-matching-prefix conn "")
                      (fn [k]
                        (locking *out*
                          (prn (swap! c inc) "/" (count (store/keys-matching-prefix conn ""))))
                        (store/delete conn k)))

      (store/set-string conn "single-test-object" "some data")

    (testing "All keys matching prefix should be returned."
      (let [keys-matching (store/keys-matching-prefix conn "")]
        (is (= 1 (count keys-matching)) "Bucket should initially be empty, adding a single key should result in a single key."))

      (doseq [k included-keys]
        (store/set-string conn k "some data"))
      
      (doseq [k not-included-keys]
        (store/set-string conn k "some data"))
      
      (let [keys-matching (store/keys-matching-prefix conn "included")]
        (is (= (count keys-matching) num-keys) "The right number of keys should be returned.")

        ; Every key we get should start with the right prefix.
        (is (every? true? (map #(.startsWith % "included") keys-matching)))))

    (testing "Can retrieve directory-like listing with list-with-delimiter"
      (let [list-with-delimter-result (s3/list-with-delimiter conn "included" "/")]
        (is (= ["included/"] list-with-delimter-result) "list-with-delimiter should stop at delimiter")))))

