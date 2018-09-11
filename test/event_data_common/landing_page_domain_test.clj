(ns event-data-common.landing-page-domain-test
  (:require [clojure.test :refer :all]
            [event-data-common.artifact :as artifact]
            [clj-time.core :as clj-time]
            [clojure.data.json :as json]
            [event-data-common.landing-page-domain :as lp]
            [clojure.java.io :refer [reader resource]]))

(deftest ^:unit retrieve-domain-structure
  (with-redefs [artifact/fetch-latest-version-link (constantly "http://example.com/domain-decision-structure/1")
                artifact/fetch-latest-artifact-stream (fn [url]
                                                        (-> "artifacts/domain-decision-structure.json"
                                                            resource reader))]

    (let [[version-url content] (lp/retrieve-domain-decision-structure)]
      (is (= version-url "http://example.com/domain-decision-structure/1") "Version URL should be returned.")
      (is (-> "domains" content set?) "Domains are represented as a set.")
      (is (-> "prefixes" content set?) "Prefixes are represented as a set.")
      (is (= (get-in content ["domains-prefixes" "www.example.com" "10.5555" "domain-recognised"])
             true)
             "Tree structure can be traversed.")

      (let [prev-structure {:this :that :one :two}]
        (is (= (lp/assoc-domain-decision-structure prev-structure)
               {:this :that
                :one :two
                :domain-decision-structure-artifact-version "http://example.com/domain-decision-structure/1"
                :domain-decision-structure content})
            "URLs should be set, content of context should remain.")))))

(def test-structure
  (-> "artifacts/domain-decision-structure.json"
      resource
      reader
      json/read
      lp/parse-domain-decision-structure))

(def context
  {:domain-decision-structure test-structure})

(deftest ^:unit domain-recognised?
  (testing "url-recognised should return true when the URL matches the domain list"
    (is (true? (lp/domain-recognised? context "https://www.xyz.net/this/that")))
    (is (true? (lp/domain-recognised? context "http://www.xyz.net")))
    (is (false? (lp/domain-recognised? context "https://www.abc.net/not-this")) "Domain not on list should return false")))

(deftest ^:unit domain-confirmed-for-doi?
  (testing "domain-confirmed-for-doi? should lookup confirmation status for domain / DOI prefix combination"
    (is (true? (lp/domain-confirmed-for-doi? context "http://www.example.com" "https://doi.org/10.5555/12345678")))
    (is (true? (lp/domain-confirmed-for-doi? context "http://www.xyz.net" "https://doi.org/10.5555/12345678")))
    (is (false? (lp/domain-confirmed-for-doi? context "http://zombo.com" "https://doi.org/10.5555/12345678"))
        "Wrong domain for the prefix.")))

(deftest ^:unit domain-recognised-for-doi?
  (testing "domain-recognised-for-doi? should lookup recognition status for domain / DOI prefix combination"
    (is (true? (lp/domain-recognised-for-doi? context "http://www.example.com" "https://doi.org/10.5555/12345678")))
    (is (true? (lp/domain-recognised-for-doi? context "http://www.xyz.net" "https://doi.org/10.5555/12345678")))
    (is (false? (lp/domain-recognised-for-doi? context "http://zombo.com" "https://doi.org/10.7777/12345678")))))


(deftest ^:unit get-recognised-domains
  (testing "get-recognised-domains returns all domains"
    (is (= (lp/get-recognised-domains context) #{"www.zombo.com" "www.xyz.net" "www.example.com"}))))

