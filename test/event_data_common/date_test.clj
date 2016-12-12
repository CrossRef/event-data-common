(ns event-data-common.date-test
  (:require [clojure.test :refer :all]
            [clj-time.core :as clj-time]
            [event-data-common.date :as date]))

(deftest ^:unit back-days
  (testing "Back-days should return correct values"
    (let [days 6
          date (clj-time/date-time 1986 02 05)
          result (date/back-days date days)]
      (is (= (set result) (set ["1986-02-05" "1986-02-04" "1986-02-03" "1986-02-02" "1986-02-01" "1986-01-31"]))))))

(deftest ^:unit adjacent-days
  (testing "Adjacent-days should return correct values"
    (let [result (date/adjacent-days "1986-02-01")]
      (is (= result ["1986-01-31" "1986-02-02"])))))
