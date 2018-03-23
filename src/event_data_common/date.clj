(ns event-data-common.date
  "Date and time handling functions.
   Some accept clj-time.date-time objects, some accept YYYY-MM-DD strings. Chosen according to convenience at call site."
  (:require [clj-time.core :as clj-time]
            [clj-time.format :as clj-time-format]
            [clj-time.coerce :as clj-time-coerce])
  (:require [config.core :refer [env]])
  (:gen-class))

(def yyyy-mm-dd-format (clj-time-format/formatter "yyyy-MM-dd"))

(defn back-dates
  "Generate a sequence of days back in history from given date as clj-time ."
  [date days]
  (map #(clj-time/minus date (clj-time/days %)) (range 0 days)))

(defn back-days
  "Generate a sequence of days back in history from given date as YYYY-MM-DD strings."
  [date days]
  (map (partial clj-time-format/unparse yyyy-mm-dd-format)
       (back-dates date days)))

(defn adjacent-days
  "Given a date-str, return a tuple of [day-before, day-after]"
  [date-str]
  (let [date (clj-time-format/parse yyyy-mm-dd-format date-str)]
    [(clj-time-format/unparse yyyy-mm-dd-format (clj-time/minus date (clj-time/days 1)))
     (clj-time-format/unparse yyyy-mm-dd-format (clj-time/plus date (clj-time/days 1)))]))


(defn ->yyyy-mm-dd
  [date-time]
  (clj-time-format/unparse yyyy-mm-dd-format date-time))
