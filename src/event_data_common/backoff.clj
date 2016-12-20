(ns event-data-common.backoff
  "Try a function, with square backoff.
   Uses a threadpool, so can run lots of concurrent things at once."
  (:require [clojure.core.async :as async]
            [overtone.at-at :as at-at]))

(def pool (at-at/mk-pool))

(defn try-backoff
  "Try a function. If it throws an exception, back off.
  The exception is passed to error-f. If it returns true, keep trying.
  If errors continue to max-attempts, call terminate-f.
  Finally-f is called on finish, on success or failure.

  Back-off starts with sleep-ms milliseconds and doubles each time."
  ; Don't use try-try-again because it blocks the thread.
  [f sleep-ms max-attempts error-f terminate-f finally-f]
  ; Recurse until no more attempts.
  (if (zero? max-attempts)
    (do
      (terminate-f)
      (finally-f))
    (try
      (f)
      ; If an exception is raised, finally-f isn't called this time round.
      (finally-f)
      (catch Exception e
        (do
          (error-f e)
          (at-at/after sleep-ms
                       #(try-backoff f (* 2 sleep-ms) (dec max-attempts) error-f terminate-f finally-f)
                       pool))))))

(defn get-pool-size
  []
  (count (at-at/scheduled-jobs pool)))

