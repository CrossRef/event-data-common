(ns event-data-common.core
  (:require [taoensso.timbre :as timbre]
            [overtone.at-at :as at-at]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json])
  (:gen-class))

(def pool (delay (at-at/mk-pool)))

(defn gigabyte
  [x]
  (float (/ x 1000000000)))

(defn log-memory
  []
  (let [runtime (Runtime/getRuntime)
          free-bytes (.freeMemory runtime)
          total-bytes (.totalMemory runtime)
          used-bytes (- total-bytes free-bytes)
          
          max-bytes (.maxMemory runtime)]

      (log/info (json/write-str
                 {:type "Memory"
                  ; GB free in currently reserved heap.
                  :free (gigabyte free-bytes)
                  ; GB currently used.
                  :used (gigabyte used-bytes)
                  ; GB currently reserved to heap.
                  :total (gigabyte total-bytes)
                  ; GB maximum heap could get to.
                  :max (gigabyte max-bytes)
                  ; Amount of reserved heap used.
                  :max-ratio (float (/ used-bytes max-bytes))
                  ; Amount of possible memory used.
                  :total-ratio (float (/ used-bytes total-bytes))}))))

(defn start-memory-log
  "Start a scheduled log of memory once a minute."
  []
  (at-at/every 60000 log-memory @pool))
    

(defn init
  "Initialize common functions.
    - Default logging configuration.
    - Start memory logging schedule."
  []
  (timbre/merge-config!
    {:level :info
     :ns-blacklist []})
  (start-memory-log))

