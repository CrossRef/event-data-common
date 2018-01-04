(ns event-data-common.core
  (:require [taoensso.timbre :as timbre]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json])
  (:gen-class))

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
  ; Use a simple daemon thread rather than a thread pool so we can be sure that it
  ; doesn't block the main thread, allowing the process to exit when the normal main method
  ; exits. Also a thread pool would be pointless as there is exactly one of these.
  (clojure.core.async/thread
    (loop []
      (log-memory)
      (Thread/sleep 60000)
      (recur))))

(defn init
  "Initialize common functions.
    - Default logging configuration.
    - Start memory logging schedule."
  []
  (timbre/merge-config!
    {:level :info
     :ns-blacklist []})
  (start-memory-log))

