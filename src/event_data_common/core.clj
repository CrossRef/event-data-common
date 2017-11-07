(ns event-data-common.core
  (:require [taoensso.timbre :as timbre])
  (:gen-class))

(defn init
  "Initialize whatever needs initializing.
   Currently this is just logging."
  []
  (timbre/merge-config!
    {:level :info
     :ns-blacklist []}))


