(ns event-data-common.storage.memory
  "In-memory storage. For unit testing ONLY."
  (:require [event-data-common.storage.store :refer [Store]]))

; An object that implements a Store (see `event-data-common.storage.store` namespace).
(defrecord Memory
  [data]
  
  Store
  (get-string [this k]
    (get @data k))

  (set-string [this k v]
    (swap! data assoc k v))

  (keys-matching-prefix [this the-prefix]
    (->> @data keys (filter #(.startsWith % the-prefix))))

  (delete [this k]
    (swap! data dissoc k)))
  
(defn build
  "Build a Memory object."
  []
  (Memory. (atom {})))
