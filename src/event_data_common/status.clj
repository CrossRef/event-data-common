(ns event-data-common.status
  "Report to status service. Signs requests using secret."
  (:require [event-data-common.jwt :as jwt]
            [robert.bruce :refer [try-try-again]]
            [clojure.tools.logging :as log]
            [org.httpkit.client :as client]
            [config.core :refer [env]]))

(def jwt-auth (delay (jwt/build (:jwt-secrets env))))

; Create an empty token. Specific claims not required to send to status.
(def jwt-token (delay (jwt/sign @jwt-auth {})))

(defn send!
  "Send an update for a component/fragment/heartbeat"
  [service component fragment heartbeat-count]
  (let [the-path (str "/status/" service "/" component "/" fragment)]
    (try 
      (try-try-again {:sleep 10000 :tries 10} ; TODO
         #(let [result @(client/post (str (:status-service env) the-path)
                         {:headers {"Content-type" "text/plain" "Authorization" (str "Bearer " @jwt-token)}
                          :body (str heartbeat-count)})]
           (when-not (= (:status result) 201)
             (log/error "Can't send status update" the-path "response status:" (:status result))
             ; Exception caught by try-try-again n times. 
             (throw (new Exception "Couldn't send status update.")))))
     (catch Exception e (log/error "Gave up sending status update" the-path "response" e)))))
