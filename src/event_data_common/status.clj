(ns event-data-common.status
  "Report to status service. Signs requests using secret."
  (:require [event-data-common.jwt :as jwt]
            [event-data-common.backoff :as backoff]
            [robert.bruce :refer [try-try-again]]
            [clojure.tools.logging :as log]
            [clj-http.client :as client]
            [config.core :refer [env]]))

(def jwt-auth (delay (jwt/build (:jwt-secrets env))))

; Get a token. Either accept one from env, or make an empty one using the secret.
; Specific claims not required to send to status.
(def jwt-token (delay (or (:jwt-token env)
                          (jwt/sign @jwt-auth {}))))

; This can be reset in unit tests.
(def wait-delay (atom 10000))

(defn send!
  "DEPRECATED. Chose add! or replace!."
  [service component fragment heartbeat-count]
  (when-let [base (:status-service env)]
    (let [the-path (str "/status/" service "/" component "/" fragment)]
      (backoff/try-backoff
        #(let [result (client/post (str base the-path)
                           {:headers {"Content-type" "text/plain" "Authorization" (str "Bearer " @jwt-token)}
                            :body (str heartbeat-count)})]
             (when-not (= (:status result) 201)
               (log/error "Can't send status update" the-path "response status:" (:status result))
               ; Exception caught by try-try-again n times. 
               (throw (new Exception "Couldn't send status update."))))
          @wait-delay
          ; Only try 3 times. If the status service is unavailable, tough luck.
          ; A lot of data is passed to it, and it's time-sensitive and not mission-critical.
          ; Unsent heartbeats are a significant signal.
          3
          #(log/info "Failed to send status update" (.getMessage %1) "times :" the-path)
          #(log/info "Gave up sending status update:" the-path)
          #()))))

(defn add!
  "Send an update for a component/fragment/heartbeat"
  [service component fragment heartbeat-count]
  (when-let [base (:status-service env)]
    (let [the-path (str "/status/" service "/" component "/" fragment)]
      (backoff/try-backoff
        #(let [result (client/post (str base the-path)
                           {:headers {"Content-type" "text/plain" "Authorization" (str "Bearer " @jwt-token)}
                            :body (str heartbeat-count)})]
             (when-not (= (:status result) 201)
               (log/error "Can't send status update" the-path "response status:" (:status result))
               ; Exception caught by try-try-again n times. 
               (throw (new Exception "Couldn't send status update."))))
          @wait-delay
          ; Only try 3 times. If the status service is unavailable, tough luck.
          ; A lot of data is passed to it, and it's time-sensitive and not mission-critical.
          ; Unsent heartbeats are a significant signal.
          3
          #(log/info "Failed to send status update" (.getMessage %1) "times :" the-path)
          #(log/info "Gave up sending status update:" the-path)
          #()))))

(defn replace!
  "Send an update for a component/fragment/heartbeat"
  [service component fragment heartbeat-count]
  (when-let [base (:status-service env)]
    (let [the-path (str "/status/" service "/" component "/" fragment)]
      (backoff/try-backoff
        #(let [result (client/put (str base the-path)
                           {:headers {"Content-type" "text/plain" "Authorization" (str "Bearer " @jwt-token)}
                            :body (str heartbeat-count)})]
             (when-not (= (:status result) 201)
               (log/error "Can't send status update" the-path "response status:" (:status result))
               ; Exception caught by try-try-again n times. 
               (throw (new Exception "Couldn't send status update."))))
          @wait-delay
          ; Only try 3 times. If the status service is unavailable, tough luck.
          ; A lot of data is passed to it, and it's time-sensitive and not mission-critical.
          ; Unsent heartbeats are a significant signal.
          3
          #(log/info "Failed to send status update" (.getMessage %1) "times :" the-path)
          #(log/info "Gave up sending status update:" the-path)
          #()))))
