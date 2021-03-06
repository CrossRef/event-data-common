(ns event-data-common.event-bus
  "Interact with the Event Bus.
   Retrieval of Events from the Archive is done by prefix.
   A callback to a lazy sequence, rather than returning that sequence, is done so that the stream can be closed after.
   The alternative is buffering a very large amount of data."
  (:require [cheshire.core :as cheshire]
            [clojure.math.combinatorics :as combinatorics]
            [event-data-common.date :as date]
            [config.core :refer [env]]
            [robert.bruce :refer [try-try-again]]
            [clj-time.core :as clj-time]
            [clojure.tools.logging :as log]
            [clj-http.client :as client]
            [clojure.java.io :as io]
            [event-data-common.jwt :as jwt]
            [com.climate.claypoole :as cp]
            [clojure.data.json :as json]
            [slingshot.slingshot :refer [throw+ try+]]))

(def jwt-verifier
  (delay (jwt/build (:global-jwt-secrets env))))

(def wildcard-jwt
  "A minimal JWT for access to the bus."
  (delay (jwt/sign @jwt-verifier {"sub" "*"})))

(def prefix-length 2)
(def hexadecimal [\0 \1 \2 \3 \4 \5 \6 \7 \8 \9 \a \b \c \d \e \f])

(defn event-bus-prefixes-length
  [length]
  (map #(apply str %) (combinatorics/selections hexadecimal length)))

(def prefixes (event-bus-prefixes-length prefix-length))

(def retry-delay
  "Time to sleep between unsuccessful HTTP requests.
   Configurable for tests."
  60000)

(defn retrieve-events-for-date-prefix
  "Retrieve a realized list of Events for this date and prefix of the archive, keywords for keys.
   Calback with lazy seq for flexibility."
  [the-date prefix callback]
  (let [date-str (date/->yyyy-mm-dd the-date)]
    (try-try-again
      {:sleep retry-delay :tries 10}
      (fn []
        (log/info "Try" date-str prefix)
        (let [url (str (:global-event-bus-base env) "/events/archive/" date-str "/" prefix)
              response (client/get url
                         {:as :stream
                          :timeout 900000
                          :headers
                          {"Authorization" (str "Bearer " @wildcard-jwt)}})]
            (log/info "Got" date-str prefix)
            (with-open [body (io/reader (:body response))]
              (let [stream (cheshire/parse-stream body keyword)]
                (callback (:events stream)))))))))

(defn retrieve-events-for-date
  "Retrieve all events for all prefixes of the given date, keywords for keys."
  [the-date callback]
  (cp/pdoseq 10 [prefix prefixes]
    (retrieve-events-for-date-prefix the-date prefix callback)))

(defn event-ids-for-prefix
  "Retrieve a list of IDs for Events for this date and prefix of the archive."
  [the-date prefix]
  (let [date-str (date/->yyyy-mm-dd the-date)]
    (try-try-again
      {:sleep retry-delay :tries 10}
      (fn []
        (log/info "Try" date-str prefix)
        (let [url (str (:global-event-bus-base env) "/events/archive/" date-str "/" prefix "/ids")
              response (client/get url
                         {:as :stream
                          :timeout 900000
                          :headers
                          {"Authorization" (str "Bearer " @wildcard-jwt)}})]
            (log/info "Got" date-str prefix)
            (with-open [body (io/reader (:body response))]
              (let [stream (cheshire/parse-stream body keyword)]
                (doall (:event-ids stream)))))))))

(defn event-ids-for-day
  "Retrieve a set of IDs for Events for this date and prefix of the archive."
  [the-date]
  (->> prefixes
       (cp/pmap 10 (partial event-ids-for-prefix the-date))
       (apply concat)
       set))

(defn get-event
  "Retrieve an Event by its ID."
  [event-id]
  (try-try-again
    {:sleep retry-delay :tries 10}
    (fn []
      (let [url (str (:global-event-bus-base env) "/events/" event-id)
            response (client/get url
                       {:as :stream
                        :timeout 900000})]
          (with-open [body (io/reader (:body response))]
            (cheshire/parse-stream body keyword))))))
              
(defn send-event
  "Update an extant Event."
  [event jwt]
  (try-try-again
      {:sleep retry-delay :tries 10}
      (fn []
        (client/put (str (:global-event-bus-base env) "/events/" (:id event))
        {:body (json/write-str event)
         :headers {"Authorization" (str "Bearer " jwt)}}))))


(defn jwt-for-source
  "Create a JWT that can "
  [source-id]
  (jwt/sign @jwt-verifier {"sub" source-id}))

(def jwt-for-source-cached
  (memoize jwt-for-source))

(defn post-event
  "Send an Event to the Bus. Derive an appropriate JWT. Ignore duplicates.
   Since this may potentially be called more than once with the same Event, it's OK to get a 409 duplicate."
  [event]
  (let [jwt (-> event :source_id jwt-for-source-cached)]
  
    (try-try-again
      {:sleep retry-delay :tries 10}
      (fn []
        (log/debug "Send event id" (:id event))

        (try+ 
          (client/post (str (:global-event-bus-base env) "/events")
            {:body (json/write-str event)
             :headers {"Authorization" (str "Bearer " jwt)}
             :throw-exceptions true})

           ; Duplicates are fine.
           (catch [:status 409] {:keys [request-time headers body]}
             (log/debug "Event ID " (:id event) "was duplicated, ignoring."))

           ; Anything else isn't.
           (catch Object ex
             (log/debug "Event ID " (:id event) "failed to send...")
             (throw+)))))))

