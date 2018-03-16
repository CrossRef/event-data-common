(ns event-data-common.query-api
  "Access the Query API"
  (:require [event-data-common.date :as date]
            [clj-http.client :as client]
            [clojure.tools.logging :as log]
            [robert.bruce :refer [try-try-again]]
            [clojure.data.json :as json]
            [clojure.java.io :as io])
  (:gen-class))

(def url "https://api.eventdata.crossref.org")

(defn fetch-query-body
  "Fetch a HTTP body from the Query API."
  [endpoint query-params]
  (let [url (str url endpoint)
        body (try-try-again
               {:sleep 60000 :tries 1}
                  (fn []
                    (log/debug "Try url" url "query" query-params)
                    (let [response (client/get url 
                         {:query-params
                          (merge 
                            {:mailto "eventdata@crossref.org"}
                            query-params)
                          :as :stream
                          :timeout 90000})]
                      (when-not (= 200 (:status response))
                        (log/error "ERROR STATUS " (:status response))
                        (log/error "Error" (:status response))
                        (throw (Exception.)))

                    (-> response :body io/reader (json/read :key-fn keyword)))))]
      body))

(defn fetch-query-api
  "Fetch a lazy seq of Events from the Query API that match the filter.
   endpoint should be e.g. /v1/events
   message-key should be the key under the message object. e.g. :events"

  ([endpoint query-params message-key] (fetch-query-api endpoint query-params message-key "" 0))
  ([endpoint query-params message-key cursor cnt]
    (log/info "Fetch Query API:" endpoint "params:" query-params "cursor:" cursor)
    (let [query-params (merge query-params {:cursor cursor})
          body (fetch-query-body endpoint query-params)

          items (-> body :message message-key)
          next-cursor (-> body :message :next-cursor)
          cnt (+ cnt (count items))
          total (-> body :message :total-results)]

      (when (and total
                 (> total 0))
      
        (log/info "Retrieved" cnt "/" total "=" (int (* 100 (float (/ cnt total)))) "%"))

      (if next-cursor
        (lazy-cat items (fetch-query-api endpoint query-params message-key next-cursor cnt))
        items))))

(defn event-ids-for-day
  "Return set of all event IDs for the given date.
   Looks in the 'standard' and 'deleted' collections, as together these represent all Events."
  [date]
  (let [date-str (date/->yyyy-mm-dd date)
        standard-results (fetch-query-api "/v1/events/ids"
                                          {:from-collected-date date-str :until-collected-date date-str :rows 1000}
                                          :event-ids)

        deleted-results (fetch-query-api "/v1/events/deleted/ids"
                                         {:from-collected-date date-str :until-collected-date date-str :rows 1000}
                                         :event-ids)]

    (set (concat standard-results deleted-results))))
    
