(ns event-data-common.url-cleanup
  "Clean up URLs.
   Currently removes known tracking parameters."
  (:require [clojure.tools.logging :as log])
  (:import [java.net URL URI]
           [org.apache.http.client.utils URLEncodedUtils URIBuilder])
   (:gen-class))

(def ignore-parameter-res
  "Regular expressions of URL parameters that should be ignored.
   These are parameters that are used for tracking."
  [
    #"utm_.*"      ; Google Analytics / Urchin
    #"WT\..*"      ; WebTrends
    #"dm_.*"       ; DotMailer
    #"pk_.*"       ; Piwik
    #"mc_.*"       ; Mailchimp
    #"campaign_.*" ; iOS
  ])

(def ignore-parameter-re
  (re-pattern (str "^" (clojure.string/join "|" ignore-parameter-res) "$")))

(defn remove-tracking-params
  "Given a URL and the set of tracking params, remove tracking params."
  [url]
  (try
    (let [original-uri (new URI url)
          params (URLEncodedUtils/parse original-uri "UTF-8")
          
          ; Remove those parameters that we don't want.
          filtered-params (remove #(re-matches ignore-parameter-re (.getName %)) params)

          ; URIBuilder adds a path value of "/" when input path is "".
          ; Explicity set null if there is none.
          url-path (not-empty (.getPath original-uri))

          new-uri (-> (URIBuilder. original-uri)
                      
                      ; If it turns out that there are no parameters [after removal] then
                      ; we need to explicitly remove them.
                      ; This ensures we don't get a trailing "?" with no parameters.
                      (#(if-not (empty? filtered-params)
                          (.setParameters % filtered-params)
                          (.removeQuery %)))

                      (.setPath url-path)

                      (.build))]
      (str new-uri))

    ; This can happen because the URL isn't a vailid URI, e.g. illegal chars in fragment.
    ; No big deal, this is best-effort. 
    (catch Exception ex
      (do
        (log/info "Failed to remove tracking params" url)
        url))))
