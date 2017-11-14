(ns event-data-common.artifact
  "Retrieve things from the Artifact Repository."
  (:require [event-data-common.jwt :as jwt]
            [robert.bruce :refer [try-try-again]]
            [clojure.tools.logging :as log]
            [clj-http.client :as client]
            [clojure.data.json :as json]
            [config.core :refer [env]]))

(defn fetch-index
  "Retrieve the Artifact index."
  []
  (let [the-path (str (:global-artifact-url-base env) "/a/artifacts.json")]
    (try 
      (try-try-again {:sleep 10000 :tries 10}
         #(let [result (client/get the-path)
                parsed (when-let [body (:body result)] (json/read-str body))]
           (when-not (= (:status result) 200)
             (log/error "Can't get Artifact list from" the-path "response status:" (:status result))
             ; Exception caught by try-try-again n times. 
             (throw (new Exception "Couldn't get Artifact list.")))
           parsed))
     (catch Exception e
      (do
        (log/error "Gave up getting Artifact List" the-path "response" e)
        (throw e))))))


(defn fetch-artifact-names
  []
  (keys (get-in (fetch-index) ["artifacts"])))

(defn fetch-latest-version-link
  "Get the URL of the latest version of the Artifact, or nil."
  [artifact-name]
  (get-in (fetch-index) ["artifacts" artifact-name "current-version-link"]))

(defn fetch-latest-artifact
  "Get the content of the latest version of the artifact.
   typ should be one of [:stream or :text]
   Return tuple of [version-url, content]"
   [artifact-name typ]
   (when-let [the-path (fetch-latest-version-link artifact-name)]
    (try 
      (try-try-again {:sleep 10000 :tries 10}
         #(let [result (client/get the-path {:as typ})]
           (when-not (= (:status result) 200)
            
             (log/error "Can't get Artifact list from" the-path "response status:" (:status result))
             ; Exception caught by try-try-again n times. 
             (throw (new Exception "Couldn't get Artifact list.")))

           (:body result)))
     (catch Exception e
      (do
        (log/error "Gave up getting Artifact List" the-path "response" e)
        (throw e))))))

(defn fetch-latest-artifact-string
  [artifact-name]
  "Get the content of the latest version of the artifact as a string. Only for small artifacts!"
  (fetch-latest-artifact artifact-name :text))


(defn fetch-latest-artifact-stream
  [artifact-name]
  "Get the content of the latest version of the artifact as a stream."
  (fetch-latest-artifact artifact-name :stream))


(defn fetch-content-heuristics
  "Fetch the latest content-heuristics Artifact.
   Transform those parts that should be sets into sets.
   Format is:
   {:prefixes set-of-prefixes
    :domains set-of-domains
    :domain-prefixes {domain set-of-prefixes}"
  [])

(defn content-heuristics->prefixes
  "Given content-heuristics artifact content, return a set of DOI prefixes."
  [input])

(defn content-heuristics->domains
  "Given content-heuristics artifact content, return a set of domain names."
  [input])

(def threshold
  0.2)

(defn content-heuristics->domain-prefix
  "Given content-heuristics artifact content, return a mapping of {domain: prefix-set}.
   This records which domains have reliably been seen to report that they contain content that "
  [input])

