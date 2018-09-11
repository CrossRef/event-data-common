(ns event-data-common.landing-page-domain
  "Work with landing page domains.
   The landing-page-structure Artifact is periodically retrieved
   and cached, and this is used for decision-making.
   While the value is stored in this namespace, it is associated
   into the context object so the precise version is well-known
   and on the record."
  (:require [event-data-common.artifact :as artifact]
            [clojure.core.memoize :as memo]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [event-data-common.evidence-log :as evidence-log]
            [crossref.util.doi :as crdoi])
  (:import [java.net URL URI]))

(defn parse-domain-decision-structure
  "Parse a deserialized JSON structure into something we can query.
   We expect the Artifact to have the following fields:
   'domains' - the list of recognised domains
   'prefixes' - the list of recognised DOI prefixes
   'domains-prefixes' - Structure of domain -> prefix -> heuristics.
   Heuristics are:
   'domain-recognised' - Have we seen this domain redirected to from this prefix?
   'domain-confirmed' - Have we seen pages on this domain correctly display their DOI?

   Transform the structure into almost exactly the same, but turn domains 
   and prefxies into sets."
  [input]
  (-> input
    (assoc "domains" (set (get input "domains")))
    (assoc "prefixes" (set (get input "prefixes")))

    ; Swap out string key for symbol, but the keys in the tree below this point
    ; remain strings.
    (assoc "domains-prefixes" (get input "domains-prefixes"))))

(defn retrieve-domain-decision-structure
  "Return tuple of [version-url, domain-decision-structure]"
  []
  (log/info "Retrieving domain-decision-structure artifact")

  ; Fetch the cached copy of the domain list.
  ; This is quite heavy, weighing in at ~3 MB of serialized JSON. But it stays resident in memory and is heavily used.
  (let [version-url (artifact/fetch-latest-version-link "domain-decision-structure")

        value (-> version-url
                  artifact/fetch-latest-artifact-stream
                  clojure.java.io/reader
                  ; Explicitly get keys back as strings.
                  ; Some of them are in parse-domain-structure.
                  (json/read :key-fn str)
                  parse-domain-decision-structure)]
    [version-url value]))

(def cache-milliseconds
  "One hour"
  3600000)

(def cached-domain-structure
  "Cache the domain list and version url. It's very rarely actually updated."
  (memo/ttl retrieve-domain-decision-structure {} :ttl/threshold cache-milliseconds))


(defn assoc-domain-decision-structure
  "Associate the context with a domain-decision-structure."
  [context]
  (let [[version-url value] (cached-domain-structure)]
    (assoc context :domain-decision-structure-artifact-version version-url
                   :domain-decision-structure value)))

(defn domain-recognised?
  "Is the URL recognised as belonging to a domain that hosts landing pages in general?
   Expect a context with a landing-page-structure object."
  [{domain-decision-structure :domain-decision-structure} url]
  (try
    (let [domain (-> url URL. (.getHost))]
      (contains? (get domain-decision-structure "domains") domain))
    ; If there's error parsing the URL, nope.
    (catch Exception _ false)))

(defn domain-recognised-for-doi?
  "Is the URL recognised as belonging to a domain that hosts landing pages for this DOI prefix?"
  [{structure :domain-decision-structure} url doi]
  (try
    (let [domain (-> url URL. (.getHost))
          prefix (crdoi/get-prefix doi)]
      (true? (get-in structure ["domains-prefixes" domain prefix "domain-recognised"])))
    ; If there's error parsing the URL, nope.
    (catch Exception _ false)))

(defn domain-confirmed-for-doi?
  "Is the URL recognised as belonging to a domain that hosts landing pages with confirmed metadata
   for this DOI prefix?"
  [{structure :domain-decision-structure} url doi]
  (try
    (let [domain (-> url URL. (.getHost))
          prefix (crdoi/get-prefix doi)]
      (true? (get-in structure ["domains-prefixes" domain prefix "domain-confirmed"])))
    ; If there's error parsing the URL, nope.
    (catch Exception _ false)))

(defn get-recognised-domains
  "Return a sequence of all recognised domains."
  [{structure :domain-decision-structure}]
  (get structure "domains" []))
