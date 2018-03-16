(ns event-data-common.whitelist
  "Whitelist Events for suitability in the Crossref services.
   Filter Events for conformity against Artifacts which are cached for an hour."
  (:require [event-data-common.artifact :as artifact]
            [config.core :refer [env]]
            [clojure.tools.logging :as log]
            [clojure.core.cache :as cache]
            [crossref.util.doi :as cr-doi]))

(defn retrieve-source-whitelist
  "Retrieve set of source IDs according to config, or nil if not configured."
  []
  (when-let [artifact-name (:query-whitelist-artifact-name env)]
    (let [source-names (-> artifact-name artifact/fetch-latest-artifact-string (clojure.string/split #"\n") set)]
      (log/info "Retrieved source names:" source-names)
      source-names)))

(defn retrieve-prefix-whitelist
  "Retrieve set of DOI prefixes as a set according to config, or nil if not configured."
  []
  (when-let [artifact-name (:query-prefix-whitelist-artifact-name env)]
    (let [prefixes (-> artifact-name artifact/fetch-latest-artifact-string (clojure.string/split #"\n") set)]
      (log/info "Retrieved " (count prefixes) "prefixes" (type prefixes))
      prefixes)))

(def artifact-cache (atom (cache/ttl-cache-factory {} :ttl 3600000)))

(defn get-prefixes-cached []
  (cache/lookup (swap! artifact-cache
                       #(if (cache/has? % :prefix)
                          (cache/hit % :prefix)
                          (cache/miss % :prefix (retrieve-prefix-whitelist))))
                :prefix))

(defn get-sources-cached []
  (cache/lookup (swap! artifact-cache
                       #(if (cache/has? % :source)
                          (cache/hit % :source)
                          (cache/miss % :source (retrieve-source-whitelist))))
                :source))

(defn filter-prefix-whitelist
  [events]
  (if-let [prefixes (get-prefixes-cached)]
    (filter #(let [subj-id (:subj_id %)
                   obj-id (:obj_id %)]
             (or 
                 ; There's no DOI in either subj or obj position.
                 ; This can happen in theory.
                 (not (or (when subj-id (cr-doi/well-formed subj-id))
                          (when obj-id (cr-doi/well-formed obj-id))))
                 ; Or there's a whitelisted DOI prefix in the subject or object.
                 (prefixes (when subj-id (cr-doi/get-prefix subj-id)))
                 (prefixes (when obj-id (cr-doi/get-prefix obj-id))))) events)
    events))

(defn filter-source-whitelist
  [events]
  (if-let [sources (get-sources-cached)]
    (filter #(sources (:source_id %)) events)
    events))

(defn filter-events
  [events]
  (let [filtered (-> events filter-source-whitelist filter-prefix-whitelist)]
    (log/info "Whitelist filter kept:" (count filtered) "/" (count events) "removed:" (- (count events) (count filtered)))
    filtered))

