(ns event-data-common.whitelist-test
  (:require [clojure.test :refer :all]
            [event-data-common.artifact :as artifact]
            [event-data-common.whitelist :as whitelist]
            [clj-http.fake :as fake]
            [clojure.data.json :as json]))


(deftest ^:unit filters-by-prefix-and-source
  (testing "Can retrieve artifact, cached, and filter Events by their prefix and source."

       (let [ok-events 
              ; Allowed source_id, subj_id prefix and obj_id prefix.
             [{:source_id "crossref" :subj_id "https://doi.org/10.5555/12345678" :obj_id "https://doi.org/10.6666/87654321"}
              ; Allowed source_id and obj_id prefix. subj_id prefix is off the list, but we only need to match subj_id or obj_id.
              {:source_id "crossref" :subj_id "https://doi.org/10.9999/12345678" :obj_id "https://doi.org/10.6666/87654321"}
              ; Allowed source_id and obj_id prefix. obj_id prefix is off the list, but we only need to match subj_id or obj_id.
              {:source_id "crossref" :subj_id "https://doi.org/10.5555/12345678" :obj_id "https://doi.org/10.9999/87654321"}
              ; Different, allowed source name. 
              {:source_id "bogomips" :subj_id "https://doi.org/10.5555/12345678" :obj_id "https://doi.org/10.6666/87654321"}]

             bad-events 
               ; Unacceptable source
              [{:source_id "crossedfingers" :subj_id "https://doi.org/10.5555/12345678" :obj_id "https://doi.org/10.6666/87654321"}
               ; Both subj_id and obj_id prefixes unacceptable.
               {:source_id "crossref" :subj_id "https://doi.org/10.9999/12345678" :obj_id "https://doi.org/10.99999/87654321"}]


              cache-count (atom {"crossref-doi-prefix-list" 0
                                 "crossref-sourcelist" 0})]

             ; Mock out artifact fetching.

             (with-redefs [artifact/fetch-latest-artifact-string 
                           (fn [artifact-name]
                             (is (= (get
                                      (swap! cache-count #(update % artifact-name inc))
                                      artifact-name)
                                    1)
                                 "Artifact fetch should be called only once during a TTL window.")

                             ({"crossref-doi-prefix-list" "10.5555\n10.6666"
                               "crossref-sourcelist" "crossref\nbogomips"} artifact-name))]

                     (is (= (whitelist/filter-events ok-events) ok-events)
                         "Acceptable Events should be kept.")

                     (is (= (whitelist/filter-events bad-events) [])
                         "Acceptable Events should be removed.")))))
