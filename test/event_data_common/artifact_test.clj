(ns event-data-common.artifact-test
  (:require [clojure.test :refer :all]
            [event-data-common.artifact :as artifact]
            [org.httpkit.fake :as fake]
            [clojure.data.json :as json]))

(def artifact-index-structure
  {"type" "artifact-name-list"
   "updated" "2016-12-16T10:19:28.124Z"
   "artifacts" {"crossref-sourcelist"
                {"name" "crossref-sourcelist"
                  "current-version" "1481825550368"
                  "versions-link" "http://d1v52iseus4yyg.cloudfront.net/a/crossref-sourcelist/versions.json"
                  "current-version-link" "http://d1v52iseus4yyg.cloudfront.net/a/crossref-sourcelist/versions/1481825550368"}
                  
                  "doi-prefix-list"
                 {"name" "doi-prefix-list"
                  "current-version" "1481821349348"
                  "versions-link" "http://d1v52iseus4yyg.cloudfront.net/a/doi-prefix-list/versions.json"
                  "current-version-link" "http://d1v52iseus4yyg.cloudfront.net/a/doi-prefix-list/versions/1481821349348"}
                  
                  "domain-list"
                 {"name" "domain-list"
                  "current-version" "1481821314866"
                  "versions-link" "http://d1v52iseus4yyg.cloudfront.net/a/domain-list/versions.json"
                  "current-version-link" "http://d1v52iseus4yyg.cloudfront.net/a/domain-list/versions/1481821314866"}
                  
                  "newsfeed-list"
                 {"name" "newsfeed-list"
                  "current-version" "1481821264217"
                  "versions-link" "http://d1v52iseus4yyg.cloudfront.net/a/newsfeed-list/versions.json"
                  "current-version-link" "http://d1v52iseus4yyg.cloudfront.net/a/newsfeed-list/versions/1481821264217"}
                  
                  "other"
                 {"name" "other"
                  "current-version" "1481820920798"
                  "versions-link" "http://d1v52iseus4yyg.cloudfront.net/a/other/versions.json"
                  "current-version-link" "http://d1v52iseus4yyg.cloudfront.net/a/other/versions/1481820920798"}}})

(def artifact-index-json (json/write-str artifact-index-structure))

(def artifact-versions-structure
  {"type" "artifact-version-list"
   "updated" "2016-12-16T10:19:28.124Z"
   "versions" [
    {"version-link" "http://d1v52iseus4yyg.cloudfront.net/a/newsfeed-list/versions/1481821264217"
     "version" "1481821264217"}]
    "artifact" {
      "name" "newsfeed-list"
      "current-version" "1481821264217"
      "versions-link" "http://d1v52iseus4yyg.cloudfront.net/a/newsfeed-list/versions.json"
      "current-version-link" "http://d1v52iseus4yyg.cloudfront.net/a/newsfeed-list/versions/1481821264217"}})

(def artifact-versions-json (json/write-str artifact-versions-structure))


(deftest ^:unit can-retrieve-artifacts
  (testing "Can succesfully retrieve the list of artifacts."
      (fake/with-fake-http
        ["http://artifact-test/a/artifacts.json"
         artifact-index-json
         
         "http://d1v52iseus4yyg.cloudfront.net/a/crossref-sourcelist/versions/1481825550368"
         "version 1481825550368 of artifact crossref-sourcelist"

         "http://d1v52iseus4yyg.cloudfront.net/a/newsfeed-list/versions/1481821264217"
         "version 1481821264217 of artifact newsfeed-list"]
        
          (is (= (set (artifact/fetch-artifact-names)) #{"crossref-sourcelist" "doi-prefix-list" "domain-list" "newsfeed-list" "other"} ) "List of all artifacts should be retrieved.")

          (is (= (artifact/fetch-latest-artifact-string "crossref-sourcelist")  "version 1481825550368 of artifact crossref-sourcelist") "fetch-latest-artifact-string should follow link to latest and download it.")
          (is (= (artifact/fetch-latest-artifact-string "newsfeed-list")  "version 1481821264217 of artifact newsfeed-list") "fetch-latest-artifact-string should follow link to latest and download it.")
          (is (= (artifact/fetch-latest-artifact-string "elephant-list")  nil) "fetch-latest-artifact-string should return nil if non-existent artifact requested"))))
