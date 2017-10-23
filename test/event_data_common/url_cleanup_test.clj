(ns event-data-common.url-cleanup-test
  (:require [clojure.test :refer :all]
            [event-data-common.url-cleanup :as url-cleanup]))

(deftest ^:unit remove-tracking-params
  (testing "URLs with no tracking aren't affected"
    (is (= (url-cleanup/remove-tracking-params "http://www.example.com")
           "http://www.example.com")
        "No parameters, should be unaffected")

    (is (= (url-cleanup/remove-tracking-params "http://www.example.com:8080/")
           "http://www.example.com:8080/")
        "Port number should be included.")

    (is (= (url-cleanup/remove-tracking-params "http://www.example.com:80/")
           "http://www.example.com:80/")
        "Port number should be included.")

    (is (= (url-cleanup/remove-tracking-params "https://www.example.com")
           "https://www.example.com")
        "No parameters, should be unaffected")

    (is (= (url-cleanup/remove-tracking-params "http://www.example.com?q=1")
           "http://www.example.com?q=1")
        "Parameters carried through")

    (is (= (url-cleanup/remove-tracking-params "http://www.example.com?q=1&a=2&c=3&k=4")
           "http://www.example.com?q=1&a=2&c=3&k=4")
        "Parameters carried through in same order")

    (is (= (url-cleanup/remove-tracking-params "http://www.example.com?c=1&h=2&i=3&c=4&k=5&e=6&n=7")
           "http://www.example.com?c=1&h=2&i=3&c=4&k=5&e=6&n=7")
        "Duplicate parameters carried through in the same order."))

  (testing "URLs with fragments aren't affected by removal"
    (is (= (url-cleanup/remove-tracking-params "http://www.example.com/animals#pangolion")
           "http://www.example.com/animals#pangolion")
        "Simple fragment")

    (is (= (url-cleanup/remove-tracking-params "http://www.example.com/animals?beginswith=h#hoarse")
           "http://www.example.com/animals?beginswith=h#hoarse")
        "Irrelevant query params, all preserved.")

    (is (= (url-cleanup/remove-tracking-params "http://www.example.com/animals?beginswith=h&utm_medium=xyz#hoarse")
           (url-cleanup/remove-tracking-params "http://www.example.com/animals?utm_keyword=throat&beginswith=h&utm_medium=xyz#hoarse")
           "http://www.example.com/animals?beginswith=h#hoarse")
        "Mixture of params. Tracking removed, fragment remains"))

  (testing "URLs with a mixture of tracking params and non-tracking params should have only those tracking params removed"
    (is (= (url-cleanup/remove-tracking-params "http://www.example.com/animals?beginswith=h")
           (url-cleanup/remove-tracking-params "http://www.example.com/animals?beginswith=h&utm_medium=xyz")
           (url-cleanup/remove-tracking-params "http://www.example.com/animals?utm_keyword=throat&beginswith=h")
           (url-cleanup/remove-tracking-params "http://www.example.com/animals?utm_keyword=throat&beginswith=h&utm_medium=xyz")
           "http://www.example.com/animals?beginswith=h")
        "Single tracking param removed")

    (is (= (url-cleanup/remove-tracking-params "http://www.example.com/animals?beginswith=h&endswith=e")
           (url-cleanup/remove-tracking-params "http://www.example.com/animals?beginswith=h&utm_medium=xyz&endswith=e")
           (url-cleanup/remove-tracking-params "http://www.example.com/animals?utm_keyword=throat&beginswith=h&endswith=e")
           (url-cleanup/remove-tracking-params "http://www.example.com/animals?utm_keyword=throat&beginswith=h&utm_medium=xyz&endswith=e")
           "http://www.example.com/animals?beginswith=h&endswith=e")
        "Multiple mixed-up tracking params removed."))

  (testing "Similar but different params should not be removed"
    (is (= (url-cleanup/remove-tracking-params "http://www.example.com/animals?this=that&xutm_medium=1234&utm_keyword")
           "http://www.example.com/animals?this=that&xutm_medium=1234")
        "Similar, but not actual, params intact"))

  (testing "All params are removed, the trailing question mark should also be removed"
    (is (= (url-cleanup/remove-tracking-params "http://www.example.com/animals")
           (url-cleanup/remove-tracking-params "http://www.example.com/animals?")
           (url-cleanup/remove-tracking-params "http://www.example.com/animals?utm_keyword=ears")
           (url-cleanup/remove-tracking-params "http://www.example.com/animals?utm_keyword=ears&utm_medium=nose")
           (url-cleanup/remove-tracking-params "http://www.example.com/animals?utm_keyword=ears&utm_medium=nose&utm_campaign=throat")
           "http://www.example.com/animals")
        "When all params removed, question mark also removed"))

  (testing "Repeated parameters left intact"
    (is (= (url-cleanup/remove-tracking-params "http://www.example.com/animals?animal=cat&utm_campaign=mammals&purpose=porpoise&purpose=download&purpose=porpoise")
           "http://www.example.com/animals?animal=cat&purpose=porpoise&purpose=download&purpose=porpoise")
        "Dupliate parameters intact and in order."))

  (testing "If a malformed URL causes an exception, just return it verbatim."
    (is (= (url-cleanup/remove-tracking-params nil) nil) "NullPointerException causing input should return input verbatim.")
    (is (= (url-cleanup/remove-tracking-params "http://") "http://") "URISyntaxException causing input should return input verbatim.")))

