(defproject event-data-common "0.1.56"
  :description "Crossref Event Data Common"
  :url "http://eventdata.crossref.org"
  :license {:name "The MIT License (MIT)"
            :url "https://opensource.org/licenses/MIT"}
  :plugins [[lein-localrepo "0.5.3"]]
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [clj-http "3.4.1"]
                 [clj-http-fake "1.0.3"]
                 [clj-time "0.12.2"]
                 [com.amazonaws/aws-java-sdk "1.11.61"]
                 [com.auth0/java-jwt "2.2.1"]
                 [compojure "1.5.1"]
                 [liberator "0.14.1"]
                 [org.apache.kafka/kafka-clients "0.11.0.0"]
                 [org.clojure/core.async "0.4.474"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.nrepl "0.2.12"]
                 [overtone/at-at "1.2.0"]
                 [redis.clients/jedis "2.8.0"]
                 [ring "1.5.0"]
                 [ring/ring-jetty-adapter "1.5.0"]
                 [ring/ring-servlet "1.5.0"]
                 [robert/bruce "0.8.0"]
                 [yogthos/config "0.8"]
                 [com.fzakaria/slf4j-timbre "0.3.7"]
                 [org.slf4j/slf4j-api "1.7.14"]
                 [com.taoensso/timbre "4.10.0"]
                 [cheshire "5.7.0"]
                 [com.climate/claypoole "1.1.4"]
                 [org.clojure/core.cache "0.7.1"]
                 [crossref-util "0.1.15"]
                 [slingshot "0.12.2"]]
  :target-path "target/%s"
  :test-selectors {:default (constantly true)
                   :unit :unit
                   :component :component
                   :integration :integration
                   :all (constantly true)}
  :jvm-opts ["-Duser.timezone=UTC"]
  :profiles {:uberjar {:aot :all}
             :prod {:resource-paths ["config/prod"]}
             :dev  {:resource-paths ["config/dev"]}})