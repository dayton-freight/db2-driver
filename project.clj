(defproject metabase/db2-driver "1.0.0"
  :min-lein-version "2.5.0"

  :profiles
  {:provided
   {:dependencies [
     [org.clojure/clojure "1.10.0"]
     [metabase-core "1.0.0-SNAPSHOT"]
     [net.sf.jt400/jt400 "10.4"]
    ]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :omit-source   true
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "db2.metabase-driver.jar"}})
