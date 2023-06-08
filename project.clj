(defproject adapter-load-test "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [dvlopt/kafka "1.3.1"]
                 [org.clojure/data.json "2.4.0"]]
  :main ^:skip-aot adapter-load-test.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
