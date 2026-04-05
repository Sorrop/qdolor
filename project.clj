(defproject qdolor "0.1.0-SNAPSHOT"
  :description "Toolset for queue based execution by concurrent workers"
  :url "https://github.com/Sorrop/qdolor"

  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}

  :dependencies [[org.clojure/clojure "1.12.2"]
                 [org.clojure/core.async "1.9.865"]]

  :plugins [[lein-codox "0.10.8"]]

  :profiles {:dev {:dependencies [[clojure.java-time "0.3.2"]]}}

  :codox {:output-path "docs"
          :source-uri  "https://github.com/Sorrop/qdolor/blob/master/{filepath}#L{line}"
          :namespaces  [qdolor.core
                        qdolor.worker-pool.core
                        qdolor.worker-pool.impl.core-async
                        qdolor.worker-pool.impl.vthreads]
          :metadata    {:doc/format :markdown}}

  :repl-options {:init-ns user})
