(defproject net.clojars.sorrop/qdolor "0.2.2-SNAPSHOT"
  :description "Toolset for queue based execution by concurrent workers"
  :url "https://github.com/Sorrop/qdolor"

  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}

  :dependencies [[org.clojure/clojure "1.12.2"]
                 [org.clojure/core.async "1.9.865"]]

  :repositories [["releases" {:url   "https://repo.clojars.org"
                              :creds :gpg}]]

  :plugins [[lein-codox "0.10.8"]
            [lein-set-version "0.4.1"]]

  :profiles {:dev {:dependencies [[clojure.java-time "0.3.2"]
                                  [clj-test-containers "0.7.4"
                                   :exclusions [org.testcontainers/testcontainers]]
                                  [org.testcontainers/testcontainers "2.0.3"]
                                  [org.testcontainers/testcontainers-postgresql "2.0.3"]
                                  [com.github.seancorfield/next.jdbc "1.3.1093"]
                                  [org.postgresql/postgresql "42.7.10"]]}}

  :codox {:output-path "docs"
          :source-uri  "https://github.com/Sorrop/qdolor/blob/master/{filepath}#L{line}"
          :namespaces  [qdolor.core
                        qdolor.worker-pool.core
                        qdolor.worker-pool.impl.core-async
                        qdolor.worker-pool.impl.vthreads]
          :metadata    {:doc/format :markdown}}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["codox"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "--no-sign"]
                  ["deploy" "clojars"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]]

  :repl-options {:init-ns user})
