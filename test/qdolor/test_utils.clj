(ns qdolor.test-utils
  (:require [clj-test-containers.core :as tc]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [java-time])
  (:import
   [org.testcontainers.containers PostgreSQLContainer]))

(def log-datetime-format
  "yyyy-MM-dd HH:mm:ss.SSS")

(defn timestamp []
  (java-time/format
    (java-time/formatter log-datetime-format)
    (java-time/local-date-time)))

(defn from-timestamp [t]
  (java-time/local-date-time log-datetime-format t))

(defn log [s]
  (println (format "|%s|  %s" (timestamp) s)))


(defn pg-container
  [& {:keys [port network] :or {port 5432}}]
  (cond-> {:container     (PostgreSQLContainer. "postgres:15.3")
           :exposed-ports [port]
           :wait-for      {:wait-strategy :port}
           :env-vars      {"POSTGRES_PASSWORD" "supapass"
                           "POSTGRES_USER"     "kioku"}}
    (some? network) (assoc :network network :network-aliases ["postgres"])
    :always (tc/init)
    :always (tc/start!)))

(defn cont->jdbc-url [pg]
   (let [{:keys [container]} pg]
    (format "%s&user=%s&password=%s"
            (.getJdbcUrl container)
            (.getUsername container)
            (.getPassword container))))

(def create-q-table
  "CREATE TABLE queue(
     id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
     task_id INTEGER)")

(def create-results-table
  "CREATE TABLE results(
     task_id INTEGER,
     status VARCHAR,
     started_at TIMESTAMP DEFAULT NOW(),
     finished_at TIMESTAMP)")

(defn n-tasks [n]
  (format "INSERT INTO queue(task_id) SELECT generate_series(1, %s);"
          n))

(defn exec-db [jdbc-url cmd]
  (with-open [conn (-> {:jdbcUrl jdbc-url}
                       jdbc/get-datasource
                       jdbc/get-connection)]
    (jdbc/execute! conn cmd {:builder-fn rs/as-unqualified-lower-maps})))

(defn prepare-db [jdbc-url]
  (with-open [conn (-> {:jdbcUrl jdbc-url}
                       jdbc/get-datasource
                       jdbc/get-connection)]
    (jdbc/execute! conn ["DROP TABLE IF EXISTS queue;"])
    (jdbc/execute! conn ["DROP TABLE IF EXISTS results;"])
    (jdbc/execute! conn [create-q-table])
    (jdbc/execute! conn [create-results-table])
    (jdbc/execute! conn [(n-tasks 50)])))


(comment
  (def cont (qdolor.test-utils/pg-container))
  (def jdbc-url (qdolor.test-utils/cont->jdbc-url cont)))
