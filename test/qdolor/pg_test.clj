(ns qdolor.pg-test
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [qdolor.test-utils :as t.utils]))

(def lock-q
  "SELECT * 
   FROM queue 
   ORDER BY id ASC 
   LIMIT 1 
   FOR UPDATE SKIP LOCKED")

(defn delete-task
  [id]
  (format "DELETE FROM queue WHERE id = %s" id))

(defn remove-task [conn id]
  (next.jdbc/execute! conn [(delete-task id)]
                      {:builder-fn rs/as-unqualified-lower-maps}))

(defn lock-n-take [jdbc-url]
  (let [conn (-> {:jdbcUrl jdbc-url}
                 jdbc/get-datasource
                 jdbc/get-connection)
        _    (.setAutoCommit conn false)
        record (next.jdbc/execute! conn [lock-q]
                                   {:builder-fn rs/as-unqualified-lower-maps})]
    (if (empty? record)
      (.close conn)
      {:task (first record)
       :conn conn})))

(defn terminate-conn [conn-store conn]
  (.close conn)
  (reset! conn-store nil))

(defn resolve-conn [ctx]
  (let [{:keys [worker]}  ctx]
    (get-in ctx [:conn-store worker])))

(defn remove-from-queue [task ctx]
  (let [task              (.get-raw task)
        {:keys [id]}      task
        worker-conn-store (resolve-conn ctx)
        worker-conn       @worker-conn-store]
    (remove-task worker-conn id)
    (.commit worker-conn)
    (terminate-conn worker-conn-store worker-conn)))

(defn re-queue [ctx]
  (let [worker-conn-store (resolve-conn ctx)
        worker-conn       @worker-conn-store]
    (terminate-conn worker-conn-store worker-conn)))

(defn qbackend-conf [url]
  {:queue url

   :dequeue
   (fn dequeue! [jdbc-url ctx]
     (when-let [t (lock-n-take jdbc-url)]
       (let [{:keys [worker]}    ctx
             {:keys [task conn]} t
             worker-conn-store   (get-in ctx [:conn-store worker])]
         (reset! worker-conn-store conn)
         task)))

   :ack
   (fn ack! [_ task ctx]
     (remove-from-queue task ctx)
     (t.utils/log (format "Task `%s` succeeded" (.task-id task))))

   :nack
   (fn nack! [_ task ctx]
     (remove-from-queue task ctx)
     
     (t.utils/log (format "Task `%s` failed" (.task-id task))))

   :requeue
   (fn requeue! [_ _ _ ctx]
     (re-queue ctx))

   :abandon
   (fn abandon! [_this _ ctx]
     (re-queue ctx))

   :on-unexpected-error
   (fn on-unexpected-error
     [_this _ctx throwable]
     (t.utils/log (format "Error: %s" (Throwable->map throwable))))})


(def task-conf
  {:get-id :task_id

   :ready?
   (fn task-ready? [_ _]
     true)

   :execute
   (fn execute! [_this ctx]
     (let [{:keys [task-sleeps]} ctx]
       (Thread/sleep task-sleeps)
       {:status :success}))

   :on-complete
   (fn on-complete! [this ctx result]
     (println "completed"))

   :get-unreadiness-policy
   (fn get-unreadiness-policy [_this _ctx]
     {:action :requeue})

   :get-failure-policy
   (fn get-failure-policy [_this _ctx _throwable]
     {:action :discard})

   :on-failure
   (fn on-failure! [this ctx _policy throwable]
     (println throwable))})
