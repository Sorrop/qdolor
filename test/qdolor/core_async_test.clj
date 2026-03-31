(ns qdolor.core-async-test
  (:require [qdolor.worker-pool.impl.core-async :as async-wp]
            [qdolor.core :as qd]
            [java-time]
            [clojure.core.async :as async]))

(defn timestamp []
  (java-time/format
    (java-time/formatter "yyyy-MM-dd HH:mm:ss.SSS")
    (java-time/local-date-time)))

(defn log [s]
  (println (format "|%s|  %s" (timestamp) s)))

(def the-queue
  (async/chan))

(def example-tasks
  [{:id :a :depends-on [:b :c]}
   {:id :d :depends-on []}
   {:id :b :depends-on [:c]}
   {:id :c :depends-on []}])

(def the-db
  (atom (zipmap (map :id example-tasks) example-tasks)))

(defn ->QTask [t]
  (reify qd/QTask
    (task-id [_this] (:id t))

    (get-raw [_this] t)

    (ready? [_this ctx]
      (let [{:keys [depends-on]} t
            {:keys [db]}         ctx
            records              (vals (select-keys @db depends-on))]
        (if (seq records)
          (every? (fn [{:keys [status finished-at]}]
                    (and (some? finished-at)
                         (= status :success)))
                  records)
          true)))

    (execute! [_this ctx]
      (let [{:keys [task-sleeps]} ctx]
        (Thread/sleep task-sleeps)
        {:status :success}))

    (on-complete! [this ctx result]
      (let [{:keys [db]} ctx
            task-id      (.task-id this)
            record       (merge t result {:finished-at (timestamp)})]
        (swap! db assoc task-id record)))

    (get-unreadiness-policy [_this _ctx] {:action :requeue})

    (on-unreadiness! [_ _ _] nil)

    (get-failure-policy [_this _ctx _throwable] {:action :discard})
    (on-failure! [this ctx _policy throwable]
      (let [{:keys [db]} ctx
            id           (.task-id this)
            record       (merge t {:status      :failure
                                   :eror        throwable
                                   :finished-at (timestamp)})]
        (swap! db assoc id record)))))

(defn queue-backend [the-queue]
  (reify qd/QBackend
    (dequeue! [_this]
      (let [t (async/poll! the-queue)]
        (when t (->QTask t))))
    (succeed! [this task]
      (log (format "Task `%s` succeeded" (.task-id task))))
    (fail! [this task]
      (log (format "Task `%s` failed" (.task-id task))))
    (requeue! [this task opts]
      (async/>!! the-queue (.get-raw task)))
    (abandon! [this task]
      (log (format "Task `%s` abandoned" (.task-id task))))

    (on-unexpected-error [this ctx throwable]
      (log (format "Error: %s" (Throwable->map throwable))))))

(def the-ctx
  {:db          the-db
   :task-sleeps 2000})

(def worker-pool
  (async-wp/->AsyncWorkerPool 4 (queue-backend the-queue) nil nil))

(defn start-wp []
  (.start! worker-pool the-ctx))

(defn run []
  (start-wp)
  (async/>!! the-queue (get example-tasks 3))
  (async/>!! the-queue (get example-tasks 2))
  (async/>!! the-queue (first example-tasks))
  (async/>!! the-queue (second example-tasks))
  (Thread/sleep 10000)
  (.stop! worker-pool))
