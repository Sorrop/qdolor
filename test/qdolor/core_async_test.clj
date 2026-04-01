(ns qdolor.core-async-test
  (:require [clojure.core.async :as async]
            [qdolor.core :as qd]
            [qdolor.test-utils :as t.utils]))

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
            record       (merge t result {:finished-at (t.utils/timestamp)})]
        (swap! db assoc task-id record)))

    (get-unreadiness-policy [_this _ctx] {:action :requeue})

    (on-unreadiness! [_ _ _] nil)

    (get-failure-policy [_this _ctx _throwable] {:action :discard})
    (on-failure! [this ctx _policy throwable]
      (let [{:keys [db]} ctx
            id           (.task-id this)
            record       (merge t {:status      :failure
                                   :eror        throwable
                                   :finished-at (t.utils/timestamp)})]
        (swap! db assoc id record)))))

(defn queue-backend [queue]
  (reify qd/QBackend

    (dequeue! [_this]
      (let [t (async/poll! queue)]
        (when t (->QTask t))))

    (succeed! [_this task]
      (t.utils/log (format "Task `%s` succeeded" (.task-id task))))

    (fail! [_this task]
      (t.utils/log (format "Task `%s` failed" (.task-id task))))

    (requeue! [_this task _opts]
      (async/>!! queue (.get-raw task)))

    (abandon! [_this task]
      (t.utils/log (format "Task `%s` abandoned" (.task-id task))))

    (on-unexpected-error [_this _ctx throwable]
      (t.utils/log (format "Error: %s" (Throwable->map throwable))))))
