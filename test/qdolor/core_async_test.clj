(ns qdolor.core-async-test
  (:require [clojure.core.async :as async]
            [qdolor.core :as qd]
            [qdolor.test-utils :as t.utils]))

(def task-conf
  {:get-id :id

   :ready?
   (fn task-ready? [this ctx]
     (let [t                    (.get-raw this)
           {:keys [depends-on]} t
           {:keys [db]}         ctx
           records              (vals (select-keys @db depends-on))]
       (if (seq records)
         (every? (fn [{:keys [status finished-at]}]
                   (and (some? finished-at)
                        (= status :success)))
                 records)
         true)))

   :execute
   (fn execute! [_this ctx]
     (let [{:keys [task-sleeps]} ctx]
       (Thread/sleep task-sleeps)
       {:status :success}))

   :on-complete
   (fn on-complete! [this ctx result]
     (let [{:keys [db]} ctx
           t            (.get-raw this)
           task-id      (.task-id this)
           record       (merge t result {:finished-at (t.utils/timestamp)})]
       (swap! db assoc task-id record)))

   :get-unreadiness-policy
   (fn get-unreadiness-policy [_this _ctx]
     {:action :requeue})

   :get-failure-policy
   (fn get-failure-policy [_this _ctx _throwable]
     {:action :discard})

   :on-failure
   (fn on-failure! [this ctx _policy throwable]
     (let [{:keys [db]} ctx
           id           (.task-id this)
           t            (.get-raw this)
           record       (merge t {:status      :failure
                                  :eror        throwable
                                  :finished-at (t.utils/timestamp)})]
       (swap! db assoc id record)))})

(def qbackend-conf
  {:dequeue
   (fn dequeue! [queue]
     (when-let [t (async/poll! queue)]
       (qd/make-qtask (assoc task-conf :task t))))

   :ack
   (fn ack! [_this task]
     (t.utils/log (format "Task `%s` succeeded" (.task-id task))))

   :nack
   (fn nack! [_this task]
     (t.utils/log (format "Task `%s` failed" (.task-id task))))

   :requeue
   (fn requeue! [queue task _opts]
     (async/>!! queue (.get-raw task)))

   :abandon
   (fn abandon! [_this task]
     (t.utils/log (format "Task `%s` abandoned" (.task-id task))))

   :on-unexpected-error
   (fn on-unexpected-error
     [_this _ctx throwable _maybe-task]
     (t.utils/log (format "Error: %s" (Throwable->map throwable))))})
