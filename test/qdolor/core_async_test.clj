(ns qdolor.core-async-test
  (:require [clojure.core.async :as async]
            [qdolor.core :as qd]
            [qdolor.test-utils :as t.utils]))

(def task-conf
  {:get-id-fn :id

   :ready-fn
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

   :execute-fn
   (fn execute! [_this ctx]
     (let [{:keys [task-sleeps]} ctx]
       (Thread/sleep task-sleeps)
       {:status :success}))

   :on-complete-fn
   (fn on-complete! [this ctx result]
     (let [{:keys [db]} ctx
           t            (.get-raw this)
           task-id      (.task-id this)
           record       (merge t result {:finished-at (t.utils/timestamp)})]
       (swap! db assoc task-id record)))

   :get-unreadiness-policy-fn
   (fn get-unreadiness-policy [_this _ctx]
     {:action :requeue})

   :get-failure-policy-fn
   (fn get-failure-policy [_this _ctx _throwable]
     {:action :discard})

   :on-failure-fn
   (fn on-failure! [this ctx _policy throwable]
     (let [{:keys [db]} ctx
           id           (.task-id this)
           t            (.get-raw this)
           record       (merge t {:status      :failure
                                  :eror        throwable
                                  :finished-at (t.utils/timestamp)})]
       (swap! db assoc id record)))})

(def qbackend-conf
  {:dequeue-fn
   (fn dequeue! [queue]
     (when-let [t (async/poll! queue)]
       (qd/make-qtask (assoc task-conf :task t))))

   :ack-fn
   (fn ack! [_this task]
     (t.utils/log (format "Task `%s` succeeded" (.task-id task))))

   :nack-fn
   (fn nack! [_this task]
     (t.utils/log (format "Task `%s` failed" (.task-id task))))

   :requeue-fn
   (fn requeue! [queue task _opts]
     (async/>!! queue (.get-raw task)))

   :abandon-fn
   (fn abandon! [_this task]
     (t.utils/log (format "Task `%s` abandoned" (.task-id task))))

   :on-unexpected-error-fn
   (fn on-unexpected-error
     [_this _ctx throwable _maybe-task]
     (t.utils/log (format "Error: %s" (Throwable->map throwable))))})
