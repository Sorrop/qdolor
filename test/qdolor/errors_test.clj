(ns qdolor.errors-test
  (:require [clojure.core.async :as async]
            [qdolor.core :as qd]
            [qdolor.test-utils :as t.utils]))

(def task-conf
  {:get-id :id

   :ready?
   (fn task-ready? [this _ctx]
     (let [t (.get-raw this)
           {:keys [inject-error]} t]
         (when (:ready? inject-error)
           (throw (Exception. "Injected")))
         (if (contains? #{:get-unreadiness-policy
                          :on-unreadiness
                          :abandon}
                        (first (keys inject-error)))
           false
           true)))

   :execute
   (fn execute! [this ctx]
     (let [t (.get-raw this)
           {:keys [inject-error]} t]
       (when (contains? #{:get-failure-policy
                          :requeue
                          :on-failure
                          :nack}
                        (first (keys inject-error)))
         (throw (Exception. "Injected")))
       (let [{:keys [task-sleeps]} ctx]
         (Thread/sleep task-sleeps)
         {:status :success})))

   :on-complete
   (fn on-complete! [this _ctx _result]
     
     (let [t (.get-raw this)
           {:keys [inject-error]} t]
       (when (:on-complete inject-error)
         (throw (Exception. "Injected")))))

   :get-unreadiness-policy
   (fn get-unreadiness-policy [this _ctx]
     (when (:get-unreadiness-policy (:inject-error (.get-raw this)))
       (throw (Exception. "Injected")))
     (if (or (:abandon (:inject-error (.get-raw this)))
             (:on-unreadiness (:inject-error (.get-raw this))))
       {:action :discard}
       {:action :requeue}))

   :on-unreadiness
   (fn on-unreadiness [this _ctx _policy]
     (when (:on-unreadiness (:inject-error (.get-raw this)))
       (throw (Exception. "Injected"))))
   

   :get-failure-policy
   (fn get-failure-policy [this _ctx _throwable]
     (when (:get-failure-policy (:inject-error (.get-raw this)))
       (throw (Exception. "Injected")))
     (if (or (:nack (:inject-error (.get-raw this)))
             (:on-failure (:inject-error (.get-raw this))))
       {:action :discard}
       {:action :requeue}))

   :on-failure
   (fn on-failure! [this ctx _policy throwable]
     (let [t (.get-raw this)
           {:keys [inject-error]} t]
       (when (:on-failure inject-error) (throw (Exception. "Injected")))))})

(def qbackend-conf
  {:dequeue
   (fn dequeue! [queue]
     (async/poll! queue)
     #_(when-let [t (async/poll! queue)]
       (qd/make-qtask (assoc task-conf :task t))))

   :ack
   (fn ack! [_this task]
     (when (:ack (:inject-error (.get-raw task)))
       (throw (Exception. "Injected"))))

   :nack
   (fn nack! [_this task]
     (when (:nack (:inject-error (.get-raw task)))
       (throw (Exception. "Injected"))))

   :requeue
   (fn requeue! [queue task _opts]
     (when (:requeue (:inject-error (.get-raw task)))
       (throw (Exception. "Injected")))
     (async/>!! queue (.get-raw task)))

   :abandon
   (fn abandon! [_this task]
     (when (:abandon (:inject-error (.get-raw task)))
       (throw (Exception. "Injected")))
     )

   :on-unexpected-error
   (fn on-unexpected-error
     [_this ctx throwable]
     (let [{:keys [phase task]} (ex-data throwable)]
       (swap! (:db ctx) assoc :injected-error {:phase phase :task task}))
     (t.utils/log (format "Error: %s" (.getMessage throwable))))})
