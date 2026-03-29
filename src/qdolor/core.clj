(ns qdolor.core)

(defprotocol QueueBackend
  (dequeue! [this]  "Take next task from queue. Returns task or nil.")
  (succeed! [this task] "Mark task as successfully completed.")
  (fail! [this task] "Mark task as failed")
  (requeue! [this task opts] "Put task back with opts")
  (abandon! [this task] "Arbitrary, user defined action to perform when a task is not ready and will not be requeued")
  (on-unexpected-error [this ctx throwable] "Debug method. Use this when anything except task's execute! throws"))

(defprotocol Task
  (task-id [this] "Return task's unique identifier")
  (ready? [this ctx]   "Only if this fn returns true, task should be executed")
  (execute! [this ctx] "Run the task. Return result or throw")
  (on-complete! [this ctx result] "Called after successful execute!")
  (get-unreadiness-policy [this ctx]  "Returns a policy map with an :action key. If said key is :requeue, the task is re-inserted into the queue and an accompanying :requeue-opts key will be passed to requeue!")
  (on-unreadiness! [this ctx policy] "This is called in case an unreadiness policy is returned with any other :action than :requeue")
  (on-failure! [this ctx policy throwable] "Called after failed execute!")
  (get-failure-policy [this ctx throwable]  "Returns a policy map with an :action key. If said key is :requeue, the task is re-inserted into the queue and an accompanying :requeue-opts key will be passed to requeue!. Otherwise, on-failure! is called."))

(defprotocol WorkerPool
  (start! [this])
  (stop! [this]))

(defn task-handler
  [task ctx]
  (try
    (let [result (execute! task ctx)]
      {:status :success
       :result result})
    (catch Throwable t
      {:status :failure
       :err    t})))

(defn worker-loop [queue-backend ctx]
  (try
    (when-let [task (dequeue! queue-backend)]
      (if (ready? task ctx)
        (let [{:keys [status result err]} (task-handler task ctx)]
          (if (= status :success)
            (do (on-complete! task ctx result)
                (succeed! queue-backend task))
            (let [{:keys [action requeue-opts]
                   :as   failure-policy} (get-failure-policy task ctx err)]
              (if (= action :requeue)
                (requeue! queue-backend task requeue-opts)
                (do
                  (on-failure! task ctx failure-policy err)
                  (fail! queue-backend task))))))
        (let [{:keys [action requeue-opts]
               :as   unreadiness-policy} (get-unreadiness-policy task ctx)]
          (if (= action :requeue)
            (requeue! queue-backend task requeue-opts)
            (do (on-unreadiness! task ctx unreadiness-policy)
                (abandon! queue-backend task))))))
    (catch Throwable t
      (on-unexpected-error queue-backend ctx t))))
