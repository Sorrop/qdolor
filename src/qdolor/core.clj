(ns qdolor.core)

(defprotocol QueueBackend
  (dequeue! [this]           "Take next task from queue. Returns task or nil.")
  (succeed! [this task]      "Mark task as successfully completed.")
  (fail! [this task]         "Mark task as failed")
  (requeue! [this task opts] "Put task back with opts"))

(defprotocol Task
  (task-id [this]                            "Return task's unique identifier")
  (ready? [this ctx]                         "Only if this fn returns true, task should be executed")
  (execute! [this ctx]                       "Run the task. Return result or throw")
  (on-unreadiness [this ctxr queue-backend]  "Runs when ready? returns false")
  (on-complete! [this ctx result]            "Called after successful execute!")
  (on-failure! [this ctx err]                "Called after failed execute!")
  (maybe-retry! [this ctx queue-backend err] "This can be used to optionally re-queue in case of failed execute!")
  (on-unexpected-error [this ctx err]        "Debug method. Use this when anything except execute! throws"))

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
       :err    (Throwable->map t)})))

(defn worker-loop [queue-backend ctx]
  (when-let [task (dequeue! queue-backend)]
    (try
      (if (ready? task ctx)
        (let [{:keys [status result err]} (task-handler task ctx)]
          (if (= status :success)
            (do (on-complete! task ctx result)
                (succeed! queue-backend task))
            (when-not (maybe-retry! task ctx queue-backend err)
              (on-failure! task ctx err)
              (fail! queue-backend task))))
        (on-unreadiness task ctx queue-backend))
      (catch Throwable t
        (on-unexpected-error task ctx t)))))
