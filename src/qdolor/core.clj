(ns qdolor.core)

(defmacro wrapped-call
  ([phase body]
   `(try
      ~body
      (catch Throwable t#
        (throw (ex-info (format "Unexpected error on phase: %s" ~phase)
                        {:phase ~phase}
                        t#)))))
  ([phase task body]
   `(try
      ~body
      (catch Throwable t#
        (throw (ex-info (format "Unexpected error on phase: %s" ~phase)
                        {:phase ~phase
                         :task  (.get-raw ~task)}
                        t#)))))
  )

(defprotocol QBackend
  (dequeue! [this]  "Take next task from queue. Returns task or nil.")
  (ack! [this task] "Mark task as completed.")
  (nack! [this task] "Mark task as failed")
  (requeue! [this task opts] "Put task back with opts")
  (abandon! [this task] "Arbitrary, user defined action to perform when a task is not ready and will not be requeued")
  (on-unexpected-error! [this ctx throwable] "Use this when anything except task's execute! throws. Depending on the phase that the error was triggered, the task can be extracted from throwable with ex-data and the user can decide what to do with it."))

(defn make-qbackend
  [{:keys [queue
           dequeue
           requeue
           ack
           nack
           abandon
           on-unexpected-error]
    :or {dequeue             (fn [_])
         requeue             (fn [_ _ _])
         ack                 (fn [_ _])
         nack                (fn [_ _])
         abandon             (fn [_ _])
         on-unexpected-error (fn [_ _ _])}}]
  (reify QBackend

    (dequeue! [_this]
      (wrapped-call :dequeue (dequeue queue)))

    (ack! [this task]
      (wrapped-call :ack task (ack this task)))

    (nack! [this task]
      (wrapped-call :nack task (nack this task)))

    (requeue! [_this task opts]
      (wrapped-call :requeue task (requeue queue task opts)))

    (abandon! [this task]
      (wrapped-call :abandon task (abandon this task)))

    (on-unexpected-error! [this ctx throwable]
      (on-unexpected-error this ctx throwable))))

(defprotocol QTask
  (task-id [this] "Return task's unique identifier")
  (get-raw [this] "Returns raw task data")
  (ready? [this ctx]   "Only if this fn returns true, task should be executed")
  (execute! [this ctx] "Run the task. Return result or throw")
  (on-complete! [this ctx result] "Called after successful execute!")
  (get-unreadiness-policy [this ctx]  "Returns a policy map with an :action key. If said key is :requeue, the task is re-inserted into the queue and an accompanying :requeue-opts key will be passed to requeue!")
  (on-unreadiness! [this ctx policy] "This is called in case an unreadiness policy is returned with any other :action than :requeue")
  (on-failure! [this ctx policy throwable] "Called after failed execute!")
  (get-failure-policy [this ctx throwable]  "Returns a policy map with an :action key. If said key is :requeue, the task is re-inserted into the queue and an accompanying :requeue-opts key will be passed to requeue!. Otherwise, on-failure! is called."))

(defn make-qtask
  [{:keys [task
           get-id
           ready?
           execute
           on-complete
           get-unreadiness-policy
           on-unreadiness
           get-failure-policy
           on-failure]
    :or   {ready?                  (fn [_ _])
           on-complete             (fn [_ _ _])
           on-unreadiness          (fn [_ _ _])
           on-failure              (fn [_ _ _ _])
           get-failure-policy      (fn [_ _ _])
           get-unreadiness-policy  (fn [_ _])}}]
  (reify QTask
    (task-id [_this] (get-id task))

    (get-raw [_this] task)

    (ready? [this ctx]
      (wrapped-call :ready? this (ready? this ctx)))

    (execute! [this ctx]
      (execute this ctx))

    (on-complete! [this ctx result]
      (wrapped-call :on-complete this (on-complete this ctx result)))

    (get-unreadiness-policy [this ctx]
      (wrapped-call :get-unreadiness-policy this (get-unreadiness-policy this ctx)))

    (on-unreadiness! [this ctx policy]
      (wrapped-call :on-unreadiness this (on-unreadiness this ctx policy)))

    (get-failure-policy [this ctx throwable]
      (wrapped-call :get-failure-policy this (get-failure-policy this ctx throwable)))

    (on-failure! [this ctx policy throwable]
      (wrapped-call :on-failure this (on-failure this ctx policy throwable)))))

(defn task-handler
  [task ctx]
  (try
    (let [result (execute! task ctx)]
      {:status :completed
       :result result})
    (catch Throwable t
      {:status :errored
       :err    t})))

(defn worker-loop
  [{:keys [queue-backend ctx task-config]}]
  (try
    (when-let [raw-task (dequeue! queue-backend)]
      (let [task (make-qtask (merge task-config {:task raw-task} ))]
        (if (ready? task ctx)
          (let [{:keys [status result err]} (task-handler task ctx)]
            (if (= status :completed)
              (do (on-complete! task ctx result)
                  (ack! queue-backend task))
              (let [{:keys [action requeue-opts]
                     :as   failure-policy} (get-failure-policy task ctx err)]
                (if (= action :requeue)
                  (requeue! queue-backend task requeue-opts)
                  (do
                    (on-failure! task ctx failure-policy err)
                    (nack! queue-backend task))))))
          (let [{:keys [action requeue-opts]
                 :as   unreadiness-policy} (get-unreadiness-policy task ctx)]
            (if (= action :requeue)
              (requeue! queue-backend task requeue-opts)
              (do (on-unreadiness! task ctx unreadiness-policy)
                  (abandon! queue-backend task)))))))
    (catch Throwable t
      (on-unexpected-error! queue-backend ctx t))))
