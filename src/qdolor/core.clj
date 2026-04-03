(ns qdolor.core)

(defmacro wrapped-call [phase & body]
  `(try
     ~@body
     (catch Throwable t#
       (throw (ex-info (format "Unexpected error on phase: %s" ~phase)
                       {:phase ~phase}
                       t#)))))

(defprotocol QBackend
  (dequeue! [this]  "Take next task from queue. Returns task or nil.")
  (ack! [this task] "Mark task as completed.")
  (nack! [this task] "Mark task as failed")
  (requeue! [this task opts] "Put task back with opts")
  (abandon! [this task] "Arbitrary, user defined action to perform when a task is not ready and will not be requeued")
  (on-unexpected-error [this ctx throwable maybe-task] "Debug method. Use this when anything except task's execute! throws"))

(defn make-qbackend
  [{:keys [queue
           dequeue-fn
           requeue-fn
           ack-fn
           nack-fn
           abandon-fn
           on-unexpected-error-fn]
    :or {dequeue-fn             (fn [_])
         requeue-fn             (fn [_ _ _])
         ack-fn                 (fn [_ _])
         nack-fn                (fn [_ _])
         abandon-fn             (fn [_ _])
         on-unexpected-error-fn (fn [_ _ _ _])}}]
  (reify QBackend

    (dequeue! [_this]
      (wrapped-call :dequeue (dequeue-fn queue)))

    (ack! [this task]
      (wrapped-call :ack (ack-fn this task)))

    (nack! [this task]
      (wrapped-call :nack (nack-fn this task)))

    (requeue! [_this task opts]
      (wrapped-call :requeue (requeue-fn queue task opts)))

    (abandon! [this task]
      (wrapped-call :abandon (abandon-fn this task)))

    (on-unexpected-error [this ctx throwable maybe-task]
      (on-unexpected-error-fn this ctx throwable maybe-task))))

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

(defmacro wrapped-call [phase & body]
  `(try
     ~@body
     (catch Throwable t#
       (throw (ex-info (format "Unexpected error on phase: %s" ~phase)
                       {:phase ~phase}
                       t#)))))

(defn make-qtask
  [{:keys [task
           get-id-fn
           ready-fn
           execute-fn
           on-complete-fn
           get-unreadiness-policy-fn
           on-unreadiness-fn
           get-failure-policy-fn
           on-failure-fn]
    :or   {ready-fn                  (fn [_ _])
           on-complete-fn            (fn [_ _ _])
           on-unreadiness-fn         (fn [_ _ _])
           on-failure-fn             (fn [_ _ _ _])
           get-failure-policy-fn     (fn [_ _ _])
           get-unreadiness-policy-fn (fn [_ _])}}]
  (reify QTask
    (task-id [_this] (get-id-fn task))

    (get-raw [_this] task)

    (ready? [this ctx]
      (wrapped-call :ready-check (ready-fn this ctx)))

    (execute! [this ctx]
      (execute-fn this ctx))

    (on-complete! [this ctx result]
      (wrapped-call :on-completion (on-complete-fn this ctx result)))

    (get-unreadiness-policy [this ctx]
      (wrapped-call :unreadiness-policy (get-unreadiness-policy-fn this ctx)))

    (on-unreadiness! [this ctx policy]
      (wrapped-call :unreadiness-action (on-unreadiness-fn this ctx policy)))

    (get-failure-policy [this ctx throwable]
      (wrapped-call :failure-policy (get-failure-policy-fn this ctx throwable)))

    (on-failure! [this ctx policy throwable]
      (wrapped-call :failure-action (on-failure-fn this ctx policy throwable)))))

(defn task-handler
  [task ctx]
  (try
    (let [result (execute! task ctx)]
      {:status :completed
       :result result})
    (catch Throwable t
      {:status :errored
       :err    t})))

(defn worker-loop [queue-backend ctx]
  (try
    (when-let [task (dequeue! queue-backend)]
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
                (abandon! queue-backend task))))))
    (catch Throwable t
      (on-unexpected-error queue-backend ctx t nil))))
