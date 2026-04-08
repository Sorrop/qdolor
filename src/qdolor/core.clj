(ns qdolor.core
  "Core protocols, constructors, and worker loop.")

(defmacro wrapped-call
  "Optionally enrich any thrown exception with a given phase
   and possibly a task. The error is delivered to `on-unexpected-error!`."
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
                        t#))))))

(defprotocol QBackend
  "Represents a queue system. All methods except `on-unexpected-error!` 
   are automatically wrapped, enriched with phase and task context and 
   routed to `on-unexpected-error!`. All methods except `on-unexpected-error!`
   receive a context map as the last argument"

  (dequeue! [this ctx]
    "Take the next task from the queue. Returns the raw task value, or nil if
    the queue is empty.")

  (ack! [this task ctx]
    "Mark a task as successfully completed.")

  (nack! [this task ctx]
    "Mark a task as failed.")

  (requeue! [this task opts ctx]
    "Return a task to the queue. `opts` is the `:requeue-opts` map from the
     task's failure or unreadiness policy, and may contain hints such as delay.")

  (abandon! [this task ctx]
    "Called when a task is not ready and its unreadiness policy action is not
    `:requeue`.")

  (on-unexpected-error! [this ctx throwable]
    "Called when any phase other than `execute!` throws. 
     The throwable's `ex-data` contains:

    - `:phase`: keyword identifying the phase (`:ready?`, `:ack`, `:requeue` etc.)
    - `:task`: the raw task data, when the error occurred after dequeue"))

(defn make-qbackend
  "Constructs a `QBackend` from a map of functions.

  Required key:

  - `:queue` — the queue object passed as the first argument all functions

  Optional keys (all default to no-ops):

  - `:dequeue`             — `(fn [queue ctx])`
  - `:requeue`             — `(fn [queue task opts])`
  - `:ack`                 — `(fn [queue task])`
  - `:nack`                — `(fn [queue task])`
  - `:abandon`             — `(fn [queue task])`
  - `:on-unexpected-error` — `(fn [queue ctx throwable])`"
  [{:keys [queue
           dequeue
           requeue
           ack
           nack
           abandon
           on-unexpected-error]
    :or {dequeue             (fn [_ _])
         requeue             (fn [_ _ _ _])
         ack                 (fn [_ _ _])
         nack                (fn [_ _ _])
         abandon             (fn [_ _ _])
         on-unexpected-error (fn [_ _ _])}}]
  (reify QBackend

    (dequeue! [_this ctx]
      (wrapped-call :dequeue (dequeue queue ctx)))

    (ack! [_this task ctx]
      (wrapped-call :ack task (ack queue task ctx)))

    (nack! [_this task ctx]
      (wrapped-call :nack task (nack queue task ctx)))

    (requeue! [_this task opts ctx]
      (wrapped-call :requeue task (requeue queue task opts ctx)))

    (abandon! [_this task ctx]
      (wrapped-call :abandon task (abandon queue task ctx)))

    (on-unexpected-error! [_this ctx throwable]
      (on-unexpected-error queue ctx throwable))))

(defprotocol QTask
  "Specification of a task."

  (task-id [this]
    "Return the task's unique identifier.")

  (get-raw [this]
    "Return the raw task data as it was dequeued.")

  (ready? [this ctx]
    "Return true if the task's preconditions are satisfied and it should be
    executed.")

  (execute! [this ctx]
    "Perform the task's work. If it throws it is regarded as failure and next actions 
     are dictated by a failure policy. Otherwise, a result value is returned 
     and passed to `on-complete!`.")

  (on-complete! [this ctx result]
    "Called after a successful `execute!` and after `ack!` is called on the
    backend.")

  (get-unreadiness-policy [this ctx]
    "Called when `ready?` returns false. Return a policy map with an `:action`
     key:

    - `{:action :requeue, :requeue-opts {...}}` — put the task back in the queue
    - `{:action <anything-else>}` — call `on-unreadiness!` then `abandon!`")

  (on-unreadiness! [this ctx policy]
    "Applies any other unreadiness policy than :requeue after `abandon!`
     is called on the backend.")

  (get-failure-policy [this ctx throwable]
    "Called when `execute!` throws. Return a policy map with an `:action` key:

    - `{:action :requeue, :requeue-opts {...}}` — put the task back in the queue
    - `{:action <anything-else>}` — call `on-failure!` then `nack!`")

  (on-failure! [this ctx policy throwable]
    "Applies any other failure policy than :requeue, after nack! is called
     on the queue backend"))

(defn make-qtask
  "Constructs a `QTask` from a map of functions and the raw task data.

  Required keys:

  - `:task`    — the raw task value
  - `:get-id`  — fn or keyword to extract the task id from raw task data
  - `:execute` — `(fn [this ctx])` performs the work

  Optional keys (all have sane defaults):

  - `:ready?`                  — `(fn [this ctx])`, defaults to `(constantly true)`
  - `:on-complete`             — `(fn [this ctx result])`, defaults to no-op
  - `:get-unreadiness-policy`  — `(fn [this ctx])`, defaults to `{:action :requeue}`
  - `:on-unreadiness`          — `(fn [this ctx policy])`, defaults to no-op
  - `:get-failure-policy`      — `(fn [this ctx throwable])`, defaults to `{:action :nack}`
  - `:on-failure`              — `(fn [this ctx policy throwable])`, defaults to no-op"
  [{:keys [task
           get-id
           ready?
           execute
           on-complete
           get-unreadiness-policy
           on-unreadiness
           get-failure-policy
           on-failure]
    :or   {ready?                 (fn [_ _] true)
           on-complete            (fn [_ _ _])
           on-unreadiness         (fn [_ _ _])
           on-failure             (fn [_ _ _ _])
           get-failure-policy     (fn [_ _ _] {:action :nack})
           get-unreadiness-policy (fn [_ _] {:action :requeue})}}]
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
  "Executes one iteration of the worker loop against the given queue backend.

  Accepts a map with:

  - `:queue-backend` — a `QBackend` instance
  - `:ctx`           — arbitrary context map injected into all task functions
  - `:task-config`   — task configuration map passed to `make-qtask`"
  [{:keys [queue-backend ctx task-config]}]
  (try
    (when-let [raw-task (dequeue! queue-backend ctx)]
      (let [task (make-qtask (merge task-config {:task raw-task}))]
        (if (ready? task ctx)
          (let [{:keys [status result err]} (task-handler task ctx)]
            (if (= status :completed)
              (do (ack! queue-backend task ctx)
                  (on-complete! task ctx result))
              (let [{:keys [action requeue-opts]
                     :as   failure-policy} (get-failure-policy task ctx err)]
                (if (= action :requeue)
                  (requeue! queue-backend task requeue-opts ctx)
                  (do
                    (nack! queue-backend task ctx)
                    (on-failure! task ctx failure-policy err))))))
          (let [{:keys [action requeue-opts]
                 :as   unreadiness-policy} (get-unreadiness-policy task ctx)]
            (if (= action :requeue)
              (requeue! queue-backend task requeue-opts ctx)
              (do (abandon! queue-backend task ctx)
                  (on-unreadiness! task ctx unreadiness-policy)))))))
    (catch Throwable t
      (on-unexpected-error! queue-backend ctx t))))
