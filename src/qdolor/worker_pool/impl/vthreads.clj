(ns qdolor.worker-pool.impl.vthreads
  "Worker pool implementation backed by Java virtual threads (JDK 21+) or a
  fixed platform thread pool.

  Shutdown is coordinated via an atom. `stop!` blocks on each worker future
  until it exits its current iteration."
  (:require [qdolor.worker-pool.core :as wp]
            [qdolor.core :as qd]
            [qdolor.utils :as utils])
  (:import [java.util.concurrent Executors]))

(defn- vt-worker
  "Returns a `Runnable` that polls the queue on a fixed interval until
  `stop-signal` is set to `:shutdown`."
  [{:keys [poll-interval-ms stop-signal queue-backend ctx task-config]}]
  (fn []
    (while (not= @stop-signal :shutdown)
      (Thread/sleep poll-interval-ms)
      (qd/worker-loop
        {:queue-backend queue-backend
         :ctx           ctx
         :task-config   task-config}))))

(defn- get-vt-workers
  "Submits `num-workers` worker runnables to `executor`. Returns a vector of
  futures."
  [{:keys [executor queue-backend task-config num-workers ctx stop-signal poll-interval-ms]
    :or   {poll-interval-ms 1000}}]
  (reduce (fn [acc i]
            (->> (.submit executor
                          (vt-worker
                            {:queue-backend    queue-backend
                             :task-config      task-config
                             :poll-interval-ms poll-interval-ms
                             :stop-signal      stop-signal
                             :ctx              (merge {:worker i} ctx)}))
                 (conj acc)))
          []
          (range num-workers)))

(defn- choose-executor
  "Returns an executor appropriate for the runtime and `:backend-opt`.

  Uses a Virtual thread executor on JDK 21+, unless
  `:backend-opt :platform-threads` is passed, in which case a fixed platform
  thread pool of size `num-workers` is used."
  [{:keys [backend-opt num-workers]}]
  (if (or (not utils/virtual-threads-available?)
          (= :platform-threads backend-opt))
    (Executors/newFixedThreadPool num-workers)
    (let [start-exec (.getMethod Executors
                                 "newVirtualThreadPerTaskExecutor"
                                 (into-array Class []))]
      (.invoke start-exec nil (object-array 0)))))

(deftype VTWorkerPool
    [queue-backend
     task-config
     num-workers
     poll-interval-ms
     backend-opt
     ^:volatile-mutable workers
     ^:volatile-mutable stop-signal
     ^:volatile-mutable executor]
  wp/WorkerPool
  (start! [this ctx]
    (let [stop-signal (atom nil)
          executor    (choose-executor
                        {:backend-opt backend-opt
                         :num-workers num-workers})
          workers     (get-vt-workers
                        {:executor         executor
                         :queue-backend    queue-backend
                         :task-config      task-config
                         :num-workers      num-workers
                         :ctx              ctx
                         :stop-signal      stop-signal
                         :poll-interval-ms poll-interval-ms})]
      (set! (.-stop-signal this) stop-signal)
      (set! (.-executor this) executor)
      (set! (.-workers this) workers)))

  (stop! [this]
    (let [stop-signal (.-stop-signal this)
          executor    (.-executor this)
          workers     (.-workers this)]
      (reset! stop-signal :shutdown)
      (doseq [w workers]
        (try @w (catch Exception _ nil)))
      (.shutdown executor))))

(defn vt-worker-pool
  "Constructs a `WorkerPool` backed by Java virtual threads (JDK 21+).

  Options:

  - `:queue-backend`    — a `QBackend` instance
  - `:task-config`      — task configuration map passed to `make-qtask`
  - `:num-workers`      — number of concurrent workers
  - `:poll-interval-ms` — milliseconds to sleep between queue polls (default 1000)
  - `:backend-opt`      — pass `:platform-threads` to force a fixed daemon
                          thread pool, e.g. on JDK < 21"
  [{:keys [queue-backend
           task-config
           num-workers
           poll-interval-ms
           backend-opt]}]
  (->VTWorkerPool queue-backend
                  task-config
                  num-workers
                  poll-interval-ms
                  backend-opt
                  nil
                  nil
                  nil))
