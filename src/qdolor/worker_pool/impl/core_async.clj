(ns qdolor.worker-pool.impl.core-async
  "Worker pool implementation backed by `core.async` io-threads.
   Workers run as io-threads (virtual threads on JDK 21+). "
  (:require [qdolor.worker-pool.core :as wp]
            [qdolor.core :as qd]
            [clojure.core.async :as async]))

(defn- make-io-chans
  "Starts `num-workers` io-threads, each running a polling loop."
  [{:keys [queue-backend task-config num-workers ctx stop-ch poll-interval-ms]
    :or   {poll-interval-ms 1000}}]
  (reduce (fn [acc i]
            (let [ctx  (assoc ctx :worker i)
                  chan (async/io-thread
                         (loop []
                           (when-not (= (async/poll! stop-ch) :shutdown)
                             (Thread/sleep poll-interval-ms)
                             (qd/worker-loop
                               {:queue-backend queue-backend
                                :ctx           ctx
                                :task-config   task-config})
                             (recur))))]
              (conj acc chan)))
          []
          (range num-workers)))

(deftype AsyncWorkerPool
    [queue-backend
     task-config
     num-workers
     poll-interval-ms
     ^:volatile-mutable workers
     ^:volatile-mutable stop-ch]
  wp/WorkerPool
  (start! [this ctx]
    (let [stop-ch (async/promise-chan)
          chans   (make-io-chans
                    {:queue-backend    queue-backend
                     :task-config      task-config
                     :num-workers      num-workers
                     :ctx              ctx
                     :stop-ch          stop-ch
                     :poll-interval-ms poll-interval-ms})]
      (set! (.-stop-ch this) stop-ch)
      (set! (.-workers this) chans)))
  (stop! [this]
    (let [stop-ch (.-stop-ch this)]
      (async/offer! stop-ch :shutdown)
      (async/<!! (async/merge (.-workers this)))
      (async/close! stop-ch))))

(defn async-worker-pool
  "Constructs a `WorkerPool` backed by `core.async` io-threads.

  Options:

  - `:queue-backend`    — a `QBackend` instance
  - `:task-config`      — task configuration map that will be passed to `make-qtask`
  - `:num-workers`      — number of concurrent workers
  - `:poll-interval-ms` — milliseconds to sleep between queue polls (default 1000)"
  [{:keys [queue-backend task-config num-workers poll-interval-ms]}]
  (->AsyncWorkerPool queue-backend
                     task-config
                     num-workers
                     poll-interval-ms
                     nil
                     nil))
