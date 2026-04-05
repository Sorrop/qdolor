(ns qdolor.worker-pool.impl.core-async
  (:require [qdolor.worker-pool.core :as wp]
            [qdolor.core :as qd]
            [clojure.core.async :as async]))

(defn make-io-chans
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
  [{:keys [queue-backend task-config num-workers poll-interval-ms]}]
  (->AsyncWorkerPool queue-backend
                     task-config
                     num-workers
                     poll-interval-ms
                     nil
                     nil))
