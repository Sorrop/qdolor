(ns qdolor.worker-pool.impl.vthreads
  (:require [qdolor.worker-pool.core :as wp]
            [qdolor.core :as qd])
  (:import [java.util.concurrent Executors]))

(defn vt-worker
  [{:keys [poll-interval-ms stop-signal queue-backend ctx]}]
  (fn []
    (while (not= @stop-signal :shutdown)
      (Thread/sleep poll-interval-ms)
      (qd/worker-loop queue-backend ctx))))

(defn get-vt-workers
  [{:keys [executor queue-backend num-workers ctx stop-signal poll-interval-ms]
    :or   {poll-interval-ms 1000}}]
  (reduce (fn [acc i]
            (->> (.submit executor
                         (vt-worker
                           {:queue-backend    queue-backend
                            :poll-interval-ms poll-interval-ms
                            :stop-signal      stop-signal
                            :ctx              (merge {:worker i} ctx)}))
                 (conj acc)))
          []
          (range num-workers)))

(deftype VTWorkerPool
    [queue-backend
     num-workers
     poll-interval-ms
     ^:volatile-mutable workers
     ^:volatile-mutable stop-signal
     ^:volatile-mutable executor]
    wp/WorkerPool
    (start! [this ctx]
      (let [stop-signal (atom nil)
            executor    (Executors/newVirtualThreadPerTaskExecutor)
            workers     (get-vt-workers
                          {:executor executor
                           :queue-backend queue-backend
                           :num-workers num-workers
                           :ctx         ctx
                           :stop-signal stop-signal
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
          @w)
        (.shutdown executor))))

(defn vt-worker-pool
  [{:keys [queue-backend
           num-workers
           poll-interval-ms]}]
  (->VTWorkerPool queue-backend num-workers poll-interval-ms nil nil nil))
