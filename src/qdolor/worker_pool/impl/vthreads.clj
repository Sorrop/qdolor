(ns qdolor.worker-pool.impl.vthreads
  (:require [qdolor.worker-pool.core :as wp]
            [qdolor.core :as qd]
            [qdolor.utils :as utils])
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

(defn choose-executor [{:keys [backend-opt num-workers]}]
  (if (or (not utils/virtual-threads-available?)
          (= :platform-threads backend-opt))
    (Executors/newFixedThreadPool num-workers)
    (let [start-exec (.getMethod Executors
                                 "newVirtualThreadPerTaskExecutor"
                                 (into-array Class []))]
      (.invoke start-exec nil (object-array 0)))))

(deftype VTWorkerPool
    [queue-backend
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
           poll-interval-ms
           backend-opt]}]
  (->VTWorkerPool queue-backend num-workers poll-interval-ms backend-opt nil nil nil))
