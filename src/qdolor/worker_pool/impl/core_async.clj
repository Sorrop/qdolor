(ns qdolor.worker-pool.impl.core-async
  (:require [qdolor.worker-pool.core :as wp]
            [qdolor.core :as qd]
            [clojure.core.async :as async]))

(defn make-io-chans
  [queue-backend num-workers ctx stop-ch]
  (reduce (fn [acc i]
            (let [ctx  (assoc ctx :worker i)
                  chan (async/io-thread
                         (loop []
                           (when-not (= (async/poll! stop-ch) :shutdown)
                             (qd/worker-loop queue-backend ctx)
                             (recur))))]
              (conj acc chan)))
          []
          (range num-workers)))

(deftype AsyncWorkerPool
    [num-workers queue-backend
     ^:volatile-mutable workers
     ^:volatile-mutable stop-ch]
  wp/WorkerPool
  (start! [this ctx]
    (let [stop-ch (async/promise-chan)
          chans   (make-io-chans queue-backend num-workers ctx stop-ch)]
      (set! (.-stop-ch this) stop-ch)
      (set! (.-workers this) chans)))
  (stop! [this]
    (let [stop-ch (.-stop-ch this)]
      (async/offer! stop-ch :shutdown)
      (doseq [w-c (.-workers this)]
        (async/close! w-c))
      (async/close! stop-ch))))
