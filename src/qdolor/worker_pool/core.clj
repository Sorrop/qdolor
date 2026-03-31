(ns qdolor.worker-pool.core)

(defprotocol WorkerPool
  (start! [this ctx])
  (stop! [this]))
