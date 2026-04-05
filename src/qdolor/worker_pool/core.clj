(ns qdolor.worker-pool.core
  "Worker pool protocol.")

(defprotocol WorkerPool
  "A pool of concurrent workers."

  (start! [this ctx]
    "Start the worker pool. `ctx` is an arbitrary map injected into every task
    callback during this pool's lifetime. Returns nil.")

  (stop! [this]
    "Signal all workers to stop and block until they have exited their current
    iteration. Returns nil."))
