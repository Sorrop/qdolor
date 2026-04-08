# qdolor

[![Clojars Project](https://img.shields.io/clojars/v/net.clojars.sorrop/qdolor.svg)](https://clojars.org/net.clojars.sorrop/qdolor)

A Clojure library for queue-based execution of tasks by concurrent workers.

qdolor provides the consumer-side framework for processing tasks from a queue. It handles the worker loop, failure and unreadiness policies, and error propagation. 
How tasks are produced and what queue system backs the queue are left entirely to the user.

## Installation

Add to your `project.clj` dependencies:

```clojure
[net.clojars.sorrop/qdolor "0.2.0"]
```

## Concepts

### Task
A unit of work with an identity, a readiness predicate, an execution function, and lifecycle hooks. Defined via the `QTask` protocol or constructed with `make-qtask`.

### Queue backend
The connection to your queue system. Defined via the `QBackend` protocol or constructed with `make-qbackend`. Responsible for dequeuing, acknowledging, failing, and requeueing tasks.

### Worker pool
Runs N concurrent workers, each repeatedly polling the queue and processing tasks. Two implementations are provided: one backed by `core.async` io-threads, one by Java virtual (or platform) threads.

### Worker loop
The library provided execution cycle.

## Usage

### 1. Define a queue backend

```clojure
(require '[qdolor.core :as qd]
         '[clojure.core.async :as async])

(def the-queue (async/chan 64))

(def backend
  (qd/make-qbackend
    {:queue   the-queue

     :dequeue
     (fn [queue _ctx]
       (async/poll! queue))

     :requeue
     (fn [queue task _opts _ctx]
       (async/offer! queue (qd/get-raw task)))

     :ack
     (fn [queue task ctx]
       (println "Completed:" (qd/task-id task)))

     :nack
     (fn [queue task ctx]
       (println "Failed:" (qd/task-id task)))

     :abandon
     (fn [queue task ctx]
       (println "Abandoned:" (qd/task-id task)))

     :on-unexpected-error
     (fn [queue _ctx throwable]
       (println "Unexpected error:" (.getMessage throwable))
       ;; task (if available) is in (ex-data throwable) under :task
       ;; phase is in (ex-data throwable) under :phase
       )}))
```

### 2. Define a task configuration

A task configuration is a map of functions shared across all tasks processed by a worker pool. Individual task data is provided at dequeue time.

```clojure
(def task-conf
  {:get-id :id   ;; fn to extract the task id from raw task data. A keyword is fine for plain maps

   :ready?
   (fn [this ctx]
     ;; return true if the task is ready to execute
     true)

   :execute
   (fn [this ctx]
     ;; perform the work, return a result or throw
     (do-work (.get-raw this) ctx))

   :on-complete
   (fn [this ctx result]
     ;; called after successful execution, before ack
     )

   :get-unreadiness-policy
   (fn [this ctx]
     ;; return {:action :requeue} or whatever you want
     {:action :requeue})

   :on-unreadiness
   (fn [this ctx policy]
     ;; called when ready? returned false and :action is not :requeue
     )

   :get-failure-policy
   (fn [this ctx throwable]
     ;; return {:action :requeue, :requeue-opts {...}}
     ;; or     whatever you want
     {:action :nack})

   :on-failure
   (fn [this ctx policy throwable]
     ;; called when action is not :requeue
     )})
```

### 3. Start a worker pool

**core.async backed (io-threads):**

```clojure
(require '[qdolor.worker-pool.impl.core-async :as qd.async])

(def pool
  (qd.async/async-worker-pool
    {:queue-backend    backend
     :task-config      task-conf
     :num-workers      8
     :poll-interval-ms 100}))

(.start! pool {:db my-db})
```

**Virtual threads backed (JDK 21+):**

```clojure
(require '[qdolor.worker-pool.impl.vthreads :as qd.vt])

(def pool
  (qd.vt/vt-worker-pool
    {:queue-backend    backend
     :task-config      task-conf
     :num-workers      8
     :poll-interval-ms 100}))

(.start! pool {:cotext-map "here"})
```

If on JDK >= 21 virtual threads are used by default unless `:platform-threads` is passed. 
Platform threads are used by default on JDK < 21.

### 4. Enqueue tasks

qdolor does not prescribe how tasks are produced. Put raw task data into your queue by whatever means fits your system:

```clojure
(async/>!! the-queue {:id :task-1 :payload "..."})
```

### 5. Stop the pool

```clojure
(.stop! pool)
```

`stop!` blocks until all running workers have completed their current iteration.

## Task lifecycle

```
dequeue!
   │
   ▼
ready? ──false──► get-unreadiness-policy
   │                      │
   │              {:action :requeue} → requeue!
   │              other   →  abandon! → on-unreadiness!
   │
true
   │
   ▼
execute!
   │
   ├── success ──► ack! → on-complete!
   │
   └── throws ──► get-failure-policy
                         │
                 {:action :requeue} → requeue!
                 other  → nack! → on-failure!

Any phase except execute! throwing ──► on-unexpected-error!
  (ex-data of throwable contains :phase and :task when available)
```

## Dependency example

Tasks can declare dependencies on each other. The `ready?` function is the natural place to check them:

```clojure
(def db (atom {}))

(def task-conf
  {:get-id :id

   :ready?
   (fn [this _ctx]
     (let [{:keys [depends-on]} (.get-raw this)]
       (every? (fn [dep-id]
                 (= :success (get-in @db [dep-id :status])))
               depends-on)))

   :execute
   (fn [this _ctx]
     (process (.get-raw this)))

   :on-complete
   (fn [this _ctx result]
     (swap! db assoc (.task-id this) (assoc result :status :success)))

   :get-unreadiness-policy
   (fn [_ _] {:action :requeue})

   :get-failure-policy
   (fn [_ _ _] {:action :nack})})
```

## Choosing a worker pool implementation

Both pools poll the queue on a fixed interval (`poll-interval-ms`).

The **core.async pool** creates workers as `clojure.core.async/io-thread`'s.

The **virtual threads pool** submits workers to a `newVirtualThreadPerTaskExecutor`. 


## Error handling

The library does not make any decisions as to what should happen when an unexpected error happens. That is, 
any error outside of `execute!` method of a task. Any such error, will be encapsulated in a `Throwable`. 
The `Throwable` is enriched with `ex-data` containing:

- `:phase` — the keyword identifying where the error occurred (`:ready?`, `:on-complete`, `:get-failure-policy`, `:ack`, `:requeue`, etc.)
- `:task` — the raw task data, when available

The enriched exception is passed to `on-unexpected-error!` on the backend and the user 
of this library can then decide what action to take on each phase,
depending on their use-case. 


## Extension points

The library is designed to aid pluggability, so that you can switch, for example, between different Task handling and Worker pools
for a given queue backend. This can be achieved by offering your own implementations for these protocols

- `QBackend` — implement to connect any queue system
- `QTask` — implement directly if `make-qtask` is too constraining
- `WorkerPool` — implement to provide a custom concurrency strategy



## License

Copyright © 2026 Chris Klaoudianis

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
https://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
