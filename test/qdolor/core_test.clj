(ns qdolor.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [java-time]
            [qdolor.worker-pool.impl.core-async :as qd.async]
            [qdolor.worker-pool.impl.vthreads :as qd.vthreads]
            [qdolor.core :as qd.core]
            [qdolor.core-async-test :as ca.test]
            [qdolor.errors-test :as er.test]
            [qdolor.test-utils :as t.utils]
            [qdolor.pg-test :as pg.test]))

(def example-tasks
  [{:id :a :depends-on [:b :c]}
   {:id :d :depends-on []}
   {:id :b :depends-on [:c]}
   {:id :c :depends-on []}])

(defn gen-tasks
  [num-tasks max-deps]
  (reduce (fn [acc i]
            (let [id           (keyword (str i))
                  available    (mapv :id acc)
                  max-possible (min max-deps (count available))
                  num-deps     (rand-int (inc max-possible))
                  depends-on   (vec (take num-deps (shuffle available)))]
              (conj acc {:id id :depends-on depends-on})))
          []
          (range num-tasks)))

(defn when-dependencies-finished
  [db task]
  (let [{:keys [depends-on]} task]
    (->> (select-keys @db depends-on)
         vals
         (map (comp t.utils/from-timestamp :finished-at )))))

(defn check-correctness [exec-ctx]
  (let [{:keys [db]} exec-ctx
        tasks        (-> @db vals)]
    (doseq [t    tasks
            :let [{:keys [finished-at]} t
                  finished-at (t.utils/from-timestamp finished-at)
                  dependencies-finished-ats (when-dependencies-finished db t)]]
      (doseq [dep-finished-at dependencies-finished-ats]
        (is (java-time/before? dep-finished-at
                                 finished-at))))))

(defn tasks-finished? [tasks]
  (every? (comp some? :finished-at) tasks))

(defn init-db
  [task-set]
  (atom (zipmap (map :id task-set) task-set)))

(defn run
  [{:keys [worker-pool queue task-set task-sleeps completion-interval ctx]}]
  (let [db  (init-db task-set)
        ctx (merge ctx
                   {:db          db
                    :task-sleeps task-sleeps})]
    (when worker-pool
      (.start! worker-pool ctx))  

    (doseq [t task-set]
      (async/>!! queue t))

    
    (when worker-pool
      (Thread/sleep completion-interval)
      (.stop! worker-pool))

    ctx))

(deftest sequential-test
  (testing "Small example"
    (let [queue     (async/chan 4)
          q-backend (qd.core/make-qbackend
                      (merge {:queue queue} ca.test/qbackend-conf))
          exec-ctx  (run {:queue               queue
                          :task-set            example-tasks
                          :task-sleeps         1000
                          :completion-interval 10000})]
      (loop [all-tasks (vals @(:db exec-ctx))]
        (when-not (tasks-finished? all-tasks)
          (qd.core/worker-loop
            {:queue-backend q-backend
             :ctx exec-ctx
             :task-config ca.test/task-conf})
          (recur (vals @(:db exec-ctx)))))      
      (check-correctness exec-ctx)))

  (testing "Big example"
    (let [queue     (async/chan 1024)
          q-backend (qd.core/make-qbackend
                      (merge {:queue queue} ca.test/qbackend-conf))
          task-set  (gen-tasks 1024 300)
          exec-ctx  (run {:queue       queue
                          :task-set    task-set
                          :task-sleeps 1})]
      (loop [all-tasks (vals @(:db exec-ctx))]
        (when-not (tasks-finished? all-tasks)
          (qd.core/worker-loop
            {:queue-backend q-backend
             :ctx exec-ctx
             :task-config ca.test/task-conf})
          (recur (vals @(:db exec-ctx)))))
      (check-correctness exec-ctx))))

(defn concurrent-correct
  [{:keys [db]}]
  (let [tasks (-> @db vals)]
    (doseq [[_ common] (->> (group-by :finished-at tasks)
                            (reduce-kv (fn [acc k v]
                                         (conj acc [k v]))
                                       []))
            :let [all-depend-ons (map (comp set :depends-on) common)]]
      (doseq [t    common
              :let [id (:id t)]]
        (is (empty? (filter #(contains? % id) all-depend-ons)))))))

(deftest async-worker-pool-test
  (testing "Small example"
    (let [queue       (async/chan)
          q-backend   (qd.core/make-qbackend
                        (merge {:queue queue} ca.test/qbackend-conf))
          worker-pool (qd.async/async-worker-pool
                        {:queue-backend    q-backend
                         :task-config      ca.test/task-conf
                         :num-workers      4
                         :poll-interval-ms 100})
          exec-ctx    (run {:worker-pool         worker-pool
                            :queue               queue
                            :task-set            example-tasks
                            :task-sleeps         2000
                            :completion-interval 10000})]
      (t.utils/log "Async worker pool small run finished")
      (check-correctness exec-ctx)))

  (testing "Big example"
    (let [queue       (async/chan 1024)
          q-backend (qd.core/make-qbackend
                      (merge {:queue queue} ca.test/qbackend-conf))
          task-set    (gen-tasks 1024 300)
          worker-pool (qd.async/async-worker-pool
                        {:queue-backend    q-backend
                         :task-config      ca.test/task-conf
                         :num-workers      32
                         :poll-interval-ms 1})
          exec-ctx    (run {:worker-pool         worker-pool
                            :queue               queue
                            :task-set            task-set
                            :task-sleeps         1
                            :completion-interval 10000})]
      (t.utils/log "Async worker pool big run finished")
      (check-correctness exec-ctx)
      (testing "Concurrently executed tasks should not depend between themselves"
        (concurrent-correct exec-ctx)))))

(deftest virtual-threads-worker-pool-test
  (testing "Small example"
    (let [queue       (async/chan)
          q-backend (qd.core/make-qbackend
                      (merge {:queue queue} ca.test/qbackend-conf))
          worker-pool (qd.vthreads/vt-worker-pool
                        {:queue-backend    q-backend
                         :task-config      ca.test/task-conf
                         :num-workers      4
                         :poll-interval-ms 100})
          exec-ctx    (run {:worker-pool         worker-pool
                            :queue               queue
                            :task-set            example-tasks
                            :task-sleeps         2000
                            :completion-interval 10000})]
      (t.utils/log "Virtual Threads backed worker pool small run finished")
      (check-correctness exec-ctx)))

  (testing "Big example"
    (let [queue       (async/chan 4096)
          q-backend (qd.core/make-qbackend
                      (merge {:queue queue} ca.test/qbackend-conf))
          task-set    (gen-tasks 4096 300)
          worker-pool (qd.vthreads/vt-worker-pool
                        {:queue-backend    q-backend
                         :task-config      ca.test/task-conf
                         :num-workers      32
                         :poll-interval-ms 1})
          exec-ctx    (run {:worker-pool         worker-pool
                            :queue               queue
                            :task-set            task-set
                            :task-sleeps         1
                            :completion-interval 70000})]
      (t.utils/log "Virtual Threads backed worker pool big run finished")
      (check-correctness exec-ctx)
      (testing "Concurrently executed tasks should not depend between themselves"
        (concurrent-correct exec-ctx)))))

(deftest platform-threads-worker-pool-test
  (testing "Small example"
    (let [queue       (async/chan)
          q-backend   (qd.core/make-qbackend
                        (merge {:queue queue} ca.test/qbackend-conf))
          worker-pool (qd.vthreads/vt-worker-pool
                        {:queue-backend    q-backend
                         :task-config      ca.test/task-conf
                         :backend-opt      :platform-threads
                         :num-workers      4
                         :poll-interval-ms 100})
          exec-ctx    (run {:worker-pool         worker-pool
                            :queue               queue
                            :task-set            example-tasks
                            :task-sleeps         2000
                            :completion-interval 10000})]
      (t.utils/log "Platform Threads backed worker pool small run finished")
      (check-correctness exec-ctx)))

  (testing "Big example"
    (let [queue       (async/chan 4096)
          q-backend   (qd.core/make-qbackend
                        (merge {:queue queue} ca.test/qbackend-conf))
          task-set    (gen-tasks 4096 300)
          worker-pool (qd.vthreads/vt-worker-pool
                        {:queue-backend    q-backend
                         :task-config      ca.test/task-conf
                         :backend-opt      :platform-threads
                         :num-workers      32
                         :poll-interval-ms 1})
          exec-ctx    (run {:worker-pool         worker-pool
                            :queue               queue
                            :task-set            task-set
                            :task-sleeps         1
                            :completion-interval 70000})]
      (t.utils/log "Platform Threads backed worker pool big run finished")
      (check-correctness exec-ctx)
      (testing "Concurrently executed tasks should not depend between themselves"
        (concurrent-correct exec-ctx)))))

(deftest failure-test
  (testing "Throwable contains task in applicable phases"
    (let [applicable-phases [:ack
                             :nack
                             :requeue
                             :abandon
                             :ready?
                             :on-complete
                             :get-unreadiness-policy
                             :on-unreadiness
                             :get-failure-policy
                             :on-failure]]
      (doseq [phase applicable-phases
              :let [queue       (async/chan 1)
                    q-backend   (qd.core/make-qbackend
                                  (merge {:queue queue}
                                         er.test/qbackend-conf))
                    task-set    [(assoc (first example-tasks)
                                        :inject-error {phase true})]
                    worker-pool (qd.async/async-worker-pool
                                  {:queue-backend    q-backend
                                   :task-config      er.test/task-conf
                                   :num-workers      4
                                   :poll-interval-ms 100})
                    exec-ctx    (run {:worker-pool         worker-pool
                                      :queue               queue
                                      :task-set            task-set
                                      :task-sleeps         5
                                      :completion-interval 1000})
                    db (:db exec-ctx)]]
        (is (= phase (get-in @db [:injected-error :phase])))
        (is (= (first task-set)
               (get-in @db [:injected-error :task])))))))

(deftest pg-queue-test
  (testing "Postgres serial execution"
    (let [total-tasks        50
          {:keys [container
                  jdbc-url]} (t.utils/set-up-pg total-tasks)
          q-backend          (-> (pg.test/qbackend-conf jdbc-url)
                                 qd.core/make-qbackend)
          ctx                {:conn-store  {:serial (atom nil)}
                              :task-sleeps 200
                              :worker      :serial
                              :jdbc-url    jdbc-url}
          task-conf          pg.test/task-conf]
      (doseq [_ (range total-tasks)]
         (qdolor.core/worker-loop
           {:queue-backend q-backend
            :ctx           ctx
            :task-config   task-conf}))
      (let [results (->> (pg.test/db-results jdbc-url)
                         (map :finished_at))]
        (is (true? (every? some? results)))
        (is (= total-tasks (count results)))
        (is (true? (pg.test/queue-empty? jdbc-url))))
       (t.utils/log "Sequential execution from pg backed queue finished")
       (.stop (-> container :container))))

  (testing "Postgres core async based"
    (let [total-tasks        50
          num-workers        4
          {:keys [container
                  jdbc-url]} (t.utils/set-up-pg total-tasks)  
          q-backend          (-> (pg.test/qbackend-conf jdbc-url)
                                 qd.core/make-qbackend)
          ctx                {:conn-store  (->> (mapv atom (range num-workers))
                                                (zipmap (range num-workers)))
                              :task-sleeps 200
                              :jdbc-url    jdbc-url}
          worker-pool        (qd.async/async-worker-pool
                               {:queue-backend    q-backend
                                :task-config      pg.test/task-conf
                                :num-workers      num-workers
                                :poll-interval-ms 50})]
      (.start! worker-pool ctx)
      (Thread/sleep 4500)
      (let [results (->> (pg.test/db-results jdbc-url)
                         (map :finished_at))]
        (is (true? (every? some? results)))
        (is (= total-tasks (count results)))
        (is (true? (pg.test/queue-empty? jdbc-url))))
      (t.utils/log "Core async worker pool with pg backed queue finished")
      (.stop! worker-pool)
      (.stop (-> container :container)))))
