(ns qdolor.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async]
            [java-time]
            [qdolor.worker-pool.impl.core-async :as qd.async]
            [qdolor.core :as qd.core]
            [qdolor.core-async-test :as ca.test]            
            [qdolor.test-utils :as t.utils]))

(def example-tasks
  [{:id :a :depends-on [:b :c]}
   {:id :d :depends-on []}
   {:id :b :depends-on [:c]}
   {:id :c :depends-on []}])

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
  [{:keys [worker-pool queue task-set task-sleeps completion-interval]}]
  (let [db  (init-db task-set)
        ctx {:db          db
             :task-sleeps task-sleeps}]
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
          q-backend (ca.test/queue-backend queue)
          exec-ctx  (run {:queue               queue
                          :task-set            example-tasks
                          :task-sleeps         1000
                          :completion-interval 10000})]
      (loop [all-tasks (vals @(:db exec-ctx))]
        (when-not (tasks-finished? all-tasks)
          (qd.core/worker-loop q-backend exec-ctx)
          (recur (vals @(:db exec-ctx)))))      
      (check-correctness exec-ctx))))

(deftest async-worker-pool-test
  (testing "Small example"
    (let [queue       (async/chan)
          q-backend   (ca.test/queue-backend queue)
          worker-pool (qd.async/async-worker-pool
                        {:queue-backend    q-backend
                         :num-workers      4
                         :poll-interval-ms 100})
          exec-ctx    (run {:worker-pool         worker-pool
                            :queue               queue
                            :task-set            example-tasks
                            :task-sleeps         2000
                            :completion-interval 10000})]
      (check-correctness exec-ctx))))

