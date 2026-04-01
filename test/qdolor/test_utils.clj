(ns qdolor.test-utils
  (:require [java-time]))

(def log-datetime-format
  "yyyy-MM-dd HH:mm:ss.SSS")

(defn timestamp []
  (java-time/format (java-time/formatter log-datetime-format) (java-time/local-date-time)))

(defn from-timestamp [t]
  (java-time/local-date-time log-datetime-format t))

(defn log [s]
  (println (format "|%s|  %s" (timestamp) s)))
