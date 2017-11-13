(ns pik-logistic-etl.core
  (:require [clojure.tools.logging :as log]
            [mount.core :as mount]
            ;[pik-logistic-etl.config :refer [settings]]
            ;[pik-logistic-etl.db.core :refer [db]]
            [pik-logistic-etl.etl.time-in-zone :as time-in-zone])
  (:gen-class))

(def state (atom {}))

(defn init [args]
  (swap! state assoc :running true)
  ;(mount/start #'settings
  ;             #'db)
  (mount/start))


(defn- stop-main-loop []
  (swap! state assoc :running false))


(defn- stop []
  (log/info "Stopping...")
  (stop-main-loop)
  (log/info "Stopped."))


(defn- run-in-thread [period f]
  (.start
    (Thread.
      (fn []
        (try
          (while (:running @state)
            (f)
            (Thread/sleep period))
          (catch InterruptedException _)
          (catch Exception e
            (log/info e)
            (stop-main-loop)))))))


(defn- etl-time-in-zone []
  (log/info "ETL time-in-zone Start")
  (time-in-zone/process)
  (log/info "ETL time-in-zone Finish"))


(defn -main [& args]
  (init args)
  (.addShutdownHook (Runtime/getRuntime)
                    (Thread. stop))
  (log/info "PIK logistic ETL starting...")

  (run-in-thread (* 15 60 1000) etl-time-in-zone)

  (try
    (while (:running @state)
      (Thread/sleep 1000))
    (catch InterruptedException e)))
