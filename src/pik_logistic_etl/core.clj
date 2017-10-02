(ns pik-logistic-etl.core
  (:require [clojure.tools.logging :as log]
            [mount.core :as mount]
            [pik-logistic-etl.config :refer [settings]]
            [pik-logistic-etl.db.core :refer [db]]
            [pik-logistic-etl.etl.time-in-zone :as time-in-zone])
  (:gen-class))

(def state (atom {}))

(defn init [args]
  (log/info "PIK logistic ETL starting...")
  (swap! state assoc :running true)
  (mount/start #'settings
               #'db))

(defn stop []
  (swap! state assoc :running false)
  (log/info "Stopping...")
  (shutdown-agents)
  (Thread/sleep 1000)
  (log/info "Stopped!"))

(defn -main [& args]
  (init args)
  (.addShutdownHook (Runtime/getRuntime)
                    (Thread. stop))
  (while (:running @state)
    (log/info "ETL time-in-zone")
    (time-in-zone/process)
    (Thread/sleep (* 5 60 1000))))
