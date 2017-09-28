(ns pik-logistic-etl.core
  (:require [clojure.tools.logging :as log]
            [mount.core :as mount]
            [pik-logistic-etl.config :refer [settings]]
            [pik-logistic-etl.db.core :refer [db]])
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
  (while (:running @state) (Thread/sleep 1000)))
