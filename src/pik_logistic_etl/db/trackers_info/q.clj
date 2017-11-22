(ns pik-logistic-etl.db.trackers-info.q
  (:require [hugsql.core :as hugsql]
            [pik-logistic-etl.util :refer [parse-str-int]]
            [pik-logistic-etl.db.queries :refer [etl-status]]))


(hugsql/def-db-fns "trackers_info/q.sql")


(def etl-name "trackers-info")
(def etl-state-name "last-event-id")

; последнее обработанное событие
(defn etl-last-event-id [conn]
  (if-let [res (etl-status conn {:etl_name etl-name :state_name etl-state-name})]
    (parse-str-int (:state_value res))
    0))

; последнее событие в БД
(defn data-last-event-id [conn]
  (if-let [res (last-event-id conn)]
    (:id res)
    0))


(defn get-events
  ([conn last-id] (events conn {:last_id last-id}))
  ([conn] (get-events conn 0)))


(defn get-tracker-info [conn tracker-id]
  (tracker-info conn {:tracker_id tracker-id}))
