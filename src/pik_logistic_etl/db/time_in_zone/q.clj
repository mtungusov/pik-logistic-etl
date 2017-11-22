(ns pik-logistic-etl.db.time-in-zone.q
  (:require [hugsql.core :as hugsql]
            [pik-logistic-etl.util :refer [parse-str-int]]
            [pik-logistic-etl.db.queries :refer [etl-status]]))

(hugsql/def-db-fns "time_in_zone/q.sql")

(defn insert? [conn params]
  "params - {:tracker_id v1 :zone_id v2}"
  (and (empty? (last-inserted-opened-event conn params))
       (empty? (last-inserted-closed-event conn params))))

(defn get-events
  ([conn last-id] (events conn {:last_id last-id}))
  ([conn] (get-events conn 0)))


(defn etl-last-event-id [conn]
  (if-let [res (etl-status conn {:etl_name "time-in-zone" :state_name "last-event-id"})]
    (parse-str-int (:state_value res))
    0))

(defn data-last-event-id [conn]
  (if-let [res (last-event-id conn)]
    (:id res)
    0))
