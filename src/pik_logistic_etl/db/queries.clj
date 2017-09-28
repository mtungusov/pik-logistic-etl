(ns pik-logistic-etl.db.queries
  (:require [hugsql.core :as hugsql]))

(hugsql/def-db-fns "queries.sql")

(defn insert? [conn params]
  "params - {:tracker_id v1 :zone_id v2}"
  (and (empty? (last-inserted-opened-event conn params))
       (empty? (last-inserted-closed-event conn params))))

(defn get-events
  ([conn last-id] (events conn {:last_id last-id}))
  ([conn] (get-events conn 0)))

(defn last-event-id [conn]
  (if-let [res (etl-status conn)]
    (:state_value res)
    0))
