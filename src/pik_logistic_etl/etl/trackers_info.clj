(ns pik-logistic-etl.etl.trackers-info
  (:require [clojure.tools.logging :as log]
            [clojure.java.jdbc :refer [with-db-transaction]]
            [pik-logistic-etl.db.core :refer [db-etl-trackers-info] :rename {db-etl-trackers-info db}]
            [pik-logistic-etl.db.trackers-info.q :as q]
            [pik-logistic-etl.db.commands :as c-core]
            [pik-logistic-etl.db.trackers-info.c :as c]))


(defn- save-etl-status [conn event_id]
  (let [params {:etl_name q/etl-name :state_name q/etl-state-name :state_value event_id}]
    (c-core/update-etl-status! conn params)))


(defn- process-event-inzone-with-parent-zone [event conn]
  (let [prev_info (q/get-tracker-info conn (:tracker_id event))
        zone_parent_id (:zone_parent_id event)
        prev_zone_id_in (:zone_id_in prev_info)
        prev_time_in (:time_in prev_info)]
    (cond-> event
      (= zone_parent_id prev_zone_id_in) (assoc :zone_id_out prev_zone_id_in
                                                :time_out prev_time_in))))

(defn- process-event-inzone [conn event]
  (cond-> event
    true (assoc :zone_id_in (:zone_id event)
                :time_in (:time event)
                :zone_id_out nil
                :time_out nil)
    (:zone_parent_id event) (process-event-inzone-with-parent-zone conn)))


(defn- process-event-outzone-with-parent-zone [event conn]
  (let [prev_info (q/get-tracker-info conn (:tracker_id event))
        zone_parent_id (:zone_parent_id event)
        prev_zone_id_out (:zone_id_out prev_info)
        prev_time_out (:time_out prev_info)]
    (cond-> event
            (= zone_parent_id prev_zone_id_out) (assoc :zone_id_in prev_zone_id_out
                                                      :time_in prev_time_out))))

(defn- process-event-outzone [conn event]
  (cond-> event
    true (assoc :zone_id_out (:zone_id event)
                :time_out (:time event)
                :zone_id_in nil
                :time_in nil)
    (:zone_parent_id event) (process-event-outzone-with-parent-zone conn)))

;(q/get-tracker-info db 0)
;(def event1 {:tracker_id 249961,
;             :event_id 197791939,
;             :event "inzone",
;             :time "2017-11-22 13:30:51",
;             :zone_id 68989,
;             :zone_parent_id nil})

;(c/update-tracker-info! db (process-event-inzone db event1))

;(def event2 {:tracker_id 249961,
;             :event_id 197791940,
;             :event "inzone",
;             :time "2017-11-22 13:32:50",
;             :zone_id 107544,
;             :zone_parent_id 68989})
;
;(def event3 {:tracker_id 249961,
;             :event_id 197791941,
;             :event "outzone",
;             :time "2017-11-22 13:35:00",
;             :zone_id 107543,
;             :zone_parent_id nil})
;
;(def event4 {:tracker_id 249961,
;             :event_id 197791942,
;             :event "outzone",
;             :time "2017-11-22 13:35:00",
;             :zone_id 107544,
;             :zone_parent_id 68989})

;(process-event-outzone db event4)


(defn- process-event [conn event]
  (c/update-tracker-info!
    conn
    (case (:event event)
      "inzone" (process-event-inzone conn event)
      "outzone" (process-event-outzone conn event)))
      ;"online" (log/info "online")))
  (:event_id event))

;(process-event db event1)
;(process-event db event2)
;(process-event db event3)
;(process-event db event4)


(defn- process-events [conn last-event-id]
  (log/info "> > > etl-trackers-info start from event-id: " last-event-id)
  (for [event (q/get-events conn last-event-id)
        :let [r (process-event conn event)]]
    r))


;(q/get-events db 197790181)

(defn process []
  (let [data-last-id (q/data-last-event-id db)]
    (loop []
      (let [etl-event-id (q/etl-last-event-id db)]
        (when (> data-last-id etl-event-id)
          (with-db-transaction [tx db]
            (let [processed-events (process-events tx etl-event-id)]
              (save-etl-status tx (last processed-events))))
          (recur))))))


;(q/data-last-event-id db)
;(q/etl-last-event-id db)
;(process)