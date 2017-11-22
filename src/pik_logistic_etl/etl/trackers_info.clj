(ns pik-logistic-etl.etl.trackers-info
  (:require [clojure.tools.logging :as log]
            [clojure.java.jdbc :refer [with-db-transaction]]
            [pik-logistic-etl.db.core :refer [db-etl-trackers-info] :rename {db-etl-trackers-info db}]
            [pik-logistic-etl.db.trackers-info.q :as q]
            [pik-logistic-etl.db.commands :as c-core]))


(defn- save-etl-status [conn event_id]
  (let [params {:etl_name q/etl-name :state_name q/etl-state-name :state_value event_id}]
    (c-core/update-etl-status! conn params)))


(defn- process-event-inzone [conn event]
  (let [event_id (:event_id event)
        event_time (:time event)
        event_type (:event event)
        tracker_id (:tracker_id event)
        zone_id (:zone_id event)
        zone_parent_id (:zone_parent_id event)
        prev_info (q/get-tracker-info conn tracker_id)
        prev_zone_id_in (:zone_id_in prev_info)
        prev_time_in (:time_in prev_info)
        params {:tracker_id tracker_id
                :event_id event_id
                :event event_type
                :zone_id_in zone_id
                :time_in event_time
                :zone_id_out nil
                :time_out nil}]
    (cond-> params
      (and
        (not zone_parent_id)
        (= zone_parent_id prev_zone_id_in)) (assoc :zone_id_out prev_zone_id_in :time_out prev_time_in))))

    ;
    ;
    ;(if-not zone_parent_id
    ;  (log/info params)
    ;  (let [prev_info (q/get-tracker-info conn tracker_id)
    ;        prev_zone_id_in (:zone_id_in prev_info)
    ;        prev_time_in (:time_in prev_info)]
    ;    (if (= zone_parent_id prev_zone_id_in)
    ;      (log/info (merge params {:zone_id_out prev_zone_id_in
    ;                               :time_out prev_time_in}))
    ;      (log/info params))))))


;(q/get-tracker-info db 0)
;(process-event-inzone db {:tracker_id 249961,
;                          :event_id 197791939,
;                          :event "inzone",
;                          :time "2017-11-22 13:30:50",
;                          :zone_id 68989,
;                          :zone_parent_id nil})
;
;(process-event-inzone db {:tracker_id 249961,
;                          :event_id 197791940,
;                          :event "inzone",
;                          :time "2017-11-22 13:32:50",
;                          :zone_id 107544,
;                          :zone_parent_id 68989})




(defn- process-event [conn event]
  (log/info (identity event))
  (let [id (:event_id event)
        event_type (:event event)]
    (case event_type
      "inzone" (log/info "inzone")
      "outzone" (log/info "outzone")
      "online" (log/info "online"))
    id))

;(process-event db {:tracker_id 144950, :event_id 76316107, :event "online", :time "2017-01-01 03:48:57", :zone_id 107544})


(defn- process-events [conn last-event-id]
  (log/info "> > > etl-trackers-info start from event-id: " last-event-id)
  (for [event (q/get-events conn last-event-id)
        :let [r (process-event conn event)]]
    r))


(q/get-events db 197790181)

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