(ns pik-logistic-etl.etl.time-in-zone
  (:require [pik-logistic-etl.db.queries :as q]
            [pik-logistic-etl.db.commands :as c]
            [pik-logistic-etl.db.core :refer [db]]
            [clj-time.core :as t]
            [clj-time.format :as tf]
            [clojure.tools.logging :as log]))

(def navyixy-time-formatter (tf/formatter "yyyy-MM-dd HH:mm:ss"))
(def one-day (t/days 1))
(def one-sec (t/seconds 1))

(defn- open-zone [event]
  (when (q/insert? db event)
    (c/open-zone! db event)))

;
;(q/last-inserted-event-for-close db {:id 78642084, :tracker_id 144953, :event "inzone", :time "2017-01-13 16:08:40", :zone_id 68989})
; проверить расстояние между :time и :in_time
;(def tt {:in_time "2017-01-14 09:08:09" :time "2017-01-14 02:34:39"})
;(tf/parse navyixy-time-formatter "2017-01-14 09:08:09")
;(def in-time (tf/parse navyixy-time-formatter "2017-01-29 09:08:09"))
;(def out-time (tf/parse navyixy-time-formatter "2017-02-03 23:45:00"))
;(when (t/after? out-time in-time)
;(t/in-days (t/interval in-time out-time))

(defn- next-times [intime]
  (let [beg-day (apply t/date-time (map #(%1 intime) [t/year t/month t/day]))
        next-day (t/plus beg-day one-day)
        end-day (t/minus next-day one-sec)]
    [end-day next-day]))

;(next-times in-time)

(defn- inout-times [intime outtime acc]
  (let [check-day (next-times intime)
        end-day (first check-day)
        next-day (second check-day)]
    (if (or (t/before? outtime end-day) (t/equal? outtime end-day))
      (conj acc [intime outtime])
      (inout-times next-day outtime (conj acc [intime end-day])))))

;(def tt (inout-times in-time out-time []))

(defn- to-sql-time [t]
  (tf/unparse navyixy-time-formatter t))

(defn- sql-inout-times [in-time out-time]
  (let [in-t (tf/parse navyixy-time-formatter in-time)
        out-t (tf/parse navyixy-time-formatter out-time)]
    (when (t/before? in-t out-t)
      (let [r (inout-times in-t out-t [])]
        (reduce #(conj %1 (map to-sql-time %2)) [] r)))))

;(sql-inout-times "2017-01-29 09:08:09" "2017-02-01 09:08:10")

(defn- close-zone-one [event guid out-time]
  (c/close-zone! db (merge event {:guid guid :time out-time})))



(defn- close-zone-in-days [event sql-times]
  (when-not (empty? sql-times)
    (let [tt (first sql-times)]
      (open-zone (merge event {:time (first tt)}))
      (let [rec (q/last-inserted-event-for-close db event)
            guid (:guid rec)]
        (close-zone-one event guid (second tt))))
    (recur event (rest sql-times))))


(defn close-zone [event]
  (when-let [ev (q/last-inserted-event-for-close db event)]
    (let [in-time (:in_time ev)
          out-time (:time event)
          sql-times (sql-inout-times in-time out-time)]
      (close-zone-in-days event sql-times))))

;(def e1 {:id 78669525, :tracker_id 144953, :event "outzone", :time "2017-01-13 18:48:50", :zone_id 68989})

;(sql-inout-times "2017-01-13 16:08:40" "2017-01-13 18:48:50")

;(close-zone-in-days e1 (sql-inout-times "2017-01-13 16:08:40" "2017-01-13 18:48:50"))
;(t/minus (t/plus (t/date-time 2017 1 14 2) one-day) one-sec)

;[['close], ['open, 'close], ['open, 'close]]
;(def e1 {:tracker_id 144942 :zone_id 68989})
;(q/last-inserted-event-for-close db e1)


(defn- save-etl-status [event]
  (let [params {:state_name "last-event-id" :state_value (:id event)}]
    (c/update-etl-status! db params)))

(defn- process-event [event]
  (case (:event event)
    "inzone" (open-zone event)
    "outzone" (close-zone event))
  (save-etl-status event))

(defn- process-events [events]
  (doseq [event events]
    (process-event event)))

;(def e1 {:id 78669834, :tracker_id 144953, :event "inzone", :time "2017-01-13 18:51:16", :zone_id 68989})
;(open-zone e1)
;(close-zone e1)
;(save-etl-status e1)
;(q/get-events db)
;(def tmp (q/get-events db))
;(identity tmp)
;(nth tmp 19)
;(process-event (nth tmp 19))
;(q/last-inserted-event-for-close db (nth tmp 8))
;(process-events tmp)
;(q/last-event-id db)

;получить события порциями
;обратывать пока есть события
(defn process []
  (loop [last-event-id (q/last-event-id db)
         events (q/get-events db last-event-id)]
    (when-not (empty? events)
      (process-events events)
      (log/info "processed 1000 events, from event-id: " last-event-id)
      (recur (q/last-event-id db) (q/get-events db last-event-id)))))

;(process)