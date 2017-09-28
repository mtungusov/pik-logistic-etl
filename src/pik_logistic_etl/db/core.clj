(ns pik-logistic-etl.db.core
  (:require [mount.core :refer [defstate]]
            [pik-logistic-etl.config :refer [settings]]
            [pik-logistic-etl.db.queries :as q]
            [pik-logistic-etl.db.commands :as c]))

(defstate db
          :start
          {:subprotocol (get-in settings [:sql :subprotocol])
           :subname (get-in settings [:sql :subname])
           :user (get-in settings [:sql :user])
           :password (get-in settings [:sql :password])
           :domain (get-in settings [:sql :domain])})

;(q/events db {:last_id 0})
;(def r (q/last-inserted-event db {:tracker_id 1 :zone_id 1}))
;(empty? r)
;(identity r)
;(q/insert? db {:tracker_id 144953 :zone_id 68989})
;(c/insert-row! db {:tracker_id 144953 :zone_id 68989 :time "2017-01-13 16:08:40"})
;(q/etl-status db)
;(q/events db {:last_id "78741522"})
