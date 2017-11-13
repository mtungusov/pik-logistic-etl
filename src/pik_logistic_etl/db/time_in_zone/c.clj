(ns pik-logistic-etl.db.time-in-zone.c
  (:require [hugsql.core :as hugsql]
            [pik-logistic-etl.db.commands :as c]))

(hugsql/def-db-fns "time_in_zone/c.sql")

(defn update-etl-status! [conn params]
  (c/update-etl-status! conn params))

