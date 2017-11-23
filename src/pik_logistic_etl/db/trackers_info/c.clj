(ns pik-logistic-etl.db.trackers-info.c
  (:require [hugsql.core :as hugsql]))

(hugsql/def-db-fns "trackers_info/c.sql")