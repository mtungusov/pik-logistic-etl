(ns pik-logistic-etl.db.commands
  (:require [hugsql.core :as hugsql]))

(hugsql/def-db-fns "commands.sql")
