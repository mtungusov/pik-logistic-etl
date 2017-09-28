(ns pik-logistic-etl.config
  (:require [clojure.java.io :as io]
    [cprop.core :refer [load-config]]
    [mount.core :refer [defstate]])
  (:import [java.io File]))

(defn get-path [filename]
  (->> filename
       (str "." File/separator "config" File/separator)
       (io/as-file)
       (.getAbsolutePath)))

(defstate settings
          :start
          (load-config :file (get-path "secrets.edn")))
