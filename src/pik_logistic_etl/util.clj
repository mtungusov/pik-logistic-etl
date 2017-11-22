(ns pik-logistic-etl.util
  (:require [clj-time.format :as tf]))

(defonce navyixy-time-formatter (tf/formatter "yyyy-MM-dd HH:mm:ss"))


(defn parse-str-int [s]
  (if (integer? s)
    s
    (try
      (Integer/parseInt s)
      (catch Exception e 0))))

;(parse-str-int 123)
;(parse-str-int "1234")
;(parse-str-int "1sdd234")
