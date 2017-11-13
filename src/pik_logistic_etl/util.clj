(ns pik-logistic-etl.util)

(defn parse-str-int [s]
  (if (integer? s)
    s
    (try
      (Integer/parseInt s)
      (catch Exception e 0))))

;(parse-str-int 123)
;(parse-str-int "1234")
;(parse-str-int "1sdd234")
