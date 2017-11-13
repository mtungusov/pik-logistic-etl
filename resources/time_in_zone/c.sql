-- :name open-zone! :! :n
INSERT INTO st_timeinzone
(tracker_id, zone_id, in_time)
VALUES (:tracker_id, :zone_id, :time)

-- :name close-zone! :! :n
UPDATE st_timeinzone
SET out_time = :time
WHERE guid = :guid AND in_time <= :time
