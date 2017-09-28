-- :name open-zone! :! :n
INSERT INTO st_timeinzone
(tracker_id, zone_id, in_time)
VALUES (:tracker_id, :zone_id, :time)

-- :name close-zone! :! :n
UPDATE st_timeinzone
SET out_time = :time
WHERE guid = :guid AND in_time <= :time

-- :name update-etl-status! :! :n
UPDATE etl_st_timeinzone SET
  state_value = :state_value
WHERE state_name = :state_name
IF @@rowcount = 0
  BEGIN
    INSERT INTO etl_st_timeinzone VALUES (:state_name, :state_value)
  END
