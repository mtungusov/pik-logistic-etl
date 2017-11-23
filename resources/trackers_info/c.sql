-- :name update-tracker-info! :! :n
UPDATE st_trackers_info SET
  event_id = :event_id, event = :event,
  zone_id_in = :zone_id_in, time_in = :time_in,
  zone_id_out = :zone_id_out, time_out = :time_out
WHERE tracker_id = :tracker_id
IF @@rowcount = 0
  BEGIN
    INSERT INTO st_trackers_info (tracker_id, event_id, event, zone_id_in, time_in, zone_id_out, time_out)
    VALUES (:tracker_id, :event_id, :event, :zone_id_in, :time_in, :zone_id_out, :time_out)
  END
