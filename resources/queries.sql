-- :name events :? :*
SELECT TOP 1000 id, tracker_id, event, time, zone_id
FROM inoutzone_times
WHERE id > :last_id
ORDER BY id

-- :name last-inserted-opened-event :? :1
SELECT TOP 1 guid
FROM st_timeinzone
WHERE tracker_id = :tracker_id AND zone_id = :zone_id AND out_time IS NULL
ORDER BY in_time DESC

-- :name last-inserted-closed-event :? :1
SELECT TOP 1 guid
FROM st_timeinzone
WHERE tracker_id = :tracker_id AND zone_id = :zone_id
      AND out_time IS NOT NULL
      AND out_time > :time
ORDER BY in_time DESC

-- :name last-inserted-event-for-close :? :1
SELECT TOP 1 guid, in_time
FROM st_timeinzone
WHERE tracker_id = :tracker_id AND zone_id = :zone_id
ORDER BY in_time DESC

-- :name etl-status :? :1
SELECT TOP 1 state_value
FROM etl_st_timeinzone
WHERE state_name = 'last-event-id'
