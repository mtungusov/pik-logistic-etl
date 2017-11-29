-- :name events :? :*
SELECT top 100 tracker_events.tracker_id, tracker_events.id AS event_id, tracker_events.event, tracker_events.time,
  rules.zone_id, zones.parent_id AS zone_parent_id
FROM tracker_events
  LEFT JOIN rules ON tracker_events.rule_id = rules.id
  LEFT JOIN zones ON rules.zone_id = zones.id
WHERE tracker_events.id > :last_id AND tracker_events.event IN ('inzone', 'outzone')
ORDER BY event_id


-- :name last-event-id :? :1
SELECT TOP 1 id
FROM tracker_events
WHERE event IN ('inzone', 'outzone')
ORDER BY id DESC


-- :name tracker-info :? :1
SELECT TOP 1 *
FROM st_trackers_info
WHERE tracker_id = :tracker_id
