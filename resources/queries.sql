-- :name etl-status :? :1
SELECT TOP 1 state_value
FROM etl_state
WHERE etl_name = :etl_name AND state_name = :state_name
