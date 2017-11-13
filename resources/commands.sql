-- :name update-etl-status! :! :n
UPDATE etl_state SET
  state_value = :state_value
WHERE etl_name = :etl_name AND state_name = :state_name
IF @@rowcount = 0
  BEGIN
    INSERT INTO etl_state (etl_name, state_name, state_value)
    VALUES (:etl_name, :state_name, :state_value)
  END
