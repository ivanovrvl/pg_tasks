CREATE OR REPLACE FUNCTION long_task.sched_start(p_id bigint)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
  v_clone BOOLEAN;
  v_id BIGINT := NULL;
BEGIN
	SELECT t.shed_clone
    INTO v_clone
    FROM long_task.task t
    WHERE id = p_id;
    IF v_clone THEN
      INSERT INTO long_task.task(
          params, state_id, priority, worker_id, last_state_change, command, created, group_id
      )
      SELECT params, 'AW', priority, NULL, now(), command, now(), group_id
      FROM long_task.task
      WHERE id = p_id
      RETURNING id INTO v_id;    
    ELSE
    	UPDATE long_task.task 
        SET state_id='AW', worker_id=NULL
        WHERE id=p_id AND state_id like 'C%'
        RETURNING id INTO v_id;        
    END IF;
    RETURN v_id;
END;
$function$
