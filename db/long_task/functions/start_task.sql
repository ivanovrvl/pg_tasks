CREATE OR REPLACE FUNCTION long_task.start_task(p_group_id integer, p_worker_id integer)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
  v_task_id BIGINT;
  v_task_id2 BIGINT;
  v_instance_id BIGINT := NULL;
BEGIN

	LOOP
    
      SELECT id
      INTO v_task_id2
      FROM long_task.task
      WHERE group_id=p_group_id
      AND state_id = 'AW'
      AND (worker_id is null or worker_id=p_worker_id)
      ORDER BY priority, last_state_change
      LIMIT 1;
      
      IF v_task_id2 is null THEN
      	RETURN NULL;
      END IF;
      
      UPDATE long_task.task
      SET state_id = 'AE', 
          worker_id = p_worker_id,
          started = now(),
          error = NULL
      WHERE id = v_task_id2 AND state_id = 'AW'
      RETURNING id INTO v_task_id;
      
      IF v_task_id is not null THEN
        RETURN v_task_id;      
      END IF;    

	END LOOP;

END;
$function$
