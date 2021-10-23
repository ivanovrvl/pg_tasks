CREATE OR REPLACE FUNCTION long_task.lock_worker(p_id bigint, p_group_id integer, p_node_name character varying, p_task_count integer, p_lock_until timestamp without time zone, p_prev_locked_until timestamp without time zone DEFAULT NULL::timestamp without time zone)
 RETURNS timestamp without time zone
 LANGUAGE plpgsql
AS $function$
DECLARE
  v_locked_until TIMESTAMP;
  v_id BIGINT;
  n INTEGER := 100;
BEGIN

	WHILE TRUE LOOP
    
      SELECT locked_until
      INTO v_locked_until
      FROM long_task.worker
      WHERE id = p_id;

      IF NOT FOUND THEN
        INSERT INTO long_task.worker(id, active, locked_until, task_count, group_id, node_name)
        VALUES(p_id, true, p_lock_until, p_task_count, p_group_id, p_node_name);
        RETURN NULL;
      END IF;
    
      IF NOT(v_locked_until is NULL OR v_locked_until < now() OR (p_prev_locked_until is not null AND p_prev_locked_until=v_locked_until)) THEN
      	RETURN v_locked_until;
      END IF;      
    
	  IF p_prev_locked_until is not null AND p_prev_locked_until=v_locked_until THEN
        UPDATE long_task.worker
        SET locked_until = p_lock_until, task_count=p_task_count, group_id=p_group_id, node_name=p_node_name
        WHERE id = p_id AND (locked_until is NULL OR locked_until = p_prev_locked_until OR locked_until < now())
        RETURNING id INTO v_id;
      ELSE
        UPDATE long_task.worker
        SET locked_until = p_lock_until, task_count=p_task_count, group_id=p_group_id, active=true, node_name=p_node_name
        WHERE id = p_id AND (locked_until is NULL OR locked_until < now())
        RETURNING id INTO v_id;      
      END IF;

      IF v_id IS NOT NULL THEN
          RETURN NULL;
      END IF;   
       
      n := n - 1;      
      IF n <= 0 THEN
      	RAISE 'Infinite loop on lock';
      END IF;
       
    END LOOP;

END;
$function$
