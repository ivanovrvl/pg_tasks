CREATE OR REPLACE FUNCTION long_task.task_state_changed_tr()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
	IF TG_OP = 'UPDATE' THEN
    	IF OLD.state_id IS DISTINCT FROM NEW.state_id THEN
            NEW.last_state_change = now();
            IF NEW.state_id = 'AW' THEN
                NEW.error = NULL;
            END IF;
        END IF;
   		RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
		NEW.last_state_change = now();
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
  		RETURN OLD;
    END IF;
END;
$function$
