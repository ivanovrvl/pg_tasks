CREATE OR REPLACE FUNCTION long_task.task_changed_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM pg_notify('!'||TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME || '.' || NEW.group_id::varchar, 'I ' || NEW.id::varchar);
  	RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    IF OLD.group_id IS DISTINCT FROM NEW.group_id THEN
        PERFORM pg_notify('!'||TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME || '.' || OLD.group_id::varchar, 'U ' || NEW.id::varchar);
    END IF;
    PERFORM pg_notify('!'||TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME || '.' || NEW.group_id::varchar, 'U ' || NEW.id::varchar);
   	RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    PERFORM pg_notify('!'||TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME || '.' || OLD.group_id::varchar, 'D ' || OLD.id::varchar);
  	RETURN OLD;
  END IF;
END;
$function$
