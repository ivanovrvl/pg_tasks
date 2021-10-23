CREATE OR REPLACE FUNCTION long_task.task_aiud_tr()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
	IF TG_OP = 'UPDATE' THEN
        IF NEW.state_id = 'AC' THEN
            IF OLD.state_id not in ('AE', 'AW', 'AC')  THEN
                RAISE EXCEPTION 'Can`t cancel inactive task';
            END IF;
        END IF;        
        IF OLD.state_id like 'A%_' THEN
            IF NEW.state_id like 'A%' THEN
            	IF OLD.state_id in ('AE', 'AC')
                AND OLD.worker_id is not null 
                AND OLD.worker_id IS DISTINCT FROM NEW.worker_id THEN
                	RAISE EXCEPTION 'Can`t change worker_id for active task %', OLD.id;
                END IF;                
            END IF;
        END IF;
   		RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        IF NEW.state_id not in ('AW', 'DR') THEN
            RAISE EXCEPTION 'Can`t create task in the state except DR or AW';
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
  		RETURN OLD;
    END IF;
END;
$function$
