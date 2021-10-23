CREATE OR REPLACE FUNCTION public.table_notify_iud()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM pg_notify('!'||TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME, 'I ' || NEW.id::varchar);
  	RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    IF NEW != OLD THEN
        PERFORM pg_notify('!'||TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME, 'U ' || NEW.id::varchar);
    END IF;
   	RETURN NEW;
  ELSIF TG_OP = 'DELETE' THEN
    PERFORM pg_notify('!'||TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME, 'D ' || OLD.id::varchar);
  	RETURN OLD;
  END IF;
END;
$function$
