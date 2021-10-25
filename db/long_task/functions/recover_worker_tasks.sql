CREATE OR REPLACE FUNCTION long_task.recover_worker_tasks(p_worker_id integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
	-- this is a place to restart or complete failed worker`s tasks
    UPDATE long_task.task
    SET state_id='CF', error='Worker failed'
    --SET state_id='AW', worker_id=NULL
    WHERE worker_id=p_worker_id AND state_id='AE';
END;
$function$
