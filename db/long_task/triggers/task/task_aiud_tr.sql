DROP TRIGGER IF EXISTS task_aiud_tr ON long_task.task;

CREATE TRIGGER task_aiud_tr AFTER INSERT OR DELETE OR UPDATE OF state_id, worker_id ON long_task.task FOR EACH ROW EXECUTE FUNCTION long_task.task_aiud_tr()