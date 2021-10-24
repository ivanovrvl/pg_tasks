DROP TRIGGER IF EXISTS task_state_changed_tr ON long_task.task;

CREATE TRIGGER task_state_changed_tr BEFORE INSERT OR UPDATE OF state_id ON long_task.task FOR EACH ROW EXECUTE FUNCTION task_state_changed_tr()