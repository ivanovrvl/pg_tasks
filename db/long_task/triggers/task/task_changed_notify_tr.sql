DROP TRIGGER IF EXISTS task_changed_notify_tr ON long_task.task;

CREATE TRIGGER task_changed_notify_tr AFTER INSERT OR DELETE OR UPDATE OF state_id, next_start, shed_period_id, shed_period_count, shed_clone, group_id ON long_task.task FOR EACH ROW EXECUTE FUNCTION task_changed_notify()