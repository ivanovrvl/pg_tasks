DROP TRIGGER IF EXISTS task_notify_iud_tr ON long_task.task;

CREATE TRIGGER task_notify_iud_tr AFTER INSERT OR DELETE OR UPDATE OF state_id, command, next_start, shed_period_id, shed_period_count, group_id ON long_task.task FOR EACH ROW EXECUTE FUNCTION table_notify_iud()