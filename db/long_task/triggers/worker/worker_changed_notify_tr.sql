DROP TRIGGER IF EXISTS worker_changed_notify_tr ON long_task.worker;

CREATE TRIGGER worker_changed_notify_tr AFTER INSERT OR DELETE OR UPDATE OF active, stop ON long_task.worker FOR EACH ROW EXECUTE FUNCTION long_task.worker_changed_notify()