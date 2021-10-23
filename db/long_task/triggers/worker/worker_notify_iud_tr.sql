DROP TRIGGER IF EXISTS worker_notify_iud_tr ON long_task.worker;

CREATE TRIGGER worker_notify_iud_tr AFTER INSERT OR DELETE OR UPDATE OF active, stop ON long_task.worker FOR EACH ROW EXECUTE FUNCTION table_notify_iud()