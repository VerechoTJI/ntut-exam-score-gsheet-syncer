"""Independent completion-based timers, with no overlapping jobs or catch-up burst."""
from time import monotonic, sleep


def run_jobs(jobs):
    pending = [{'run':callback, 'interval':interval, 'due':monotonic()} for callback, interval in jobs]
    try:
        while pending:
            now = monotonic()
            for job in list(pending):
                if job['due'] <= now:
                    result = job['run']()
                    if result:
                        return result
                    if job['interval'] > 0:
                        job['due'] = monotonic()+job['interval']
                    else:
                        pending.remove(job)
            if pending:
                sleep(max(0, min(job['due'] for job in pending)-monotonic()))
    except KeyboardInterrupt:
        print('Periodic jobs stopped by user')
    return 0
