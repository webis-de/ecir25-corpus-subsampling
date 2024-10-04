#!/usr/bin/env python3
tracker = None

try:
    import contextlib
    with contextlib.redirect_stdout(None), contextlib.redirect_stderr(None):
        from time import sleep
        from codecarbon import OfflineEmissionsTracker
        tracker = OfflineEmissionsTracker(country_iso_code="DEU", api_call_interval=2, measure_power_secs=1)
        tracker.start()
        TODO Also monitor elapsed time and write this to somewhere
        
        while True:
            sleep(20)
finally:
    try:
        if tracker:
            tracker.stop()
    except: pass
