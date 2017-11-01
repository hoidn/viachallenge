import db
import scipy
import datetime

db.init()
pdfs = {medallion: pdf for medallion, pdf in db.get_pdfs()}

# 10 hour shift threshold in seconds
threshold = 36000

print 'finishied initialization'

def datetime_to_time_seconds(d):
    """
    Return the number of seconds from midnight
    """
    if d is None:
        return None
    t = d.time()
    return t.hour * 3600 + t.minute * 60 + t.second

def threshold_probability(medallion, curtime, onshift):
    """
    curtime: time of the day in seconds.
    """
    hour = datetime.timedelta(0, 3600).seconds
    from scipy.interpolate import interp1d
    # TODO fix this hack
    try:
        pdf = pdfs[medallion]
    except:
        return 0.
    pfunc = interp1d(*pdf, fill_value = 0.)
    
    if medallion not in onshift:
        return 0.
    else:
        if onshift[medallion][1] is not None and\
                curtime - datetime_to_time_seconds(onshift[medallion][1]) > hour:
            del onshift[medallion]
            return 0.
        else:
            start, end = map(datetime_to_time_seconds, onshift[medallion])
            if end is None:
                end = curtime
            if end - start < 9.5 * hour:
                return 0.
            else:
                delta = 10 * hour - (end - start)
                p_cur = pfunc(curtime)
                p_future = pfunc(curtime + delta)
                return float(p_future) / p_cur
