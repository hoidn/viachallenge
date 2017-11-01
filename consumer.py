#!/usr/bin/env python2
import time
import dill
import zmq
import datetime
import forecasting
import random
import threading

hour = datetime.timedelta(0, 3600)


# initialize ZMQ sub
context = zmq.Context()
subscriber = context.socket (zmq.SUB)
subscriber.connect ("tcp://localhost:5556")
subscriber.setsockopt (zmq.SUBSCRIBE, "")
 
preload_period = 10 * hour

def get_msg():
    """
    Get event form ZMQ sub socket.
    """
    return dill.loads(subscriber.recv())


# dict mapping medallion numbers for on-shift drivers to data on their ongoing shift
# k: v -> medallion: (shift start time, last dropoff time (or None if ride is ongoing))
onshift = {}

# list storing all events # TODO should really be a queue
events = []

#
threshold_probabilities = {0: []}
current_time = {0: ''}


def sorted_threshold_probabilities(curtime):
    """
    curtime: time of the day in seconds.
    return sorted list of tuples of the form (shift overrun probability, medallion, shift duration)
    """
    medallions = onshift.keys()
    probs = []
    for m in medallions:
        probability = forecasting.threshold_probability(m, curtime, onshift)
        if m in onshift:
            shiftlength = curtime - forecasting.datetime_to_time_seconds(onshift[m][0])
            if 9.5 * hour.seconds < shiftlength < 10 * hour.seconds:
                #probs.append((m, round(float(shiftlength) / 3600, 2)))
                probs.append((m, str(datetime.timedelta(0, shiftlength))))
#            probs.append((probability, m,\
#                curtime - forecasting.datetime_to_time_seconds(onshift[m][0])))
    result = sorted(probs)
    threshold_probabilities[0] = result
    return result



def event_callback(evt, verbose = True):
    t, type, medallion = evt
    current_time[0] = str(t)
    events.append(evt)
    if type == 'dropoff':
        # if statement takes care of edge case where stream is truncated and the 
        # preceding pickups are missing. in that case we set up to calculate a lower
        # bound on the shift duration instead of its actual value.
        if medallion in onshift:
            #print 'only updating'
            #print onshift[medallion]
            onshift[medallion][1] = t
            #print onshift[medallion]
        else:
            onshift[medallion] = [t, t]
    else:
        if medallion in onshift:
            dat = onshift[medallion]
            if dat[1] is not None and t - dat[1] > hour:
                onshift[medallion] = [t, None]
                #print 'new shift'
                #print t - dat[1]
            else:
                #print 'only updating'
                onshift[medallion][1] = None
        else:
            onshift[medallion] = [t, None]
                
        #onshift[medallion] = None
    if forecasting.datetime_to_time_seconds(t) - preload_period.seconds > 0:
        #grint evt
        #print onshift[evt[2]]
        #if not forecasting.datetime_to_time_seconds(t) % 60:

        #if random.choice(range(10)) == 0:
        if forecasting.datetime_to_time_seconds(t) == 54000:
            print sorted_threshold_probabilities(forecasting.datetime_to_time_seconds(t))[::-1]
        #print events[-10:]


def main():
    while 1:
        #time.sleep(.001)
        event_callback(get_msg())

t = threading.Thread(target = main)
t.start()
