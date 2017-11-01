#!/usr/bin/env python2
import db
import time
import datetime
import sys
import dill
import zmq

# run our clock at real time times this factor
acceleration = 100

# initialize ZMQ pub socket
context = zmq.Context()
publisher = context.socket(zmq.PUB)
publisher.bind ("tcp://*:5556")
 

chunksize = 100000
# Define window into the events table
state = {'rowid_start': 1000, 'rowid_end': chunksize}


event_queue = []

def load_events():
    raw_events = db.get_events(state['rowid_start'], state['rowid_end'])[::-1]
    # convert timestamps to datetime objects
    event_queue.extend(map(lambda row: (eval(row[0]), row[1], row[2]), raw_events))
    state['rowid_start'] = state['rowid_end'] + 1
    state['rowid_end'] += chunksize

def default_callback(evt):
    message = dill.dumps(evt)
    publisher.send(message)
    print evt

def run(callback = default_callback):
    db.init()
    load_events()
    # set the starting time
    current_event = event_queue.pop()
    t = current_event[0]
    delta = (datetime.datetime.now() - t) / acceleration
    while 1:
        if not event_queue:
            try:
                load_events()
            except:
                print 'Done streaming events'
                break
        callback(current_event)
        current_event = event_queue.pop()
        #time.sleep(0.01)
        # check time
        while current_event[0] > (datetime.datetime.now() - delta):
            time.sleep(0.01)
    connection.close()

if __name__ == '__main__':
    run()
