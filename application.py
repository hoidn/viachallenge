import consumer

# -*- coding: utf-8 -*-
"""
    :copyright: (c) 2015 by Armin Ronacher.
    :license: BSD, see LICENSE for more details.
"""
from flask import Flask, jsonify, render_template, request
app = Flask(__name__)

@app.route('/_update_near_threshold')
def update_near_threshold():
    dat = consumer.threshold_probabilities[0][::-1]
    try:
        ids, durations = zip(*dat)
        ids, durations = '\n'.join(map(str, ids)), '\n'.join(map(str, durations))
    except ValueError:
        ids, durations = 'initializing', 'initializing'
    return jsonify(ids=ids, durations=durations, current_time=consumer.current_time[0])


@app.route('/_update_events')
def update_events():
    return jsonify(result=consumer.events[-10:])

@app.route('/')
def index():
    return render_template('index.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0')
