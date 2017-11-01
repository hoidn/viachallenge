import numpy as np
import sqlite3
import cPickle

dbmap = {'connection': None, 'cursor': None}

def create_events():
    sql_command = """
    CREATE TABLE events ( 
    time VARCHAR(50), 
    type VARCHAR(10),
    medallion VARCHAR(35));"""
    dbmap['cursor'].execute(sql_command)

def insert_events(events):
    cursor = dbmap['cursor']
    dbmap['cursor'].executemany("""
        insert into events (time, type, medallion)
        values (?, ?, ?);""", events)
    dbmap['connection'].commit()

def create_pdfs():
    sql_command = """
    create table pdfs (
    medallion VARCHAR(35),
    pdf VARCHAR(100000));"""
    dbmap['cursor'].execute(sql_command)

def insert_one_pdf(p):
    sql_command = """INSERT INTO pdfs (medallion, pdf)
    VALUES (?, ?);"""
    dbmap['cursor'].execute(sql_command, p)
    dbmap['connection'].commit()

def insert_pdfs(pdfs):
    cursor = dbmap['cursor']
    dbmap['cursor'].executemany("""
        insert into pdfs (medallion, pdf)
        values (?, ?);""", pdfs)
    dbmap['connection'].commit()

def get_events(start_id = None, end_id = None):
    if start_id is not None and end_id is not None:
        sql_command = """
        select * from events
        where rowid>=%d and rowid<%d
        order by rowid
        """ % (start_id, end_id)
    else:
        sql_command = """
        select * from events
        order by rowid
        """
    cursor = dbmap['cursor']
    cursor.execute(sql_command) 
    result = cursor.fetchall()
    return result

def get_pdfs():
    """
    query drivers shift pdfs and return them in decoded form.

    decoded format: (medallion, (x, y))
        x and y: numpy arrays
    """
    sql_command = """
    select * from pdfs
    """
    cursor = dbmap['cursor']
    cursor.execute(sql_command) 
    result = cursor.fetchall()
    decoded = map(lambda row: (row[0], cPickle.loads(str(row[1]))), result)
    
    return decoded

def memo(f):
    """
    memoize, using object ids as cache keys
    """
    cache = {}
    def new_f(*args, **kwargs):
        ids = tuple(map(id, args)) + tuple(map(id, kwargs))
        if ids not in cache:
            cache[ids] = f(*args, **kwargs)
        return cache[ids]
    return new_f

def init(dbpath = 'tripdata.db'):
    connection = sqlite3.connect(dbpath)
    dbmap['connection'] = connection
    cursor = connection.cursor()
    dbmap['cursor'] = cursor

