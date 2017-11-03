import sqlite3
import os

def get_visits(field, tract, filt, sqlitedir='/scratch/hchiang2/parejko/'):
    with sqlite3.connect(os.path.join(sqlitedir, 'db{}.sqlite3'.format(field))) as conn:
        cursor = conn.cursor()
        cmd = "select distinct visit from calexp where tract=:tract and filter=:filt"
        cursor.execute(cmd, dict(tract=tract, filt=filt))
        result = cursor.fetchall()
    return '^'.join(str(x[0]) for x in result)

