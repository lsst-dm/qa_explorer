import sqlite3
import os

def get_visits(field, tracts, filt, sqlitedir='/scratch/hchiang2/parejko/'):
    if not isinstance(tracts, list) or isinstace(tracts, tuple):
        tracts = [tracts]

    visits = []
    with sqlite3.connect(os.path.join(sqlitedir, 'db{}.sqlite3'.format(field))) as conn:
        for t in tracts:
            cursor = conn.cursor()
            cmd = "select distinct visit from calexp where tract=:tract and filter=:filt"
            cursor.execute(cmd, dict(tract=t, filt=filt))
            result = cursor.fetchall()
            visits.append([x[0] for x in result])
    return list(set([x for y in visits for x in y]))

