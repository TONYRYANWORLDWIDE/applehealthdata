#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os
import datetime
from datetime import datetime as dt
from datetime import date as d
import glob
import zipfile
import pandas
from pandas import *


path = 'C:/Users/Tonyr/downloads/'
os.chdir(path)
cwd = os.getcwd()
sd = glob.glob('export*.zip')
sd.sort(key=os.path.getmtime)
# print("\n".join(sd))
file = sd[-1]
fullpath = path + file
production_files = 'C:/Users/tonyr/Desktop/Self Education/Production Files/'
zip_ref = zipfile.ZipFile(fullpath, 'r')
zip_ref.extractall(production_files)
zip_ref.close()





# In[3]:


# -*- coding: utf-8 -*-
"""
applehealthdata.py: Extract data from Apple Health App's export.xml.
Copyright (c) 2016 Nicholas J. Radcliffe
Licence: MIT
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import re
import sys

from xml.etree import ElementTree
from collections import Counter, OrderedDict

__version__ = '1.3'

RECORD_FIELDS = OrderedDict((
    ('sourceName', 's'),
    ('sourceVersion', 's'),
    ('device', 's'),
    ('type', 's'),
    ('unit', 's'),
    ('creationDate', 'd'),
    ('startDate', 'd'),
    ('endDate', 'd'),
    ('value', 'n'),
))

ACTIVITY_SUMMARY_FIELDS = OrderedDict((
    ('dateComponents', 'd'),
    ('activeEnergyBurned', 'n'),
    ('activeEnergyBurnedGoal', 'n'),
    ('activeEnergyBurnedUnit', 's'),
    ('appleExerciseTime', 's'),
    ('appleExerciseTimeGoal', 's'),
    ('appleStandHours', 'n'),
    ('appleStandHoursGoal', 'n'),
))

WORKOUT_FIELDS = OrderedDict((
    ('sourceName', 's'),
    ('sourceVersion', 's'),
    ('device', 's'),
    ('creationDate', 'd'),
    ('startDate', 'd'),
    ('endDate', 'd'),
    ('workoutActivityType', 's'),
    ('duration', 'n'),
    ('durationUnit', 's'),
    ('totalDistance', 'n'),
    ('totalDistanceUnit', 's'),
    ('totalEnergyBurned', 'n'),
    ('totalEnergyBurnedUnit', 's'),
))

FIELDS = {
    'Record': RECORD_FIELDS,
    'ActivitySummary': ACTIVITY_SUMMARY_FIELDS,
    'Workout': WORKOUT_FIELDS,
}


PREFIX_RE = re.compile('^HK.*TypeIdentifier(.+)$')
ABBREVIATE = True
VERBOSE = True

def format_freqs(counter):
    """
    Format a counter object for display.
    """
    return '\n'.join('%s: %d' % (tag, counter[tag])
                     for tag in sorted(counter.keys()))


def format_value(value, datatype):
    """
    Format a value for a CSV file, escaping double quotes and backslashes.
    None maps to empty.
    datatype should be
        's' for string (escaped)
        'n' for number
        'd' for datetime
    """
    if value is None:
        return ''
    elif datatype == 's':  # string
        return '"%s"' % value.replace('\\', '\\\\').replace('"', '\\"')
    elif datatype in ('n', 'd'):  # number or date
        return value
    else:
        raise KeyError('Unexpected format value: %s' % datatype)

def abbreviate(s, enabled=ABBREVIATE):
    """
    Abbreviate particularly verbose strings based on a regular expression
    """
    m = re.match(PREFIX_RE, s)
    return m.group(1) if enabled and m else s



def encode(s):
    """
    Encode string for writing to file.
    In Python 2, this encodes as UTF-8, whereas in Python 3,
    it does nothing
    """
    return s.encode('UTF-8') if sys.version_info.major < 3 else s



class HealthDataExtractor(object):
    """
    Extract health data from Apple Health App's XML export, export.xml.
    Inputs:
        path:      Relative or absolute path to export.xml
        verbose:   Set to False for less verbose output
    Outputs:
        Writes a CSV file for each record type found, in the same
        directory as the input export.xml. Reports each file written
        unless verbose has been set to False.
    """
    
    path = 'C:/Users/EMTG8AR/Desktop/export/apple_health_export/'
    def __init__(self, path, verbose=VERBOSE):
        self.in_path = path
        self.verbose = verbose
        self.directory = os.path.abspath(os.path.split(path)[0])
        with open(path) as f:
            self.report('Reading data from %s . . . ' % path, end='')
            self.data = ElementTree.parse(f)
            self.report('done')
        self.root = self.data._root
        self.nodes = self.root.getchildren()
        self.n_nodes = len(self.nodes)
        self.abbreviate_types()
        self.collect_stats()

    def report(self, msg, end='\n'):
        if self.verbose:
            print(msg, end=end)
            sys.stdout.flush()

    def count_tags_and_fields(self):
        self.tags = Counter()
        self.fields = Counter()
        for record in self.nodes:
            self.tags[record.tag] += 1
            for k in record.keys():
                self.fields[k] += 1

    def count_record_types(self):
        """
        Counts occurrences of each type of (conceptual) "record" in the data.
        In the case of nodes of type 'Record', this counts the number of
        occurrences of each 'type' or record in self.record_types.
        In the case of nodes of type 'ActivitySummary' and 'Workout',
        it just counts those in self.other_types.
        The slightly different handling reflects the fact that 'Record'
        nodes come in a variety of different subtypes that we want to write
        to different data files, whereas (for now) we are going to write
        all Workout entries to a single file, and all ActivitySummary
        entries to another single file.
        """
        self.record_types = Counter()
        self.other_types = Counter()
        for record in self.nodes:
            if record.tag == 'Record':
                self.record_types[record.attrib['type']] += 1
            elif record.tag in ('ActivitySummary', 'Workout'):
                self.other_types[record.tag] += 1
            elif record.tag in ('Export', 'Me'):
                pass
            else:
                self.report('Unexpected node of type %s.' % record.tag)

    def collect_stats(self):
        self.count_record_types()
        self.count_tags_and_fields()

    def open_for_writing(self):
        self.handles = {}
        self.paths = []
        for kind in (list(self.record_types) + list(self.other_types)):
            path = os.path.join(self.directory, '%s.csv' % abbreviate(kind))
            f = open(path, 'w')
            headerType = (kind if kind in ('Workout', 'ActivitySummary')
                               else 'Record')
            f.write(','.join(FIELDS[headerType].keys()) + '\n')
            self.handles[kind] = f
            self.report('Opening %s for writing' % path)

    def abbreviate_types(self):
        """
        Shorten types by removing common boilerplate text.
        """
        for node in self.nodes:
            if node.tag == 'Record':
                if 'type' in node.attrib:
                    node.attrib['type'] = abbreviate(node.attrib['type'])

    def write_records(self):
        kinds = FIELDS.keys()
        for node in self.nodes:
            if node.tag in kinds:
                attributes = node.attrib
                kind = attributes['type'] if node.tag == 'Record' else node.tag
                values = [format_value(attributes.get(field), datatype)
                          for (field, datatype) in FIELDS[node.tag].items()]
                line = encode(','.join(values) + '\n')
                self.handles[kind].write(line)

    def close_files(self):
        for (kind, f) in self.handles.items():
            f.close()
            self.report('Written %s data.' % abbreviate(kind))

    def extract(self):
        self.open_for_writing()
        self.write_records()
        self.close_files()

    def report_stats(self):
        print('\nTags:\n%s\n' % format_freqs(self.tags))
        print('Fields:\n%s\n' % format_freqs(self.fields))
        print('Record types:\n%s\n' % format_freqs(self.record_types))


if __name__ == '__main__':
    theexport = production_files + 'apple_health_export/export.xml'
    data = HealthDataExtractor(theexport)
    data.report_stats()
    data.extract()


# ## Push All Stats to PostgreSQL Database

# In[4]:


from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://postgres:<Password>@localhost:<Port>/<Database>')


finalpath = production_files + 'apple_health_export/'
os.chdir(finalpath)
sd2 = glob.glob('*.csv')

import datetime
from datetime import datetime as dt
nofourhundred = lambda x: dt.strptime(x,'%Y-%m-%d %H:%M:%S -0400')

for x in range(0, len(sd2)):

    thefile = sd2[x].replace('.csv','').lower()
    DF = pandas.read_csv(finalpath + sd2[x])
    
    for col in DF.columns:
        try:
            if col.lower().find('date') != -1:
                DF[col] = DF[col].apply(nofourhundred)
        except:
            print(DF.columns)
            print(col)
            
    DF.to_sql(thefile , con = engine, if_exists = 'replace')
    
    
    


# ## Group Data for each metric by minute. Create a sum and average for the value (Often only one will be relevant for a given Metric)

# In[6]:


import psycopg2
from sqlalchemy import create_engine
for x in range(0, len(sd2)):
    try:
        thefile = sd2[x].replace('.csv','').lower()
        command = (

        """
        Select 
        sum(value)  """ + thefile + '_Sum' + """ 
        ,avg(value)  """ + thefile + '_avg' + """
        ,cast("creationDate" as date) creationdate
        , cast(date_part('hour',"startDate") as varchar(2)) as Hour
        , cast(date_part('minute',"startDate") as varchar(2)) as Minute
        from """ +  thefile + """
        group by cast("creationDate" as date) 
        , date_part('hour',"startDate")
        , date_part('minute',"startDate")
        order by "creationdate", date_part('hour',"startDate")
        ;
        """
        )
        engine = create_engine('postgresql+psycopg2://postgres:<Password>@localhost:<Port>/<Database>')
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.execute(command)
        cur.close()
        conn.commit()    
        connection = engine.connect()
        execute = connection.execute(command)
        DF = pandas.DataFrame(execute.fetchall())
        DF.columns = execute.keys()
        DF.to_csv(finalpath + 'grouped/' + 'grouped_' +  sd2[x])
        DF.to_sql(thefile + '_grouped', con = engine, if_exists = 'replace')
    except:
        print(sd2[x])

