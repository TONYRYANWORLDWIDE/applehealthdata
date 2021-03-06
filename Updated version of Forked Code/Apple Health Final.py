#!/usr/bin/env python
# coding: utf-8

# In[7]:


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
import os
import datetime
from datetime import datetime as dt
from datetime import date as d
import glob
import zipfile
import pandas

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
    
#     path = 'C:/Users/EMTG8AR/Desktop/export/apple_health_export/'
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


# In[17]:


import os
import datetime
from datetime import datetime as dt
from datetime import date as d
from datetime import timedelta
import glob
import pandas
# from pandas import *
# import pandas
from sqlalchemy import create_engine
import psycopg2
from configparser import ConfigParser

class ApplePostGre():
    
    def __init__(self):
        self.productionfiles = 'C:/Users/tonyr/desktop/Self Education/Production Files/'
        self.finalpath = 'C:/Users/tonyr/desktop/Self Education/Production Files/apple_health_export/'
        os.chdir(self.finalpath)
        self.sd2 = glob.glob('*.csv')
    
    
    def config(self,filename='database.ini', section='postgresql'):
        os.chdir('C:/Users/tonyr/desktop/Self Education/Production Files/')
        parser = ConfigParser()
        parser.read(filename)    
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
        return db
    def rowstartdate (self,row):
        if row['Hour'] >= 18 :
            adjustdate = row['startDate'] + timedelta(days=1)     
        else:
            adjustdate = row['startDate']
        return adjustdate
    
    def connect(self):
        sd2 = self.sd2
        nofourhundred = lambda x: dt.strptime(x,'%Y-%m-%d %H:%M:%S -0400') # need to write utc to local time function
        finalpath = self.finalpath
        DF = pandas.DataFrame()
        conn = None
        try:
            params = self.config(filename='database.ini', section='postgresql')
            conn = psycopg2.connect(**params)
            enginestart = 'postgresql+psycopg2://' + params['user'] +':' + params['password'] + '@' + params['host'] + ':5432/' + params['database']
            engine = create_engine(enginestart)
            exclude = ['headphoneaudioexposure','flightsclimbed','applestandtime','activitysummary','applestandhour','mindfulsession','height','waistcircumference','walkingheartrateaverage','stepcount']
            for x in range(0, len(sd2)):
                thefile = self.sd2[x].replace('.csv','').lower()
                if thefile in exclude:
                    print(thefile + ' skipped')
                    continue
                    
                command = """

                DO $$                  
                BEGIN 
                    IF EXISTS
                        ( SELECT 1
                          FROM   information_schema.tables 
                          WHERE  table_schema = 'public'
                          AND    table_name = '""" +  thefile +  """'"""            """  
                        )
                    THEN
                        Delete from """ +  thefile + """ ;
                    END IF ;
                END
                $$ ;
                """
                cxn = engine.raw_connection()
                cur = cxn.cursor()
                cur.execute(command)
                cur.close()
                cxn.commit()
                DF = pandas.read_csv(finalpath + self.sd2[x])
                for col in DF.columns:
                    try:
                        if col.lower().find('date') != -1:                    
                            DF[col] = DF[col].apply(nofourhundred)                                                      
                    except(Exception) as error:                
                        print(thefile + ' ' + col +  ' Column Error')
                        print(error)
#                     finally:
#                         continue
                endDate_TheDate =['sleepanalysis', 'mindfulsession']
                if thefile in endDate_TheDate:
                    DF = DF.reset_index(drop = True)
                    DF['TheDate'] = DF['endDate'].dt.date
                    if thefile == 'sleepanalysis':
                        DF = DF[DF['value'] == 'HKCategoryValueSleepAnalysisInBed']
                        DF = DF.reset_index(drop = True)
                        hour = lambda x:  x.hour
                        DF['Hour'] = DF['startDate'].apply(hour)
                        DF['TheDate'] = DF.apply(lambda row: self.rowstartdate(row), axis=1)
                DF.to_sql(thefile , con = engine, if_exists = 'append') 
                print(thefile + ' Inserted')
#                 if thefile == 'heartrate':
#                     print(DF)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print(thefile + 'File Error')
        finally:
            if conn is not None:
                conn.close()
#             continue
                
    def createGroupedTable(self):
        sd2 = self.sd2
        finalpath = self.finalpath
        conn = None       

        try:
            params = self.config(filename='database.ini', section='postgresql')
            conn = psycopg2.connect(**params)
            enginestart = 'postgresql+psycopg2://' + params['user'] +':' + params['password'] + '@' + params['host'] + ':5432/' + params['database']
            engine = create_engine(enginestart)
            exclude = ['headphoneaudioexposure','flightsclimbed','applestandtime','activitysummary','applestandhour','mindfulsession','height','waistcircumference','walkingheartrateaverage','stepcount']
            for x in range(0, len(sd2)):
            
                try:
                    thefile = sd2[x].replace('.csv','').lower()
                    print(thefile)
                    if thefile in exclude:
                        print(thefile + ' skipped grouping')
                        continue
                    command = """
                    DO $$                  
                    BEGIN 
                        IF EXISTS
                            ( SELECT 1
                              FROM   information_schema.tables 
                              WHERE  table_schema = 'public'
                              AND    table_name = '""" +  thefile +  """_grouped'"""            """  
                            )
                        THEN
                            Delete from """ +  thefile +  """_grouped"""   """ ;
                        END IF ;
                    END
                    $$ ;
                    """
                    cxn = engine.raw_connection()
                    cur = cxn.cursor()
                    cur.execute(command)
                    cur.close()
                    cxn.commit()
                    listNeedingValueCalculated = ['sleepanalysis','mindfulsession'] #May need to add more tables here
                    groupByHourMinute = ['heartrate','activeenergyburned','stepcount','heartratevariabilitysdnn']
                    groupByCreationDate = ['appleexercisetime','dietarymolybdenum','vo2max']
                    groupByEndDate = ['basalenergyburned','restingheartrate']
                    skipTable = ['applestandtime','activitysummary','applestandhour','mindfulsession','height','waistcircumference','walkingheartrateaverage','stepcount']
                    if thefile in listNeedingValueCalculated:
                        command = (
                        """
                        ;with base as
                        (
                        Select "creationDate", "startDate", "endDate", "TheDate"::timestamp::date
                        ,(EXTRACT(EPOCH FROM ("endDate" - "startDate"))) / 3600 unithours
                        from """ +  thefile + """
                        order by "endDate" desc
                        )
                        Select sum(unithours), "TheDate"::timestamp::date from base
                        group by "TheDate"::timestamp::date
                        ;
                        """
                        )
                        print(command + ' THis Is The Command')
                    elif thefile == 'workout':
                        commandwrisl = """
select cast("creationDate" as date) creationdate,
sum(duration) filter(where "workoutActivityType" = 'HKWorkoutActivityTypeCrossTraining') as HKWorkoutActivityTypeCrossTrainingDuration
,sum(duration) filter(where "workoutActivityType" = 'HKWorkoutActivityTypeYoga') as HKWorkoutActivityTypeYogaDuration
,sum(duration) filter(where "workoutActivityType" = 'HKWorkoutActivityTypeRunning') as HKWorkoutActivityTypeRunningDuration
,sum(duration) filter(where "workoutActivityType" = 'HKWorkoutActivityTypeTraditionalStrengthTraining') as HKWorkoutActivityTypeTraditionalStrengthTrainingDuration
,sum(duration) filter(where "workoutActivityType" = 'HKWorkoutActivityTypeHighIntensityIntervalTraining') as HKWorkoutActivityTypeHighIntensityIntervalTrainingDuration
,sum(duration) filter(where "workoutActivityType" = 'HKWorkoutActivityTypeWalking') as HKWorkoutActivityTypeWalkingDuration
,sum("totalEnergyBurned") filter(where "workoutActivityType" = 'HKWorkoutActivityTypeCrossTraining') as HKWorkoutActivityTypeCrossTrainingEnergyBurned
,sum("totalEnergyBurned") filter(where "workoutActivityType" = 'HKWorkoutActivityTypeYoga') as HKWorkoutActivityTypeYogaEnergyBurned
,sum("totalEnergyBurned") filter(where "workoutActivityType" = 'HKWorkoutActivityTypeRunning') as HKWorkoutActivityTypeRunningEnergyBurned
,sum("totalEnergyBurned") filter(where "workoutActivityType" = 'HKWorkoutActivityTypeTraditionalStrengthTraining') as HKWorkoutActivityTypeTraditionalStrengthTrainingEnergyBurned
,sum("totalEnergyBurned") filter(where "workoutActivityType" = 'HKWorkoutActivityTypeHighIntensityIntervalTraining') as HKWorkoutActivityTypeHighIntensityIntervalTrainingEnergyBurned
,sum("totalEnergyBurned") filter(where "workoutActivityType" = 'HKWorkoutActivityTypeWalking') as HKWorkoutActivityTypeWalkingEnergyBurned
from workout
group by cast("creationDate" as date) 

                        """
                    elif thefile in groupByCreationDate:
                        command = (
                        """
                        Select 
                        sum(value)  """ + thefile + '_Sum' + """ 
                        ,avg(value)  """ + thefile + '_avg' + """
                        ,cast("creationDate" as date) creationdate
                        from """ +  thefile + """
                        group by cast("creationDate" as date) 
                        ;
                        """
                        )  
                    elif thefile in groupByEndDate:
                        command = (
                        """
                        Select 
                        sum(value)  """ + thefile + '_Sum' + """ 
                        ,avg(value)  """ + thefile + '_avg' + """
                        ,cast("endDate" as date) "TheDate"
                        from """ +  thefile + """
                        group by cast("endDate" as date) 
                        ;
                        """
                        )      

                    elif thefile in groupByHourMinute:            
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
                        
                        print(command + ' THis Is The Command')
                    elif thefile in skipTable: # Grouped Table created in PostGre View for mindfulsession           
                        continue
                    else:
                        continue
                    engine = create_engine('postgresql+psycopg2://postgres:Password@localhost:5432/Money')
                    conn = engine.raw_connection()
                    cur = conn.cursor()
                    cur.execute(command)
                    cur.close()
                    conn.commit()    
                    connection = engine.connect()
                    execute = connection.execute(command)
                    DF = pandas.DataFrame(execute.fetchall())
                    DF.columns = execute.keys()
                    combodatetime = lambda x: dt.strptime(dt.strftime(x['creationdate'],"%Y-%m-%d"),"%Y-%m-%d").replace(hour=int(x['hour']), minute=int(x['minute']))
                    if thefile in groupByHourMinute and thefile != 'heartratevariabilitysdnn':
                        DF['creationdatetime'] = DF.apply(combodatetime,axis =1)                        
                    DF.to_csv(finalpath + 'grouped/' + 'grouped_' +  sd2[x])
                    DF.to_sql(thefile + '_grouped', con = engine, if_exists = 'append')        
                except (Exception) as error:
                    print(error)
                    print("error:"  + sd2[x])
        except (Exception) as error:
            print(error)
            print("error:"  + sd2[x])
        finally:
            if conn is not None:
                conn.close()


# In[10]:


import os
import datetime
from datetime import datetime as dt
from datetime import date as d
import glob
import zipfile
import pandas

if __name__ == '__main__':
    path = 'C:/Users/Tonyr/downloads/'
    os.chdir(path)
    cwd = os.getcwd()
    sd = glob.glob('export*.zip')
    sd.sort(key=os.path.getmtime)
    file = sd[-1]
    fullpath = path + file
    production_files = 'C:/Users/tonyr/Desktop/Self Education/Production Files/'
    zip_ref = zipfile.ZipFile(fullpath, 'r')
    zip_ref.extractall(production_files)
    zip_ref.close()
    production_files = 'C:/Users/tonyr/Desktop/Self Education/Production Files/'
    theexport = production_files + 'apple_health_export/export.xml'
    data = HealthDataExtractor(theexport)
    data.report_stats()
    data.extract()
    applePSQL = ApplePostGre()
    applePSQL.connect() 
    applePSQL.createGroupedTable()


# In[6]:


import os
import datetime
from datetime import datetime as dt
from datetime import date as d
import glob
import zipfile
import pandas

if __name__ == '__main__':
    path = 'C:/Users/Tonyr/downloads/'
    os.chdir(path)
    cwd = os.getcwd()
    sd = glob.glob('export*.zip')
    sd.sort(key=os.path.getmtime)
    file = sd[-1]
    fullpath = path + file
    production_files = 'C:/Users/tonyr/Desktop/Self Education/Production Files/'
    zip_ref = zipfile.ZipFile(fullpath, 'r')
    zip_ref.extractall(production_files)
    zip_ref.close()
    production_files = 'C:/Users/tonyr/Desktop/Self Education/Production Files/'
    theexport = production_files + 'apple_health_export/export.xml'
    data = HealthDataExtractor(theexport)
    data.report_stats()
    data.extract()
    applePSQL = ApplePostGre()
    applePSQL.connect() 
    applePSQL.createGroupedTable()

