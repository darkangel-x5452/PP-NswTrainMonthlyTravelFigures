#########################################################################################################
#   Program Name : NSW_TRAIN_OPAL_TRIPS_LOAD.py                                                         #
#   Program Description:                                                                                #
#   This program prepares a SQLite table containing data about train line monthly opal in NSW.          #
#                                                                                                       #
#   Comment                                         Date                  Author                        #
#   ================================                ==========            ================              #
#   Initial Version                                 09/06/2019            Samson Leung                  #
#########################################################################################################
import sqlite3

#######################################################################
### Create NSW_TRAIN_OPAL_TRIPS Table                               ###
#######################################################################
conn = sqlite3.connect('NSW_TRAIN_OPAL_TRIPS_JULY_2016_APRIL_2019.sqlite')
cur = conn.cursor()

cur.executescript('''	
DROP TABLE IF EXISTS NSW_TRAIN_OPAL_TRIPS_JULY_2016_APRIL_2019;

CREATE TABLE NSW_TRAIN_OPAL_TRIPS_JULY_2016_APRIL_2019 (
	TRAIN_LINE        varchar(100),
	PERIOD            varchar(100),
	COUNT             number(10)
);

''')

fname = 'clean_data-monthly_opal_trips-july-2016-april-2019.txt'
fhand = open(fname)

#######################################################################
### Populate NSW_TRAIN_OPAL_TRIPS Table                             ###
#######################################################################
for line in fhand:
    fields = line.split('|')

    TRAIN_LINE = fields[0].strip()
    PERIOD = fields[1].strip()
    COUNT = fields[2].strip()

    cur.execute('''INSERT INTO NSW_TRAIN_OPAL_TRIPS_JULY_2016_APRIL_2019
        (
		TRAIN_LINE,
		PERIOD,
		COUNT
        )  
        VALUES ( ?, ?, ?)''',
                (
                    TRAIN_LINE,
                    PERIOD,
                    COUNT
                ))

conn.commit()

print('Done')
