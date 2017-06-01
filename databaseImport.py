import sqlite3
import psycopg2 as pg ## Postgres interface for Python
import logging
logger = logging.getLogger(__name__)

def pretty(d, indent=0):
   for key, value in d.iteritems():
      print '\t' * indent + str(key)
      if isinstance(value, dict):
         pretty(value, indent+1)
      else:
         print '\t' * (indent+1) + str(value)

class sqliteRead:
    def __init__(self, source=None, logger=None):
        ## Setting up connections to sqlite3 database. Our source
        if source == None:
            raise ValueError('No file path given or URL to sqlite3 database')
        else:   ## Setting up connection to database. Local or remote
            self.name = source
            self.conn = sqlite3.connect(self.name)
            self.cursor = self.conn.cursor()
        
        ## Setting up connection to AWS RDS Postgres Databse. Our Target
        self.pgconn = pg.connect(database='leverjs',
                              user='avikbag',
                              host='leverjs-test.cqyszyrqm2af.us-west-2.rds.amazonaws.com',
                              port=5432, 
                              password='goodluck23')
        self.pgcursor = self.pgconn.cursor()
        self.pgconn.autocommit = True
        if(self.pgconn.status == 1):
            logging.info('Connection Status with AWD RDS: Active')
        else:
            logging.info('Connection Status with AWD RDS: Failed')



    def getSchema(self):
        # command = 'select cellID, trackID, area from tblCells where time = 1'
        logger.info('\nExtracting tables and their respective schema')
        command = 'select name from sqlite_master'
        self.cursor.execute(command)
        
        # Gives me a list of tables that are in the database
        tables = self.cursor.fetchall()
        tables = [x[0] for x in tables if (('tbl' in x[0]) or ('ui' in x[0])) and ('autoindex' not in x[0])]
        logger.debug('\nTables extracted from the database\n{0}\n'.format(tables))
        schema = {}

        for i in tables:
            command = "PRAGMA table_info('{0}');".format(i)
            self.cursor.execute(command)
            col = self.cursor.fetchall()
            schema[i] = []
            for x in col:
                temp = {}
                
                temp['name'] = x[1]
                temp['type'] = x[2]
                if (x[3] != 0):
                    temp['notNull'] = True
                else:
                    temp['notNull'] = False
                temp['default'] = x[4]
                if (x[5] != 0):
                    temp['primaryKey'] = True
                else:
                    temp['primaryKey'] = False
                schema[i].append(temp)

            logger.debug('\nSchema of table {0}:\n{1}\n'.format(i, schema[i]))
	
        self.schema = schema
        return schema 
    def dropTables(self):
        command = 'drop table {0}'
        for key in self.schema.keys():
            self.pgcursor.execute(command.format(key))
        self.pgconn.commit()

    ## Creates the schema based on the schema extracted 
    ## from the sqlite database
    def createTables(self):
        logger.info('Creating tables in postgres database')
        logger.debug('keys in schema {}'.format(self.schema.keys()))
        for key in self.schema.keys():
            command = 'CREATE table ' + key + '('
            primaryCtr = 0
            primaryTracker = []
            for obj in self.schema[key]:
                if(obj['primaryKey'] == True):
                    primaryCtr = primaryCtr + 1
                if(primaryCtr > 1):
                    break

            for obj in self.schema[key]:
                command = command + ' ' + obj['name']
                
                ## Refactors sqlite types to fit postgres rules
                if(obj['type'] == 'STRING'):
                    command = ' ' + command + ' varchar(100000)'
                elif(obj['name'] == 'jsConstants'):
                    command = ' ' + command + ' json'
                elif(obj['type'] == 'BLOB'):
                    command = ' ' + command + ' bytea'
                else:
                    command = ' ' + command + ' ' + obj['type']
                
                ## Checks for mulitple primary keys 
                if (obj['primaryKey'] == True and primaryCtr <= 1):
                    command = ' ' + command + ' PRIMARY KEY'
                elif (obj['primaryKey'] == True and primaryCtr > 1):
                    primaryTracker.append(obj['name'])
                
                if (obj['notNull'] == True):
                    command = ' ' + command + ' NOT NULL '
                
                command = command + ','
            
            temp = ' PRIMARY KEY( '
            if(primaryCtr > 1):
                for i in primaryTracker:
                    temp = temp + i + ', '
                temp = temp[:-2]
                command = command + temp + ')'
                command = command + ','

            command = command[:-1]
            command = command + ')'
            try:
                logger.info('\nQuery for create table:\n{0}\n'.format(command))
                self.pgcursor.execute(command)
            except:
                logger.info('Table {0} already exists.\n'.format(key))
        
        logger.info('Commiting Create table commands')
        self.pgconn.commit()

    def populate(self):
        ## Extract data from the sqlite database to Postgres database

        for key in self.schema.keys():
            # key = 'tblEditList'
            command = 'select * from {0}'.format(key)
            
            self.cursor.execute(command)
            ## Extract select all row by row to relieve 
            ## pressure from local cache
            row = self.cursor.fetchone()
            if row is None:
                continue
            logger.debug('Result of row from {1} query:\n{0}\n'.format(row, key))
            temp = ''
            temp2 = ''
            for i in self.schema[key]:
                temp = temp + i['name'] + ', '
                temp2 = temp2 + "%s,"
            temp = temp[:-2]
            temp2 = temp2[:-1]
            
            ins = 'insert into {0} ({1}) values ({2})'.format(key, temp, temp2)
            logger.info('Insert Query:\n {}\n'.format(ins))
            self.pgcursor.execute(ins, row)
            while(row):
                logger.debug('Result of row from {1} query:\n{0}\n'.format(row, key))
                logger.info('Insert Query:\n {}\n'.format(ins))
                row = self.cursor.fetchone()
                try:
                    self.pgcursor.execute(ins, row)
                except:
                    logger.debug('Entry failed')

            # self.pgconn.commit()
            # break

