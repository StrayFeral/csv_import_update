#!/usr/bin/python3

# This script is Python 3 specific
# 2020-07-20
# Evgueni.Antonov@gmail.com

import re
import sys
import csv
import os
import os.path
import pprint
import configparser

from io import StringIO

import psycopg2                 # PostgreSQL

# ----------------------------------------------
# Remove the comment '#' from next lines, if you are using any of these
# databases. Don't forget to install the connector module !!

#import mysql.connector          # MySQL
#import cx_Oracle                # Oracle
#import pymssql                  # MS SQL Server
# ----------------------------------------------


# WHAT IS THIS:
# Small dirty quick tool to import CSV file in a table and update the
# values if they exist.



# HELP
help_string                     = """USAGE: csv_import_update.py [path]<FILENAME>[.csv] [debug|verbose|diff]

EXAMPLE:
    ./csv_import_update.py ./mycsv.csv
    ./csv_import_update.py mycsv
    ./csv_import_update.py dir1/dir2/mycsv.csv debug
    ./csv_import_update.py dir1/dir2/mycsv.csv diff

verbose
    Enables VERBOSE MODE - more messages on screen.
debug
    Enables DEBUG MODE and VERBOSE MODE - more messages on screen and
    UPDATE and INSERT queries will not be executed.
diff
    Only displays differences betwen the CSV file and the table values.
    UPDATE and INSERT queries will not be executed.

Quick n dirty CSV importer.

This script will read a CSV file line by line and will do either
INSERT or UPDATE or nothing at all to a database. The database name,
type, table and other things are specified in an INI file with the
same name as the CSV file, located in conf/ subdirectory 
(located where this script is).

The script also expects and reads a default INI file: conf/defaults.ini

SUPPORTED DATABASES:
PostgreSQL, MySQL, Oracle, MS SQL Server

TESTED ON:
!!!PostgreSQL ONLY!!!
Written in Python 3.5.3

FOR EXAMPLE INI FILE, CHECK conf/example.ini
***FOR EVERYTHING ELSE, READ README.txt***

"""


# SETUP

execpath                        = os.path.dirname(os.path.abspath(__file__))    # This script full path
confpath                        = "{0}/conf".format(execpath)
csvpath_default                 = "csv"

required_database_settings      = ['type', 'host', 'name', 'table', 'user', 'port', 'pass']
required_csv_settings           = ['fields_list']
required_select_settings        = ['columns_list', 'keys_list']
required_update_settings        = ['columns_list']

cursor                          = None
connection                      = None
debug_mode                      = False
verbose_mode                    = False
diff_mode                       = False

application_issues_list         = {}
csv_file_issue                  = "csv"

pp                              = pprint.PrettyPrinter(indent=4)




def debug_print(s):
    if (diff_mode):
        return
        
    if (verbose_mode or debug_mode):
        if (s == ""):
            print()
        else:
            print("DEBUG: {0}".format(s))


def diff_print(s):
    if (not diff_mode):
        return
    print(s)


def regular_print(s):
    if (not diff_mode):
        print(s)


def add_application_issue(category, issuemsg):
    if (category not in application_issues_list):
        application_issues_list[category] = []
    
    application_issues_list[category].append("[{0}]{1}".format(category, issuemsg))


def get_all_application_issues():
    issues                      = []
    for cat in application_issues_list:
        issues                  = issues + application_issues_list[cat]
    
    return issues


def print_all_application_issues():
    issues                      = get_all_application_issues()
    
    if (len(issues) > 0):
        print()
        print("LIST OF ALL APPLICATION ISSUES")
        print("------------------------------")
    
    for imsg in issues:
        print(imsg)


def get_database_connection(conf):
    db                          = conf["db"]["type"]
    dhost                       = conf["db"][db]["host"]
    dport                       = conf["db"][db]["port"]
    dname                       = conf["db"][db]["name"]
    duser                       = conf["db"][db]["user"]
    dpass                       = conf["db"][db]["pass"]
    
    print("* Connecting to database '{0}' (*{1}* {2}:{3}) ...".format(dname, db, dhost, dport))
    print()
    
    if (db == 'postgresql'):
        return psycopg2.connect(host=dhost, port=dport, database=dname, user=duser, password=dpass)
    
    # Remove the comment '#' from next lines, if you are using any of these
    # databases. Don't forget to install the connector module !!
    
    #if (db == 'mysql'):
    #    return mysql.connector.connect(host=dhost, port=dport, database=dname, user=duser, password=dpass)
    #if (db == 'oracle'):
    #    return cx_Oracle.connector.connect(host=dhost, port=dport, database=dname, user=duser, password=dpass)
    #if (db == 'sqlserver'):
    #    return pymssql.connector.connect(host=dhost, port=dport, database=dname, user=duser, password=dpass)


# Any key name ending in "_list" will be resolved as a CSV value and returned as list.
# Any other value will be returned as is.
def get_ini_value(name, sval):
    match                       = re.search(r'_list$', name)
    if not match:
        return sval.strip() # If not a list, return as is
    
    fh                          = StringIO(sval)
    reader                      = csv.reader(fh, delimiter=',', quotechar='"') # INI values format
    l                           = list(reader)
    result_list                 = []
    
    # Trimming spaces
    for val in l[0]:
        result_list.append(val.strip())
    
    return result_list # l[0] # We need just the first element


def get_ini_section(sdict):
    for key in sdict:
        sdict[key]              = get_ini_value(key, sdict[key])
    
    return sdict


def read_csv_file(conf, fn):
    global application_issues_list
    dchar                       = conf["csv"]["delimiter"]
    qchar                       = conf["csv"]["quotechar"]
    sql_get_next_id             = get_next_id_query(conf)
    
    rowcnt                      = 0
    inscnt                      = 0
    updcnt                      = 0
    skipcnt                     = 0
    
    # with open('eggs.csv', newline='') as csvfile:
    with open(fn) as fh:
        reader                  = csv.reader(fh, delimiter=dchar, quotechar=qchar)
        
        diff_print("\"STATUS\",\"DATABASE_COLUMN\",\"DATABASE_VALUE\",\"LINE\",\"{0}\"".format(os.path.basename(fn)))
        
        for csvrow in reader:
            rowcnt              = rowcnt + 1
            conf["current_csv_line"] = rowcnt
            increment_column_val= None
            
            debug_print("csv {0}> {1}".format(rowcnt, csvrow))
            
            if (len(conf["csv2db_fields_map"].keys()) != len(csvrow)):
                raise Exception("CSV Line: {0}; Number of fields mismatch. Expected {1}, got {2}. Invalid CSV file.".format(rowcnt, len(conf["csv2db_fields_map"].keys()), len(csvrow)))
            
            row                 = []
            for col in csvrow:
                row.append(col.strip()) # Trim spaces
            csvrow              = row.copy()
            
            csvrowdict          = get_csv_row_dict(conf, csvrow)
            sql_select          = get_select_query(conf, csvrow)
            sql                 = ""
            
            if (sql_select == ""):
                #skipcnt         = skipcnt + 1
                #continue ### Nah, this is not a minor exception, so we will crash
                raise Exception("CSV Line: {0}; Blank value for a key column. All values for key columns must not be blank. Invalid CSV file.".format(rowcnt))
            
            debug_print("EXECUTING: {0}".format(sql_select))
            
            cursor.execute(sql_select)
            total_records       = cursor.rowcount
            debug_print("Total SELECTed rows: {0}".format(total_records))
            # results                     = cursor.fetchall()
            
            if (total_records > 1):
                print()
                print("PROBLEM: More than one record found, as a result of this SELECT. This is not what we expect. The keys you specify in your INI, section [get_database_data].keys_list must uniquely identify only one record. Unless somebody messed-up your database.")
                print("SELECT: {0}".format(sql_select))
                print("Key columns: {0}".format(conf["select"]["keys_list"]))
                raise Exception("Key columns do not identify uniqely one record or multiple records exist in database.")
            
            # We have 1 result
            dbrowdict           = {}
            for dbrow in cursor:
                debug_print("SELECT: {0}".format(dbrow))
                
                dbrowdict       = get_db_row_dict(conf, dbrow)
                
                for colname in list(conf["update"]["columns_list"]):
                    csvcolname  = conf["db2csv_fields_map"][colname]
                    dbval       = dbrowdict[colname]
                    csvval      = csvrowdict[csvcolname]
                    valtype     = conf["column_types"][colname]
                    
                    # Special situation - if value type is not varchar,
                    # then if we get empty value in the CSV, it should be
                    # interpreted as NULL
                    if (valtype == "other" and csvval == ''):
                        csvval  = None
                    
                    if ("null_equals_to_empty" in conf["select"] and valtype == "varchar" and dbval == None):
                        dbval   = ''
                    
                    debug_print(">>>> compare: (db)'{0}' != '{1}'(csv)".format(dbval, csvval))
                    if (dbval != csvval):
                        sql     = get_update_query(conf, csvrow)
                        regular_print("[{0}] updating: {1}".format(rowcnt, csvrowdict))
                        diff_print("\"diff\",\"{0}\",\"{1}\",\"{2}\",\"{3}\"".format(colname, dbval, rowcnt, csvrow))
                        updcnt  = updcnt + 1
                        break
            
            if (total_records == 0):
                cursor.execute(sql_get_next_id)
                singlerow       = cursor.fetchone()
                increment_column_val = singlerow[0] # Single value
                debug_print("next id: {0}".format(increment_column_val))
                sql             = get_insert_query(conf, csvrow, increment_column_val)
                regular_print("[{0}] inserting: {1}".format(rowcnt, csvrowdict))
                diff_print("\"NEW\",\"\",\"\",\"{0}\",\"{1}\"".format(rowcnt, csvrow))
                inscnt          = inscnt + 1
            
            
            # If anything to do (values exist and differ)
            debug_print("EXECUTING: {0}".format(sql))
            if (not debug_mode and not diff_mode):
                if (sql != ""):
                    cursor.execute(sql)
                    connection.commit()
                else:
                    print("[{0}] skipping: {1}".format(rowcnt, csvrowdict))
                    skipcnt     = skipcnt + 1
            else:
                debug_print("SQL query not executed, because of DEBUG_MODE being enabled.")
    
    print()
    print()
    print("[SUMMARY] total:{0}, inserts:{1}, updates:{2}, skips:{3}".format(rowcnt, inscnt, updcnt, skipcnt))
    diff_print("total differences found:{0}".format(inscnt+updcnt))
 

def check_prerequisites():
    if (not os.path.isdir(confpath)):
        raise Exception("Config directory not found: {0}".format(confpath))
    
    if (len(sys.argv) == 1):
        print(help_string)
        raise Exception("No filename given.")


def get_filenames():
    global debug_mode
    global verbose_mode
    global diff_mode
    
    fn                          = sys.argv[1]
    conffn                      = "{0}/{1}".format(confpath, os.path.basename(fn))
    defaultsconf                = "{0}/defaults.ini".format(confpath)
    
    if (len(sys.argv) - 1 > 1):
        param2                  = sys.argv[2]
        param2.lower()
        
        if (param2 == "debug"):
            debug_mode          = True
        
        if (param2 == "verbose"):
            verbose_mode        = True
        
        if (param2 == "diff"):
            diff_mode           = True
    
    if (re.match(r'\/', fn)):
        fn                      = "{0}/{1}".format(csvpath_default, fn)
    
    if (re.search(r'\.[A-z]+$', conffn)):
        conffn                  = re.sub(r'\.[A-z]+$', '.ini', conffn)
    else:
        conffn                  = "{0}.ini".format(conffn)
        fn                      = "{0}.csv".format(fn)
    
    return fn, conffn, defaultsconf


def check_files(fn, conffn, defaultsconf):
    if (not os.path.exists(defaultsconf)):
        raise Exception("Config file not found: {0}".format(defaultsconf))
    if (not os.path.isfile(defaultsconf)):
        raise Exception("Not a file: {0}".format(defaultsconf))
    
    if (not os.path.exists(fn)):
        raise Exception("File not found: {0}".format(fn))
    if (not os.path.isfile(fn)):
        raise Exception("Not a file: {0}".format(fn))
    
    if (not os.path.exists(conffn)):
        raise Exception("Config file not found: {0}".format(conffn))
    if (not os.path.isfile(conffn)):
        raise Exception("Not a file: {0}".format(conffn))


def check_default_settings(conf, dbconf):
    if ("type" not in dbconf):
        raise Exception("Database type not specified in INI file.")
    
    if (len(conf["csv2db_fields_map"].keys()) == 0):
        raise Exception("No CSV fields to database columns mapping specified in the INI file.")
    
    db                          = dbconf["type"].lower() # Just in case
    conf["db"]["type"]          = db
    
    for setting in dbconf:
        conf["db"][db][setting] = dbconf[setting]
    
    for setting in required_database_settings:
        if setting not in conf["db"][db]:
            raise Exception("Required database setting not found: {0}".format(setting))
    
    for setting in required_csv_settings:
        if setting not in conf["csv"]:
            raise Exception("Required CSV setting not found: {0}".format(setting))
    
    for setting in required_select_settings:
        if setting not in conf["select"]:
            raise Exception("Required SELECT setting not found: {0}".format(setting))
    
    for setting in required_update_settings:
        if setting not in conf["update"]:
            raise Exception("Required UPDATE setting not found: {0}".format(setting))


def set_db2csv_fields_map(conf):
    db2csv_fields_map           = {}
    
    for csvcol in conf["csv2db_fields_map"].keys():
        dbcol                   = conf["csv2db_fields_map"][csvcol]
        db2csv_fields_map[dbcol]= csvcol
    
    conf["db2csv_fields_map"]   = db2csv_fields_map


def set_csv_column_indexes(conf):
    index_map                   = {}
    
    index                       = -1
    for col in conf["csv"]["fields_list"]:
        index                   = index + 1
        index_map[col]          = index
    
    conf["index_map"]           = index_map


def set_column_types(conf):
    types                       = {}
    
    for colname in conf["select"]["columns_list"]:
        if (re.search(r'_varchar$', colname)):
            colname             = re.sub(r'_varchar$', '', colname)
            types[colname]      = "varchar"
        else:
            types[colname]      = "other"
    
    conf["column_types"]        = types


def format_value(conf, col, val):
    if (conf["column_types"][col] == "varchar"):
        val                     = "'{0}'".format(val.strip())
    return val


def get_predicate(conf, csvdata):
    global application_issues_list
    i                           = 0
    predicate                   = ""
    rowcnt                      = conf["current_csv_line"]
    
    for column in conf["select"]["keys_list"]:
        csvcol                  = conf["db2csv_fields_map"][column]
        csvcol_index            = conf["index_map"][csvcol]
        val                     = csvdata[csvcol_index]
        
        # Key columns cannot be empty
        if (val == None or val == ""):
            add_application_issue(csv_file_issue, "[Ln:{0}][Col:{1}] Empty value for key column '{2}'.".format(rowcnt, csvcol_index, csvcol))
            continue
        
        val                     = format_value(conf, column, val)
        i                       = i + 1
        
        if (i == 1):
            predicate           = "{0} {1} = {2}".format(predicate, column, val)
        else:
            predicate           = "{0} and {1} = {2}".format(predicate, column, val)
    
    return predicate


def get_select_query(conf, csvdata):
    db                          = conf["db"]["type"]
    column_list                 = conf["column_types"].keys()
    predicate                   = get_predicate(conf, csvdata)
    
    if (predicate == ""):
        return ""
    
    sql                         = "select {0} from {1} where {2};".format(', '.join(column_list), conf["db"][db]["table"], predicate)
    
    if "select" in conf["select"]:
        sql                     = conf["select"]["select"]
    
    return sql


def get_update_query(conf, csvdata):
    db                          = conf["db"]["type"]
    column_list                 = conf["update"]["columns_list"]
    predicate                   = get_predicate(conf, csvdata)
    
    if (predicate == ""):
        return ""
        
    i                           = 0
    set_clause                  = ""
    for column in column_list:
        i                       = i + 1
        csvcol                  = conf["db2csv_fields_map"][column]
        csvcol_index            = conf["index_map"][csvcol]
        val                     = csvdata[csvcol_index]
        val                     = format_value(conf, column, val)
        
        if (i == 1):
            set_clause          = "{0} {1} = {2}".format(set_clause, column, val)
        else:
            set_clause          = "{0}, {1} = {2}".format(set_clause, column, val)
    
    sql                         = "update {0} set{1} where {2};".format(conf["db"][db]["table"], set_clause, predicate)
    
    return sql
    

def get_next_id_query(conf):
    db                          = conf["db"]["type"]
    column_list                 = conf["update"]["columns_list"]
    column                      = conf["update"]["increment_column"]
    sql                         = "select max({0}) + 1 as next_id from {1};".format(column, conf["db"][db]["table"])
    
    return sql


def get_insert_query(conf, csvdata, nextid=None):
    db                          = conf["db"]["type"]
    column_list                 = list(conf["db2csv_fields_map"].keys())
    predicate                   = get_predicate(conf, csvdata)
    
    if (predicate == ""):
        return ""
        
    values                      = []
    for column in column_list:
        csvcol                  = conf["db2csv_fields_map"][column]
        csvcol_index            = conf["index_map"][csvcol]
        val                     = csvdata[csvcol_index]
        val                     = format_value(conf, column, val)
        values.append(val)
    
    if (nextid):
        increment_column        = conf["update"]["increment_column"]
        column_list.append(increment_column)
        values.append(str(nextid))
    
    sql                         = "insert into {0} ({1}) values ({2});".format(conf["db"][db]["table"], ', '.join(column_list), ', '.join(values))
    
    return sql


def get_db_row_dict(conf, dbrow):
    column_list                 = list(conf["column_types"].keys())
    results                     = {}
    
    for i in range(len(column_list)):
        results[column_list[i]] = dbrow[i]
    
    return results


def get_csv_row_dict(conf, csvrow):
    column_list                 = conf["csv"]["fields_list"]
    results                     = {}
    
    for i in range(len(column_list)):
        results[column_list[i]] = csvrow[i]
    
    return results





# ========================================================== MAIN BEGIN
try:
    print()
    print("***CSV IMPORT-UPDATE TOOL***   2020-07-20 Evgueni.Antonov@gmail.com")
    print()
    
    check_prerequisites()
    fn, conffn, defaultsconf    = get_filenames()
    
    if (debug_mode):
        print("***DEBUG AND VERBOSE MODE ENABED***")
        print()
    elif (verbose_mode):
        print("***VERBOSE MODE ENABLED***")
        print()
    elif (diff_mode):
        print("***DIFF MODE ENABLED***")
        print()
    
    print("PARSING: {0}".format(fn))
    print()
    
    check_files(fn, conffn, defaultsconf)
    
    global config
    global conf
    config                      = configparser.ConfigParser()
    conf                        = {
        "db"        : {}
    }
    
    config.read(defaultsconf)   # Reading defaults.ini
    
    for db in config.sections():
        conf["db"][db] = get_ini_section(dict(config.items(db)))
    
    config.read(conffn)         # Reading the actual INI
    
    dbconf                      = get_ini_section(dict(config.items('database')))
    conf["csv"]                 = get_ini_section(dict(config.items('csv')))
    conf["select"]              = get_ini_section(dict(config.items('get_database_data')))
    conf["update"]              = get_ini_section(dict(config.items('update_database_data')))
    conf["csv2db_fields_map"]   = get_ini_section(dict(config.items('database_mapping')))
    
    check_default_settings(conf, dbconf)                            # We also get many defaults here
    set_column_types(conf)
    set_csv_column_indexes(conf)
    set_db2csv_fields_map(conf)
    
    debug_print("Current config:")
    debug_print(pp.pformat(conf))
    debug_print("")
    
    connection                  = get_database_connection(conf)     # Connect to database
    cursor                      = connection.cursor()
    
    # csv_content = get_csv_file_content(conf, fn)
    read_csv_file(conf, fn)
    
    # Close connections
    cursor.close()
    connection.close()
    
    print_all_application_issues()
    
    print()
    print("Application end.")


except Exception as e:
    exc_type, exc_obj, exc_tb   = sys.exc_info()
    fname                       = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    
    print()
    print("--------------------[ EXCEPTION")
    sys.exit("{0} ({1}); Code line: {2};".format(e, exc_type, exc_tb.tb_lineno))

finally:
    # Release the connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()

