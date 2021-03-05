csv_import_update.py README
=======================================================================
2020-07-17
Evgueni.Antonov@gmail.com
Written in Python 3.5.3

https://github.com/StrayFeral/csv_import_update

This python script is a quick n dirty tool for import/update of a CSV
file into a database. It is a fully generic tool.

The intention is that this script could be used by developers, but also
by any beginner user, so this README file is written for users of all levels.

USAGE: csv_import_update.py [path]<FILENAME>[.csv] [debug|verbose|diff]

EXAMPLE:
    ./csv_import_update.py ./mycsv.csv verbose
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

SUPPORTED DATABASES:
PostgreSQL, MySQL, Oracle, MS SQL Server

TESTED ON:
!!!PostgreSQL ONLY!!!

FOR EXAMPLE INI FILE, CHECK conf/example.ini


HOW IT WORKS:
For convenience in this file, these will be used:
    PYPATH: The path where the executable Python script is located 
        (the main script)
    CSV: The CSV input file
    INI: The INI input file (it must have same file name as the CSV,
        but extension .ini.
        EXAMPLE: myproject.csv myproject.ini

1) Reads PYPATH/conf/defaults.ini
2) Reads PYPATH/conf/INI
3) Reads the CSV

This script is written with large files in mind, so the CSV will be
read line-by-line. Each row will be tested if present in the database
and if present, values will be compared. If the values to be updated
are different (at least one of them), the database row will be updated.
If values are the same - this row will be skipped and next CSV row will
be read. If no such record is found in the database, the CSV row will
be inserted as a new record.

For convenience, if something does not work and you need to debug,
just run with either "verbose" or "debug" parameter after the filename
(but not both! only "verbose" or "debug" -- "debug" turns on the
VERBOSE MODE too). They do the same thing - you will see much more
messages on the screen, except if in DEBUG MODE, database will not
be updated in any way (no INSERT or UPDATE queries will be executed).

IMPORTANT: 
In VERBOSE/DEBUG MODE each SQL query will be shown on screen, before
being executed, which is useful for database debugging.

*** TESTED ONLY ON POSTGRESQL on Debian linux ***

Yep. For everybody else - please excuse, no time, nor resources at
the moment, but you can test and report me the result.


WHAT ABOUT OTHER DATABASES (aside of PostgreSQL):

Database        : Python connector class
----------------------------------------
postgresql      : psycopg2
mysql           : mysql.connector
oracle          : cx_Oracle
sqlserver       : pymssql

I never tried it, but here is what I know - the table above lists the
connector classes you need to have. For each database first you need to
install these modules yourself, manually. Use your linux package manager
(never tried on Windows).

After you install the connector class, open in an editor the script
itself (csv_import_update.py) and in the very begining you will see
few "import" clauses, which are commented-out (they have "#" in front).
Remove the comment symbol for these modules, which you already installed.
Also go to the line "def get_database_connection" and few lines down
you will see few "if (db == " lines also commented out. Remove the
comment for the databases you use.

That's it. This is how to enable the use of other databases.


THE INI FILES: PYPATH/conf/defaults.ini
This file is straight forward. It defines all the possible databases and
how to access them. Here is how:

[mysql]
port                            = 3306
user                            = mysql
pass                            = password_goes_here

Obviously each [section] is a database name. Then follow settings for
default port, user and password. This INI is read first. If you set the
same setting in another INI under section [database], it will overwrite
these settings.

THE OTHER INI FILES:
When you run the script, after reading defaults.ini, it expects to find
a CSV file (you may put these anywhere and specify the path on the
command-line) and an INI file with the same name, but extension .ini,
located where this script is located in conf/ subdirectory.

The CSV file is straight-forward. This INI file however defines the fine
details on how the CSV file should be read, which database to connect
to, which table to use and what to use with the CSV data we find or not
in this table.

All sections in this INI are mandatory to be present.

[database]
host                            = myhost.com
type                            = postgresql
name                            = mydatabase
table                           = mytable001

This is straight-forward. Thing is - "type" refers to defaults.ini,
so this value is an actual section there. Check conf/example.ini,
the provided conf/defaults.ini to see what I talk about.

[csv]
fields_list                     = first_name_csv,last_name_csv,account_csv,city_csv
delimiter                       = ,
quotechar                       = "

Delimiter and quotechar are not mandatory.

This must be pretty clear too. "fields_list" simply shows us what fields
are in the CSV file and most important ------ the order in which the
fields are to be found in the CSV file !!

******IMPORTANT:
ANY INI SETTING NAME ENDING IN "_list" INDICATES, THAT ITS VALUE IS
A LIST.

In this case, "fields_list" value is a CSV value itself. And few other
values are lists too.

[get_database_data]
columns_list                    = first_name_varchar,last_name_varchar,account_number,city_varchar
keys_list                       = first_name,last_name,city
null_equals_to_empty            = yes

"null_equals_to_empty" is not mandatory, but if present and set to "yes"
will enforce when comparing varchar values from CSV and database, if in
database the value is NULL to be interpreted as empty string.

This section defines what data we are looking from the database table.

IMPORTANT: Each CSV file will be imported/updated in only ONE database
table. If you need to do import in multiple tables - create CSV and
INI file for each table.

"columns_list" describes which table columns we are interested in. Data
will be pulled from these (SELECT) and will be compared to the currently
read CSV row.

********VERY IMPORTANT:
"columns_list" in [get_database_data] ALSO DEFINES THE COLUMNS DATA TYPE!

In this case we need to know only one thing - should we quote the value
or not, so only VARCHAR type is specified as appending "_varchar" to the
name of the column.

"keys_list" is important - it defines the columns which will be used to
create the SQL query predicate also in the UPDATE and INSERT queries.

So based on the above, the SELECT will be:

SELECT first_name, last_name, account_number, city
FROM mytable001
WHERE first_name = 'blah'
AND last_name = 'bleh'
AND city = 'some city';

[update_database_data]
columns_list                    = account_number
increment_column                = table_name_goes_here_id

This defines how to set the UPDATE and INSERT queries. Note that
"columns_list" is a list (it contains "_list" in its name and will be
interpreted as a list, even with a single column value), because while
currently I need to update only one column, I suspect somebody might
need to update multiple columns. To UPDATE multiple columns feature 
is not tested, but I think it should be fine and working.

"increment_column" is not mandatory, but it is very important. 
Usually in a table we have a primary key column, which we either 
auto-increment or increment ourselves. If we need to increment it 
ourselves, here we specify the name of this column, so the INSERT 
query is created accordingly. So to be precise, what will happen is
before creating the INSERT query, a SELECT will be executed to check
MAX(column_name)+1 value. This value then will be used in the INSERT.

[database_mapping]
first_name_csv                  = first_name
last_name_csv                   = last_name
city_csv                        = city
account_csv                     = account

This is the last section. It defines the relation (mapping) of each
CSV field to each database table column.

*********DEBUGGING**********

If you get some exception and you need to debug, just run the script
either with "verbose" or "debug" (but not both) parameter, after
the filename parameter. "verbose" just enables verbose printing
(more messages on screen) and "debug" prevents any INSERT or UPDATE
query to be executed. It also enables the "verbose" mode.

In this mode, lot will be printed on screen, so better don't log it
to file and better use small CSV file with only few rows. However
you will see every SQL query being created, printed on screen. Again,
UPDATE or INSERTs won't be executed in DEBUG mode, but you will see
the queries on the screen anyway. If not in DEBUG mode, you will also
see the database results.

FINAL WORDS:
Hope this script will be useful to you, as it is to me. Importing data
in the database is a common task and there are some tools, however
I did not found any data-update tools which to be convenient to me,
so I wrote one myself.

***EOF
