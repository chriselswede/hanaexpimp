#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import sys, os, time, subprocess, re
from difflib import Differ
import signal
import fnmatch

def printHelp():
    print("                                                                                                                                   ")    
    print("DESCRIPTION:                                                                                                                       ")
    print(" The HANA exporter could help to follow the SAP Note 3568889 in the case of not enough memory to export the full table.            ")
    print(" This script could then help to export views in parts, sleep between each export and then import the data back into the table.     ")
    print(" It is assumed that the views to be exported are named as <view_name>_1, <view_name>_2, ..., <view_name>_<number_views>            ")
    print(" The views must be created before running this script.                                                                             ")
    print(" Please also see https://help.sap.com/docs/SAP_HANA_PLATFORM/4fe29514fd584807ac9f2a04f6754767/6a6f59bbfbb64ade84d83d7f87789753.html?locale=en-US&version=LATEST")
    print("                                                                                                                                   ")
    print("INPUT ARGUMENTS:                                                                                                                   ")
    print("         ----  USER KEY  ----                                                                                                      ")     
    print(" -k      DB user key, this one has to be maintained in hdbuserstore, i.e. as <sid>adm do                                           ")               
    print("         > hdbuserstore SET <DB USER KEY> <ENV> <USERNAME> <PASSWORD>                     , default: SYSTEMKEY                     ")
    print("         ---- OUTPUT  ----                                                                                                         ")
    print(" -os     output sql [true/false], prints all crucial tasks, default: false                                                         ")
    print(" -op     output path, full literal path of the folder for the output logs (will be created if not there), default = '' (not used)  ")
    print("         ---- EXECUTE  ----                                                                                                        ")
    print(" -es     execute sql [true/false], execute all crucial tasks (useful to turn off for investigation with -os=true,                  ")
    print("         a.k.a. chicken mode :)  default: true                                                                                     ")
    print("         ---- SLEEP  ----                                                                                                          ")
    print(" -st     sleep time [s], time to sleep, in seconds, between each export,                                                           ")
    print("         ---- TABLE and VIEWS  ----                                                                                                ")
    print(" -ts     table schema, the schema where the table is located, default: SAPHANADB                                                   ")
    print(" -tn     table name, the name of the table to be imported back into, default: ""  (must be provided)                               ")
    print(" -vs     view schema, the schema where the views to be exported are located, default: ""   (must be provided)                      ")
    print(" -vn     view name, the first part of the name of views to be exported,                                                            ")
    print("         the end of the names must be _1 ... _<number views>, default: ""  (must be provided)                                      ")
    print(" -vp     view path, the full path to where the views will be exported, default: "" (must be provided)                              ")
    print(" -nv     number of views, the number of views to be exported, default: 10                                                          ")
    print(" -sv     start view number, the number of the first view to be exported, default: 1                                                ")
    print("         ---- EXPORT/IMPORT  ----                                                                                                  ")
    print(" -exp    export [true/false], true --> export, false --> import, default: export                                                   ")
    print("                                                                                                                                   ")
    print("                                                                                                                                   ")
    print("EXAMPLES:                                                                                                                          ")
    print(" Before below example, following views were created:                                                                               ")
    print("  CREATE VIEW VIEWMASS_1 AS SELECT * FROM PLAYGROUND.MASS_DATA_PART_DEMO where COL1 < 20171214;                                    ")
    print("  CREATE VIEW VIEWMASS_2 AS SELECT * FROM PLAYGROUND.MASS_DATA_PART_DEMO where COL1 >= 20171214;                                   ")
    print("  SELECT SUM(row_count) AS total_row_count FROM (  --check that in total the views have the same number of rows as the table       ")
    print("  SELECT COUNT(*) AS row_count FROM SYSTEM.VIEWMASS_1                                                                              ")
    print("  UNION ALL                                                                                                                        ")
    print("  SELECT COUNT(*) AS row_count FROM SYSTEM.VIEWMASS_2                                                                              ") 
    print("  ) AS counts;                                                                                                                     ")
    print(" This shows an example of a 'chicken mode' of checking the statements of exporting 1 out of 2 views without executing them         ")
    print("  python hanaexpimp.py -k SYSTEMKEYT1 -ts PLAYGROUND -tn MASS_DATA_PART_DEMO -vs SYSTEM -vn VIEWMASS -vp /usr/sap/CHP/HDB00/work/  ")
    print("  -nv 2 -sv 2 -exp true -st 1 -es false -os true                                                                                   ")
    print(" This shows an example of a 'chicken mode' of checking the statements of importing back 1 out of 2 views without executing them    ")
    print("  python hanaexpimp.py -k SYSTEMKEYT1 -ts PLAYGROUND -tn MASS_DATA_PART_DEMO -vs SYSTEM -vn VIEWMASS -vp /usr/sap/CHP/HDB00/work/  ")
    print("  -nv 2 -sv 2 -exp false -st 1 -es false -os true                                                                                  ")
    print("                                                                                                                                   ")
    print("                                                                                                                                   ")
    print("AUTHOR: Christian Hansen                                                                                                           ")
    print("                                                                                                                                   ")
    print("                                                                                                                                   ")
    os._exit(1)
    
def printDisclaimer():
    print("                                                                                                                                   ")    
    print("ANY USAGE OF HANAEXPIMP ASSUMES THAT YOU HAVE UNDERSTOOD AND AGREED THAT:                                                          ")
    print(" 1. HANAExpImp is NOT SAP official software, so normal SAP support of hanaexpimp cannot be assumed                                 ")
    print(" 2. HANAExpImp is open source                                                                                                      ") 
    print(' 3. HANAExpImp is provided "as is"                                                                                                 ')
    print(' 4. HANAExpImp is to be used on "your own risk"                                                                                    ')
    os._exit(1)

class SQLManager:
    def __init__(self, execute_sql, hdbsql_string, dbuserkey, DATABASE, log_sql):
        self.execute = execute_sql
        self.key = dbuserkey
        self.db = DATABASE
        self.log = log_sql
        if len(DATABASE) > 1:
            self.hdbsql_jAU = hdbsql_string + " -j -A -U " + self.key + " -d " + self.db
            self.hdbsql_jAxU = hdbsql_string + " -j -A -x -U " + self.key + " -d " + self.db
            self.hdbsql_jAaxU = hdbsql_string + " -j -A -a -x -U " + self.key + " -d " + self.db
            self.hdbsql_jAQaxU = hdbsql_string + " -j -A -Q -a -x -U " + self.key + " -d " + self.db
        else:
            self.hdbsql_jAU = hdbsql_string + " -j -A -U " + self.key
            self.hdbsql_jAxU = hdbsql_string + " -j -A -x -U " + self.key
            self.hdbsql_jAaxU = hdbsql_string + " -j -A -a -x -U " + self.key
            self.hdbsql_jAQaxU = hdbsql_string + " -j -A -Q -a -x -U " + self.key

class LogManager:
    def __init__(self, log_path, out_prefix, print_to_std):
        self.path = log_path
        self.out_prefix = out_prefix
        if self.out_prefix:
            self.out_prefix = self.out_prefix + "_"
        self.print_to_std = print_to_std

def run_command(cmd, check = True):
    if check:
        out = ''
        try:
            out = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True).stdout.strip("\n")
        except subprocess.CalledProcessError as e:
            print("ERROR: Could not run\n\t"+cmd+"\nERROR MESSAGE:\n"+e.stderr)
    else:
        out = subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip("\n")
    return out

def try_execute_sql(sql, errorlog, sqlman, logman, exit_on_fail = True, always_execute = False):
    succeeded = True
    out = ""
    try:
        if sqlman.log:
            log(sql, logman)
        if sqlman.execute or always_execute:
            sql = sqlman.hdbsql_jAaxU + " \""+sql+"\""
            out = subprocess.run(sql, shell=True, capture_output=True, text=True, check=True).stdout.strip("\n")
    except subprocess.CalledProcessError as e:
        errorMessage = "ERROR: Could not execute\n\t"+sql+"\nERROR MESSAGE:\n"+e.stderr+"\n"+errorlog
        succeeded = False
        if exit_on_fail:
            log(errorMessage, logman)
            os._exit(1)
        else:
            log(errorMessage, logman)
    return [out, succeeded]

def log(message, logmanager):
    if logmanager.print_to_std:
        print(message)
    if logmanager.path:
        file_name = "hanaexpimplog"
        logfile = open(logmanager.path+"/"+file_name+"_"+logmanager.out_prefix+datetime.now().strftime("%Y-%m-%d"+".txt").replace(" ", "_"), "a")
        logfile.write(message+"\n")   
        logfile.flush()
        logfile.close()

def checkAndConvertBooleanFlag(boolean, flagstring, logman = ''):     
    boolean = boolean.lower()
    if boolean not in ("false", "true"):
        if logman:
            log("INPUT ERROR: "+flagstring+" must be either 'true' or 'false'. Please see --help for more information.", logman)
        else:
            print("INPUT ERROR: "+flagstring+" must be either 'true' or 'false'. Please see --help for more information.")
        os._exit(1)
    boolean = True if boolean == "true" else False
    return boolean

def get_key_info(dbuserkey, local_host, logman):
    try:
        key_environment = run_command('''hdbuserstore LIST '''+dbuserkey) 
    except:
        log("ERROR, the key "+dbuserkey+" is not maintained in hdbuserstore.", logman)
        os._exit(1)
    if "NOT FOUND" in key_environment:
        log("ERROR, the key "+dbuserkey+" is not maintained in hdbuserstore.", logman)
        os._exit(1)
    key_environment = key_environment.split('\n')
    key_environment = [ke for ke in key_environment if ke and not ke == 'Operation succeed.']
    ENV = key_environment[1].replace('  ENV : ','').replace(';',',').split(',')
    key_hosts = [env.split(':')[0].split('.')[0] for env in ENV]  #if full host name is specified in the Key, only the first part is used
    DATABASE = ''
    if len(key_environment) == 4:   # if DATABASE is specified in the key, this will by used in the SQLManager (but if -dbs is specified, -dbs wins)
        DATABASE = key_environment[3].replace('  DATABASE: ','').replace(' ', '')
    if not local_host in key_hosts:
        print("ERROR, local host, ", local_host, ", should be one of the hosts specified for the key, ", dbuserkey)
        os._exit(1)
    return  [key_hosts, ENV, DATABASE]

def export_view(view_number, view_schema, view_name, view_path, number_views, sleep_time, sqlman, logman):
    sql_for_export = "EXPORT INTO '"+view_path+"exported_"+view_name+"_"+str(view_number)+".csv' FROM "+view_schema+"."+view_name+"_"+str(view_number)
    log("Will now export "+view_name+"_"+str(view_number)+" to "+view_path+"exported_"+view_name+"_"+str(view_number)+".csv", logman)
    errorlog = "ERROR: Could not export "+view_name+"_"+str(view_number)+" to "+view_path+"exported_"+view_name+"_"+str(view_number)+".csv"
    try_execute_sql(sql_for_export, errorlog, sqlman, logman) 
    nbrRows = run_command("cat "+view_path+"exported_"+view_name+"_"+str(view_number)+".csv|wc -l")
    log("Number of rows in "+view_path+"exported_"+view_name+"_"+str(view_number)+".csv is now "+nbrRows, logman)
    if int(view_number) < int(number_views):
        log("Will now sleep for "+sleep_time+" seconds before exporting "+view_name+"_"+str(view_number+1), logman)
    time.sleep(int(sleep_time))
    return nbrRows

def import_view(view_number, view_name, view_path, table_schema, table_name, number_views, sleep_time, sqlman, logman):
    sql_for_import = "IMPORT FROM CSV FILE '"+view_path+"exported_"+view_name+"_"+str(view_number)+".csv' INTO \""+table_schema+"\".\""+table_name+"\""
    log("Will now import all data from "+view_name+"_"+str(view_number)+" into \""+table_schema+"\".\""+table_name+"\"", logman)
    errorlog = "ERROR: Could not import data from"+view_name+"_"+str(view_number)+" into \""+table_schema+"\".\""+table_name+"\""
    try_execute_sql(sql_for_import, errorlog, sqlman, logman) 
    sql_to_count = "SELECT COUNT(*) FROM \""+table_schema+"\".\""+table_name+"\""
    [count_out, succeeded] = try_execute_sql(sql_to_count, errorlog, sqlman, logman, True, True)
    count_out = count_out.strip("\n").strip("|").strip(" ")
    log("Number of rows in \""+table_schema+"\".\""+table_name+"\" is now "+count_out, logman)
    if int(view_number) < int(number_views):
        log("Will now sleep for "+sleep_time+" seconds before importing data from "+view_name+"_"+str(view_number+1), logman)
    time.sleep(int(sleep_time))

def get_sid():
    SID = run_command('echo $SAPSYSTEMNAME').upper()
    return SID

def is_integer(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def getParameterFromCommandLine(sysargv, flag_string, flag_log, parameter):
    if flag_string in sysargv:
        flag_value = sysargv[sysargv.index(flag_string) + 1]
        parameter = flag_value
        flag_log[flag_string] = [flag_value, "command line"]
    return parameter

def checkIfAcceptedFlag(word):
    if not word in ["-h", "--help", "-d", "--disclaimer", "-ff", "-k", "-os", "-op", "-es", "-st", "-ts", "-tn", "-vs", "-vn", "-vp", "-nv", "-sv", "-exp"]:
        print("INPUT ERROR: ", word, " is not one of the accepted input flags. Please see --help for more information.")
        os._exit(1)

def main():
 
    #####################   DEFAULTS   ####################
    dbuserkeys = "SYSTEMKEY"   # The KEY must be maintained in hdbuserstore  
    out_sql = 'false'
    out_path = ""
    execute_sql = 'true'
    sleep_time = '60'   # in seconds
    table_schema = "SAPHANADB"
    table_name = ""
    view_schema = ""
    view_name = ""
    view_path = ""
    number_views = '10'
    start_view_number = '1'
    export_flag = 'true'   # true --> export, false --> import
    
    #####################  CHECK INPUT ARGUMENTS #################
    if len(sys.argv) == 1:
        print("INPUT ERROR: hanaexpimp needs input arguments. Please see --help for more information.")
        os._exit(1) 
    if len(sys.argv) != 2 and len(sys.argv) % 2 == 0:
        print("INPUT ERROR: Wrong number of input arguments. Please see --help for more information.")
        os._exit(1)
    for i in range(len(sys.argv)):
        if i % 2 != 0:
            if sys.argv[i][0] != '-':
                print("INPUT ERROR: Every second argument has to be a flag, i.e. start with -. Please see --help for more information.")
                os._exit(1)

    ############ GET SID ##########
    SID = get_sid()

    #####################  PRIMARY INPUT ARGUMENTS   ####################
    flag_log = {}     
    if '-h' in sys.argv or '--help' in sys.argv:
        printHelp()   
    if '-d' in sys.argv or '--disclaimer' in sys.argv:
        printDisclaimer()

    #####################   INPUT ARGUMENTS  ####################
    for word in sys.argv:
        if word[0:1] == '-':
            checkIfAcceptedFlag(word)
    dbuserkey                         = getParameterFromCommandLine(sys.argv, '-k', flag_log, dbuserkeys)
    out_sql                           = getParameterFromCommandLine(sys.argv, '-os', flag_log, out_sql)
    out_path                          = getParameterFromCommandLine(sys.argv, '-op', flag_log, out_path)
    execute_sql                       = getParameterFromCommandLine(sys.argv, '-es', flag_log, execute_sql)
    sleep_time                        = getParameterFromCommandLine(sys.argv, '-st', flag_log, sleep_time)
    table_schema                      = getParameterFromCommandLine(sys.argv, '-ts', flag_log, table_schema)
    table_name                        = getParameterFromCommandLine(sys.argv, '-tn', flag_log, table_name)
    view_schema                       = getParameterFromCommandLine(sys.argv, '-vs', flag_log, view_schema)
    view_name                         = getParameterFromCommandLine(sys.argv, '-vn', flag_log, view_name)
    view_path                         = getParameterFromCommandLine(sys.argv, '-vp', flag_log, view_path)
    number_views                      = getParameterFromCommandLine(sys.argv, '-nv', flag_log, number_views)
    start_view_number                 = getParameterFromCommandLine(sys.argv, '-sv', flag_log, start_view_number)
    export_flag                       = getParameterFromCommandLine(sys.argv, '-exp', flag_log, export_flag)

    ############ GET LOCAL HOST ##########
    local_host = run_command("hostname").replace('\n','') 
    local_host = local_host.replace(' ', '')  
    if not is_integer(local_host.split('.')[0]):    #first check that it is not an IP address
        local_host = local_host.split('.')[0]  #if full host name is specified in the local host (or virtual host), only the first part is used

    ############# LOG MANAGER #########
    log_path = out_path.replace(" ","_").replace(".","_")
    log_path = log_path.replace('%SID', SID)     
    if log_path and not os.path.exists(log_path):
        os.makedirs(log_path)
    out_prefix = "hanaexpimp"
    print_to_std = True
    logman = LogManager(log_path, out_prefix, print_to_std)

    ############ GET LOCAL INSTANCE and SID ##########
    [key_hosts, ENV, DATABASE] = get_key_info(dbuserkey, local_host, logman)
    local_host_index = key_hosts.index(local_host)
    key_sqlports = [env.split(':')[1] for env in ENV]        
    dbinstances = [port[1:3] for port in key_sqlports]
    if not all(x == dbinstances[0] for x in dbinstances):
        print("ERROR: The hosts provided with the user key, "+dbuserkey+", does not all have the same instance number")
        os._exit(1)
    local_dbinstance = dbinstances[local_host_index]

    ############ CHECK AND CONVERT INPUT PARAMETERS #################
    ### execute_sql, -es
    execute_sql = checkAndConvertBooleanFlag(execute_sql, "-es", logman)
    ### out_sql, -os
    out_sql = checkAndConvertBooleanFlag(out_sql, "-os", logman)
    ### sleep_time, -st
    if not is_integer(sleep_time):
        log("INPUT ERROR: -st must be an integer. Please see --help for more information.", logman)
        os._exit(1)
    ### table_name, -tn
    if table_name == "":
        log("INPUT ERROR: -tn must be provided with the name of the table to be imported back into. Please see --help for more information.", logman)
        os._exit(1)
    ### view_schema, -vs
    if view_schema == "":
        log("INPUT ERROR: -vs must be provided with the schema where the views to be exported are located. Please see --help for more information.", logman)
        os._exit(1)
    ### view_name, -vn
    if view_name == "":
        log("INPUT ERROR: -vn must be provided with the first part of the name of views to be exported. Please see --help for more information.", logman)
        os._exit(1)
    ### view_path, -vp
    if view_path == "": 
        log("INPUT ERROR: -vp must be provided with the full path to where the views will be exported. Please see --help for more information.", logman)
        os._exit(1)
    ### number_views, -nv
    if not is_integer(number_views):
        log("INPUT ERROR: -nv must be an integer. Please see --help for more information.", logman)
        os._exit(1)
    ### start_view_number, -sv
    if not is_integer(start_view_number):
        log("INPUT ERROR: -sv must be an integer. Please see --help for more information.", logman)
        os._exit(1)
    ### export_flag, -exp
    export_flag = checkAndConvertBooleanFlag(export_flag, "-exp", logman)

    ############# SQL MANAGER ##############
    hdbsql_string = "hdbsql "
    sqlman = SQLManager(execute_sql, hdbsql_string, dbuserkey, DATABASE, out_sql)

    ############ CHECK THAT USER CAN CONNECT TO HANA ###############  
    sql = "SELECT * from DUMMY" 
    errorlog = "USER ERROR: The user represented by the key "+dbuserkey+" cannot connect to the system. Make sure this user is properly saved in hdbuserstore."
    [dummy_out, succeeded] = try_execute_sql(sql, errorlog, sqlman, logman, True, True) # always check key
    dummy_out = dummy_out.strip("\n").strip("|").strip(" ") 
    if dummy_out != 'X' or not succeeded:
        log("USER ERROR: The user represented by the key "+dbuserkey+" cannot connect to the system. Make sure this user is properly saved in hdbuserstore", logman, True)
        os._exit(1)

    ################ START #################
    if export_flag:
        tot_nbr_exported_rows = 0
        for view_number in range(int(start_view_number), int(number_views)+1):
            tot_nbr_exported_rows += export_view(view_number, view_schema, view_name, view_path, number_views, sleep_time, sqlman, logman)
    else:
        for view_number in range(int(start_view_number), int(number_views)+1):
            import_view(view_number, view_name, view_path, table_schema, table_name, number_views, sleep_time, sqlman, logman)    

if __name__ == '__main__':
    main()
                
