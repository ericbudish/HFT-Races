'''
process_msg_data_main.py

Main Python script to run Message Data Processing.
This includes: 
    (1) event classification,
    (2) order book reconstruction,
    (3) trading and order book stats, including a trade level dataset 
        and a symbol-date level dataset.

Reference: 
Code and Data Appendix for “Quantifying the High-Frequency Trading ‘Arms Race’“
by Matteo Aquilina, Eric Budish and Peter O’Neill.
Please follow Section 6 of the Code and Data Appendix to run this script.

Environment: MacOS / Linux; 
             Our code does not support the Windows operating system. 
             We recommend Windows 10 users set up Windows Subsystem for Linux and 
             run the code in the virtual Linux system.

Dependency: Python (>=3.6), Pandas (>=1.1.0), Numpy (>=1.18.0)

Instructions:
    0. Data pre-processing and computing environment setup: Please follow Section 3 
        of the Code and Data Appendix for message data pre-processing and Section 5 
        of the Code and Data Appendix for computing environment setup.
    1. Set Parameters (also see Section 6.1 of the Code and Data Appendix): 
        1.1 Set Paths: Create the following directories and set the paths
            accordingly in the "Set Paths" section in this script below. 
            (1) path_msg_data = 'path/to/RawData': 
                Directory for pre-processed message data.
                This directory should contain sub-folders named by dates (YYYY-MM-DD).
                Each sub-folder contains the pre-processed message data of
                all symbols in that day. Specifically, put the pre-processed 
                message data files in /RawData/{date}/ with each file named  
                as "Raw_Msg_Data_{date}_{sym}.csv.gz". 
                Please refer to Section 5.2 of the Code and Data Appendix
                for the file structure setup.
            (2) file_ticktables = 'path/to/Ticktables.pkl' and 
                file_symdates = 'path/to/All_SymDates.csv.gz': 
                Paths to the reference data files.
                Please refer to Section 3.4 of the Code and Data Appendix 
                for the preparation of the reference data files, 
                and Section 5.2 of the Code and Data Appendix
                for the file structure setup.
            (3) path_logs = 'path/to/Logs/': 
                Directory for log files. Everytime the user runs the script, the 
                program automatically creates a subfolder inside /Logs for
                the log files generated in that run. Log files can be 
                used for debugging and progress monitoring. 
                WARNING: Do NOT remove files from this directory before
                the analysis is finished.
            (4) path_temp = 'path/to/Temp/': 
                Directory for intermediate files. This includes the event
                classification and order book reconstruction results. 
                WARNING: Do NOT remove files from this directory before
                the analysis is finished.
            (5) path_output = 'path/to/Output':
                Directory for the output files (trade and symdate stats). 
                The program will output the trade-level and the 
                symdate-level datasets to this directory.
                Please refer to Section 8 of the Code and Data Appendix 
                for a complete list of output statistics.
        1.2 Set Technical Parameters: Set the following technical parameters
            in the section "Specify Technical Parameters" in this script below.
            Please refer to Section 6.1 of the Code and Data Appendix.
            (1) num_workers: The number of cores to use for multi-processing.
            (2) max_dec_scale: The max decimal scale of price-related variables.
                This parameter is used to avoid floating point errors. 
    2. Execute the Code: Please follow Section 6.2 of the Code and Data Appendix.
        We recommend users to run the code on a small sample before running on a full sample.
        The output files of the Python program will be saved to path_output.

'''
###################################################################################
################################## IMPORT MODULES #################################
###################################################################################
import multiprocessing
import datetime
import pprint
import traceback
import os
import pandas as pd
import random

from LatencyArbitrageAnalysis.Classify_Messages import classify_messages
from LatencyArbitrageAnalysis.Prep_Order_Book import prepare_order_book
from LatencyArbitrageAnalysis.Trading_and_Order_Book_Stats import calculate_symdate_stats

from LatencyArbitrageAnalysis.utils.Logger import getLogger
from LatencyArbitrageAnalysis.utils.Monitor_Logs import MonitorLogs
from LatencyArbitrageAnalysis.utils.Collect_Statistics import collect_stats
from LatencyArbitrageAnalysis.utils.Dtypes import dtypes_raw_msgs, dtypes_msgs, dtypes_top
###################################################################################
################################## SET PARAMETERS #################################
###################################################################################
# This section allows users to specify paths, # cores, max decimal scale of 
# the price-related variables. 
###################################################################################
###### Set Paths 
### Path to the pre-processed message data directory
path_msg_data = '/path/to/pre-processed/RawData/'
### File Path to Ticktables.pkl
# tick tables - dict of {(date, sym): ticktable}
file_ticktables = '/path/to/Ticktables.pkl'
### File Path to All_SymDates.csv.gz
# A .csv file containing a list of all symbol-dates in the data.
# This file should have two columns, 
# 'Symbol' for the symbol, and 
# 'Date' for the date.
# E.g., 
# |    Date    |Symbol|
# ---------------------
# | 2000-01-01 | ABCD |
# | 2000-01-02 | ABCD |
# | 2000-01-01 | EFGH |
# | 2000-01-02 | EFGH |
file_symdates = '/path/to/All_SymDates.csv.gz'
### Path to log file directory 
path_logs = '/path/to/Logs/'
### Path to intermediate file directory
# Note: Files in the path_temp folder are used in the subsequent process.
# Do NOT remove files from the directory before the analysis is finished.
path_temp = '/path/to/Temp/'
### Path to the output file directory
path_output = '/path/to/Output/'
###################################################################################
###### Specify Technical Parameters
###### Multi-Processing Cores to Use
num_workers = 1
###### Max Decimal Scale of the Price-Related Variables
# For example, if the prices are quoted as 12.34560 with at most 5 digits
# after the decimal point, then max_dec_scale = 5
# This should be the scale of the smallest tick size in the data. 
max_dec_scale = 5

###################################################################################
###################################################################################
# Warning: Please do not modify the script after this line unless 
#          you fully understand the code and know what you are doing 
###################################################################################
###################################################################################

###################################################################################
################################### MAIN PROGRAM ##################################
###################################################################################
# Define the main function for Message Data Processing
def main(runtime, date, sym, args, paths):
    # Initialize logger
    logpath = '%s/%s/' %(paths['path_logs'], 'MessageDataProcessing_'+runtime)
    logfile = 'Process_Msg_Data_Main_Log_%s_%s_%s.log' % (runtime, date, sym)
    logger = getLogger(logpath, logfile, __name__)
    timer_st, pid = datetime.datetime.now(), os.getpid()
    logger.info('Processing: %s %s' % (date, sym))
    logger.info('Timer Start: %s' % (str(timer_st)))
    logger.info('Process ID: %s' % (pid))

    # Event Classification
    logger.info('Launching message classification...')
    run_process(logger, classify_messages, runtime, date, sym, args, paths)

    # Order Book Preparation
    logger.info('Launching order book preparation...')
    run_process(logger, prepare_order_book, runtime, date, sym, args, paths)

    # Symbol-Date and Trade Level Statistics
    logger.info('Launching symbol date and trade level stats')
    run_process(logger, calculate_symdate_stats, runtime, date, sym, args, paths)

    timer_end = datetime.datetime.now()
    logger.info('Complete.')
    logger.info('Timer End: %s' % str(timer_end))
    logger.info('Time Elapsed: %s' % str(timer_end - timer_st))

    ans = [date, sym, pid, timer_st, timer_end]
    return (ans)

# Define the process runner
def run_process(logger, process, runtime, date, sym, args, paths):
    try:
        process(runtime, date, sym, args, paths)
    except (SystemExit, KeyboardInterrupt):
        logger.error('Failed. SystemExit or KeyboardInterrupt')
        return None
    except Exception as e:
        logger.error('Failed. Error: %s' % e)
        logger.error(traceback.format_exc())
        return None

# Define the multiprocessing wrapper
def multi_process_wrapper(args):
    return main(*args)

# Define a helper function for file structure
def create_folder_structure(pairs, temp_path_list):
    for path in temp_path_list:
        if not os.path.exists(path):
            os.makedirs(path)
        dates = set([date for date, sym in pairs])
        for date in dates:
            if not os.path.exists('%s/%s/' % (path, date)):
                os.makedirs('%s/%s/' % (path, date))

###################################################################################
#################################### EXECUTION ####################################
###################################################################################

if __name__ == '__main__':
    # Get runtime information
    now = datetime.datetime.now()
    runtime = '%02d%02d%02d_%02d%02d%02d' % (now.year, now.month, now.day, now.hour, now.minute, now.second)
    print('The runtime is: ' + runtime)
    # Set up the log folder
    logpath = '%s/%s/' %(path_logs, 'MessageDataProcessing_'+runtime)
    if not os.path.exists(logpath):
        os.makedirs(logpath)
    else:
        print('Warning: runtime %s already exists. Log files will be overwritten!' % runtime)

    ###################################################################################
    # Organize input parameters
    ###### Read in Reference Data
    ## Read in the symbol-date pairs
    pairs = pd.read_csv(file_symdates, dtype={'Date':'O','Symbol':'O'})[['Date','Symbol']].dropna().to_records(index=False).tolist()
    # Shuffle the pairs for parallel processing
    random.shuffle(pairs)
    ## Read in the tick table reference data
    ticktables = pd.read_pickle(file_ticktables)
    ###### Construct Input
    ## price unit to convert price-related variables into integers by multiplication.
    price_factor = int(10 ** (max_dec_scale+1))
    ## Construct paths
    paths = {
        'path_logs': path_logs,
        'path_data': path_msg_data,
        'path_temp': path_temp,
        'path_output': path_output}
    ## Write args and paths to a txt file for retaining parameters of runs
    technical_arg_log = '%s/%s/' %(path_logs, 'TechnicalSpecifications')
    if not os.path.exists(technical_arg_log):
        os.makedirs(technical_arg_log)
    with open(technical_arg_log+'%s.txt' % runtime, 'w') as f:
        print('Running: Message Data Processing', file=f)
        print('Arguments specified for runtime %s:' % runtime, file=f)
        pprint.pprint({
        'max_dec_scale': max_dec_scale}, f)
        print('Paths specified for runtime %s:' % runtime, file=f)
        pprint.pprint(paths, f)

    ###################################################################################
    # Organize args
    # Create a list of args to be passed into the multi-processing pool. Each item is 
    # the args for one symbol-date. 
    # The rationale for creating this list is that ticktables may vary across symbol-dates.
    args_list = [(runtime, date, sym, 
        {
        'dtypes_raw_msgs': dtypes_raw_msgs,
        'dtypes_msgs': dtypes_msgs,
        'dtypes_top': dtypes_top,
        'price_factor': price_factor,
        'ticktable': ticktables[(date,sym)],
        },
        paths) for date, sym in pairs]

    ###################################################################################
    # Create folder structure within the temp dir if it doesn't exist
    temp_path_list = ['%s/ClassifiedMsgData/' % paths['path_temp'], '%s/BBOData/' % paths['path_temp'],
                    '%s/DepthData/' % paths['path_temp'], '%s/RaceRecords/' % paths['path_temp'],
                    '%s/RaceStats/' % paths['path_temp'], '%s/SymDateStats/' % paths['path_temp'], 
                    '%s/TradeStats/' % paths['path_temp']]
    create_folder_structure(pairs, temp_path_list)

    ###################################################################################
    # Run the main program
    # start multi-processing
    print('#######################################################')
    time_st = datetime.datetime.now()
    print('Start Processing Message Data: %s' % str(time_st))
    print('runtime: %s' % runtime)
    pool = multiprocessing.Pool(num_workers)
    results = pool.map(multi_process_wrapper, args_list)
    print('Finished Processing Message Data: %s' % str(datetime.datetime.now() - time_st))
    ###################################################################################
    # Monitor logs to check if all sym-dates are finished.
    # Logs will be printed out in the function call.
    logs = MonitorLogs(runtime, pairs, paths)
    ###################################################################################
    # Combine stats files
    print('#################################')
    time_st = datetime.datetime.now()
    print('Start Collecting Trade and Symdate Statistics: %s' % str(time_st))
    if logs[(logs['Status_Trading_and_Order_Book_Stats'] == 'Done')].shape[0] == len(pairs):
        # Trade Level Statistics
        collect = 'Trade_Stats'
        collect_stats(runtime, paths, pairs, collect)
        # Symbol-date Level Statistics
        collect = 'SymDate_Stats'
        collect_stats(runtime, paths, pairs, collect)
        print('Runtime %s Finished Collecting Trade and Symdate Statistics: %s' % (runtime, str(datetime.datetime.now() - time_st)))
    else: 
        print('Runtime %s Did not complete Trade and SymDate Stats collection, use MonitorLogs to check for reasons' % (runtime))

    
