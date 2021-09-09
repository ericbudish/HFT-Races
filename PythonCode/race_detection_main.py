'''
race_detection_main.py

Main Python script to run Race Detection.
This includes: 
    (1) detecting races
    (2) producing race-level statistics.

Reference: 
Code and Data Appendix for “Quantifying the High-Frequency Trading ‘Arms Race’“
by Matteo Aquilina, Eric Budish and Peter O’Neill.
Please follow Section 7 of the Code and Data Appendix to run this script.

Environment: MacOS / Linux;
             Our code does not support the Windows operating system. 
             We recommend Windows 10 users set up Windows Subsystem for Linux and 
             run the code in the virtual Linux system.

Dependency: Python (>=3.6), Pandas (>=1.1.0), Numpy (>=1.18.0)

Instructions:
    0. Pre-process the data and run message_data_processing_main.py: 
        Please follow Section 3 of the Code and Data Appendix for data pre-processing, 
        Section 5 of the Code and Data Appendix for computing environment setup, 
        and Section 6 of the Code and Data Appendix to run message_data_processing_main.py.
    1. Set Parameters: 
        1.1 Set Paths: Create the following directories and set the paths
            accordingly in the "Set path" section in this script below. 
            (1) file_ticktables = 'path/to/Ticktables.pkl' and 
                file_symdates = 'path/to/All_SymDates.csv.gz': 
                Paths to the reference data files.
                Please refer to Section 3.4 of the Code and Data Appendix 
                for the preparation of the reference data files, 
                and Section 5.2 of the Code and Data Appendix
                for the file structure setup.
                This MUST be the same file as in process_msg_data_main.py.
            (2) path_logs = 'path/to/Logs/': 
                Directory for log files. Everytime the user runs the script, the 
                program automatically creates a subfolder inside /Logs for
                the log files generated in that run. Log files can be 
                used for debugging and progress monitoring. 
                WARNING: Do NOT remove files from this directory before
                the analysis is finished.
                It is recommended to use the same folder for log files as in
                process_msg_data_main.py.
            (3) path_temp = 'path/to/Temp/': 
                Directory for intermediate files. This includes the event
                classification and order book reconstruction results. 
                This MUST be the same folder as in process_msg_data_main.py.
                WARNING: Do NOT remove files from this directory before
                the analysis is finished.
            (4) path_output = 'path/to/Output':
                Directory for the output files (race-level stats).
                The program will aggregate the race-level
                statistics and output the dataset to this directory.
                Please refer to Section 8 of the Code and Data Appendix 
                for a complete list of output statistics.
                It is recommended to use the same folder for output files as in
                process_msg_data_main.py.
        1.2 Set Technical Parameters: Set the following technical parameters
            in the section "Specify Technical Parameters" in this script below.
            Please refer to Section 7.1 of the Code and Data Appendix.
            (1) num_workers: The number of cores to use for multi-processing.
            (2) max_dec_scale: The max decimal scale of price-related variables.
                This parameter is used to avoid floating point errors.
                This parameter MUST have the same value as in process_msg_data_main.py.
        1.3 Set Race Detection Parameters: Construct a .csv file with the race
            detection parameters for each race run to explore. Each column of the 
            .csv file should represent a race parameter in Table 2 of the Code and Data Appendix 
            with the variable names as the column headers. Each row should represent 
            a race run to be explored with the parameters for the run specified in each column.
            Please refer to Section 4 of the Code and Data Appendix.
    2. Execute the Code: Please follow Section 7.2 of the Code and Data Appendix.
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

from LatencyArbitrageAnalysis.Race_Detection_and_Statistics import detect_races_and_race_statistics

from LatencyArbitrageAnalysis.utils.Logger import getLogger
from LatencyArbitrageAnalysis.utils.Monitor_Logs import MonitorLogs
from LatencyArbitrageAnalysis.utils.Collect_Statistics import CollectStats

###################################################################################
################################## SET PARAMETERS #################################
###################################################################################
# This section allows users to specify paths, # cores, max decimal scale of 
# the price-related variables. 
###################################################################################
###### Set Paths
### File Path to Ticktables.pkl
# This MUST be the same file in message_data_processing_main.py. 
# tick tables - dict of {(date, sym): ticktable}
file_ticktables = '/path/to/Ticktables.pkl'
### File Path to All_SymDates.csv.gz
# This MUST be the same file in message_data_processing_main.py. 
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
# This MUST be the same path_temp in message_data_processing_main.py. 
# This is because race_detection_main.py makes use of the intermediate
# output files generated by message_data_processing_main.py. 
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
# This MUST have the same value in message_data_processing_main.py.
max_dec_scale = 5
###################################################################################
###### Set Race Detection Parameters
### File Path to Race Detection Parameter .csv file
file_race_params = '/path/to/Input_Race_Parameters.csv'

###################################################################################
###################################################################################
# Warning: Please do not modify the script after this line unless 
#          you fully understand the code and know what you are doing 
###################################################################################
###################################################################################

###################################################################################
################################### MAIN PROGRAM ##################################
###################################################################################
# Define the main function
def main(runtime, date, sym, args, paths):
    # Initialize logger
    logpath = '%s/%s/' %(paths['path_logs'], 'RaceDetection_'+runtime)
    logfile = 'Process_Msg_Data_Main_Log_%s_%s_%s.log' % (runtime, date, sym)
    logger = getLogger(logpath, logfile, __name__)
    timer_st, pid = datetime.datetime.now(), os.getpid()
    logger.info('Processing: %s %s' % (date, sym))
    logger.info('Timer Start: %s' % (str(timer_st)))
    logger.info('Process ID: %s' % (pid))

    # Race Detection
    logger.info('Launching race detection and statistics...')
    try:
        detect_races_and_race_statistics(runtime, date, sym, args, paths)
    except (SystemExit, KeyboardInterrupt):
        logger.error('Failed. SystemExit or KeyboardInterrupt')
        return None
    except Exception as e:
        logger.error('Failed. Error: %s' % e)
        logger.error(traceback.format_exc())
        return None

    timer_end = datetime.datetime.now()
    logger.info('Complete.')
    logger.info('Timer End: %s' % str(timer_end))
    logger.info('Time Elapsed: %s' % str(timer_end - timer_st))

    ans = [date, sym, pid, timer_st, timer_end]
    return (ans)

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
    # Read in Race Detection Parameters
    race_params = pd.read_csv(file_race_params)
    num_remaining_spec = race_params.shape[0]
    race_params = race_params.to_dict('row')
    # Loop through each race run
    for race_param in race_params:
        num_remaining_spec = num_remaining_spec - 1
        # Get runtime information
        now = datetime.datetime.now()
        runtime = '%02d%02d%02d_%02d%02d' % (now.year, now.month, now.day, now.hour, now.minute)
        print('The runtime is: ' + runtime)
        # Set up the log folder
        logpath = '%s/%s/' %(path_logs, 'RaceDetection_'+runtime)
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
        ## Dict of data type
        dtypes_msgs = {
            'ClientOrderID':'O', 'UniqueOrderID':'O', 'TradeMatchID': 'O', 
            'UserID':'O', 'FirmID':'O', 'SessionID':'float64',
            'MessageTimestamp':'O', 'MessageType':'O', 'OrderType':'O',
            'ExecType':'O', 'OrderStatus':'O', 'TradeInitiator':'O', 
            'TIF':'O', 'CancelRejectReason': 'O',
            'Side':'O', 'OrderQty':'float64', 'DisplayQty':'float64', 
            'LimitPrice':'float64', 'StopPrice':'float64',
            'ExecutedPrice': 'float64', 'ExecutedQty': 'float64', 'LeavesQty': 'float64',
            'QuoteRelated':'bool',
            'BidPrice':'float64', 'BidSize':'float64', 
            'AskPrice':'float64', 'AskSize':'float64',
            'RegularHour':'bool','OpenAuctionTrade':'bool', 'AuctionTrade':'bool',
            'UnifiedMessageType': 'O',
            'PrevPriceLvl': 'float64', 'PrevQty': 'float64', 'PriceLvl': 'float64', 
            'Categorized': 'bool', 'EventNum': 'float64', 'Event': 'O', 
            'MinExecPriceLvl':'float64', 'MaxExecPriceLvl':'float64', 
            'PrevBidPriceLvl': 'float64', 'PrevBidQty': 'float64', 'BidPriceLvl': 'float64', 
            'BidCategorized': 'bool', 'BidEventNum': 'float64', 'BidEvent': 'O', 
            'BidMinExecPriceLvl':'float64', 'BidMaxExecPriceLvl':'float64', 
            'PrevAskPriceLvl': 'float64', 'PrevAskQty': 'float64', 'AskPriceLvl': 'float64', 
            'AskCategorized': 'bool', 'AskEventNum': 'float64', 'AskEvent': 'O',
            'AskMinExecPriceLvl':'float64', 'AskMaxExecPriceLvl':'float64'}

        dtypes_top = {
            'MessageTimestamp': 'O', 'Side': 'O','UnifiedMessageType': 'O',
            'RegularHour':'bool','OpenAuctionTrade':'bool','AuctionTrade':'bool',
            'BestBid': 'float64','BestBidQty': 'float64', 'BestAsk': 'float64','BestAskQty': 'float64', 
            'Spread': 'float64','MidPt': 'float64', 
            'last_BestBid': 'float64', 'last_BestAsk': 'float64','last_MidPt': 'float64', 
            't_last_chg_BestBid': 'O', 't_last_chg_BestAsk': 'O','t_last_chg_MidPt': 'O',
            'Corrections_OrderAccept': 'float64','Corrections_Trade': 'float64',  
            'Corrections_notA': 'float64', 
            'Corrections_OrderAccept_h': 'float64','Corrections_Trade_h': 'float64',  
            'Corrections_notA_h': 'float64', 
            'DepthKilled': 'float64', 'DepthKilled_h': 'float64', 
            'BestBid_TickSize': 'float64', 'BestAsk_TickSize': 'float64','Diff_TickSize': 'O',
            'Trade_Pos': 'O', 'BookUpdateParentMsgID': 'float64'}
            
        paths = {
            'path_logs': path_logs,
            'path_temp': path_temp,
            'path_output': path_output}

        ## Write args and paths to a txt file for retaining parameters of runs
        technical_arg_log = '%s/%s/' %(path_logs, 'TechnicalSpecifications')
        if not os.path.exists(technical_arg_log):
            os.makedirs(technical_arg_log)
        with open(technical_arg_log+'%s.txt' % runtime, 'w') as f:
            print('Running: Race Detection', file=f)
            print('Arguments specified for runtime %s:' % runtime, file=f)
            pprint.pprint({
            'max_dec_scale': max_dec_scale,
            'race_param':race_param}, f)
            print('Paths specified for runtime %s:' % runtime, file=f)
            pprint.pprint(paths, f)
        ###################################################################################
        # Organize args
        # Create a list of args to be passed into the multi-processing pool. Each item is 
        # the args for one symbol-date. 
        # The reason for this is that ticktables may vary across symbol-dates.
        args_list = [(runtime, date, sym, 
            {
            'dtypes_msgs': dtypes_msgs,
            'dtypes_top': dtypes_top,
            'price_factor': price_factor,
            'race_param': race_param,
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
        time_st = datetime.datetime.now()
        print('#######################################################')
        print('Start Race Detection: %s' % str(time_st))
        print('runtime: %s' % runtime)
        print('Number of remaining specifications: %s' % num_remaining_spec)
        print('Race Parameters:')
        pprint.pprint(race_param)
        pool = multiprocessing.Pool(num_workers)
        results = pool.map(multi_process_wrapper, args_list)
        print('Finished Detecting Races: %s' % str(datetime.datetime.now() - time_st))
        ###################################################################################
        # Monitor logs to check if all sym-dates are finished.
        # Logs will be printed out in the function call.
        logs = MonitorLogs(runtime, pairs, paths)
        ###################################################################################
        # Combine stats files
        print('#################################')
        print('Start Collecting Statistics')
        # Race Level Statistics
        if logs[(logs['Status_Race_Detection_Statistics'] == 'Done')].shape[0] == len(pairs):
            # Generate a csv file to record the race parameters of a run
            pd.DataFrame.from_dict(race_param, orient = 'index' ,columns=['Value']).to_csv('%s/Parameters_%s.csv' % (paths['path_output'], runtime))
            # Collect stats
            collect = 'Race_Stats'
            CollectStats(runtime, paths, pairs, collect)
        else: 
            print('Failed to collect race level statistics: some symbol-dates are not finished in program run %s' % runtime)            

