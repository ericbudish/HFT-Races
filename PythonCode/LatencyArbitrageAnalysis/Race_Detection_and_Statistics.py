'''
Race_Detection_and_Statistics.py

This module detects races and calculates race level statistics based on 
the message data, matching engine events, and order book information
constructed in the previous steps. This module calls modules in ./RaceDetection

References: Section 10.4 and 10.5 of the Code and Data Appendix.

Input: 
    1. Message data with economic events assigned (output of event classification).
    2. Top-of-book data (output of order book reconstruction)
    3. Depth-of-book data (output of order book reconstruction)

Output:
    1. Race records: Dataset with message indices for each race
    2. Race stats: Race level dataset with key race statistics
        e.g. profits per share, race profits, race duration...

'''
# 
################################################################################################################################

####################################################################

### IMPORT AND PREPARATION ###
import pandas as pd
import datetime
import pickle
import os
import time

from .RaceDetection import Prep_Race_Data as PrepData
from .RaceDetection import Race_Detection_Functions as RaceDetection
from .RaceDetection import Race_Statistics_Functions as RaceStats
from .utils.Logger import getLogger

def detect_races_and_race_statistics(runtime, date, sym, args, paths):
    '''
    race detection and statistics main function operating on a symbol-date

    Params:
        runtime: str, for log purpose.
        date:    str, symbol-date identifier.
        sym:     str, symbol-date identifier.
        args:    dictionary of arguments, including:
                    - dtypes_msgs: dtype dict for message data with event classification.
                    - dtypes_top: dtype dict for top-of-book data.
                    - price_factor: int. convert price from monetary unit to large integer,
                                         obtained by int(10 ** (max_dec_scale+1)).
                    - ticktable: dataframe, columns including min_p, max_p, and ticksize.
                    - race_params: dict of race parameters, see Section 4 of the Code and
                                   Data Appendix for instructions on selecting the race parameters.
        paths:   dictionary of file paths, including:
                     - path_temp: path to the temp data file.
                     - path_logs: path to log files.

    Output:
        race_recs:  record-of-race dataframe with basic information of all races detected.
        race_stats: dataframe with race-level stats. See Section 8 of the Code and Data Appendix
                    for a complete list of output statistics.

    Steps:
        0. Load data
        1. Prepare Data, call prepare_data() in Prep_Race_Data.py
            1.1 Identify messages that could potentially be included in a race and flag them as race
                relevant and as cancel or take attempts.
                New orders and cancel-and-replace (c/r) moving towards the bbo are attempts to take.
                Cancels and c/r moving away from the bbo are attempts to cancel.
            1.2 Unify price and quantity info for race relevant messages. Generate a single price field
                with race relevant prices for flagged messages (prev prices for cancel attempts and
                current prices for take attempts).
            1.3 Add fields with information on processing time (time from inbound to 1st outbound),
                prices measured in ticks and signed prices.
                Signed price fields are used to compare ask and bid prices with the same logic operators.
                e.g. by having negative bids the highest bid is now the smallest signed price.
        2. Find races, call find_single_lvl_races() in Race_Detection_Functions.py
            2.1 Consider cancels at the BBO or takes at the BBO or better as possible race starting messages
            2.2 For each race-starting message and price level get all messages 
                within the race horizon of the starting message.
            2.3 Check that these messages satisfy the baseline race criterion:
                - At least 2 unique users
                - At least one attempt to take
                - At least one successful message
                - At least one failed message
            2.4 If a race is detected, add a row in race records for each singlelvlrace identified as above 
                with all indices for the race msgs and race information (e.g. race horizon and timestamp).
        3. Generate race statistics, call generate_race_stats() in Race_Statistics_Functions.py
            For each race, calculate race stats.
    '''
    ### INITIALIZE ###
    logpath = '%s/%s/' %(paths['path_logs'], 'RaceDetection_'+runtime)
    logfile = 'Step_4_Detect_Races_%s_%s_%s.log' % (runtime, date, sym)
    if not os.path.exists(logpath):
        os.makedirs(logpath)
    logger = getLogger(logpath, logfile, __name__)

    logger.info('Processing: %s %s' % (date, sym))

    ### GET PARAMETERS ###
    dtypes_msgs = args['dtypes_msgs']
    dtypes_top = args['dtypes_top']
    price_factor = args['price_factor']
    ticktable = args['ticktable']
    race_param = args['race_param']

    infile_msgs = '%s/ClassifiedMsgData/%s/Classified_Msgs_Data_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym)
    infile_top = '%s/BBOData/%s/BBO_Data_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym)
    infile_depth = '%s/DepthData/%s/Depth_%s_%s.pkl' % (paths['path_temp'], date, date, sym)
    outfile_race_recs = '%s/RaceRecords/%s/Race_Records_%s_%s.pkl' % (paths['path_temp'], date, date, sym)
    outfile_stats = '%s/RaceStats/%s/Race_Stats_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym)

    # Adjust ticktable
    ticktable['p_int64'] = (ticktable['p'] * price_factor).astype('Int64') 
    ticktable['tick_int64'] = (ticktable['tick'] * price_factor).astype('Int64')

    ### Load data ###
    timer_st = datetime.datetime.now()
    logger.info('Timer Start: %s' % str(timer_st))
    logger.info('Loading data...')
                
    msgs = pd.read_csv(infile_msgs, dtype = dtypes_msgs, parse_dates=['MessageTimestamp'])
    top = pd.read_csv(infile_top, index_col = 0, dtype = dtypes_top, parse_dates = ['MessageTimestamp'])
    depth = pickle.load(open(infile_depth, 'rb'))

    # Keep regular hours
    reg_hours = msgs.RegularHour
    msgs, top = msgs.loc[reg_hours], top.loc[reg_hours]
    
    ### Start Race Detection and Statistics Calculation ###
    # Step 1: prepare_data()
    logger.info('Running prepare_data()...')
    st_temp = time.time()
    msgs_prepared, top_prepared = PrepData.prepare_data(msgs, top)
    logger.info('Finish prepare_data(), time used: ' + str(time.time()-st_temp))

    # Step 2: Race Detection find_single_lvl_races()
    # Single Level Race Detection
    logger.info('Running find_single_lvl_races()...')
    st_temp = time.time()
    race_recs = RaceDetection.find_single_lvl_races(msgs_prepared, top_prepared, ticktable, race_param)
    logger.info('Finish find_single_lvl_races(), time used: ' + str(time.time()-st_temp))
    # If we detect some races
    if race_recs.shape[0] > 0:
        # Save to file
        pickle.dump(race_recs, open(outfile_race_recs, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
        # Step 3: Race Statistics generate_race_stats()
        logger.info('Running generate_race_stats()...')
        st_temp = time.time()
        race_stats = RaceStats.generate_race_stats(date, sym, msgs_prepared, top_prepared, depth, race_recs, ticktable, price_factor, \
                                                      race_param)
        race_stats.to_csv(outfile_stats, index=False, compression='gzip')
        logger.info('Finish generate_race_stats(), time used: ' + str(time.time()-st_temp))
    # Else, If there is no race
    else:
        logger.info('No race detected in symbol-date %s %s, skipping race stats.' % (date, sym))
        
    # End timer
    timer_end = datetime.datetime.now()

    # Add info to log
    logger.info('Complete.')
    logger.info('Timer End: %s' % str(timer_end))
    logger.info('Time Elapsed: %s' % str(timer_end - timer_st))


