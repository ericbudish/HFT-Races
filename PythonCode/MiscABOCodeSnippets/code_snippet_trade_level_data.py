'''
code_snippet_trade_level_data.py

This snippet is the code we used to generate the trade level data with race information. It can be used to 
produce the statistics for 
    1. Figure 4.3 Panel B % Liquidity provided in races
    2. Table 4.4 Taker provider matrix
    3. Table 4.11 Realized spread 
    4. Table 4.11 Number of races where a firm group provides liquidity (this is for 
       Cancel Attempt Rate Columns. Please also refer to code_snippet_cancel_activities.py)
       
The code is specific to the LSE settings and may not be applicable to other exchange message 
data directly. Note that the structure of the code makes it difficult to apply race filters 
when marking whether or not a trade is in race. If users want to apply a race filter, they
should first detect races for the baseline, and then apply the race filter on the race records
dataset before applying this code. Users have to do this themselves.

We provide this code for the purpose of disclosing how we did the analysis and helping our user 
reproduce the related results. Note that the coding convention (e.g., naming) in this script 
is slightly different from the code in /PythonCode, because this is part of an earlier version of 
our code.

To reproduce the results, users need to
    1. Go through the code snippet carefully and understand the logic.
    2. Adapt/rewrite the code to their context depending on the specific details of their setting.
       Users can make use of this code snippet in this step.
    3. Execute the code to obtain the statistics.
    4. Produce the tables. Users need to do this by themselves.
        1. To calculate % Liquidity provided in races by firm group, users need to
           use the Race_Trade indicator and the FirmID associated with the passive side of each trade
           (i.e., FirmID_CP). Please refer to the footnote of Figure 4.3.
        2. To produce the Taker provider matrix, users need to use FirmID, FirmID_CP and Race_Trade.
           Please refer to the footnote of Table 4.4 for detail.
        3. To produce realized spread in race by firm groups, users need to use FirmID, price impact 
           and effective spread statistics in the output dataset (to calculate realized spread).  
           Please refer to the footnote of Table 4.11 for detail.
        4. To produce the number of races where a firm group provides liquidity, users need to use
           FirmID_CP, SingleLvlRaceID. Specifically, to calculate how many races Firm X provide liquidity
           in, users need to count the distinct SingleLvlRaceID's where Firm X is the liquidity provider
           (identified by FirmID_CP) in at least one race trade.
           
'''
###################################################################################
####################### COMPUTE TRADE STATS WITH RACE INFO ########################
###################################################################################

# Import packages
from collections import OrderedDict
import pandas as pd
import numpy as np
import datetime
import pickle
import os

from .utils.Logger import getLogger

def calculate_trade_stats(runtime, date, sym, args, paths):
    '''
    This function calculates Trade_Stats, adding an indicator (Race_Trade) to tell if a trade is in a race.
    Note that this indicator should not be used with filters.

    Params:
        runtime: int, for log purpose
        date, sym: symbol-date
        args: dictionary of arguments, including 
            price_factor: int, big integer to convert the standard price unit into large integers
                          to avoid floating point error problem. For LSE (in GBX), we used 1e8 
            dtypes_msgs, dtype_top: dtypes dict for msg and bbo datasets.
        paths: dictionary of file paths, including
            path_temp: path to temp folder
            path_logs: path to log files
    '''
    # Initialize log
    logpath = '%s/%s/' %(paths['path_logs'], runtime)
    logfile = 'Step_4_SymDate_Stats_%s_%s_%s.log' % (runtime, date, sym)
    if not os.path.exists(logpath):
        os.makedirs(logpath)
    logger = getLogger(logpath, logfile, __name__)
    logger.info('Processing: %s %s' % (date, sym))
    timer_st = datetime.datetime.now()
    logger.info('Timer Start: %s' % str(timer_st))

    # Get parameters
    price_factor = args['price_factor'] 
    dtypes_msgs = args['dtypes_msgs']
    dtypes_top = args['dtypes_top']
    infile_msgs = '%s/ClassifiedMsgData/%s/Classified_Msgs_Data_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym)
    infile_top = '%s/BBOData/%s/BBO_Data_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym)
    infile_race_recs = '%s/RaceRecords/%s/Race_Records_%s_%s.pkl' % (paths['path_temp'], date, date, sym)
    outfile_trade_stats = '%s/TradeStats/%s/Trade_Stats_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym)

    # LOAD DATA
    logger.info('Loading data...')

    # Load data
    msgs = pd.read_csv(infile_msgs, dtype = dtypes_msgs, skipinitialspace=True, parse_dates=['MessageTimestamp'])
    top = pd.read_csv(infile_top, index_col = 0, dtype = dtypes_top, parse_dates = ['MessageTimestamp'])
    if os.path.exists(infile_race_recs): # if there is no race in the sym-date then there is no race record file
        race_recs = pd.read_pickle(infile_race_recs)
        exist_races = True
    else: 
        exist_races = False

    ### Keep regular hour msgs
    reg_hours = msgs['RegularHour']
    msgs, top = msgs.loc[reg_hours], top.loc[reg_hours]
    redux_top = top[['MessageTimestamp', 'MidPt']]

    ##################
    ### PREP DATA ###
    #################

    ## Calculate volatility
    logger.info('Performing calculations...')

    # Spread calculated in ticks and basis points
    top['Spread_Tx'] = top['Spread'] // top['MidPt_TickSize']
    top['Spread_bps'] = 10000. * top['Spread'] // top['MidPt']

    # Copy relevant columns from the book dataset to the main dataset
    msgs['MidPt'] = top['MidPt']
    msgs['MidPt_h'] = top['MidPt_h']
    msgs['TradePos'] = top['TradePos']
    msgs['idx'] = msgs.index

    # Add Race Trade flag to msgs
    msgs['Race_Trade'] = False
    msgs['Race_Msg'] = False
    msgs['MidPt_TickSize'] = top['MidPt_TickSize']
    
    if exist_races:            
        msgs = get_race_indcs(race_recs, msgs)
        msgs = get_race_related_msgs(race_recs, msgs)
    else:
        msgs['Race_Msg'] = False
        msgs['SingleLvlRaceID'] = np.nan

    ######################
    ### Trade activity ###
    ######################
    # We count trade on the execution outbounds of the aggressor in each trades
    trades = msgs.loc[(msgs['AuctionTrade'] == False) & (msgs['ExecType'] == 'Order_Executed') \
                 & (msgs['UnifiedMessageType'].isin({'ME: Partial Fill (A)', 'ME: Full Fill (A)'})), 
                        ['MessageTimestamp','UniqueOrderID', 'idx', 'TradeMatchID', 'SingleLvlRaceID',
                         'UnifiedMessageType', 'Race_Trade', 'Side', 'TradePos',  'UserID','FirmID',
                         'EventNum','Event','BidEventNum','BidEvent','AskEventNum','AskEvent',
                         'MidPt', 'ExecutedQty','ExecutedPrice']]
    
    ## Get the relevant inbound midpoint for each trade. 
    ## We do this using merging because it is faster than looping over messages.
    ## We do this separately for regular order trades and for quote related trades
    inbounds = msgs[msgs['QuoteRelated']==False].groupby(['UniqueOrderID', 'EventNum'], as_index=False)[['MidPt','UnifiedMessageType', 'Inbound']].first().reset_index()
    inbounds['Inbound_Missing'] = ~inbounds['Inbound']
    inbounds = inbounds.rename(index=str, columns = {"MidPt": "MidPt_Inb", 'UnifiedMessageType': 'UnifiedMessageType_Inb'}) 
    inbounds = inbounds[['UniqueOrderID', 'EventNum', 'MidPt_Inb', 'UnifiedMessageType_Inb', 'Inbound_Missing']]
    # inbounds['EventNum'] = inbounds['EventNum'].astype('Int64')
    trades = trades.merge(inbounds, on=['UniqueOrderID', 'EventNum'], how='left') 
    
    # Repeat the same process for quote related messages. 
    inbounds_bqr = msgs[msgs['QuoteRelated']==True].groupby(['UniqueOrderID', 'BidEventNum'], as_index=False)[['MidPt','UnifiedMessageType', 'Inbound']].first().reset_index()
    inbounds_bqr['Inbound_Missing_bqr'] = ~inbounds_bqr['Inbound']
    inbounds_bqr = inbounds_bqr.rename(index=str, columns = {"MidPt": "MidPt_Inb_bqr", "UnifiedMessageType": "UnifiedMessageType_bqr"})
    inbounds_bqr = inbounds_bqr[['UniqueOrderID', 'BidEventNum', 'MidPt_Inb_bqr', 'UnifiedMessageType_bqr', 'Inbound_Missing_bqr']]
    # inbounds_bqr['BidEventNum'] = inbounds_bqr['BidEventNum'].astype('Int64')
    trades = trades.merge(inbounds_bqr, on=['UniqueOrderID', 'BidEventNum'], how='left')
    
    inbounds_aqr = msgs[msgs['QuoteRelated']==True].groupby(['UniqueOrderID', 'AskEventNum'], as_index=False)[['MidPt','UnifiedMessageType', 'Inbound']].first().reset_index()
    inbounds_aqr['Inbound_Missing_aqr'] = ~inbounds_aqr['Inbound']
    inbounds_aqr = inbounds_aqr.rename(index=str, columns = {"MidPt": "MidPt_Inb_aqr", "UnifiedMessageType": "UnifiedMessageType_aqr"})
    inbounds_aqr = inbounds_aqr[['UniqueOrderID', 'AskEventNum', 'MidPt_Inb_aqr', 'UnifiedMessageType_aqr', 'Inbound_Missing_aqr']]
    # inbounds_aqr['AskEventNum'] = inbounds_aqr['AskEventNum'].astype('Int64')
    trades = trades.merge(inbounds_aqr, on=['UniqueOrderID', 'AskEventNum'], how='left')
    
    # If bid/ (ask) event numbers are populated then copy the inbound info/flags from the quote to the general inbound info/flags variable 
    trades.loc[(trades['BidEventNum'].notnull()), 'MidPt_Inb'] = trades.loc[trades['BidEventNum'].notnull(), 'MidPt_Inb_bqr']
    trades.loc[(trades['BidEventNum'].notnull()), 'Inbound_Missing'] = trades.loc[trades['BidEventNum'].notnull(), 'Inbound_Missing_bqr']
    trades.loc[(trades['BidEventNum'].notnull()), 'UnifiedMessageType_Inb'] = trades.loc[trades['BidEventNum'].notnull(), 'UnifiedMessageType_bqr']

    trades.loc[(trades['AskEventNum'].notnull()), 'MidPt_Inb'] = trades.loc[trades['AskEventNum'].notnull(), 'MidPt_Inb_aqr']
    trades.loc[(trades['AskEventNum'].notnull()), 'Inbound_Missing'] = trades.loc[trades['AskEventNum'].notnull(), 'Inbound_Missing_aqr']
    trades.loc[(trades['AskEventNum'].notnull()), 'UnifiedMessageType_Inb'] = trades.loc[trades['AskEventNum'].notnull(), 'UnifiedMessageType_aqr']
    
    # Drop intermediate variables
    # These fields have been copied to other fields and they are dropped to avoid confusion
    trades = trades.drop(columns = ['UnifiedMessageType_aqr','UnifiedMessageType_bqr','MidPt_Inb_aqr','MidPt_Inb_bqr','Inbound_Missing_aqr','Inbound_Missing_bqr'])
    
    # if the inbound message is missing, set all inbound related fields to NA
    trades.loc[trades['Inbound_Missing'], ['MidPt_Inb','UnifiedMessageType_Inb']] = np.nan

    # Get info of the passive party of the trade
    trades_cp = trades.apply(get_CP_info, args=(msgs,), axis=1, result_type='expand')
    trades_cp.columns = ['MessageTimestamp_CP','UniqueOrderID_CP','UserID_CP','FirmID_CP', 'UnifiedMessageType_CP']
    trades = pd.concat([trades, trades_cp], axis=1)

    # Convert the price to monetary unit in trade dataset
    trades['ExecutedPrice'] = trades['ExecutedPrice'] / price_factor
    trades['MidPt'] = trades['MidPt'] / price_factor
    trades['MidPt_Inb'] = trades['MidPt_Inb'] / price_factor
    
    # Sign for Effective Spread: +1 if aggressor is buyer and -1 if aggressor is seller.
    # Then generate the uncorrected effective spread per share
    trades['Sign'] = np.where(trades['Side'] == 'Bid', 1, -1)
    trades['Eff_Spread_PerShare'] = (trades['ExecutedPrice'] - trades['MidPt_Inb'])*trades['Sign']
    
    # Flagged Trades 
    # because of imperfect data, some spread decomposition variables cannot be calculated.
    # The number of flagged trades is supposed to be small.
    trades['Flag_Spread'] = False
    trades.loc[trades['MidPt_Inb'].isnull(), 'Flag_Spread'] = True  # Missing Midpoint            
    trades.loc[trades['Eff_Spread_PerShare'] < 0, 'Flag_Spread'] = True # Effective Spread < 0
    trades.loc[trades['Inbound_Missing'] == True, 'Flag_Spread'] = True # Missing Inbound
    

    ## PriceImpact Measure
    # List of timestamps for calculating price impact
    T_list = ['10ms', '100ms', '1s', '10s'] 

    # Calculate the Midpt after T us of the race starting msg
    if trades.shape[0]>0:
        midpt_columns = ['MidPt_f_%s'%x for x in T_list]
        trades[midpt_columns] = trades['MessageTimestamp'].apply(lambda x: pd.Series(get_midpt_T(redux_top, x, T_list, price_factor)))
    else:
        for T in T_list:
            trades['MidPt_f_%s' % T] = np.nan # Add these columns to trades if trades dataframe is empty
    
    # For each T, calculate the price impact when marked to market at T
    for T in T_list:
        # Calculate the uncorrected price impact per share in monetary unit
        trades['PriceImpact_PerShare_%s'% T] = (trades['MidPt_f_%s'% T] - trades['MidPt_Inb'])*trades['Sign']

        # Generate a flag for spreads that need correcting at this price and sum the volume beyond the effective spread volume.
        # Flagged volume includes volume flagged for the effective spread + volume for trades where the midpt at 10s out is <= 0
        trades['Flag_Spread_%s'% T] = (trades['MidPt_f_%s'% T].isnull()) | (trades['Flag_Spread'] == True)
    
    # add sym-date info
    trades['InstrumentID'] = sym
    trades['Date'] = date
    
    # Save to file
    logger.info('Writing to file...')
    trades.to_csv(outfile_trade_stats, index=False, compression='gzip')

    timer_end = datetime.datetime.now()
    logger.info('Complete.')
    logger.info('Timer End: %s' % str(timer_end))
    logger.info('Time Elapsed: %s' % str(timer_end - timer_st))

######################
## Helper Functions ##
######################
def get_midpt_T(redux_top, time_stamp, Ts, price_factor):
    '''
    This function returns the midpt forward T, given a row 
    Convert all Ts to timedelta, then cut the dataset to the subset from the given row to 
    the T that is furthest away (no further messages after that) will be necessary. 
    Finally, loop over Ts to get the last midpoint for each T and return the set of midpoints.
    '''   
    Ts = [np.timedelta64(pd.to_timedelta(x)) for x in Ts]
    top_small = redux_top[(redux_top['MessageTimestamp']>= time_stamp)&(redux_top['MessageTimestamp'] <= (time_stamp + max(Ts)))]
    midpts = []
    
    for T in Ts:
        try:
            midpt = top_small[top_small['MessageTimestamp'] <=(time_stamp + T)]['MidPt'].iloc[-1]/price_factor
            midpts.append(midpt)
        except:
            # if the midpt_f is missing due to out of the regular hour, use the last valid midpt of the day
            # this adreeses the missing midpt_f_10s due to trades happening in the last 10 seconds in the day
            midpt =  top_small.MidPt[top_small.Midpt > 0].iloc[-1]/price_factor
            midpts.append(midpt)
    return midpts

def get_trade_at_P(df_event, P):
    '''
    This function gets the aggressive execution message outbounds for a given event that executed at P 
    This function is called by get_race_indcs
    '''
    trades = df_event[df_event['UnifiedMessageType'].isin({'ME: Partial Fill (A)', 'ME: Full Fill (A)'})]
    race_trades = trades[trades['ExecutedPrice'] == P].index.values
    
    return race_trades
    
def get_race_indcs(race_recs, msgs):
    '''
    This function gets all the indicies for outbounds for aggressive trades in races that happen at P
    For each race,
    get the indicies in the race. For each race message, check that the even is an aggressive execution. If it is,
    get the order/event information. Use that information to get all messages in the event and then check if any of the
    relevant trades are at the race price. Then, set the race trade flag to True if the outbound is the result of an inbound
    in the race and occurs at the race price. We also attach the SingleLvlRaceID to the outbound message.
    '''
    for i, row in race_recs.iterrows():
        # Unsign the race price and get the relevant race indicies
        P = abs(race_recs.at[i, 'P_Signed'])
        indcs = row['Race_Msgs_Idx']
        single_lvl_race_id = row['SingleLvlRaceID']
        #
        for idx in indcs:
            # Treat quotes seperately from non-quotes
            if msgs.at[idx, 'QuoteRelated']:
                # Get the side for the event in the race. Take messages will be on the opposite side of the race messages
                # (e.g. In an ask race, attempts to take are bid messages)
                race_side = row['Side']
                if race_side == 'Bid':
                    take_side = 'Ask'
                else:
                    take_side = 'Bid'
                #
                event = msgs.at[idx, '%sEvent'% take_side]
                price_take = msgs.at[idx, '%sPriceLvl' % take_side]
                #
                # If the race event is an aggressive quote execution, check for the subset of outbounds that execute at the race price.
                # Then flag those messages which execute at the race price. Quote related takes must have a take price <= race price if they are 
                # sell orders or >= race price if they are buy orders.
                if (event in ['New quote aggressively executed in full', 'New quote aggressively executed in part']):
                    if ((take_side == 'Ask') and (price_take <= P)) or ((take_side == 'Bid') and (price_take >= P)):
                        orderid = msgs.at[idx, 'UniqueOrderID']
                        eventnum = msgs.at[idx, '%sEventNum' % take_side]
                        df_event = msgs[(msgs['UniqueOrderID'] == orderid) & (msgs['%sEventNum'% take_side] == eventnum)][['UnifiedMessageType', 'ExecutedPrice']]
                        #     
                        outbound_indcs = get_trade_at_P(df_event, P)
                        for out_idx in outbound_indcs:
                            msgs.at[out_idx, 'Race_Trade'] = True
                            msgs.at[out_idx, 'SingleLvlRaceID'] = single_lvl_race_id
            #
            else:
                # Get the race event for non-quotes (no side information is necessary). If the race event is an aggressive execution (cancel/replace or new order), 
                # check for the subset of outbounds that execute at the race price.
                # Then flag those messages which execute at the race price
                event = msgs.at[idx, 'Event']
                if event in ['New order aggressively executed in full', 'New order aggressively executed in part',
                            'Cancel/replace request aggr executed in full', 'Cancel/replace request aggr executed in part']:
                    #
                    orderid = msgs.at[idx, 'UniqueOrderID']
                    eventnum = msgs.at[idx, 'EventNum']
                    df_event = msgs[(msgs['UniqueOrderID'] == orderid) & (msgs['EventNum'] == eventnum)][['UnifiedMessageType', 'ExecutedPrice']]
                    #
                    outbound_indcs = get_trade_at_P(df_event, P)
                    for out_idx in outbound_indcs:
                        msgs.at[out_idx, 'Race_Trade'] = True
                        msgs.at[out_idx, 'SingleLvlRaceID'] = single_lvl_race_id
    return msgs

def get_race_related_msgs(race_recs, msgs):
    '''
    This function add an indicator variable in msgs dataframe to flag race related messages
    '''
    for i, row in race_recs.iterrows():
        
        # Unsign the race price and get the relevant race indicies
        P = abs(race_recs.at[i, 'P_Signed'])
        indcs = row['Race_Msgs_Idx']
        
        for idx in indcs:
        
            # Treat quotes seperately from non-quotes
            if msgs.at[idx, 'QuoteRelated']:
                # Get the side for the event in the race. Take messages will be on the opposite side of the race messages
                # (e.g. In an ask race, attempts to take are bid messages). Take attempts require a bid price >= race price in ask races or 
                # an ask price <= race prices in bid races.
                race_side = row['Side']
                if race_side == 'Ask':
                    take_side = 'Bid'
                    price_take =  msgs.at[idx, 'BidPriceLvl']
                    if (P <= price_take) and (price_take > 0) and (msgs.at[idx, 'BidEvent'] in \
                        ['New quote aggressively executed in full', 'New quote aggressively executed in part', 
                            'New quote updated','New quote accepted','New quote no response']):
                        take_attempt = True
                        S = take_side
                    else:
                        take_attempt = False
                        S = race_side
                else:
                    take_side = 'Ask'
                    price_take = msgs.at[idx, 'AskPriceLvl']
                if (P >= price_take) and (price_take > 0) and (msgs.at[idx, 'AskEvent'] in \
                    ['New quote aggressively executed in full', 'New quote aggressively executed in part',  
                        'New quote updated','New quote accepted','New quote no response']):
                        take_attempt = True
                        S = take_side
                else:
                        take_attempt = False
                        S = race_side


                orderid = msgs.at[idx, 'UniqueOrderID']
                eventnum = msgs.at[idx, '%sEventNum' %S]
                df_event = msgs[(msgs['UniqueOrderID'] == orderid) & (msgs['%sEventNum' % S] == eventnum)][['UnifiedMessageType', 'ExecutedPrice', 'Race_Trade']]

                all_indcs = df_event[((df_event['UnifiedMessageType'].isin({'ME: Partial Fill (A)', 'ME: Full Fill (A)'})) & 
                                        (df_event['Race_Trade'] == True)) | (~df_event['UnifiedMessageType'].isin({'ME: Partial Fill (A)', 'ME: Full Fill (A)'}))]
                all_indcs = all_indcs.index.values

                for idx_val in all_indcs:
                    msgs.at[idx_val, 'Race_Msg'] = True

                            
            else:
                # Get the race event for non-quotes (no side information is necessary). If the race event is an aggressive execution (cancel/replace or new order), 
                # check for the subset of outbounds that execute at the race price.
                # Then flag those messages which execute at the race price
                event = msgs.at[idx, 'Event']
                orderid = msgs.at[idx, 'UniqueOrderID']
                eventnum = msgs.at[idx, 'EventNum']
                df_event = msgs[(msgs['UniqueOrderID'] == orderid) & (msgs['EventNum'] == eventnum)][['UnifiedMessageType', 'ExecutedPrice', 'Race_Trade']]
                
                all_indcs = df_event[((df_event['UnifiedMessageType'].isin({'ME: Partial Fill (A)', 'ME: Full Fill (A)'})) & 
                                    (df_event['Race_Trade'] == True)) | (~df_event['UnifiedMessageType'].isin({'ME: Partial Fill (A)', 'ME: Full Fill (A)'}))].index.values

                for idx_val in all_indcs:
                    msgs.at[idx_val, 'Race_Msg'] = True
    return msgs

def get_CP_info(aggr_trade_msg, msgs):
    '''
    This function takes in the trade confirmation msg of the aggressive
    party of a trade and returns the info of the passive party of the trade.
    '''
    trade_match_id = aggr_trade_msg['TradeMatchID']
    idx = aggr_trade_msg['idx']
    passive_trade_msg = msgs.loc[(msgs['TradeMatchID'] == trade_match_id) & (msgs['idx'] != idx)]
    if passive_trade_msg.shape[0] == 1:
        return passive_trade_msg.iloc[0][['MessageTimestamp','UniqueOrderID','UserID','FirmID','idx','UnifiedMessageType']].tolist()
    else:
        return [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
