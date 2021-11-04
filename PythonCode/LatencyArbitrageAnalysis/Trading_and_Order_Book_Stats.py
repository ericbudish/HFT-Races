'''
Trading_and_Order_Book_Stats.py

Calculates symbol-date level stats for trading activities based on
order book data and message data.

References: Section 10.3 of the Code and Data Appendix.

Input:
    1. Message data with economic events assigned (output of event classification).
    2. Top-of-book data (output of order book reconstruction)
    3. Depth-of-book data (output of order book reconstruction)

Output: 
    1. Symbol-date level stats
    2. Trade level stats
'''
####################################################################

# Import packages
from collections import OrderedDict
import pandas as pd
import numpy as np
import datetime
import pickle
import os

from .utils.Logger import getLogger
# import PrepData to count # NBBO inbounds, this is independent 
# of the definition of a race
from .RaceDetection import Prep_Race_Data as PrepData 

def calculate_symdate_stats(runtime, date, sym, args, paths):
    '''
    This function calculates trading and order book stats from message data and order book data. It is called 
    from the master script. 
    Params:
        runtime: str, for log purpose.
        date:    str, symbol-date identifier.
        sym:     str, symbol-date identifier.
        args:    dictionary of arguments, including:
                    - dtypes_msgs: dtype dict for message data with event classification.
                    - dtypes_top: dtype dict for top-of-book data.
                    - price_factor: int. convert price from monetary unit to large integer,
                                         obtained by int(10 ** (max_dec_scale+1)).
        paths: dictionary of file paths, including:
                     - path_temp: path to the temp data file.
                     - path_logs: path to log files.
    
    Output:
        trades: trade-level statistical dataset with statistics for each trade.
                See Section 8 of the Code and Data Appendix for a complete list of output statistics.
        stats:  symbol-date level statistical dataset on volume, midpoint, and spread.
                See Section 8 of the Code and Data Appendix for a complete list of output statistics.
    '''
    # Initialize log
    logpath = '%s/%s/' %(paths['path_logs'], 'MessageDataProcessing_'+runtime)
    logfile = 'Step_3_Trading_and_Order_Book_Stats_%s_%s_%s.log' % (runtime, date, sym)
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
    outfile_SymDate_Stats = '%s/SymDateStats/%s/SymDate_Stats_%s_%s.pkl' % (paths['path_temp'], date, date, sym)
    outfile_trade_stats = '%s/TradeStats/%s/Trade_Stats_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym)

    # LOAD DATA
    logger.info('Loading data...')

    # Load data
    msgs = pd.read_csv(infile_msgs, dtype = dtypes_msgs, skipinitialspace=True, parse_dates=['MessageTimestamp'])
    top = pd.read_csv(infile_top, index_col = 0, dtype = dtypes_top, parse_dates = ['MessageTimestamp'])

    ### INITIALIZE ###
    stats = OrderedDict()

    # Metadata
    stats['Date'] = date
    stats['Symbol'] = sym

    ### Keep regular hour msgs
    reg_hours = msgs['RegularHour']
    msgs, top = msgs.loc[reg_hours], top.loc[reg_hours]

    #################
    ### PREP DATA ###
    #################
    msgs, top = PrepData.prepare_data(msgs, top)
    
    ## New BBO Inbound counts
    msgs['NBBO_Relevant'] = False
    msgs.loc[((msgs['AskRaceRlvtType'] == 'Take Attempt') & (msgs['AskRaceRlvtPriceLvlSigned'] >= top['BestAskSigned'])) |
             ((msgs['BidRaceRlvtType'] == 'Take Attempt') & (msgs['BidRaceRlvtPriceLvlSigned'] >= top['BestBidSigned'])) |
             ((msgs['AskRaceRlvtType'] == 'Cancel Attempt') & (msgs['AskRaceRlvtPriceLvlSigned'] == top['BestAskSigned'])) |
             ((msgs['BidRaceRlvtType'] == 'Cancel Attempt') & (msgs['BidRaceRlvtPriceLvlSigned'] == top['BestBidSigned'])), 'NBBO_Relevant'] = True

    ## Calculate volatility
    logger.info('Performing calculations...')

    # Spread calculated in ticks and basis points
    top['Spread_Tx'] = top['Spread'] // top['MidPt_TickSize']
    top['Spread_bps'] = 10000. * top['Spread'] // top['MidPt']

    # Copy relevant columns from the top of book dataset to the msg dataset
    msgs['MidPt'] = top['MidPt']
    msgs['MidPt_h'] = top['MidPt_h']
    msgs['TradePos'] = top['TradePos']
    msgs['MidPt_TickSize'] = top['MidPt_TickSize']
    msgs['idx'] = msgs.index

    ########################
    ### DAILY STATISTICS ###
    ########################
    logger.info('Computing daily statistics...')

    # Message activity
    stats['N_Msgs'] = msgs.shape[0]
    stats['N_Msgs_Inbound'] = (msgs['Inbound'] == True).sum()
    stats['N_Msgs_Inbound_NBBO'] = (msgs['NBBO_Relevant'] == True).sum()

    # Volatility (midpoint distance traveled in monetary terms and in Ticks)
    volatility_top = top.loc[(top['Chg_MidPt'].notnull())]
    stats['MidPt_Distance'] = (1. / price_factor) * volatility_top['Chg_MidPt'].abs().sum()
    stats['MidPt_Distance_Tx'] = volatility_top['Chg_MidPt_Tx'].abs().sum()

    # Time-weighted avg depth when depth on both sides exists in shares and monetary terms
    two_sided_top = top.loc[(top['BestBid'].notnull()) & (top['BestBidQty'] > 0) & (top['BestAsk'].notnull()) & (top['BestAskQty'] > 0)]
    time_to_next = (two_sided_top['MessageTimestamp'].shift(-1) - two_sided_top['MessageTimestamp']).dt.total_seconds()
    stats['Avg_Depth_Sh'] = (time_to_next * (two_sided_top['BestBidQty'] + two_sided_top['BestAskQty'])).sum() / time_to_next.sum()
    stats['Avg_Depth'] = (time_to_next * ((1. / price_factor) * two_sided_top['BestBid'] * two_sided_top['BestBidQty'] + 
                                          (1. / price_factor) * two_sided_top['BestAsk'] * two_sided_top['BestAskQty'])).sum() / time_to_next.sum()

    # Time-weighted avg spread and percentage of day in which spread is 1 tick wide or 2 ticks wide
    spread_top = top.loc[top['Spread'].notnull()]
    time_to_next = (spread_top['MessageTimestamp'].shift(-1) - spread_top['MessageTimestamp']).dt.total_seconds()
    stats['Avg_Half_Spr_Time_Weighted'] = (time_to_next * (1. / price_factor) * spread_top['Spread']/2).sum() / time_to_next.sum()
    stats['Avg_Half_Spr_Time_Weighted_Tx'] = (time_to_next * spread_top['Spread_Tx']/2).sum() / time_to_next.sum()
    stats['Avg_Half_Spr_Time_Weighted_bps'] = (time_to_next * spread_top['Spread_bps']/2).sum() / time_to_next.sum()
    stats['Pct_Spr_1Tx'] = (time_to_next * (spread_top['Spread_Tx'] == 1.)).sum() / time_to_next.sum()
    stats['Pct_Spr_2Tx'] = (time_to_next * (spread_top['Spread_Tx'] == 2.)).sum() / time_to_next.sum()

    # Time-weighted avg tick size
    midpt_top = top.loc[top['MidPt'].notnull()]
    midpt_top_all_prices = np.unique(np.concatenate([midpt_top['BestBid_TickSize'].unique(), midpt_top['BestAsk_TickSize'].unique()]))
    time_to_next = (midpt_top['MessageTimestamp'].shift(-1) - midpt_top['MessageTimestamp']).dt.total_seconds()
    stats['Avg_Tick'] = (time_to_next * (1. / price_factor) * midpt_top['MidPt_TickSize']).sum() / time_to_next.sum()
    stats['Avg_Tick_bps'] = 10000. * (time_to_next * (midpt_top['MidPt_TickSize'] / midpt_top['MidPt'])).sum() / time_to_next.sum()
    stats['Min_Tick'] = (1. / price_factor) * midpt_top_all_prices.min()
    stats['Max_Tick'] = (1. / price_factor) * midpt_top_all_prices.max()

    # Time-weighted avg midpt
    stats['Avg_MidPt'] = (time_to_next * (1. / price_factor) * midpt_top['MidPt']).sum() / time_to_next.sum()

    # Percent of day one-sided
    time_to_next = (top['MessageTimestamp'].shift(-1) - top['MessageTimestamp']).dt.total_seconds()
    top['midpt_missing'] = (top['MidPt'].isnull())
    stats['Time_MidPt_Missing'] = (top['midpt_missing']*time_to_next).sum()

    ######################
    ### Trade activity ###
    ######################
    # We count trade on the execution outbounds of the aggressor in each trades
    trades = msgs.loc[(msgs['AuctionTrade'] == False) & (msgs['ExecType'] == 'Order_Executed') \
                 & (msgs['UnifiedMessageType'].isin({'ME: Partial Fill (A)', 'ME: Full Fill (A)'})), 
                        ['MessageTimestamp','UniqueOrderID', 'idx', 'TradeMatchID', 'UserID','FirmID',
                         'UnifiedMessageType', 'Side', 'TradePos',  
                         'EventNum','Event','BidEventNum','BidEvent','AskEventNum','AskEvent',
                         'MidPt', 'MidPt_TickSize', 'ExecutedQty','ExecutedPrice']]

    ## Get the relevant inbound midpoint for each trade. 
    ## We do this using merging because it is faster than looping over messages.
    ## We do this separately for regular order trades and for quote related trades
    inbounds = msgs[msgs['QuoteRelated']==False].groupby(['UniqueOrderID', 'EventNum'], as_index=False)[['MidPt','UnifiedMessageType', 'Inbound']].first().reset_index()
    inbounds['Inbound_Missing'] = ~inbounds['Inbound']
    inbounds = inbounds.rename(index=str, columns = {"MidPt": "MidPt_Inb", 'UnifiedMessageType': 'UnifiedMessageType_Inb'}) 
    inbounds = inbounds[['UniqueOrderID', 'EventNum', 'MidPt_Inb', 'UnifiedMessageType_Inb', 'Inbound_Missing']]
    trades = trades.merge(inbounds, on=['UniqueOrderID', 'EventNum'], how='left') 
    
    # Repeat the same process for quote related messages. 
    inbounds_bqr = msgs[msgs['QuoteRelated']==True].groupby(['UniqueOrderID', 'BidEventNum'], as_index=False)[['MidPt','UnifiedMessageType', 'Inbound']].first().reset_index()
    inbounds_bqr['Inbound_Missing_bqr'] = ~inbounds_bqr['Inbound']
    inbounds_bqr = inbounds_bqr.rename(index=str, columns = {"MidPt": "MidPt_Inb_bqr", "UnifiedMessageType": "UnifiedMessageType_bqr"})
    inbounds_bqr = inbounds_bqr[['UniqueOrderID', 'BidEventNum', 'MidPt_Inb_bqr', 'UnifiedMessageType_bqr', 'Inbound_Missing_bqr']]
    trades = trades.merge(inbounds_bqr, on=['UniqueOrderID', 'BidEventNum'], how='left')
    
    inbounds_aqr = msgs[msgs['QuoteRelated']==True].groupby(['UniqueOrderID', 'AskEventNum'], as_index=False)[['MidPt','UnifiedMessageType', 'Inbound']].first().reset_index()
    inbounds_aqr['Inbound_Missing_aqr'] = ~inbounds_aqr['Inbound']
    inbounds_aqr = inbounds_aqr.rename(index=str, columns = {"MidPt": "MidPt_Inb_aqr", "UnifiedMessageType": "UnifiedMessageType_aqr"})
    inbounds_aqr = inbounds_aqr[['UniqueOrderID', 'AskEventNum', 'MidPt_Inb_aqr', 'UnifiedMessageType_aqr', 'Inbound_Missing_aqr']]
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
    trades_cp.columns = ['MessageTimestamp_CP','UniqueOrderID_CP','UserID_CP','FirmID_CP','idx_CP','UnifiedMessageType_CP']
    trades = pd.concat([trades, trades_cp], axis=1)

    # Convert the price to monetary unit in trade dataset
    trades['ExecutedPrice'] = trades['ExecutedPrice'] / price_factor
    trades['MidPt'] = trades['MidPt'] / price_factor
    trades['MidPt_Inb'] = trades['MidPt_Inb'] / price_factor
    trades['MidPt_TickSize'] = trades['MidPt_TickSize'] / price_factor

    # Executed value for each trade
    trades['ExecutedValue'] = trades['ExecutedPrice'] * trades['ExecutedQty']

    # Sign for effective spread: +1 if aggressor is buyer and -1 if aggressor is seller.
    # Then generate effective spread per share
    trades['Sign'] = np.where(trades['Side'] == 'Bid', 1, -1)
    trades['Eff_Spread_PerShare'] = (trades['ExecutedPrice'] - trades['MidPt_Inb'])*trades['Sign']
    
    # Flagged Trades 
    # A trade is flagged if either 
    #     1. The midpoint price at the inbound of the aggressive trade confirmation is missing (zero or one sided market), OR
    #     2. The effective spread is weakly negative, OR
    #     3. The inbound of the aggressive trade confirmation is missing.
    # If one of these happens, we are not able to calculate the effective spread for a trade. 
    trades['Flag_Spread'] = False
    trades.loc[trades['MidPt_Inb'].isnull(), 'Flag_Spread'] = True  # Missing Midpoint            
    trades.loc[trades['Eff_Spread_PerShare'] <= 0, 'Flag_Spread'] = True # Effective Spread <= 0 
    trades.loc[trades['Inbound_Missing'] == True, 'Flag_Spread'] = True # Missing Inbound

    # Get the total number of trades, trade levels, traded shares and traded volume in monetary unit
    exist_trades = trades.shape[0] > 0
    stats['N_Tr'] = trades.shape[0]
    stats['Vol_Sh'] = trades['ExecutedQty'].sum()
    stats['Vol'] = (trades['ExecutedQty'] * trades['ExecutedPrice']).sum()
    
    ## Effective spread
    # Calculate effective spread bps and Tx using the nonflagged trades (value-weighted)
    # Calculate effective spread paid by effective spread bps * total volume
    nonflagged_trades = trades[trades['Flag_Spread']==False]
    # Eff_Spr_pershare * qty / non_flagged volume
    stats['Eff_Spread_bps'] = 10000. * (nonflagged_trades['Eff_Spread_PerShare'] * nonflagged_trades['ExecutedQty']).sum() / (nonflagged_trades['ExecutedValue']).sum() if exist_trades else 0
    stats['Eff_Spread_Paid'] = stats['Eff_Spread_bps'] * stats['Vol'] / 10000. if exist_trades else 0

    ## Qty weighted average spread (Qty weighted)
    stats['Avg_Half_Spr_Qty_Weighted_bps'] = 10000. * (nonflagged_trades['Eff_Spread_PerShare']/nonflagged_trades['MidPt']*nonflagged_trades['ExecutedQty']).sum() / (nonflagged_trades['ExecutedQty']).sum() if exist_trades else 0
    stats['Avg_Half_Spr_Qty_Weighted_Tx'] =  (nonflagged_trades['Eff_Spread_PerShare']/nonflagged_trades['MidPt_TickSize']*nonflagged_trades['ExecutedQty']).sum() / (nonflagged_trades['ExecutedQty']).sum() if exist_trades else 0

    ## Price impact
    # List of price impact mark-to-market time horizons
    T_list = ['1ms', '10ms', '100ms', '1s', '10s', '30s', '60s', '100s']

    # Calculate the MidPt T after the trade
    if exist_trades:
        midpt_columns = ['MidPt_f_%s'%x for x in T_list]
        trades[midpt_columns] = trades['MessageTimestamp'].apply(lambda x: pd.Series(get_midpt_T(top, x, T_list, price_factor)))
    else:
        for T in T_list:
            trades['MidPt_f_%s' % T] = np.nan # Add these columns to trades if trades dataframe is empty
    
    # For each T, calculate the price impact mark to market T
    for T in T_list:
        # Calculate price impact per share in monetary unit for each trade
        trades['PriceImpact_PerShare_%s'% T] = (trades['MidPt_f_%s'% T] - trades['MidPt_Inb'])*trades['Sign']

        # Flag trade. A trade is flagged if 
        #     1. The midpoint price at the inbound of the aggressive trade confirmation is missing (zero or one sided market), OR
        #     2. The effective spread is weakly negative, OR
        #     3. The inbound of the aggressive trade confirmation is missing, OR
        #     4. The midpoint at T after the aggressive trade confirmation is missing.
        # Cases 1-3 are flags for effective spread calculation ('Flag_Spread').
        # Case 4 is another situation where we are not able to calculate price impact.
        trades['Flag_Spread_%s'% T] = (trades['MidPt_f_%s'% T].isnull()) | (trades['Flag_Spread'] == True)

        # Calculate price impact bps using the non-flagged trades
        # Calculate price impact paid by price impact bps * total volume
        nonflagged_trades = trades[trades['Flag_Spread_%s' % T]==False]
        vol_no_flag = (nonflagged_trades['ExecutedValue']).sum()
        stats['PriceImpact_bps_%s' % T] = 10000. * (nonflagged_trades['PriceImpact_PerShare_%s'% T]*nonflagged_trades['ExecutedQty']).sum() / vol_no_flag if exist_trades else 0
        stats['PriceImpact_Paid_%s' % T] = stats['PriceImpact_bps_%s' % T] * stats['Vol'] / 10000. if exist_trades else 0
    
    # add sym-date info
    trades['Symbol'] = sym
    trades['Date'] = date

    # drop unnecessary columns from trades
    # These columns are intermediate variables generated in event classification. 
    # We don't need them in the final output.
    trades.drop(['Sign', 'TradePos', 'EventNum', 'Event', 
        'BidEventNum', 'BidEvent', 'AskEventNum', 'AskEvent'], axis=1, inplace=True)
    
    # Save to file
    logger.info('Writing to file...')
    trades.to_csv(outfile_trade_stats, index=False, compression='gzip')
    pickle.dump(pd.Series(stats), open(outfile_SymDate_Stats, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)

    timer_end = datetime.datetime.now()
    logger.info('Complete.')
    logger.info('Timer End: %s' % str(timer_end))
    logger.info('Time Elapsed: %s' % str(timer_end - timer_st))

######################
## Helper Functions ##
######################
def get_midpt_T(top, time_stamp, Ts, price_factor):
    '''
    This function returns the midpt forward T after time_stamp 
    Convert all Ts to timedelta and loop over Ts to get the last 
    midpoint for each T and return the set of midpoints.
    '''   
    Ts = [np.timedelta64(pd.to_timedelta(x)) for x in Ts]
    top_small = top[(top['MessageTimestamp']>= time_stamp)&(top['MessageTimestamp'] <= (time_stamp + max(Ts)))][['MessageTimestamp','MidPt']]
    midpts = []
    
    for T in Ts:
        try:
            midpt = top_small[top_small['MessageTimestamp'] <=(time_stamp + T)]['MidPt'].iloc[-1]/price_factor
            midpts.append(midpt)
        except:
            # if the midpt_f is missing due to out of the regular hour, use the last valid midpt of the day
            # this addresses the missing midpt_f_10s due to trades happening in the last 10 seconds in the day
            midpt =  top_small.MidPt[top_small.MidPt > 0].iloc[-1]/price_factor
            midpts.append(midpt)
    return midpts

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