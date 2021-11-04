'''
code_snippet_take_provide_cancel.py

This snippet is the code we used to analyze the activities in races. It decomposes the 
active qty in races (qty traded and canceled) into 3 groups: 
    1. qty traded without an attempt to cancel
    2. qty traded with a failed attempt to cancel
    3. qty canceled successfully
for each firm. This is the input for Figure 4.3 Panel B (takes, cancels and liquidity
provision by firm group) and the Cancel Attempt Rate Columns in Table 4.11 (see also
code_snippet_cancel_activities.py). The code is specific to the LSE settings and may 
not be applicable to other exchange message data directly. 

We provide this code for the purpose of disclosing how we did the analysis and helping our user 
reproduce the related results. Note that the coding convention (e.g., naming) in this script 
is slightly different from the code in /PythonCode, because this is part of an earlier version of 
our code.

To reproduce the results, users need to
    1. Go through the code snippet carefully and understand the logic.
    2. Adapt/rewrite the code to their context depending on the specific details of their setting.
       Users can make use of this code snippet in this step.
    3. Execute the code to obtain the active qty decomposition in races. 
    4. Compute Figure 4.3 Panel B data. Users have to do this by themselves. 
       (1) % Races won: This can be obtained directly from the Python race-level statistical data.
       (2) % Successful taking in races: This can be obtained from the output of this snippet
           (qty traded with a failed cancel + qty traded without an attempt to cancel)
       (3) % Successful canceling in races: This can be obtained from the output of this snippet 
           (qty canceled successfully data).
       (4) % Liquidity provided in races: This can be obtained from the trade level data. Please
           refer to code_snippet_trade_level_data.py
    5. Compute the number of races where a group of firms provide liquidity: This can be obtained 
       from the trade level data. Please refer to code_snippet_trade_level_data.py.
'''
###################################################################################

import pandas as pd
import numpy as np
import multiprocessing
import importlib
import os
import datetime
import warnings
import logging
from ast import literal_eval
import random
import os
from LatencyArbitrageAnalysis.utils.Dtypes import dtypes_msgs, dtypes_top
from LatencyArbitrageAnalysis.RaceDetection.Race_Msg_Outcome import get_msg_outcome
warnings.filterwarnings("ignore")
PrepData = importlib.import_module('04a_Prep_Race_Data')

now = datetime.datetime.now()
runtime = '%02d%02d%02d_%02d%02d' % (now.year, now.month, now.day, now.hour, now.minute)
logpath = '/data/proc/data_analysis/active_qty_decompose/logs/' 
logger = logging.getLogger('decompose_active_qty')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logpath + f'/decompose_active_qty_{runtime}.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def decompose_active_qty(race, msgs, top_15, me):
    '''
    Given a race, this function returns the decomposition of 
    the active qty (qty traded and cancelled) in the race

    param:
        race: a row in the race record dataset
        msgs: msgs dataset (output of 4a)
        top_15: list of firm ID of the top 15 firms.
        me: outbound messages in the msgs dataset 
    output: 
        pd.Series. Decomposition of active qty in GBP
    '''
    # extract the information about the race, including the msgs in race, 
    # race price & side, and race starting time
    race_msgs = msgs.loc[race['ix_race']]
    race_start_time = race_msgs['MessageTimestamp'].iloc[0]
    race_ID = race['SingleLvlRaceID']
    S = race['S']
    P_Signed = race['P']
    race_msgs[f'{S}RaceRlvtMsgOutcome'] = get_msg_outcome(S, P_Signed, race_msgs, False) # Strict fail = False
    Sign = 1 * (S == 'Ask') - 1 * (S == 'Bid')
    price_factor = int(1e8)
    P = Sign * P_Signed
    P_GBP = (P/price_factor)/100

    # initialize output data structure. The output is a row with following columns
    output = {
              # basic race information
              'SingleLvlRaceID':race['SingleLvlRaceID'], 'RacePrice':P_GBP, 'Side': S, 
              # decompose active qty 
              'no_cancel_top_1-3':0,'no_cancel_top_4-6':0,'no_cancel_top_7-10':0,'no_cancel_top_11-15':0,'no_cancel_nontop15':0,
              'cancel_succ_top_1-3':0,'cancel_succ_top_4-6':0,'cancel_succ_top_7-10':0,'cancel_succ_top_11-15':0,'cancel_succ_nontop15':0,
              'cancel_fail_top_1-3':0, 'cancel_fail_top_4-6':0,'cancel_fail_top_7-10':0,'cancel_fail_top_11-15':0,'cancel_fail_nontop15':0,
              # decompose active qty based on activity in 50 us
              'no_cancel_top_1-3_in_50us':0,'no_cancel_top_4-6_in_50us':0,'no_cancel_top_7-10_in_50us':0,'no_cancel_top_11-15_in_50us':0,'no_cancel_nontop15_in_50us':0,
              'cancel_succ_top_1-3_in_50us':0,'cancel_succ_top_4-6_in_50us':0,'cancel_succ_top_7-10_in_50us':0,'cancel_succ_top_11-15_in_50us':0,'cancel_succ_nontop15_in_50us':0,
              'cancel_fail_top_1-3_in_50us':0, 'cancel_fail_top_4-6_in_50us':0,'cancel_fail_top_7-10_in_50us':0,'cancel_fail_top_11-15_in_50us':0,'cancel_fail_nontop15_in_50us':0
              }
    # get information about the firm group
    # python indices are left-closed / right-open so [0:3] is elements 0, 1, 2. 
    top_list = {'top_11-15':top_15[10:], 'top_7-10':top_15[6:10], 'top_4-6':top_15[3:6], 'top_1-3':top_15[0:3]}        
    firm_group_map = {firm:firm_group for firm_group, firm_list in top_list.items() for firm in firm_list}  
    # get the set of uid of the cancel attempts in this race
    cancel_uid = race_msgs.loc[(race_msgs[f'{S}RaceRlvtType']=='Cancel Attempt'), 'UniqueOrderID'].values
    # get the set of uid of the cancel attempts in 50us in this race
    cancel_uid_50us = race_msgs.loc[(race_msgs[f'{S}RaceRlvtType']=='Cancel Attempt') &
                                                          ((race_msgs['MessageTimestamp'] - race_start_time).dt.total_seconds()<=0.00005) 
                                                          ,'UniqueOrderID'].values 

    # loop over race messages 
    for _, race_msg in race_msgs.iterrows():

        # get the firm id, firm group, race message type and race outcome of the race message
        firm = race_msg['FirmNum']
        firm_group = firm_group_map[firm] if firm in firm_group_map.keys() else 'nontop15' # 'top 1-3', 'top 4-6' etc.
        RaceRlvtType = race_msg[f'{S}RaceRlvtType'] # 'Cancel Attempt', 'Take Attempt'
        outcome = race_msg[f'{S}RaceRlvtMsgOutcome'] # 'Success', 'Fail', 'Unknown'. 

        # if the message is a successful cancel, increment 'cancel_succ_{firm_group}' by GBP canceled 
        # There is a corner case where the cancel is "partially successful" -- say I have 1000 shares, get sniped for 800, cancel the last 200. This method
        # will sometimes assign successful cancel to all 1000 shares: if the cancel inbound is received when I still have 1000 shares outstanding.
        # this case should be rare because in a race a sniper should take all my shares 
        if RaceRlvtType == 'Cancel Attempt' and outcome == 'Success':
            Qty = max(0, race_msg[f'{S}RaceRlvtQty'])
            output[f'cancel_succ_{firm_group}'] += Qty * P_GBP

            # in addition, if the successful cancel is within 50us, 
            # increment 'cancel_succ_{firm_group}_in_50us' by GBP canceled 
            if (race_msg['MessageTimestamp'] - race_start_time).total_seconds()<=0.00005:
                output[f'cancel_succ_{firm_group}_in_50us'] += Qty * P_GBP
            else: 
                # else, the firm did not try to cancel in 50 us. 
                # Hence, increment 'no_cancel_{firm_group}_in_50us' by GBP canceled 
                 output[f'no_cancel_{firm_group}_in_50us'] += Qty * P_GBP    


        # if the message is a successful take 
        if RaceRlvtType == 'Take Attempt' and outcome == 'Success':

            # if it is non-quote related, and the outbound messages of this inbound is not missing:
            # Note: we do not deal with quote and packet loss here, because the code will be tedious and it does not 
            # improve the accuracy much since those are rare cases.
            if (not race_msg['QuoteRelated']) and ((race_msg['UniqueOrderID'], race_msg['EventNum']) in me.index):
                # get the messages in the event that the message belongs to
                ev = me.loc[(race_msg['UniqueOrderID'], race_msg['EventNum'])]
                # get the execution outbounds in the event
                Exec_out_P = ev.loc[ev['UnifiedMessageType'].isin({'ME: Partial Fill (A)', 'ME: Full Fill (A)'}) & 
                                   (ev['ExecutedPrice'] == P)]
                # loop over execution outbounds
                for _, exec_msg in  Exec_out_P.iterrows():
                    # take out the trade match id
                    trade_match_id = exec_msg['TradeMatchID']
                    # using the trade matching id, get the counterparty's execution outbound (i.e. the resting order's outbound)
                    # this works because the two execution outbounds in a trade share the same TradeMatchID
                    counterpart_msg = msgs[(msgs['TradeMatchID'] == trade_match_id) & 
                                           (msgs['TradePos'] != exec_msg['TradePos'])]
                    if counterpart_msg.shape[0] == 0:
                        continue
                    # get the firm id of the counterparty
                    counterpart_id = counterpart_msg['FirmNum'].iloc[0]
                    # get the unique order id for the counterparty's order
                    counterpart_unique_order_id = counterpart_msg['UniqueOrderID'].iloc[0]
                    # get counterparty's firm group based on its firm id
                    counterpart_firm_group = firm_group_map[counterpart_id] if counterpart_id in firm_group_map.keys() else 'nontop15'      
                    # get the executed quantity of this trade
                    Qty_P = max(0,exec_msg['ExecutedQty'])  
                    # if counterparty tried to cancel this order during the race
                    # i.e., the counterparty's unique order id is in cancel_uid
                    # this means he tried but failed to cancel the order. 
                    # Hence increment 'cancel_fail_{counterpart_firm_group}' by GBP traded at P
                    if counterpart_unique_order_id in cancel_uid:
                        output[f'cancel_fail_{counterpart_firm_group}'] += Qty_P * P_GBP
                    else: 
                        # else, the counterparty did not try to cancel the order during the race
                        # hence, increment 'no_cancel_{firm_group}' by GBP traded at P
                        output[f'no_cancel_{counterpart_firm_group}'] += Qty_P * P_GBP   
                        
                    # increment 'cancel_fail_{firm_group}_in_50us' and 'no_cancel_{firm_group}_in_50us' similarly                      
                    if counterpart_unique_order_id in cancel_uid_50us:
                        output[f'cancel_fail_{counterpart_firm_group}_in_50us'] += Qty_P * P_GBP
                    else:
                        output[f'no_cancel_{counterpart_firm_group}_in_50us'] += Qty_P * P_GBP
                            
    return pd.Series(output)

def process_sym_date(symdate):
    date, sym  = symdate
    try:
        infile_msgs = '/data/proc/data/clean/%s/CleanMsgData_%s_%s.csv.gz' % (date, date, sym)
        infile_top = '/data/proc/data/book/%s/BBO_%s_%s.csv.gz' % (date, date, sym)
        infile_race_records = '/data/proc/output/race_stats/%s/%s/Race_Recs_%s_%s.pkl' % ('daily_500us', date, date, sym)
    
        price_factor = int(1e8)
        # This rank is from the firm dynamics analysis in the paper. 
        # Firms are sorted based on the proportion of races won in the whole sample
        top_15 = [41,7,19,24,32,43,4,27,11,45,127,26,39,12,6] 
        # top_15 = [f'{i}.0' for i in top_15]
        msgs = pd.read_csv(infile_msgs, dtype = dtypes_msgs, parse_dates=['MessageTimestamp'])
        top = pd.read_csv(infile_top, index_col = 0, dtype = dtypes_top, parse_dates = ['MessageTimestamp'])
        race_recs = pd.read_pickle(infile_race_records)
        if race_recs.shape[0]==1:
            logger.info('Finish processing: '+str(symdate))
            return None
    
        info = pd.read_csv('/data/proc/reference_data/Symbol_Date_Info.csv')
        info = info.loc[(info['InstrumentID'].astype('str') == sym) & (info['Date'] == date)]
        ticktables = pd.read_pickle('/data/proc/reference_data/Ticktables.pkl')
        ticktable = ticktables[info['Segment_ID'].item()][info['TickTable'].astype('int').item()][info['Curr'].item()]
        reg_hours = pd.Series(False, index=msgs.index)
        sess_id = pd.Series(np.nan, index=msgs.index)
        i = 1
        while i <= info['Sess_Max_N'].iloc[0]:
            sess_st = pd.to_datetime(info['Sess_St_%s' % i].iloc[0])
            sess_end = pd.to_datetime(info['Sess_End_%s' % i].iloc[0])
            # Session starts on first inbound in New Order / New Quote in regular hours
            sess_st_id = msgs.index[(msgs['MessageTimestamp'] > sess_st) & (msgs['MessageType'].isin({'D', 'S'}))][0]
            # Session ends on last outbound in regular hours
            sess_end_id = msgs.index[(msgs['MessageTimestamp'] < sess_end) & (msgs['MessageType'].isin({'8'}))][-1]
            sess_msgs = ((msgs.index >= sess_st_id) & (msgs.index <= sess_end_id))
            sess_id[sess_msgs] = i
            reg_hours = reg_hours | sess_msgs
            i += 1
    
        post_open_auction = msgs.PostOpenAuction
        msgs, top = msgs.loc[post_open_auction & reg_hours], top.loc[post_open_auction & reg_hours]
        
        msgs, top = PrepData.prepare_data(msgs, top, ticktable, price_factor, sess_id)
        msgs['TradePos'] = top['TradePos']
        me_cols = ['UniqueOrderID', 'EventNum', 'MessageType', 'ExecType', 'UnifiedMessageType', 
                    'LeavesQty', 'ExecutedPrice', 'ExecutedQty', 'TradeMatchID', 'TradePos', 'FirmNum']
        me = msgs.loc[msgs['EventNum'].notnull() & ((msgs['MessageType'] == '8') | 
                     (msgs['MessageType'] == '9')), me_cols].copy()
        me = me.reset_index()
        me = me.set_index(['UniqueOrderID', 'EventNum'])
        me = me.sort_index(level=0)
    
        Output = race_recs.iloc[1:].apply(decompose_active_qty, args = (msgs, top_15, me), axis=1)
        Output.insert(loc = 0, column = 'InstrumentID', value = sym) 
        Output.insert(loc = 0, column = 'Date', value = date) 
        if not os.path.isdir(f'/data/proc/data_analysis/active_qty_decompose/temp/{date}/'):
          os.mkdir(f'/data/proc/data_analysis/active_qty_decompose/temp/{date}/')
        Output.to_csv(f'/data/proc/data_analysis/active_qty_decompose/temp/{date}/decompose_active_qty_{date}_{sym}.csv.gz', compression='gzip')
        logger.info('Finish processing: '+str(symdate))
        return Output 
    except:
        logger.critical('Critical Error: '+str(symdate))
        return None
    
if __name__ == "__main__":
    out_dir = '/data/proc/data_analysis/active_qty_decompose/output/DecomposeActiveQty.csv.gz'
    pairs = pd.read_pickle('/data/proc/reference_data/PairsOrdered.pkl')

    num_worker = 30
    pool = multiprocessing.Pool(num_worker)
    results = pool.map(process_sym_date, pairs)
    active_qty_decomposed = pd.concat(results).reset_index(drop=True)
    active_qty_decomposed.to_csv(out_dir, compression='gzip')