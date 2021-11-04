'''
code_snippet_cancel_activities.py

This snippet is the code we used to analyze the cancel activities in races. It produces the
statistics for the Cancel Attempt Rate Columns in Table 4.11. The code is specific to the LSE
settings and may not be applicable to other exchange message data directly. 

We provide this code for the purpose of disclosing how we did the analysis and helping our user 
reproduce the related results. Note that the coding convention (e.g., naming) in this script 
is slightly different from the code in /PythonCode, because this is part of an earlier version of 
our code.

To reproduce the Cancel Attempt Rate Columns in Table 4.11, users need to
    1. Go through the code snippet carefully and understand the logic.
    2. Adapt/rewrite the code to their context depending on the specific details of their setting.
       Users can make use of this code snippet in this step.
    3. Execute the code to obtain the statistics for cancel activities for each firm group. 
       The output from the code should contain the number of cancel attempts within T by each firm
       for a wide range of T.
    4. Compute the cancel attempt rate for each firm group based on the number of cancel attempts 
       in races and the number of races where they provide liquidity. The number of races where
       a group of firms provide liquidity can be computed following code_snippet_trade_level_data.py.
       Users have to do this by themselves.
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
import traceback
from LatencyArbitrageAnalysis.utils.Dtypes import dtypes_msgs, dtypes_top
from LatencyArbitrageAnalysis.RaceDetection.Race_Msg_Outcome import get_msg_outcome
warnings.filterwarnings("ignore")
PrepData = importlib.import_module('Prep_Race_Data')
base = '/data/workspace/gatewaydata/workingarea/proc/'

now = datetime.datetime.now()
runtime = '%02d%02d%02d_%02d%02d' % (now.year, now.month, now.day, now.hour, now.minute)
logpath = base+'data_analysis/cancel_activities/logs/' 
logger = logging.getLogger('cancel_activities')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logpath + f'/cancel_activities_{runtime}.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def cancel_activities(race, msgs, firm_list, me):
    '''
    Given a race, this function returns cancel activity stats in the race

    param:
        race: a row in the race record dataset
        msgs: msgs dataset
        firm_list: list of firm IDs of interest
        me: outbound messages in the msgs dataset 
    output: 
        pd.Series.
    '''
    # extract the information about the race, including the msgs in race, 
    # race price & side, and race starting time
    race_msgs = msgs.loc[race['ix_race']]
    race_start_time = race_msgs['MessageTimestamp'].iloc[0]
    S = race['S']
    P_Signed = race['P']
    race_msgs[f'{S}RaceRlvtMsgOutcome'] = get_msg_outcome(S, P_Signed, race_msgs, False) # Strict fail = False
    Sign = 1 * (S == 'Ask') - 1 * (S == 'Bid')
    price_factor = int(1e8)
    P = Sign * P_Signed
    P_GBP = (P/price_factor)/100

    # initialize output data structure. The output is a row with following columns
    # t_list is a list of time windows to search for cancels
    t_list = ['1ms','3ms', '5ms', '10ms', '100ms' , '1s', '10s', '60s' , '1d']
    output = {'SingleLvlRaceID':race['SingleLvlRaceID'], 'RacePrice':P_GBP, 'Side': S}
    for firm in [int(i) for i in firm_list]:
        for t in t_list:
            output[f'N_attempt_to_cancel_in_{t}_by_{firm}'] = 0

    liq_providers = []
    # loop over race messages 
    for _, race_msg in race_msgs.iterrows():

        # get the firm id, firm group, race message type and race outcome of the race message
        firm = race_msg['FirmNum']
          
        RaceRlvtType = race_msg[f'{S}RaceRlvtType'] # 'Cancel Attempt', 'Take Attempt'
        outcome = race_msg[f'{S}RaceRlvtMsgOutcome'] # 'Success', 'Fail', 'Unknown'. 

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
                    else:
                        # get the firm id of the counterparty
                        counterpart_id = counterpart_msg['FirmNum'].iloc[0]
                        liq_providers.append(counterpart_id)
                        for t in t_list:
                            canc_attempt_msg = msgs[(msgs['FirmNum'] == counterpart_id) & 
                                                    (msgs['MessageTimestamp'] >= race_start_time) & (msgs['MessageTimestamp'] <= (race_start_time + pd.Timedelta(t).to_timedelta64())) &
                                                    (msgs['%sRaceRlvtType' % S] == 'Cancel Attempt') & (msgs['%sRaceRlvtPriceLvlSigned' % S] == P_Signed) &
                                                    (msgs['UniqueOrderID'] == counterpart_msg['UniqueOrderID'].iloc[0])]
                            output[f'Attempt_to_cancel_in_{t}_by_{counterpart_id}'] = 1 if canc_attempt_msg.shape[0] > 0 else 0
                            
    output_df = {}
    for firm in set(liq_providers):
        output_df[firm] = [race['SingleLvlRaceID'], P_GBP, S] + [output[f'Attempt_to_cancel_in_1ms_by_{firm}'], 
                                                                 output[f'Attempt_to_cancel_in_3ms_by_{firm}'], 
                                                                 output[f'Attempt_to_cancel_in_5ms_by_{firm}'], 
                                                                 output[f'Attempt_to_cancel_in_10ms_by_{firm}'],
                                                                 output[f'Attempt_to_cancel_in_100ms_by_{firm}'],
                                                                 output[f'Attempt_to_cancel_in_1s_by_{firm}'],
                                                                 output[f'Attempt_to_cancel_in_10s_by_{firm}'],
                                                                 output[f'Attempt_to_cancel_in_60s_by_{firm}'],
                                                                 output[f'Attempt_to_cancel_in_1d_by_{firm}']]
    col_names = ['SingleLvlRaceID', 'Race_Price', 'Side'] + [f'Attempt_to_cancel_in_{t}' for t in t_list]
    output_df = pd.DataFrame.from_dict(output_df, orient='index',  columns=col_names).reset_index()
    output_df = output_df.rename(columns={'index': 'FirmNum'})
    return output_df

def process_sym_date(symdate):
    date, sym  = symdate
    try:
        infile_msgs = base+'data/clean/%s/CleanMsgData_%s_%s.csv.gz' % (date, date, sym)
        infile_top = base+'data/book/%s/BBO_%s_%s.csv.gz' % (date, date, sym)
        infile_race_records = base+'output/race_stats/%s/%s/Race_Recs_%s_%s.pkl' % ('daily_500us', date, date, sym)
        price_factor = int(1e8)
    
        # Firms are sorted based on the proportion of races won in the whole sample
        RankData = pd.read_csv(base+'/data_analysis/cancel_activities/firm_prevelance_FTSE350.csv')
        firm_list = RankData['FirmNum'].to_list()
        msgs = pd.read_csv(infile_msgs, dtype = dtypes_msgs, parse_dates=['MessageTimestamp'])
        top = pd.read_csv(infile_top, index_col = 0, dtype = dtypes_top, parse_dates = ['MessageTimestamp'])
        race_recs = pd.read_pickle(infile_race_records)
        if race_recs.shape[0]==1:
            logger.info('Finish processing: '+str(symdate))
            return None
    
        info = pd.read_csv(base+'reference_data/Symbol_Date_Info.csv')
        info = info.loc[(info['InstrumentID'].astype('str') == sym) & (info['Date'] == date)]
        ticktables = pd.read_pickle(base+'reference_data/Ticktables.pkl')
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
    
        Output = race_recs.iloc[1:].apply(cancel_activities, args = (msgs, firm_list, me), axis=1).tolist()
        Output = pd.concat(Output)
        Output.insert(loc = 0, column = 'InstrumentID', value = sym) 
        Output.insert(loc = 0, column = 'Date', value = date) 
        if not os.path.isdir(f'{base}data_analysis/cancel_activities/temp/{date}/'):
          os.mkdir(f'{base}data_analysis/cancel_activities/temp/{date}/')
        Output.to_csv(f'{base}data_analysis/cancel_activities/temp/{date}/cancel_activities_{date}_{sym}.csv.gz', compression='gzip', index = False)
        logger.info('Finish processing: '+str(symdate))
        return Output 
    except:
        logger.critical('Critical Error: '+str(symdate))
        traceback.print_exc()
        return None
      
if __name__ == "__main__":
    out_file = base+'data_analysis/cancel_activities/output/CancelActivity.csv.gz'
    pairs = pd.read_pickle(base+'reference_data/PairsOrdered.pkl')

    num_worker = 25
    pool = multiprocessing.Pool(num_worker)
    random.shuffle(pairs)
    results = pool.map(process_sym_date, pairs)

    output = pd.concat([pd.read_csv(f'{base}data_analysis/cancel_activities/temp/{date}/cancel_activities_{date}_{sym}.csv.gz')\
      for (date, sym) in pairs if os.path.isfile(f'{base}data_analysis/cancel_activities/temp/{date}/cancel_activities_{date}_{sym}.csv.gz')])
    output.to_csv(out_file, index=False)
    
