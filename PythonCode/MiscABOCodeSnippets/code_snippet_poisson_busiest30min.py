
'''
code_snippet_poisson_busiest30min.py

This snippet is the code we used to compute the arrival rate of potentially-race-relevant 
messages for each 30-minute sessions of the trading day. It produces the statistics for the 
busiest 30 minutes Poisson exercise (Table 4.5). The code is specific to the LSE settings
and may not be applicable to other exchange message data directly. 

We provide this code for the purpose of disclosing how we did the analysis and helping our user 
reproduce the related results. Note that the coding convention (e.g., naming) in this script 
is slightly different from the code in /PythonCode, because this is part of an earlier version of 
our code.

To reproduce the busiest 30 minutes Poisson exercise (Busiest 30 Min columns in Table 4.5 and
Appendix Table B.7), users need to
    1. Go through the code snippet carefully and understand the logic.
    2. Adapt/rewrite the code to their context depending on the specific details of their setting.
       Users can make use of this code snippet in this step.
    3. Execute the code to obtain the arrival rate of potentially-race-relevant 
       messages for each 30-minute sessions of the trading day.
    4. Find the busiest 30 minutes arrival rate for each symbol-date. Users have to do 
       this by themselves.
    5. Compute the expected number of potential race activities based on the busiest 30 minutes 
       arrival rate. Users can refer to write.poisson.tables in /RCode/Functions.R.
'''
###################################################################################

# Import packages
import pandas as pd
import numpy as np
import datetime
import logging
import pickle

import sys
import os
import importlib

base = '/data/workspace2/gatewaydata/workingarea/proc/'

sys.path.insert(1, base + '/code/')
PrepData = importlib.import_module('04a_Prep_Race_Data')

# Collect arguments
runtime, date, sym = sys.argv[1], sys.argv[2], sys.argv[3]

# Initialize log
logpath = base + '/code/logs/%s/' % runtime
if not os.path.exists(logpath):
  os.makedirs(logpath)
class LoggerWriter(object):
  def __init__(self, level):
    self.level = level
  def write(self, message):
    for line in message.rstrip().splitlines():
      self.level(line.rstrip())
  def flush(self):
    self.level(sys.stderr)
logger = logging.getLogger(__name__)
sys.stdout = LoggerWriter(logger.warning)
sys.stderr = LoggerWriter(logger.error)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logpath + 'Temp_Counter_%s_%s_%s.log' % (runtime, date, sym))
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


### INITIALIZE ###
logger.info('Processing: %s %s' % (date, sym))

# Specify paths
infile_msgs = base + '/data/clean/%s/CleanMsgData_%s_%s.csv.gz' % (date, date, sym)
infile_depth = base + '/data/book/%s/DepthInfo_%s_%s.pkl' % (date, date, sym)
infile_top = base + '/data/book/%s/BBO_%s_%s.csv.gz' % (date, date, sym)
outfile_stats = base + '/output/poisson_by_session/%s/Counters_%s_%s.pkl' % (date, date, sym)

# Start timer
timer_st = datetime.datetime.now()

# Add info to log
logger.info('Timer Start: %s' % str(timer_st))

### LOAD DATA ###
logger.info('Loading data...')

# Load symbol-date info
info = pd.read_csv(base + '/reference_data/Symbol_Date_Info.csv')
info = info.loc[(info['InstrumentID'].astype('str') == sym) & (info['Date'] == date)]

# Load ticktables
ticktables = pd.read_pickle(base + '/reference_data/Ticktables.pkl')
ticktable = ticktables[info['Segment_ID'].item()][info['TickTable'].astype('int').item()][info['Curr'].item()]

# Load message data
dtypes_df = {'Source': 'O', 'SourceID': 'int64', 'StreamID': 'O','ConnectionID': 'O',
             'GatewayIPAddress': 'O', 'GatewayPort': 'O', 'ID': 'int64', 'MessageTimestamp': 'O',
             'UserID': 'O', 'MessageType': 'O', 'InstrumentID': 'O', 'ClientOrderID': 'O',
             'OrderID': 'O', 'OrderStatus': 'O', 'OrderType': 'O', 'PublicOrderID': 'O',
             'ExecType': 'O', 'TIF': 'O', 'ClOrdLinkID': 'O', 'ExpireDateTime': 'O',
             'RawSide': 'O', 'OrderQty': 'float64', 'DisplayQty': 'float64',
             'LimitPrice': 'int64', 'Capacity': 'O', 'OrderSubType': 'O', 'StoppedPrice': 'int64',
             'Anonymity': 'O', 'PassiveOnlyOrder': 'O', 'OriginalClientOrderID': 'O',
             'BidPrice': 'int64', 'BidSize': 'float64', 'AskPrice': 'int64',
             'AskSize': 'float64', 'ExecutionID': 'O', 'OrderRejectCode': 'O',
             'ExecutedPrice': 'int64', 'ExecutedQty': 'float64', 'LeavesQty': 'float64',
             'Container': 'O', 'TradeMatchID': 'O', 'TransactTime': 'O', 'TypeOfTrade': 'O',
             'MinQty': 'float64', 'DisplayMethod': 'O', 'PriceDifferential': 'O',
             'CancelRejectReason': 'O', 'Symbol_Type': 'O', 'Segment_ID': 'O', 'Symbol': 'O',
             'Date': 'O', 'Timestamp': 'O', 'FirmClass': 'O', 'FirmNum': 'float64',
             'UserNum': 'float64', 'OrderNum': 'float64', 'QuoteRelated': 'bool',
             'UniqueOrderID': 'O', 'Side': 'O', 'UnifiedMessageType': 'O',
             'PrevPriceLvl': 'int64', 'PrevQty': 'float64', 'PriceLvl': 'int64',
             'Classified': 'bool', 'EventNum': 'float64', 'Event': 'O', 'MinExecPriceLvl':'int64',
             'MaxExecPriceLvl':'int64', 'PrevBidPriceLvl': 'int64', 'PrevBidQty': 'float64',
             'BidPriceLvl': 'int64', 'BidClassified': 'bool', 'BidEventNum': 'float64',
             'BidEvent': 'O', 'BidMinExecPriceLvl':'int64', 'BidMaxExecPriceLvl':'int64',
             'PrevAskPriceLvl': 'int64', 'PrevAskQty': 'float64', 'AskPriceLvl': 'int64',
             'AskClassified': 'bool', 'AskEventNum': 'float64', 'AskEvent': 'O',
             'MinExecPriceLvl':'int64', 'MaxExecPriceLvl':'int64','PostOpenAuction':'bool', 'OpenAuction':'bool'}
             
df = pd.read_csv(infile_msgs, dtype = dtypes_df, parse_dates=['MessageTimestamp', 'Timestamp', 'ExpireDateTime', 'TransactTime'])

# Load top-of-book data
dtypes_top = {'Source': 'O', 'ID': 'int64', 'MessageTimestamp': 'O', 'Side': 'O',
              'UnifiedMessageType': 'O', 'PrevBidPriceLvl': 'int64', 'BidPriceLvl': 'int64',
              'PrevAskPriceLvl': 'int64', 'AskPriceLvl': 'int64', 'PrevPriceLvl': 'int64',
              'PriceLvl': 'int64', 'PriceDifferential': 'O', 'BestBid': 'int64',
              'BestBidQty': 'float64', 'BestBidQtyWithFQte': 'float64', 'BestAsk': 'int64',
              'BestAskQty': 'float64', 'BestAskQtyWithFQte': 'float64', 'Spread': 'int64',
              'MidPt': 'int64', 'last_BestBid': 'int64', 'last_BestAsk': 'int64',
              'last_MidPt': 'int64', 't_last_chg_BestBid': 'O', 't_last_chg_BestAsk': 'O',
              't_last_chg_MidPt': 'O', 'AskCorrections_notA': 'O', 'BidCorrections_notA': 'O',
              'Corrections_OrderAccept': 'O', 'Corrections_PriceDiff': 'O', 'Corrections_Trade': 'O',
              'DepthKilled': 'float64', 'BestBid_TickSize': 'float64', 'BestAsk_TickSize': 'float64',
              'Diff_TickSize': 'O', 'f_Regular_Hours': 'O', 'Trade_Pos': 'O', 'BookUpdateParentMsgID': 'int64'}
top = pd.read_csv(infile_top, index_col = 0, dtype = dtypes_top, parse_dates = ['MessageTimestamp', 't_last_chg_MidPt'])

# Load other order book data
depth = pickle.load(open(infile_depth, 'rb'))

# Clean timestamps
df['MessageTimestamp'] = pd.to_datetime(df['MessageTimestamp'])

# Restrict to regular hours
# Start with all False and then for each group of session messages update reg_hours by index to True 
# if the messages belong to the session. Outcome is a True/False flag for every message on whether they
# belong to a session in regular hours
reg_hours = pd.Series(False, index=df.index)
sess_id = pd.Series(np.nan, index=df.index)
i = 1
while i <= info['Sess_Max_N'].item():
    sess_st = pd.to_datetime(info['Sess_St_%s' % i].item())
    sess_end = pd.to_datetime(info['Sess_End_%s' % i].item())
    # Session starts on first inbound in New Order / New Quote in regular hours
    sess_st_id = df.index[(df['MessageTimestamp'] > sess_st) & (df['MessageType'].isin({'D', 'S'}))][0]
    # Session ends on last outbound in regular hours
    sess_end_id = df.index[(df['MessageTimestamp'] < sess_end) & (df['MessageType'].isin({'8'}))][-1]
    sess_msgs = ((df.index >= sess_st_id) & (df.index <= sess_end_id))
    sess_id[sess_msgs] = i
    reg_hours = reg_hours | sess_msgs
    i += 1

# Keep regular hours
df, top = df.loc[reg_hours], top.loc[reg_hours]

# Keep post open auction
post_open_auction = df.PostOpenAuction 
df, top = df.loc[post_open_auction], top.loc[post_open_auction]

# Currency conversion (FTSE 350 is in GBX)
price_factor, to_GBX, to_GBP = 100000000, 1, .01

reaction_time = np.timedelta64(29, 'us')
df_races, top_races = PrepData.PrepareData(df, top, ticktable, price_factor, sess_id, reaction_time)
df_races['N_Inbound_NBBO'] = 0
df_races.loc[((df_races['AskRaceRlvtType'] == 'Take Attempt') & (df_races['AskRaceRlvtPriceLvlSigned'] >= top_races['BestAskSigned'])) |
             ((df_races['BidRaceRlvtType'] == 'Take Attempt') & (df_races['BidRaceRlvtPriceLvlSigned'] >= top_races['BestBidSigned'])) |
             ((df_races['AskRaceRlvtType'] == 'Cancel Attempt') & (df_races['AskRaceRlvtPriceLvlSigned'] == top_races['BestAskSigned'])) |
             ((df_races['BidRaceRlvtType'] == 'Cancel Attempt') & (df_races['BidRaceRlvtPriceLvlSigned'] == top_races['BestBidSigned'])), 'N_Inbound_NBBO'] = 1

df_races['N_Inbound'] = 0
df_races.loc[df_races['MessageType'].isin(['D','F', 'q', 'G', 'S', 'C', 'H', 'Z', 's', 'u']), 'N_Inbound'] = 1

df_races['N_Msgs'] = 1

## Count # relevant msgs by session
df_races = df_races.set_index('MessageTimestamp')
stats = df_races[['N_Msgs','N_Inbound','N_Inbound_NBBO']].resample('30Min', how='sum')
stats = stats.reset_index()
stats['Session'] = stats['MessageTimestamp'].dt.time
# Metadata
stats['Date'] = date
stats['InstrumentID'] = sym

pickle.dump(stats, open(outfile_stats, 'wb'))
 
 # End timer
timer_end = datetime.datetime.now()

# Add info to log
logger.info('Complete.')
logger.info('Timer End: %s' % str(timer_end))
logger.info('Time Elapsed: %s' % str(timer_end - timer_st))
