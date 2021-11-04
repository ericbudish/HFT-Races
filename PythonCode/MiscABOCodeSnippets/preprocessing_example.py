'''
preprocessing_example.py

This script is an example of pre-processing message data before applying the 
python code. Please refer to Section 3 of the Code and Data Appendix. For the 
meaning of the values of each column in the LSE message data, please refer to 
"London Stock Exchange MIT201 - Guide to the Trading System Issue 12.3"
(https://web.archive.org/web/20150703141903/https://www.londonstockexchange.com/products-and-services/trading-services/guide-to-new-trading-system.pdf)

Note that this script may not be directly used by researchers using message data 
from other exchanges or from LSE but during a different time period, as LSE makes 
changes to the trading protocol from time to time. We provide this script as an 
example and hope it can help the users of our code pre-process their message data.

'''
import pandas as pd
import numpy as np
import os
import multiprocessing
import datetime
import random
import sys

base = os.path.expanduser('~') + '/Dropbox/Project - HFT Measurement of the Arms Race/FCA Collaboration - confidential/Materials for Data Appendix/PackageTesting'
path = base + '/Testing/'
path_reference_data = path + '/ReferenceData/'
file_symdates = path_reference_data + '/Sample_All_SymDates.csv.gz'

def preprocess_msgs(date, sym, in_dir, out_dir):
    infile_msgs = '%s/%s/CleanMsgData_%s_%s.csv.gz' % (in_dir, date, date, sym)
    # Note: variable names and data types in user's data may be different from the ones below.
    dtypes_msgs = {'Source': 'O', 'SourceID': 'int64', 'StreamID': 'O','ConnectionID': 'O',
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
             'MinExecPriceLvl':'int64', 'MaxExecPriceLvl':'int64','PostOpenAuction':'bool','OpenAuction':'bool','AuctionTrade':'bool'}
    msgs = pd.read_csv(infile_msgs, dtype = dtypes_msgs, parse_dates=['MessageTimestamp'])
    msgs.drop(['Symbol'], axis=1, inplace=True)
    msgs = msgs.rename(columns = {'FirmNum':'FirmID','OpenAuction':'OpenAuctionTrade',
                                  'StoppedPrice':'StopPrice','InstrumentID':'Symbol',
                                  'OriginalClientOrderID':'OrigClientOrderID',
                                  'OrderID':'MEOrderID'})
    
    ### Get source format (if the exchange has multiple message formats, eg. Fix/native
    is_ntv, is_fix = msgs['Source'] == 'Native', msgs['Source'] == 'FIX'
    
    ### MessageType
    MessageTypes = {'D':'New_Order', 'F':'Cancel_Request','q':'Mass_Cancel_Request','G':'Cancel_Replace_Request',
                    'S':'New_Quote', '8':'Execution_Report','9':'Cancel_Reject','r':'Mass_Cancel_Report',
                    '3':'Other_Reject','j':'Other_Reject'}
    msgs['MessageType'] = msgs['MessageType'].map(MessageTypes)
    
    ### OrderType
    OrderTypes = {'1':'Market', '2':'Limit', '3':'Stop', '4':'Stop_Limit', 'P':'Pegged'}
    msgs['OrderType'] = msgs['OrderType'].map(OrderTypes)
    # Set native messages pegged orders based on OrderSubType
    msgs.loc[is_ntv & (msgs['MessageType'] == 'New_Order') & (msgs['OrderType'].isin({'Market','Limit'})) & (msgs['OrderSubType'] == '5'), 'OrderType'] = 'Pegged'
    # Set passive-only orders to OrderType Passive_Only
    msgs.loc[(msgs['MessageType'] == 'New_Order') & (msgs['OrderType'] == 'Limit') & (msgs['PassiveOnlyOrder'].notnull()), 'OrderType'] = 'Passive_Only'
    msgs.loc[msgs['MessageType'] != 'New_Order', 'OrderType'] = np.nan
    
    ### TIF - check data type
    TIFs_ntv = {'5':'GFA','10':'GFA','12':'GFA','50':'GFA','51':'GFA','52':'GFA',
                '3':'IOC','4':'FOK',
                '0':'GoodTill','6':'GoodTill','7':'GoodTill'}
    TIFs_fix = {'2':'GFA','7':'GFA','9':'GFA','8':'GFA','C':'GFA',
                '3':'IOC','4':'FOK',
                '0':'GoodTill','6':'GoodTill'}
    msgs.loc[is_ntv, 'TIF'] = msgs['TIF'].map(TIFs_ntv)
    msgs.loc[is_fix, 'TIF'] = msgs['TIF'].map(TIFs_fix)
    
    ### ExecType
    ExecTypes = {'0':'Order_Accepted','8':'Order_Rejected','F':'Order_Executed',
                 'C':'Order_Expired','4':'Order_Cancelled','5':'Order_Replaced',
                 'D':'Order_Restated','9':'Order_Suspended'}
    msgs['ExecType'] = msgs['ExecType'].map(ExecTypes)
    
    ### Cancel Reject Reason # others are NA. check whether this is fine
    msgs.loc[is_ntv & (msgs['MessageType'] == 'Cancel_Reject') & (msgs['CancelRejectReason'] == '2000'), 'CancelRejectReason'] = 'TLTC'
    msgs.loc[is_fix & (msgs['MessageType'] == 'Cancel_Reject') & (msgs['CancelRejectReason'] == '1'), 'CancelRejectReason'] = 'TLTC'
    msgs.loc[(msgs['MessageType'] == 'Cancel_Reject') & (msgs['CancelRejectReason'] != 'TLTC'), 'CancelRejectReason'] = 'Other'
    
    ### OrderStatus
    OrderStatus = {'1':'Partial_Fill','2':'Full_Fill'}
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed'), 'OrderStatus'] = msgs['OrderStatus'].map(OrderStatus)
    msgs.loc[(~msgs['OrderStatus'].isin(['Partial_Fill','Full_Fill'])), 'OrderStatus'] = np.nan
    
    ### TradeInitiator: A P Other 
    # Set auction trades to Other
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['AuctionTrade'] == True), 'TradeInitiator'] = 'Other'
    # Set TradeInitiator for native msgs
    msgs.loc[is_ntv & (msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['AuctionTrade'] == False), 'TradeInitiator'] = 'Other'
    msgs.loc[is_ntv & (msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['AuctionTrade'] == False) & (msgs['Container'].isin({'1'})), 'TradeInitiator'] = 'Passive'
    msgs.loc[is_ntv & (msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['AuctionTrade'] == False) & (msgs['Container'].isin({'0','3'})), 'TradeInitiator'] = 'Aggressive'
    # Set TradeInitiator for fix msgs
    msgs.loc[is_fix & (msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['AuctionTrade'] == False), 'TradeInitiator'] = 'Other'
    msgs.loc[is_fix & (msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['AuctionTrade'] == False) & (msgs['TypeOfTrade'].isin({'0','1'})), 'TradeInitiator'] = 'Passive'
    msgs.loc[is_fix & (msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['AuctionTrade'] == False) & (msgs['TypeOfTrade'].isin({'2'})), 'TradeInitiator'] = 'Aggressive'
    
    
    ### Price factor conversion 
    # Set Int values to dtype Int64 to allow np.nan mixed with int64
    # Set missing prices to NA
    prices = ['LimitPrice', 'StopPrice', 'ExecutedPrice', 'BidPrice', 'AskPrice']
    for col in prices:
        msgs[col] = msgs[col]/1e8
        msgs.loc[msgs[col] <= 0, col] = np.nan
    ### Make SessionID and RegularHour
    # Load symbol-date info
    # Note: Depending on the format of the user's reference data, 
    # it is likely that the way to add session ID info and regular hours info to
    # the message data is different from below
    info = pd.read_csv(base+'/Testing/ReferenceData/Symbol_Date_Info.csv')
    info = info.loc[(info['InstrumentID'].astype('str') == sym) & (info['Date'] == date)]
    # Generate reg hour and session ID
    reg_hours = pd.Series(False, index=msgs.index)
    sess_id = pd.Series(np.nan, index=msgs.index)
    i = 1
    while i <= info['Sess_Max_N'].iloc[0]:
        sess_st = pd.to_datetime(info['Sess_St_%s' % i].iloc[0])
        sess_end = pd.to_datetime(info['Sess_End_%s' % i].iloc[0])
        # Session starts on first inbound in New Order / New Quote in regular hours
        sess_st_id = msgs.index[(msgs['MessageTimestamp'] > sess_st) & (msgs['MessageType'].isin({'New_Order', 'New_Quote'}))][0]
        # Session ends on last outbound in regular hours
        sess_end_id = msgs.index[(msgs['MessageTimestamp'] < sess_end) & (msgs['MessageType'].isin({'Execution_Report'}))][-1]
        sess_msgs = ((msgs.index >= sess_st_id) & (msgs.index <= sess_end_id))
        sess_id[sess_msgs] = i
        reg_hours = reg_hours | sess_msgs
        i += 1
    post_open_auction = msgs['PostOpenAuction']
    msgs['RegularHour'] = reg_hours & post_open_auction
    msgs['SessionID'] = sess_id
    
    ### Add unique order id
    msgs = add_UniqueOrderID(msgs)
    
    ###  add quote identifier
    msgs['QuoteRelated'] = msgs['UniqueOrderID'].str[-2:] == 'QR'
    
    ### Flag for FIX Quote, since we are missing their outbounds.
    ### Note: this is a specific problem to our LSE dataset and users may not need to 
    ### to do this.
    msgs['Flag_FixQuote'] = (msgs['QuoteRelated']) & (msgs['Source'] == 'FIX')
    
    ### Replace mass cancel and mass cancel report
    msgs.loc[(msgs['MessageType']=='Mass_Cancel_Request'), 'MessageType'] = 'Other_Inbound'
    msgs.loc[(msgs['MessageType']=='Mass_Cancel_Report'),  'MessageType'] = 'Other_Outbound'

    ### Get the columns
    msgs['Date'] = date
    msgs['Symbol'] = sym
    cols = [
        'Date','Symbol',
        'ClientOrderID', 'UniqueOrderID','TradeMatchID', 'SessionID', 'MEOrderID','OrigClientOrderID',
        'UserID', 'FirmID', 'FirmClass', 
        'MessageTimestamp', 'Side', 'MessageType', 'OrderType', 'ExecType', 
        'OrderStatus', 'TradeInitiator', 'TIF', 'CancelRejectReason', 
        'LimitPrice', 'OrderQty', 'DisplayQty', 'ExecutedPrice', 'StopPrice', 'ExecutedQty', 'LeavesQty', 
        'QuoteRelated', 'BidPrice', 'BidSize', 'AskPrice', 'AskSize',
        'OpenAuctionTrade', 'AuctionTrade', 'RegularHour', 'Flag_FixQuote'
    ]

    if not os.path.exists('%s/%s'%(out_dir, date)):
        os.makedirs('%s/%s'%(out_dir, date))
    
    msgs[cols].to_csv('%s/%s/Raw_Msg_Data_%s_%s.csv.gz' % (out_dir, date, date, sym), index=False, compression = 'gzip')    

    return date, sym

def add_UniqueOrderID(msgs):
    '''
    Add unique order id to the msgs dataframe. This is necessary because the order identifiers in the raw data may not be ideal: 
    some of them are not populated for all messages in the same order (MEOrderID is only populated for outbounds, missing in inbounds) and
    some of them are not fixed during the lifetime of an order (ClientOrderID changes when user modifies the order). Thus, we need to assign 
    an order identifier that is populated for all messages in the order and does not change when user modifies the order. 

    Reference: Section 3.2.1 of the code and data appendix.
    
    Note that this function might not apply to other exchanges. User needs to rewrite this function if necessary, based on the specific institution 
    details of the exchange in the user' data. This function can serve as an example of how to generate UniqueOrderID. The requirement is that the UniqueOrderID 
    is populated for all messages in the order and does not change when user modifies the order. The UniqueOrderID column should never be missing. 
    If the user's data contains the columns we use (listed below) to generate our unique order id, he/she may directly apply this function 
    for generating UniqueOrderID, but the user must make sure that the variables they use fit the descriptions below and the logic behind the function 
    can indeed generate a unique order id with the desired property (does not change throughout the life time of an order) under the rules of the 
    exchange in the user's data.
     
    Input: sym-date level message dataset
    
    Used columns are:
    'Source' : In many exchanges there are multiple interfaces (different message format). 
               This field indicates which interface the message is from. Note that messages from all interfaces 
               for a sym-date should be combined into a single message dataframe and be processed together.
    'ClientOrderID': ClientOrderID is provided by users when submitting new orders. They can also change the 
                     ClientOrderID of an existing order when modifying the order. We assume that a user uses 
                     unique ClientOrderID when submitting new orders and modifying orders. This uniqueness
                     is suggested by LSE and it is required in other exchanges such as NYSE.
    'OrigClientOrderID': In cancel and cancel/replace requests users use OrigClientOrderID or MEOrderID
                             to refer to the order they are trying to cancel/modify. When MEOrderID is populated
                             in a cancel or cancel/replace request, this field is ignored.
    'MEOrderID': MEOrderID is created by the matching engine after users submit new orders. It is populated in 
               matching engine messages (outbounds) and cancel and cancel/replace messages. 
    'UserNum': In raw data users have a UserID. We enumerate all users and use index to refer to users

    The procedure:  We loop over all messages for each User-MessageFormat pair. For new orders and mass cancel requests, 
    we start a new UniqueOrderID. For cancel requests, cancel/replace requests, execution reports, rejection messages, and mass cancel reports, 
    in cases where the MEOrderID is populated with a MEOrderID we have seen before, we assign the UniqueOrderID of the earlier message 
    with the same MEOrderID to the current message. Otherwise, we assign the UniqueOrderID of the earlier message with the same
    ClientOrderID/OrigClientOrderID to the current message. The handling of those order is slightly different, please refer to the comments
    below to see the difference). We index all orders from the same user by integers (OrderNum). All quote related messages from 
    the same user are in the same order (OrderNum='QR').
    
    The format of the unique order id is Source_UserNum_OrderNum. 
    
    Return: Message dataframe with UniqueOrderID added.
    '''
    msgs['OrderNum'] = None
    msgs['UniqueOrderID'] = None
    originalid_orderid_conflict = 0
    no_inbound_outbound_id = 0

    # Loop over users:
    for _, user_msgs in msgs.groupby('UserID'):
        # Initialize counter
        counter = 0        
        # Loop over Native and FIX messages
        for source in set(msgs['Source'].unique()):

            # Initialize sets of previously observed IDs and message times
            prev = {'MEOrderID': {}, 'ClientOrderID': {}}
            time = {'MEOrderID': {}, 'ClientOrderID': {}}

            # Loop over user messages from source
            for i in user_msgs.loc[user_msgs['Source'] == source].index:
                
                order_id = msgs.at[i, 'MEOrderID']
                client_order_id = msgs.at[i, 'ClientOrderID']
                original_client_order_id = msgs.at[i, 'OrigClientOrderID']

                # Gateway New Quote
                # For a given user, all quotes will have the same UniqueOrderID
                if msgs.at[i, 'MessageType'] == 'New_Quote':
                    msgs.at[i, 'UniqueOrderID'] = '%s_%06d_QR' % (source, msgs.at[i, 'UserNum'])
                    continue

                # Gateway New Order and Gateway Mass Cancel 
                # Increment counter then set OrderNum to counter
                elif msgs.at[i, 'MessageType'] in {'New_Order',  'Mass_Cancel_Request'}:
                    counter += 1
                    msgs.at[i, 'OrderNum'] = counter

                # Gateway Cancel/Replace and Gateway Cancel
                # C/R and Cancel should only happen if there has been a previous Outbound ID.
                # Else conditions are added for cases in which 
                # there was no previous outbound message due to pack loss
                elif msgs.at[i, 'MessageType'] in {'Cancel_Replace_Request', 'Cancel_Request'}:
                
                    # Case 1: User references both OrigClientOrderID and the ME MEOrderID
                    if pd.notnull(order_id) & pd.notnull(original_client_order_id):
                        # If both have already been seen, use the order number from the order ID.
                        # Otherwise use MEOrderID or ClientOrderID depending on which has been seen
                        if (order_id in prev['MEOrderID'].keys()) & (original_client_order_id in prev['ClientOrderID'].keys()):
                            msgs.at[i, 'OrderNum'] = prev['MEOrderID'][order_id]
                            # If the clientOrderID is associated with a different ordernum increment the counter and test counter
                            if  prev['MEOrderID'][order_id] != prev['ClientOrderID'][original_client_order_id]:
                                originalid_orderid_conflict += 1
                        elif order_id in prev['MEOrderID'].keys():
                            msgs.at[i, 'OrderNum'] = prev['MEOrderID'][order_id]
                        elif original_client_order_id in prev['ClientOrderID'].keys():
                            msgs.at[i, 'OrderNum'] = prev['ClientOrderID'][original_client_order_id]
                        else:
                            counter += 1 
                            msgs.at[i, 'OrderNum'] = counter
                    
                    # Case 2: User references Outbound ID (MEOrderID)
                    elif pd.notnull(order_id):
                        if order_id in prev['MEOrderID'].keys():
                            msgs.at[i, 'OrderNum'] = prev['MEOrderID'][order_id]
                        else:
                            counter += 1 
                            msgs.at[i, 'OrderNum'] = counter
                            
                    # Case 3: User references Inbound ID (OrigClientOrderID)
                    elif pd.notnull(original_client_order_id):
                        if original_client_order_id in prev['ClientOrderID'].keys():
                            msgs.at[i, 'OrderNum'] = prev['ClientOrderID'][original_client_order_id]
                        else:
                            counter += 1 
                            msgs.at[i, 'OrderNum'] = counter
                    else:             
                        # It should never happen that there is neither an Inbound nor an Outbound ID.
                        no_inbound_outbound_id += 1 
                        counter += 1 
                        msgs.at[i, 'OrderNum'] = counter

                # Execution Report
                # We should either have seen the MEOrderID or the ClientOrderID previously.
                # An else condition is added for cases in which we have not previously 
                # seen the outbound or inbound due to pack loss.
                elif msgs.at[i, 'MessageType'] == 'Execution_Report':
                    # Case 1: Both order id and client order id are populated
                    if pd.notnull(order_id) & pd.notnull(client_order_id):
                        # If both have already been seen, use the order number from the order ID.
                        # Otherwise use MEOrderID or ClientOrderID depending on which has been seen
                        if (order_id in prev['MEOrderID'].keys()) & (client_order_id in prev['ClientOrderID'].keys()):
                            msgs.at[i, 'OrderNum'] = prev['MEOrderID'][order_id]
                            # If the ClientOrderID is associated with a different ordernum increment the testing counter
                            if  prev['MEOrderID'][order_id] != prev['ClientOrderID'][client_order_id]:
                                originalid_orderid_conflict += 1                                
                        elif order_id in prev['MEOrderID'].keys():
                            msgs.at[i, 'OrderNum'] = prev['MEOrderID'][order_id]
                        elif client_order_id in prev['ClientOrderID'].keys():
                            msgs.at[i, 'OrderNum'] = prev['ClientOrderID'][client_order_id]
                        else:
                            counter += 1 
                            msgs.at[i, 'OrderNum'] = counter
                            
                    # Case 2: only MEOrderID is populated
                    elif order_id in prev['MEOrderID'].keys():
                        msgs.at[i, 'OrderNum'] = prev['MEOrderID'][order_id]
                    
                    # Case 3: only ClientOrderID is populated
                    elif client_order_id in prev['ClientOrderID'].keys():
                        msgs.at[i, 'OrderNum'] = prev['ClientOrderID'][client_order_id]

                    else:
                        counter += 1  
                        msgs.at[i, 'OrderNum'] = counter

                # Order Reject or Order Mass Cancel Report
                # Note that order Mass Cancel Report is a response to an Order Mass Cancel Request message. 
                elif msgs.at[i, 'MessageType'] in {'Cancel_Reject', 'Mass_Cancel_Report', 'Other_Reject'}:
                    # Case 1: both are populated
                    if pd.notnull(order_id) & pd.notnull(client_order_id):
                        # If both have already been seen, use the order number from the order ID. If the ClientOrderID
                        # is associated with a different ordernum increment the testing counter
                        if (order_id in prev['MEOrderID'].keys()) & (client_order_id in prev['ClientOrderID'].keys()):
                            msgs.at[i, 'OrderNum'] = prev['MEOrderID'][order_id]
                            # If the ClientOrderID is associated with a different ordernum increment the testing counter
                            if  prev['MEOrderID'][order_id] != prev['ClientOrderID'][client_order_id]:
                                originalid_orderid_conflict += 1
                        
                        elif order_id in prev['MEOrderID'].keys():
                            msgs.at[i, 'OrderNum'] = prev['MEOrderID'][order_id]

                        elif client_order_id in prev['ClientOrderID'].keys():
                            # We require that the message occurs within 1min of an earlier message 
                            # from the same User-ClientOrderID pair.
                            # In the LSE data the Symbol field is not populated for
                            # 'Cancel_Reject', 'Mass_Cancel_Report', 'Other_Reject' 
                            # (including protocol reject and business message reject) messages. 
                            # When we split the messages into symbol-dates, we include those messages 
                            # in a symbol-date if we observe the same User-ClientOrderID pair in that 
                            # symbol-date. To further confirm that those messages are actually in that
                            # symbol-date and avoid mismatch, we require that there is at least one 
                            # messages from the same user-client order id within one minute beofre the 
                            # reject message/mass cancel message. While this method is not theoretically 
                            # perfect, we believe that it is good enough for the purpose of this project,
                            # because it is reasonable to assume that users use unique Client Order IDs across symbols.
                            # (While this not a strict requirement in LSE, it appears in our data tht
                            #  most users follow this rule, and in many other exchanges this is a strict requirement)  
                            if msgs.at[i, 'MessageTimestamp'] < (time['ClientOrderID'][client_order_id] + pd.Timedelta('1 m')):
                                msgs.at[i, 'OrderNum'] = prev['ClientOrderID'][client_order_id]
                            else:
                                counter += 1
                                msgs.at[i, 'OrderNum'] = counter
                        else:
                            counter += 1 
                            msgs.at[i, 'OrderNum'] = counter
                            
                    # Case 2: Only ClientOrderID is populated
                    # Note that ClientOrderID must be populated, while MEOrderID may not, according to the LSE document
                    elif client_order_id in prev['ClientOrderID'].keys():
                        if msgs.at[i, 'MessageTimestamp'] < (time['ClientOrderID'][client_order_id] + pd.Timedelta('1 m')):
                            msgs.at[i, 'OrderNum'] = prev['ClientOrderID'][client_order_id]
                        else:
                            counter += 1
                            msgs.at[i, 'OrderNum'] = counter
                    else:
                        counter += 1
                        msgs.at[i, 'OrderNum'] = counter

                # Update the set of previously observed MEOrderID values and times
                if pd.notnull(order_id):
                    prev['MEOrderID'][order_id] = msgs.at[i, 'OrderNum']
                    time['MEOrderID'][order_id] = msgs.at[i, 'MessageTimestamp']

                # Update the set of previously observed ClientOrderID values and times
                if pd.notnull(client_order_id):
                    prev['ClientOrderID'][client_order_id] = msgs.at[i, 'OrderNum']
                    time['ClientOrderID'][client_order_id] = msgs.at[i, 'MessageTimestamp']
                # the format of UniqueOrderID is Source_UserNum_OrderNum
                msgs.at[i, 'UniqueOrderID'] = '%s_%06d_%08d' % (source, msgs.at[i, 'UserNum'], msgs.at[i, 'OrderNum'])
                
    return msgs

def multi_process_wrapper(args):
    date, sym, in_dir, out_dir = args
    try:
        preprocess_msgs(*args)
    except:
        print(f'Error: {date}, {sym}')

if __name__ == '__main__':
    num_workers = 1
    pairs = pd.read_csv(file_symdates, dtype={'Date':'O','Symbol':'O'})[['Date','Symbol']].to_records(index=False).tolist()
    # Shuffle the pairs for parallel processing
    random.shuffle(pairs) 
    in_dir = base+'/Testing/Sample_Processed_Data'
    out_dir = base+'/Testing/RawData'
    args_list = [(date, sym, in_dir, out_dir) for date, sym in pairs]
    time_st = datetime.datetime.now()
    print('Start Pre-Processing Message Data: %s' % str(time_st), file = sys.stdout)
    pool = multiprocessing.Pool(num_workers)
    results = pool.map(multi_process_wrapper, args_list)
    print('Finished Processing Message Data: %s' % str(datetime.datetime.now() - time_st), file = sys.stdout)
