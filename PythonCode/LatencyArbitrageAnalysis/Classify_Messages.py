'''
Classify_Messages.py

This script classifies the messages into meaningful economic events.

Reference:
    Section 10.1 of the Code and Data Appendix.

Input: 
    1. Message data, pre-processed as in Sections 3 of the Code and Data Appendix.
    2. Reference data, prepared as in Sections 3 of the Code and Data Appendix.

Output: 
    Message data, with inbounds and outbounds grouped into economic events.
    Specifically, the output file is also a dataframe of messages with 
    the following fields added.
        1. UnifiedMessageType: A type description assigned to each message that 
            combines all the different type information variables into one. 
            This field is created to reduce the number of type variables 
            and improve readability.
        2. Event/EventNum: The economic event assigned based on the combination 
            of matching engine (outbound) and gateway (inbound) messages 
            (e.g. 'New order accepted'). Event is populated for the first message 
            of each event and EventNum for all messages. Event/EventNum are assigned
            within each order (i.e. messages with the same UniqueOrderID) for a user. 
            Note that for quote-related events, the variable names are 
            BidEvent/AskEvent, BidEventNum/AskEventNum.
        3. PriceLvl: The program assigns prices to outbound messages with the most 
            recent inbound message price under the same UniqueOrderID. This is
            because outbound messages do not have price information of the original 
            order. For events where an order is executed at multiple price levels, 
            the program also populates the min/max execution prices for the first 
            message in the event.

Steps:
    1. Add UnifiedMessageType. Combine multiple fields with message type information 
        into a single field. 
        E.g. A message with MessageType 'New_Order', OrderType 'Limit', TIF 'GoodTill'
        is assigned a UnifiedMessageType 'Gateway New Order (Limit)'.
    2. Classify messages into Events. For each user and order, assign Event, 
        EventNum and PriceLvl by looping over messages and considering the 
        UnifiedMessageType.
        E.g. A message with UnifiedMessageType 'Gateway New Order (Limit)'
        followed by the message 'ME: Full Fill (A)' will be assigned the
        Event 'New order aggressively executed in Full.' Both messages
        will be assigned the same EventNum. The price level from the
        new order message will be assigned to the matching engine fill
        as the PriceLvl variable.

'''
######################################################################################################################################
import pandas as pd
import numpy as np
import datetime
import os

from .OrderBook.Orders import Order, Quote
from .utils.Logger import getLogger

def opposite(S):
    if S == 'Ask':
        return('Bid')
    elif S == 'Bid':
        return('Ask')

def classify_messages(runtime, date, sym, args, paths):
    '''
    Function to classify message and events. 
    
    Params:
        runtime: str, for log purpose.
        date:    str, symbol-date identifier.
        sym:     str, symbol-date identifier.
        args:    dictionary of arguments, including:
                     - dtypes_raw_msgs: dtype dict for raw messages.
                     - price_factor: int. convert price from monetary unit to large 
                                          integer, obtained by int(10 ** (max_dec_scale+1)).
        paths:   dictionary of file paths, including:
                     - path_data: path to pre-processed message data files.
                     - path_temp: path to temp data files.
                     - path_logs: path to log files.
    Output:
        message dataset with the following columns added, saved to path_temp/ClassifiedMsgData/.
            {'UnifiedMessageType': 'O',
            'Categorized': 'bool', 'EventNum': 'float64', 'Event': 'O', 
            'PrevPriceLvl': 'int64', 'PrevQty': 'float64', 'PriceLvl': 'int64', 
            'MinExecPriceLvl':'int64', 'MaxExecPriceLvl':'int64', 
            'BidCategorized': 'bool', 'BidEventNum': 'float64', 'BidEvent': 'O', 
            'PrevBidPriceLvl': 'int64', 'PrevBidQty': 'float64', 'BidPriceLvl': 'int64', 
            'BidMinExecPriceLvl':'int64', 'BidMaxExecPriceLvl':'int64', 
            'AskCategorized': 'bool', 'AskEventNum': 'float64', 'AskEvent': 'O',
            'PrevAskPriceLvl': 'int64', 'PrevAskQty': 'float64', 'AskPriceLvl': 'int64', 
            'AskMinExecPriceLvl':'int64', 'AskMaxExecPriceLvl':'int64'}
    '''
    # Initialize log
    logpath = '%s/%s/' %(paths['path_logs'], 'MessageDataProcessing_'+runtime)
    logfile = 'Step_1_Classify_Messages_%s_%s_%s.log' % (runtime, date, sym)
    if not os.path.exists(logpath):
        os.makedirs(logpath)
    logger = getLogger(logpath, logfile, __name__)

    ### INITIALIZE ###
    logger.info('Processing: %s %s' % (date, sym))

    # Get arguments
    dtypes_raw_msgs = args['dtypes_raw_msgs']
    price_factor = args['price_factor']
    # Get paths
    infile_msgs = '%s/%s/Raw_Msg_Data_%s_%s.csv.gz' % (paths['path_data'], date, date, sym)
    outfile_msgs = '%s/ClassifiedMsgData/%s/Classified_Msgs_Data_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym)

    # Start timer
    timer_st = datetime.datetime.now()

    # Add info to log
    logger.info('Timer Start: %s' % str(timer_st))

    ### LOAD INPUT FILES ###
    logger.info('Loading raw data...')

    # Load data
    # msgs refers to the full dataframe of messages of the symbol-date
    msgs = pd.read_csv(infile_msgs, 
                     dtype = dtypes_raw_msgs, 
                     parse_dates = ['MessageTimestamp'], 
                     skipinitialspace = True, 
                     compression = 'gzip')
    
    # Data check: assertion that all required fields are included
    col_required = [
        'ClientOrderID', 'UniqueOrderID', 'UserID', 'MessageTimestamp', 'Side', 
        'MessageType', 'OrderType', 'ExecType', 'OrderStatus', 'TradeInitiator', 'TIF', 'CancelRejectReason', 
        'LimitPrice', 'OrderQty', 'DisplayQty', 'ExecutedPrice', 'ExecutedQty', 'LeavesQty', 
        'QuoteRelated', 'BidPrice', 'BidSize', 'AskPrice', 'AskSize'
    ]
    missing_columns = set(col_required).difference(set(msgs.columns))
    assert len(missing_columns) == 0, 'Missing Data in Symbol-Date (%s, %s) Raw Message Data: missing fields %s' % (date, sym, missing_columns)
    
    ## Replace the NAs in AuctionTrade and OpenAuctionTrade with False
    bool_cols_trades = ['AuctionTrade','OpenAuctionTrade']
    for col in bool_cols_trades:
        msgs.loc[msgs[col].isna(), col] = False

    ## Convert prices into integers by multiplying price_factor.
    # price_factor = int(10 ** (max_dec_scale+1)). 
    # max_dec_scale is the max decimal scale of the price-related variables. 
    # The prices are turned to integers to avoid floating point error 
    # that can mess up race detection.
    prices = ['LimitPrice', 'StopPrice', 'ExecutedPrice', 'BidPrice', 'AskPrice']
    for col in prices:
        msgs[col] = (price_factor * msgs[col]).round(-1).astype('Int64')
    
    ###################################
    ### ADDING UNIFIED MESSAGE TYPE ###
    ###################################
    # 
    ## Description
    # Combine multiple fields with message type information into a single field,
    # UnifiedMessageType, for each message. 
    #
    # For Example, a message with MessageType 'New_Order', OrderType 'Limit', 
    # and TIF 'GoodTill' is assigned a UnifiedMessageType 'Gateway New Order (Limit)'.
    #   
    # Please refer to Table 7 and Section 10.1 of the Code and Data Appendix.
    #                               
    # Inputs
    #   MessageType:
    #   - Inbounds (Gateway): 
    #     1. New_Order: submit a new order (single sided order).
    #     2. New_Quote: submit a new quote (double sided executable order).
    #     3. Cancel_Request: the request to cancel of an order.
    #     4. Cancel_Replace_Request: the request to cancel and replace an order.
    #     5. Other_Inbound: other inbound cannot be categorized above
    #        (these inbounds will not have an effect on the order book or race detection).
    #   - Outbounds (ME): 
    #     1. Execution_Report: outbound message to confirm an execution.
    #     2. Cancel_Reject: outbound message to reject a cancel request.
    #     3. Other_Reject: other types of reject, e.g. business reject and protocol reject.
    #     4. Other_Outbound: other outbound cannot be categorized above 
    #        (these outbounds will not have an effect on the order book or race detection).
    # 
    #   OrderType: Type of the order populated for messages with MessageType New_Order:
    #     1. Limit
    #     2. Market
    #     3. Stop
    #     4. Stop_Limit
    #     5. Pegged
    #     6. Passive_Only
    #
    #   TIF: Time-in-force for New_Order Cancel_Replace_Request:
    #     1. GoodTill: Including good for day, good till date, etc. 
    #                  Any non-auction and non-IOC orders that participate in continuous trading.
    #     2. IOC: Immediate-or-Cancel. The order trades aggressively against
    #             the resting orders and the unmatched part expires immediately.
    #     3. FOK: Fill-or-Kill. The order expires immediately if not fully filled.
    #     4. GFA: Good-for-Auction Orders.
    #
    #   ExecType: Describes details of outbound Execution_Report:
    #     1. Order_Accepted
    #     2. Order_Cancelled
    #     3. Order_Executed
    #     4. Order_Expired
    #     5. Order_Rejected
    #     6. Order_Replaced
    #     7. Order_Suspended
    #     8. Order_Restated
    # 
    #   OrderStatus: Whether an execution is a full fill or a partial fill for 
    #                outbound Execution_Report with ExecType == 'Order_Executed'
    #     1. Partial_Fill
    #     2. Full_Fill
    # 
    #   TradeInitiator: For outbound Execution_Report with ExecType == 'Order_Executed', 
    #                   indicating whether it is aggressive, passive, or other (auction trades).
    #     1. Aggressive
    #     2. Passive
    #     3. Other
    # 
    #   CancelRejectReason: This lists a reason for a Cancel_Reject:
    #     1. TLTC: Too-late-to-cancel. Only cancels rejected because of TLTC 
    #              are counted as failed cancels in race detection.
    #     2. Other
    # 
    ## Outputs
    #   UnifiedMessageType: Simplified Message Type with the following categories
    #     1. Inbound: Gateway New Order (Market), Gateway New Order (Limit), 
    #                 Gateway New Order (IOC), Gateway New Order (Stop), 
    #                 Gateway New Order (Stop Limit), Gateway New Order (Other),
    #                 Gateway New Quote, 
    #                 Gateway Cancel, Gateway Cancel/Replace, 
    #                 Gateway Other Inbound
    #     2. Outbound: ME: New Order Accept, 
    #                  ME: Order Reject, ME: Order Expire,  ME: Order Suspend, ME: Order Restated, 
    #                  ME: Partial Fill (P), ME: Partial Fill (A), ME: Partial Fill (Other), 
    #                  ME: Full Fill (P), ME: Full Fill (A), ME: Full Fill (Other),
    #                  ME: Cancel Accept, ME: Cancel/Replace Accept, 
    #                  ME: Cancel Reject (TLTC), ME: Cancel Reject (Other),
    #                  ME: Other Reject, ME: Other Outbound


    ### Create Unified Message Type variable for Gateway Messages (Inbound)
    ## New_Order messages
    # All new order messages are first assigned the default 
    # UnifiedMessageType 'Gateway New Order (Other)', and then categorized according 
    # to the order type.
    # Please note that 'Gateway New Order (IOC)' includes both IOCs and FOKs.
    # We will use the TIF field to separate IOC and FOK orders later in the code.
    msgs.loc[(msgs['MessageType'] == 'New_Order'), 'UnifiedMessageType'] = 'Gateway New Order (Other)'
    msgs.loc[(msgs['MessageType'] == 'New_Order') & (msgs['OrderType'] == 'Market'), 'UnifiedMessageType'] = 'Gateway New Order (Market)'
    msgs.loc[(msgs['MessageType'] == 'New_Order') & (msgs['OrderType'] == 'Limit') & (msgs['TIF'] == 'GoodTill'), 'UnifiedMessageType'] = 'Gateway New Order (Limit)'
    msgs.loc[(msgs['MessageType'] == 'New_Order') & (msgs['OrderType'] == 'Limit') & (msgs['TIF'].isin({'IOC','FOK'})), 'UnifiedMessageType'] = 'Gateway New Order (IOC)'
    msgs.loc[(msgs['MessageType'] == 'New_Order') & (msgs['OrderType'] == 'Stop'), 'UnifiedMessageType'] = 'Gateway New Order (Stop)'
    msgs.loc[(msgs['MessageType'] == 'New_Order') & (msgs['OrderType'] == 'Stop_Limit'), 'UnifiedMessageType'] = 'Gateway New Order (Stop Limit)'
    msgs.loc[(msgs['MessageType'] == 'New_Order') & (msgs['OrderType'] == 'Pegged'), 'UnifiedMessageType'] = 'Gateway New Order (Pegged)'
    msgs.loc[(msgs['MessageType'] == 'New_Order') & (msgs['OrderType'] == 'Passive_Only'), 'UnifiedMessageType'] = 'Gateway New Order (Passive Only)'
    ## New_Quote messages
    msgs.loc[(msgs['MessageType'] == 'New_Quote'), 'UnifiedMessageType'] = 'Gateway New Quote'
    ## Cancel messages 
    msgs.loc[(msgs['MessageType'] == 'Cancel_Request'), 'UnifiedMessageType'] = 'Gateway Cancel'
    msgs.loc[(msgs['MessageType'] == 'Cancel_Replace_Request'), 'UnifiedMessageType'] = 'Gateway Cancel/Replace'
    ## Other Inbound
    msgs.loc[(msgs['MessageType'] == 'Other_Inbound'), 'UnifiedMessageType'] = 'Gateway Other Inbound'

    ### Create Unified Message Type variable for Matching Engine messages (outbound)
    ## Execution_Report
    # Set Unified Message Type for all Execution_Report to Other and then update Type according to other variables
    msgs.loc[(msgs['MessageType'] == 'Execution_Report'), 'UnifiedMessageType'] = 'ME: Execution Report (Other)'
    # Non-fill Execution_Report
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Expired'), 'UnifiedMessageType'] = 'ME: Order Expire'
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Cancelled'), 'UnifiedMessageType'] = 'ME: Cancel Accept'
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Replaced'), 'UnifiedMessageType'] = 'ME: Cancel/Replace Accept'
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Suspended'), 'UnifiedMessageType'] = 'ME: Order Suspend'
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Restated'), 'UnifiedMessageType'] = 'ME: Order Restated'
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Accepted'), 'UnifiedMessageType'] = 'ME: New Order Accept'
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Rejected'), 'UnifiedMessageType'] = 'ME: Order Reject'
    # Fill Execution_Report
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['OrderStatus'] == 'Partial_Fill'), 'UnifiedMessageType'] = 'ME: Partial Fill (Other)'
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['OrderStatus'] == 'Partial_Fill') & (msgs['TradeInitiator'] == 'Passive'), 'UnifiedMessageType'] = 'ME: Partial Fill (P)'
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['OrderStatus'] == 'Partial_Fill') & (msgs['TradeInitiator'] == 'Aggressive'), 'UnifiedMessageType'] = 'ME: Partial Fill (A)'
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['OrderStatus'] == 'Full_Fill'), 'UnifiedMessageType'] = 'ME: Full Fill (Other)'
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['OrderStatus'] == 'Full_Fill') & (msgs['TradeInitiator'] == 'Passive'), 'UnifiedMessageType'] = 'ME: Full Fill (P)'
    msgs.loc[(msgs['MessageType'] == 'Execution_Report') & (msgs['ExecType'] == 'Order_Executed') & (msgs['OrderStatus'] == 'Full_Fill') & (msgs['TradeInitiator'] == 'Aggressive'), 'UnifiedMessageType'] = 'ME: Full Fill (A)'
    ## Cancel_Reject
    msgs.loc[(msgs['MessageType'] == 'Cancel_Reject'), 'UnifiedMessageType'] = 'ME: Cancel Reject (Other)'
    msgs.loc[(msgs['MessageType'] == 'Cancel_Reject') & (msgs['CancelRejectReason'] == 'TLTC'), 'UnifiedMessageType'] = 'ME: Cancel Reject (TLTC)'
    ## Other reject 
    # In the LSE dataset, this includes protocol reject and business reject cancel for cancel requests or new orders/quotes
    msgs.loc[(msgs['MessageType'] == 'Other_Reject'), 'UnifiedMessageType'] = 'ME: Other Reject'
    ## Other Outbounds
    msgs.loc[(msgs['MessageType'] == 'Other_Outbound'), 'UnifiedMessageType'] = 'ME: Other Outbound'

    # Add Inbound and Outbound indicators
    msgs['Inbound'] = msgs['MessageType'].isin({'New_Order','New_Quote','Cancel_Request','Cancel_Replace_Request','Other_Inbound'})
    msgs['Outbound'] = msgs['MessageType'].isin({'Execution_Report','Cancel_Reject','Other_Reject','Other_Outbound'})
    ###################################
    ### ADDING EVENT CLASSIFICATION ###
    ###################################
    # 
    ## Description
    # For each user and order, assign Event, EventNum and PriceLvl by looping over messages 
    # and considering the UnifiedMessageTypes. There are separate loops for orders and quotes                                       
    #
    ## Example
    # A message with UnifiedMessageType 'Gateway New Order (Limit)' followed by the message 'ME: Full Fill (A)' 
    # will be assigned the Event 'New order aggressively executed in Full.' Both messages will be assigned the
    # same EventNum. The price level from the new order message will be assigned to the matching engine fill.
    #                                              
    # Please refer to Table 8 and Section 10.1 of the Code and Data Appendix.
    #
    # Inputs
    #   MessageType
    #
    #   UnifiedMessageType: created in the previous section
    #   
    #   LimitPrice, AskPrice, BidPrice: This is the price from inbound messages
    # 
    #   ExecutedPrice: This is the price at which the order executes
    # 
    #   OrderQty:  This is the total number of shares in an order
    # 
    #   LeavesQty: This is the remaining quantity of shares on the order after the Matching Engine event
    # 
    #   Bid/AskSize: This is the quantity on each Side of a quote
    # 
    #   ClientOrderID: This is an ID used to link the outbound to its corresponding inbound.
    #                  ClientOrderID should be unique with respect to inbound-outbound groups.
    #                  The responding outbound will have the same ClientOrderID as its inbound.
    #                  It should also change when an order is cancel/replaced.
    #                  e.g. When a user cancel/replace a previous order, ClientOrderID will change.
    #                  When a user's aggressive order fully executed against several resting orders,
    #                  ClientOrderID should be the same for the new order inbound and the outbounds
    #                  received by the aggressor. 
    # 
    #   Side: Bid or Ask 
    # 
    #   QuoteRelated: This flags whether the message is about a quote or not
    # 
    # Outputs
    #   Event: This labels a type of economic event from the combination of matching engine (outbounds) and
    #          gateway (inbounds) messages (e.g. 'New order accepted') within a UniqueOrderID. 
    #          This is populated for the first message of each event.
    # 
    #   EventNum: This is the event number to which a message belongs. 
    # 
    #   PriceLvl: This is the order price from the inbound message.
    # 
    #   PrevPriceLvl: This is the order price as of the previous message in an event.
    #                 This variable is relevant when a user tries to cancel the order or
    #                 update the order. In that case, PrevPriceLvl is the 
    #                 price at which she attempts to cancel or update.
    #                 
    # 
    #   MinExecPriceLvl, MaxExecPriceLvl: This is the min/max price at which an order executes. 
    #                                     It is relevant for fills over multiple prices
    #
    # For quotes we assign these variables for each side (Bid/Ask) - e.g. AskEvent/BidEvent, AskEventNum/BidEventNum
    
    logger.info('Adding event classifications...')

    # Initialize classification and other message variables
    for side in ['', 'Bid', 'Ask']:
        msgs['Prev%sPriceLvl' % side] = np.nan # Previous price level (for C/R)
        msgs['Prev%sQty' % side] = np.nan # Previous qty (for C/R, C)
        msgs['%sPriceLvl' % side] = np.nan # Price level
        msgs['%sCategorized' % side] = False # Indicator for whether message has been classified
        msgs['%sEventNum' % side] = np.nan # Event number
        msgs['%sEvent' % side] = '' # Event classification. Empty string for missing value
        msgs['%sMinExecPriceLvl' % side] = np.nan  # Minimum price at which order executes
        msgs['%sMaxExecPriceLvl' % side] = np.nan  # Maximum price at which order executes

    # Initialize Testing Counters
    # Those counters are used to monitor corner cases and are printed in the log file
    # The main usage of those counters is to see how frequently those corner cases happen
    # and they do not influence the output directly. If the user finds that the counters are very high,
    # it may indicate that the quality of data is low (e.g., severe packet loss), which might influence 
    # the accuracy of the output.
    # for orders
    order_testing_counter = {}
    order_testing_counter['other_me_activity'  ] = 0 # Lone ME response that aren't otherwise accounted for
    order_testing_counter['partial_other_me'   ] = 0 # Other ME activity following partial fills for new orders
    order_testing_counter['nocr_other_me'      ] = 0 # New order or Cancel/Replace encounters other ME responses (Not in any of the elifs for new order or c/r)
    order_testing_counter['no_no_reply'        ] = 0 # New order no reply
    order_testing_counter['pf_no_further_reply'] = 0 # No further reply after aggressive partial fills
    order_testing_counter['cancel_no_reply'    ] = 0 # No ME response after a cancel (If we never resolve i in loop j, it means there wasn't a reply )
    order_testing_counter['cancel_other_me'    ] = 0 # Other ME response after a cancel request (Not in any of the elifs for the cancel)
    order_testing_counter['cr_no_reply'        ] = 0 # Cancel replace no reply

    # for quotes
    quote_testing_counter = {}
    quote_testing_counter['quote_reply_not_exp'   ] = 0 # Not expecting a reply for a quote
    quote_testing_counter['op_side_break'         ] = 0 # Entering the next event on the opposite side of the loop before encountering the ME response (2+ accepts on the opposite side of the book or cancel break)
    quote_testing_counter['pf_no_further_reply'   ] = 0 # No further ME response after aggressive partial fills
    quote_testing_counter['expected_never_arrived'] = 0 # No response while expecting the outbounds (Never resolve in the j loop)
    quote_testing_counter['nqt_me_cancel'         ] = 0 # New quote followed by cancel accept
    quote_testing_counter['nqt_other_me'          ] = 0 # new quotes followed by Other ME msgs 
    quote_testing_counter['partial_other_me'      ] = 0 # Partial Fill followed by other ME activity
    quote_testing_counter['partial_me_cancel'     ] = 0 # Partial Fill followed by ME cancel
    quote_testing_counter['partial_me_passive'    ] = 0 # Partial fill followed by passive fill
    quote_testing_counter['cancel_no_reply'       ] = 0 # Cancel no Response
    quote_testing_counter['cancel_other_me'       ] = 0 # Cancel Other ME activity
    quote_testing_counter['other_me_activity'     ] = 0 # Lone ME responses that aren't otherwise accounted for

    ### Loop over users and orders to classify messages into events ###
    # For each user we first loop over each order (quotes are processed separately) and classify its messages in Events.
    # Then, for each user, we loop over all quote related messages and classify them in Events. 
    # 
    # We classify inbound messages according to their UnifiedMessageType and the UnifiedMessageType of the 
    # outbound messages that follow. If an outbound message does not have an inbound message, it is categorized
    # as a passive execution or 'Other ME activity'. 'Other ME activity' also catches corner cases like  
    # Business Rejects or packet loss (missing inbound message)
    #
    # The structure of the loop is as follows:
    # for each user:
    #     for each order submitted by that user:
    #         Initialize event counter. 
    #         for each message in the order:
    #             Skip the message if it is already classified.
    #             Depending on the message type: increment event counter; populate some message variables; if necessary, 
    #             loop over subsequent messages to identify messages associated to the message currently looping over
    #             and populate some message variables for those associated messages; assign the same event number to 
    #             the message looping over and its associated messages; and mark them as classified.
    #     for quote:
    #         for each side:
    #             Initialize event counter. 
    #             for each quote message:
    #                 Skip the message if it is already classified.
    #                 Depending on the message type: increment event counter; populate some message variables; if necessary, 
    #                 loop over subsequent messages to identify messages associated to the message currently looping over
    #                 and populate some message variables for those associated messages; assign the same event number to 
    #                 the message looping over and its associated messages; and mark them as classified.
    
    for _, user_msgs in msgs.groupby('UserID'):

        # Loop over orders that are not quote related
        for _, order_msgs in user_msgs[~user_msgs['QuoteRelated']].groupby('UniqueOrderID'):

            counter = 0  # Initialize event counter

            order = Order()

            # Loop over messages
            for i in order_msgs.index:

                # Skip previously classified messages
                if msgs.at[i, 'Categorized']:
                    continue

                # Gateway New Order
                # This includes the following Unified Message Types:  
                # 'Gateway New Order (Market)',  'Gateway New Order (Limit)', 
                # 'Gateway New Order (IOC)', 'Gateway New Order (Stop)', 'Gateway New Order (Stop Limit)'
                # 'Gateway New Order (Market)', 'Gateway New Order (Pegged)'
                # and classifies message i into the following Events:
                # 'New order accepted', 'New order aggressively executed in full', 'New order aggressively executed in part',
                # 'New order expired', 'New order suspended', 'New order failed', 'New order no response'
                # Note that for Event 'New order accepted', it can either be that the new order is posted to book, 
                # or accepted to the auction queue (Good-for-Auction orders)
                # We don't need to separate these two because GFA orders will be handled separately in the code.
                # GFA orders will not update the order book or participate in races.
                if order_msgs.at[i, 'MessageType'] == 'New_Order':
                    counter += 1  # Increment event counter
                    # Handle order types with price information
                    if msgs.at[i, 'UnifiedMessageType'] in ('Gateway New Order (Limit)', 
                                                          'Gateway New Order (IOC)',  
                                                          'Gateway New Order (Stop Limit)',
                                                          'Gateway New Order (Passive Only)',
                                                          'Gateway New Order (Other)'):
                        order = Order()
                        order.add(p=msgs.at[i, 'LimitPrice'], q=msgs.at[i, 'OrderQty'])
                    # Handle order types without price information. This includes
                    # 'Gateway New Order (Market)', 'Gateway New Order (Stop)', 'Gateway New Order (Pegged)'
                    # They are separated because they don't have a limit price and they 
                    # participate in the trading in a slightly different way.
                    else: 
                        order = Order()
                        order.add(p=np.nan, q=msgs.at[i, 'OrderQty'])
                    
                    msgs.at[i, 'EventNum'] = counter  # Populate EventNum
                    msgs.at[i, 'PriceLvl'] = order.gw_prc
                    msgs.at[i, 'Categorized'] = True  # Flag message i as categorized
                    resolved_i = False  # Initialize indicator for loop

                    # Loop over subsequent messages to assign the event to message i based on outbound messages
                    # of the following Unified Message Types:
                    # ME: New Order Accept, ME: Order Reject, ME: Partial Fill (P), ME: Partial Fill (A),
                    # ME: Full Fill (P), ME: Full Fill (A), ME: Order Expire, ME: Order Suspend,
                    # ME: Other Reject
                    for j in order_msgs.index[order_msgs.index > i]:
                        
                        # Skip if message has already been categorized or if it has a different ClientOrderID value.
                        # Gateway messages with the same ClientOrderID will not trigger any of the if statements in the j loop
                        if msgs.at[j, 'Categorized'] or msgs.at[j, 'ClientOrderID'] != msgs.at[i, 'ClientOrderID']:
                            continue

                        # ME New Order Accept
                        if order_msgs.at[j, 'UnifiedMessageType'] == 'ME: New Order Accept':
                            order.update_me(q=msgs.at[j, 'LeavesQty'])
                            msgs.at[i, 'Event'] = 'New order accepted'  # Assign event to message i
                            msgs.at[j, 'EventNum'] = counter # Populate EventNum
                            msgs.at[j, 'PriceLvl'] = order.me_prc
                            msgs.at[j, 'Categorized'] = True  # Flag message j as categorized
                            resolved_i = True  # Update indicator

                        # ME Full Fill - (A) for aggressive
                        elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Full Fill (A)':
                            order.update_me(q=msgs.at[j, 'LeavesQty'])
                            msgs.at[i, 'Event'] = 'New order aggressively executed in full'  # Assign event to message i
                            msgs.at[i, 'MinExecPriceLvl'] = msgs.at[j, 'ExecutedPrice']
                            msgs.at[i, 'MaxExecPriceLvl'] = msgs.at[j, 'ExecutedPrice']
                            msgs.at[j, 'EventNum'] = counter  # Populate EventNum
                            msgs.at[j, 'PriceLvl'] = order.me_prc
                            msgs.at[j, 'Categorized'] = True  # Flag message j as categorized
                            resolved_i = True  # Update indicator

                        # ME Partial Fill - (A) for aggressive
                        # Loop over subsequent messages until the order is filled or fails
                        elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Partial Fill (A)':
                            order.update_me(q=msgs.at[j, 'LeavesQty'])
                            msgs.at[j, 'EventNum'] = counter  # Populate EventNum
                            msgs.at[j, 'PriceLvl'] = order.me_prc
                            msgs.at[j, 'Categorized'] = True  # Flag message as categorized
                            executed_prices = [msgs.at[j, 'ExecutedPrice']]
                            resolved_j = False  # Initialize indicator for loop

                            for k in order_msgs.index[order_msgs.index > j]:

                                # Skip if message has already been categorized or ClientOrderID doesn't match
                                # or message has different ClientOrderID value
                                if msgs.at[k, 'Categorized'] or msgs.at[k, 'ClientOrderID'] != msgs.at[i, 'ClientOrderID']:
                                    continue

                                # ME Full Fill - (A) for aggressive
                                if order_msgs.at[k, 'UnifiedMessageType'] == 'ME: Full Fill (A)':
                                    order.update_me(q=msgs.at[k, 'LeavesQty'])
                                    executed_prices = executed_prices + [msgs.at[k, 'ExecutedPrice']]
                                    msgs.at[i, 'Event'] = 'New order aggressively executed in full'  # Assign event to message i
                                    msgs.at[i, 'MinExecPriceLvl'] = min(executed_prices)
                                    msgs.at[i, 'MaxExecPriceLvl'] = max(executed_prices)
                                    msgs.at[k, 'EventNum'] = counter  # Populate EventNum
                                    msgs.at[k, 'PriceLvl'] = order.me_prc
                                    msgs.at[k, 'Categorized'] = True  # Flag message k as categorized
                                    resolved_j, resolved_i = True, True  # Update indicators

                                # ME Partial Fill - (A) for aggressive
                                elif order_msgs.at[k, 'UnifiedMessageType'] == 'ME: Partial Fill (A)':
                                    order.update_me(q=msgs.at[k, 'LeavesQty'])
                                    executed_prices = executed_prices + [msgs.at[k, 'ExecutedPrice']]
                                    msgs.at[k, 'EventNum'] = counter  # Populate
                                    msgs.at[k, 'PriceLvl'] = order.me_prc
                                    msgs.at[k, 'Categorized'] = True  # Flag message k as categorized
                                    resolved_j, resolved_i = False, False  # Do not update indicators

                                # ME Order Expire for IOC messages that immediately cancelled the order 
                                elif order_msgs.at[k, 'UnifiedMessageType'] == 'ME: Order Expire' and order_msgs.at[i, 'UnifiedMessageType'] == 'Gateway New Order (IOC)':
                                    order.update_me(q=np.nan)
                                    msgs.at[i, 'Event'] = 'New order aggressively executed in part'  # Assign event to message i
                                    msgs.at[i, 'MinExecPriceLvl'] = min(executed_prices)
                                    msgs.at[i, 'MaxExecPriceLvl'] = max(executed_prices)
                                    msgs.at[k, 'EventNum'] = counter  # Populate EventNum
                                    msgs.at[k, 'PriceLvl'] = order.me_prc
                                    msgs.at[k, 'Categorized'] = True  # Flag message k as categorized
                                    resolved_j, resolved_i = True, True  # Update indicators
                                
                                # ME New Order Accept after partial fills 
                                # If the Matching Engine sends a post-to-book confirmation after an 
                                # aggressive order is executed in part (the rest is posted to book),
                                # then the loop will end up in this case. For example,
                                # In the LSE data, since there is NO post-to-book confirmation after 
                                # an aggressive order is executed in part, this case is not captured here. 
                                # It is captured two blocks below in the No further ME Response
                                # after partial fills case.
                                elif order_msgs.at[k, 'UnifiedMessageType'] == 'ME: New Order Accept':
                                    order.update_me(q=msgs.at[k, 'LeavesQty'])
                                    executed_prices = executed_prices + [msgs.at[k, 'ExecutedPrice']]
                                    msgs.at[i, 'Event'] = 'New order aggressively executed in full'  # Assign event to message i
                                    msgs.at[i, 'MinExecPriceLvl'] = min(executed_prices)
                                    msgs.at[i, 'MaxExecPriceLvl'] = max(executed_prices)
                                    msgs.at[k, 'EventNum'] = counter  # Populate EventNum
                                    msgs.at[k, 'PriceLvl'] = order.me_prc
                                    msgs.at[k, 'Categorized'] = True  # Flag message k as categorized
                                    resolved_j, resolved_i = True, True  # Update indicators

                                # Other ME Message 
                                # When observing other ME messages following the passive fills, 
                                # the current event at least one passive fill is ended.
                                # This case includes, for instance, a new order aggressively executed in part
                                # and then the remaining part rests in the book and after that, it is executed
                                # passively. These passive fills end the current event and will be classified
                                # into other events.
                                elif order_msgs.at[k, 'MessageType'] == 'Execution_Report':
                                    msgs.at[i, 'Event'] = 'New order aggressively executed in part'  # Assign event to message i
                                    msgs.at[i, 'MinExecPriceLvl'] = min(executed_prices)
                                    msgs.at[i, 'MaxExecPriceLvl'] = max(executed_prices)
                                    resolved_j, resolved_i = True, True  # Update indicators
                                    order_testing_counter['partial_other_me'] += 1 # test counter

                                if resolved_j:
                                    break

                            # No further ME Response after partial fills
                            # In cases where there are no further outbound messages after the partial fill
                            # This could be that an order is executed aggressively in part
                            # and then stays in the order book for the rest of the day, or packet loss. 
                            # Since an aggressive partial fill message is already observed, the event is  
                            # categorized as aggressive partial fill.
                            # For instance, in the LSE data, after the new order is aggressively executed in part, 
                            # the rest of the order will be posted to book but there will NOT be a post to book message.
                            # Hence if we see no further ME response after partial fills, we assume they will be
                            # posted to book. If the data has this post-to-book message after partial fills, 
                            # the same code logic still applies, except that we will end up observing 
                            # ME New Order Accept after partial fills (two blocks above) since we will see 
                            # a post-to-book confirmation after partial execution. 
                            if not resolved_j:
                                msgs.at[i, 'Event'] = 'New order aggressively executed in part'  # Assign event to message i
                                msgs.at[i, 'MinExecPriceLvl'] = min(executed_prices)
                                msgs.at[i, 'MaxExecPriceLvl'] = max(executed_prices)
                                order_testing_counter['pf_no_further_reply'] += 1 # test counter
                                resolved_i = True  # Update indicator

                        # ME Order Expire
                        elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Order Expire':
                            order.update_me(q=np.nan)
                            msgs.at[i, 'Event'] = 'New order expired'  # Assign event to message i
                            msgs.at[j, 'EventNum'] = counter  # Populate EventNum
                            msgs.at[j, 'PriceLvl'] = order.me_prc
                            msgs.at[j, 'Categorized'] = True  # Flag message j as categorized
                            resolved_i = True  # Update indicator

                        # ME Order Reject
                        elif order_msgs.at[j, 'UnifiedMessageType'] in ('ME: Order Reject', 'ME: Other Reject'):
                            order.update_me(q=np.nan)
                            msgs.at[i, 'Event'] = 'New order failed'  # Assign event to message i
                            msgs.at[j, 'EventNum'] = counter  # Populate EventNum
                            msgs.at[j, 'PriceLvl'] = order.me_prc
                            msgs.at[j, 'Categorized'] = True  # Flag message j as categorized
                            resolved_i = True  # Update indicator

                        # ME Order Suspend
                        elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Order Suspend':
                            order.update_me(q=msgs.at[j, 'LeavesQty'])
                            msgs.at[i, 'Event'] = 'New order suspended'  # Assign event to message i
                            msgs.at[j, 'EventNum'] = counter  # Populate EventNum
                            msgs.at[j, 'PriceLvl'] = order.me_prc
                            msgs.at[j, 'Categorized'] = True  # Flag message j as categorized
                            resolved_i = True  # Update indicator
                        
                        else:
                            # counter if other ME message after new order inbound
                            order_testing_counter['nocr_other_me'] += 1 

                        if resolved_i:
                            break

                    # No ME Response (corner cases)
                    # The inbound message has no Accept, Fill, Expire, Reject or Suspend outbound message 
                    if not resolved_i:
                        # Counter for new order no response
                        order_testing_counter['no_no_reply'] += 1
                        msgs.at[i, 'Event'] = 'New order no response'  # Assign event to message i
                        resolved_i = True  # Update indicator

                # Gateway Cancel
                # This includes the following Type:  'Gateway Cancel'
                # and classifies message i in the following Events:
                # 'Cancel request accepted', 'Cancel request rejected', 'Cancel request failed', 'Cancel request no response'
                # The difference between cancel request rejected and failed is that:
                #     - Cancel request rejected: If a ME: Cancel Reject (TLTC) outbound is observed.
                #                                This is the failed cancel cases we use in the race detection section.
                #     - Cancel request failed: If the cancel request is rejected by other reasons,
                #                              that is, 'ME: Cancel Reject (Other)' or 'ME: Other Reject'.
                #                              This does NOT count as failed cancels we define in the race detection section
                elif order_msgs.at[i, 'MessageType'] == 'Cancel_Request':
                    counter += 1  # Increment event counter
                    msgs.at[i, 'EventNum'] = counter  # Populate EventNum
                    msgs.at[i, 'PrevPriceLvl'] = order.gw_prc  # Assume cancel affects last submitted GW message
                    msgs.at[i, 'Categorized'] = True  # Flag message i as categorized
                    resolved_i = False  # Initialize indicator for loop

                    # Loop over subsequent messages to assign the event to message i based on outbound messages
                    # of the following Unified Message Types:
                    # 'ME: Cancel Accept', 'ME: Cancel Reject (TLTC)',
                    # 'ME: Cancel Reject (Other)', 'ME: Other Reject'
                    for j in order_msgs.index[order_msgs.index > i]:

                        # Skip if message has already been categorized or if it has a different ClientOrderID value.
                        # Gateway messages with the same ClientOrderID will not trigger any of the if statements in the j loop.
                        if msgs.at[j, 'Categorized'] or msgs.at[j, 'ClientOrderID'] != msgs.at[i, 'ClientOrderID']:
                            continue

                        # ME Cancel Accept
                        if order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Cancel Accept':
                            order.cancel()
                            msgs.at[i, 'Event'] = 'Cancel request accepted'  # Assign event to message
                            msgs.at[j, 'EventNum'] = counter  # Populate EventNum
                            msgs.at[j, 'PrevPriceLvl'] = order.cancel_prc
                            msgs.at[i, 'PrevQty'] = order.cancel_qty
                            msgs.at[j, 'Categorized'] = True  # Flag message j as categorized
                            resolved_i = True  # Update indicator

                        # ME Cancel Reject (TLTC)
                        if order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Cancel Reject (TLTC)':
                            order.cancel_reject()
                            msgs.at[i, 'Event'] = 'Cancel request rejected'  # Assign event to message
                            msgs.at[j, 'EventNum'] = counter  # Populate EventNum
                            msgs.at[j, 'PrevPriceLvl'] = order.cancel_prc
                            msgs.at[i, 'PrevQty'] = order.cancel_qty
                            msgs.at[j, 'Categorized'] = True  # Flag message j as categorized
                            resolved_i = True  # Update indicator

                        # ME Other Cancel Reject
                        if order_msgs.at[j, 'UnifiedMessageType'] in ('ME: Cancel Reject (Other)', 'ME: Other Reject'):
                            order.cancel_reject()
                            msgs.at[i, 'Event'] = 'Cancel request failed'  # Assign event to message
                            msgs.at[j, 'EventNum'] = counter  # Populate EventNum
                            msgs.at[j, 'PrevPriceLvl'] = order.cancel_prc
                            msgs.at[i, 'PrevQty'] = order.cancel_qty
                            msgs.at[j, 'Categorized'] = True  # Flag message j as categorized
                            resolved_i = True  # Update indicator

                        # Break loop if message resolves the event
                        if resolved_i:
                            break
                        else:
                            # Counter for other ME activity after cancel inbound
                            order_testing_counter['cancel_other_me'] += 1

                    # Classify unresolved events as no response
                    if not resolved_i:
                        order_testing_counter['cancel_no_reply'] += 1 # cancel message but no ME response
                        msgs.at[i, 'Event'] = 'Cancel no response'  # Assign event to message i
                        resolved_i = True  # Update indicator

                # Gateway Cancel/Replace
                # This includes the following Type:  'Gateway Cancel/Replace'
                # and classifies message i in the following Events:
                # 'Cancel/replace request aggr executed in full', 'Cancel/replace request rejected', 
                # 'Cancel/replace request failed', 'Cancel/replace request no response'
                # The difference between cancel/replace request rejected and failed is similar to 
                # that of cancel request rejected and failed. 
                # Only Cancel/replace request rejected is counted as failed cancels in race detection.
                elif order_msgs.at[i, 'MessageType'] == 'Cancel_Replace_Request':
                    counter += 1  # Increment event counter
                    order.amend(p=msgs.at[i, 'LimitPrice'], q=msgs.at[i, 'OrderQty'])
                    msgs.at[i, 'EventNum'] = counter  # Populate EventNum
                    msgs.at[i, 'PrevPriceLvl'] = order.cancel_prc
                    msgs.at[i, 'PriceLvl'] = order.gw_prc
                    msgs.at[i, 'Categorized'] = True  # Flag message i as categorized
                    resolved_i = False  # Initialize indicator for loop

                    # Loop over subsequent messages
                    for j in order_msgs.index[order_msgs.index > i]:

                        # Skip if message j has already been categorized
                        # or message j has different ClientOrderID value than message i
                        if msgs.at[j, 'Categorized'] or msgs.at[j, 'ClientOrderID'] != msgs.at[i, 'ClientOrderID']:
                            continue

                        # ME Cancel/Replace Accept
                        if order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Cancel/Replace Accept':
                            order.update_me(q=msgs.at[j, 'LeavesQty'])
                            msgs.at[j, 'EventNum'] = counter  # Populate EventNum
                            msgs.at[j, 'PrevPriceLvl'] = order.cancel_prc
                            msgs.at[i, 'PrevQty'] = order.cancel_qty
                            msgs.at[j, 'PriceLvl'] = order.me_prc
                            msgs.at[j, 'Categorized'] = True  # Flag message j as categorized
                            resolved_j = False  # Initialize indicators for loop

                            # Loop over subsequent messages
                            for k in order_msgs.index[order_msgs.index > j]:

                                # Skip if message is not an uncategorized response
                                # or message k has different ClientOrderID value than message i
                                if msgs.at[k, 'Categorized'] or msgs.at[k, 'ClientOrderID'] != msgs.at[i, 'ClientOrderID']:
                                    continue

                                # ME Full Fill - (A) for aggressive
                                if order_msgs.at[k, 'UnifiedMessageType'] == 'ME: Full Fill (A)':
                                    order.update_me(q=msgs.at[k, 'LeavesQty'])
                                    msgs.at[i, 'Event'] = 'Cancel/replace request aggr executed in full'  # Assign event to message i
                                    msgs.at[i, 'MinExecPriceLvl'] = msgs.at[j, 'ExecutedPrice']
                                    msgs.at[i, 'MaxExecPriceLvl'] = msgs.at[j, 'ExecutedPrice']
                                    msgs.at[k, 'EventNum'] = counter  # Populate EventNum
                                    msgs.at[k, 'PriceLvl'] = order.me_prc
                                    msgs.at[k, 'Categorized'] = True # Flag message k as categorized
                                    resolved_j, resolved_i = True, True  # Update indicators

                                # ME Partial Fill - (A) for aggressive
                                elif order_msgs.at[k, 'UnifiedMessageType'] == 'ME: Partial Fill (A)':
                                    order.update_me(q=msgs.at[k, 'LeavesQty'])
                                    executed_prices = [msgs.at[k, 'ExecutedPrice']]
                                    msgs.at[k, 'EventNum'] = counter  # Populate EventNum
                                    msgs.at[k, 'PriceLvl'] = order.me_prc
                                    msgs.at[k, 'Categorized'] = True  # Flag message k as categorized
                                    resolved_k = False  # Initialize indicator for loop

                                    # Loop over subsequent messages
                                    for l in order_msgs.index[order_msgs.index > k]:

                                        # Skip if message l is not an uncategorized response
                                        # message l has different ClientOrderID value than message i
                                        if msgs.at[l, 'Categorized'] or msgs.at[l, 'ClientOrderID'] != msgs.at[i, 'ClientOrderID']:
                                            continue

                                        # ME Full Fill - (A) for aggressive
                                        if order_msgs.at[l, 'UnifiedMessageType'] == 'ME: Full Fill (A)':
                                            order.update_me(q=msgs.at[l, 'LeavesQty'])
                                            executed_prices = executed_prices + [msgs.at[l, 'ExecutedPrice']]
                                            msgs.at[i, 'Event'] = 'Cancel/replace request aggr executed in full'  # Assign event to message i
                                            msgs.at[i, 'MinExecPriceLvl'] = min(executed_prices)
                                            msgs.at[i, 'MaxExecPriceLvl'] = max(executed_prices)
                                            msgs.at[l, 'EventNum'] = counter # Populate EventNum
                                            msgs.at[l, 'PriceLvl'] = order.me_prc
                                            msgs.at[l, 'Categorized'] = True  # Flag message l as categorized
                                            resolved_k, resolved_j, resolved_i = True, True, True  # Update indicators

                                        # ME Partial Fill - (A) for aggressive
                                        elif order_msgs.at[l, 'UnifiedMessageType'] == 'ME: Partial Fill (A)':
                                            order.update_me(q=msgs.at[l, 'LeavesQty'])
                                            executed_prices = executed_prices + [msgs.at[l, 'ExecutedPrice']]
                                            msgs.at[i, 'Event'] = '' # Do not assign event to message i 
                                            msgs.at[l, 'EventNum'] = counter  # Populate EventNum
                                            msgs.at[l, 'PriceLvl'] = order.me_prc
                                            msgs.at[l, 'Categorized'] = True  # Flag message l as categorized
                                            resolved_k, resolved_j, resolved_i = False, False, False  # Do not update indicators
                                            

                                        # Other ME Message 
                                        # This includes any Execution Report for an inbound message that relates to a different Event.
                                        # These messages mark the end of the current event.
                                        # No EventNum assigned to k as in this case the message is related to a different Event
                                        # e.g. passive fills after some aggressive partial fills (the order executed aggressively
                                        # in part and then traded passively after resting in the book for a while).
                                        elif order_msgs.at[l, 'MessageType'] == 'Execution_Report':
                                            msgs.at[i, 'Event'] = 'Cancel/replace request aggr executed in part'  # Assign event to message i
                                            msgs.at[i, 'MinExecPriceLvl'] = min(executed_prices)
                                            msgs.at[i, 'MaxExecPriceLvl'] = max(executed_prices)
                                            resolved_k, resolved_j, resolved_i = True, True, True  # Update indicators
                                            order_testing_counter['partial_other_me'] += 1
                                            
                                        if resolved_k:
                                            break

                                    # No further ME Response
                                    # No further response, but an aggressive partial fill is already observed
                                    # e.g. aggressively executed in part then posted to book and wait for 
                                    # passive execution. Note that no post-to-book message will be generated 
                                    # by the LSE matching engine in this case.
                                    if not resolved_k:
                                        msgs.at[i, 'Event'] = 'Cancel/replace request aggr executed in part'  # Assign event to message i
                                        msgs.at[i, 'MinExecPriceLvl'] = min(executed_prices)
                                        msgs.at[i, 'MaxExecPriceLvl'] = max(executed_prices)
                                        resolved_k, resolved_j, resolved_i = True, True, True  # Update indicators\

                                if resolved_i:
                                    break

                                # Other ME Message
                                # This includes any Execution Report for an inbound message that relates to a different Event.
                                # These messages mark the end of the current event.
                                # Since the cancel/replace accept message is already observed, the event is C/R accepted.
                                # NOTE: In principle C/R accept may trigger a suspend (if the stop price is affected), 
                                # expire (such as by SEP - Self Execution Prevention, client can set to prevent execution between 
                                # her own orders), or reject, that is, the order is suspended after being accepted.
                                # The code will treat the ME message in such cases (e.g. suspend, expire, etc. after the C/R accept)
                                # as part of an 'Other ME activity' and the current event is still C/R accepted.
                                elif order_msgs.at[k, 'MessageType'] == 'Execution_Report':
                                    msgs.at[i, 'Event'] = 'Cancel/replace request accepted'  # Assign event to message i
                                    resolved_j, resolved_i = True, True  # Update indicators

                                if resolved_j:
                                    break

                            # No ME Response
                            if not resolved_j:
                                msgs.at[i, 'Event'] = 'Cancel/replace request accepted'  # Assign event to message i
                                resolved_i = True  # Update indicator

                        # ME Cancel Reject
                        elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Cancel Reject (TLTC)':
                            msgs.at[i, 'Event'] = 'Cancel/replace request rejected'
                            msgs.at[j, 'EventNum'] = counter  # Populate EventNum
                            msgs.at[j, 'PrevPriceLvl'] = order.cancel_prc
                            msgs.at[i, 'PrevQty'] = order.cancel_qty
                            msgs.at[j, 'PriceLvl'] = order.me_prc
                            msgs.at[j, 'Categorized'] = True  # Flag message j as categorized
                            resolved_i = True

                        # ME Other Reject
                        elif order_msgs.at[j, 'UnifiedMessageType'] in ('ME: Cancel Reject (Other)', 'ME: Other Reject'):
                            msgs.at[i, 'Event'] = 'Cancel/replace request failed'
                            msgs.at[j, 'PrevPriceLvl'] = order.cancel_prc
                            msgs.at[i, 'PrevQty'] = order.cancel_qty
                            msgs.at[j, 'PriceLvl'] = order.me_prc
                            msgs.at[j, 'Categorized'] = True  # Flag message j as categorized
                            resolved_i = True
                            
                        else:
                            # other ME message after CR inbound
                            order_testing_counter['nocr_other_me'] += 1

                        if resolved_i:
                            break

                    # No ME Response (corner case) 
                    # No related outbound message for the C/R inbound
                    if not resolved_i:
                        # Counter for no reply after cancel replace inbound
                        order_testing_counter['cr_no_reply'] += 1
                        msgs.at[i, 'Event'] = 'Cancel/replace no response'  # Assign event to message i
                        resolved_i = True  # Update indicator

                # Gateway Other Inbound
                # This includes the following Type: 'Other_Inbound'
                # and classifies message i in  the following Events:
                # 'Other Gateway activity'
                elif order_msgs.at[i, 'MessageType'] == 'Other_Inbound':
                    counter += 1
                    msgs.at[i, 'Event'] = 'Other Gateway activity'
                    msgs.at[i, 'EventNum'] = counter
                    msgs.at[i, 'Categorized'] = True
                
                # Execution Report
                # Passive, other fills and reject cases for outbounds
                # These messages refer to orders passively executed and 
                # to outbound messages with missing inbound messages (packet loss)
                # This includes the following Type: 
                # 'ME: Partial Fill (P)', 'ME: Full Fill (P)', 
                # 'ME: Partial Fill (Other)', 'ME: Full Fill (Other)', 
                # 'ME: Cancel Accept', 'ME: Cancel/Replace Accept', 
                # and classifies message i in the following Events:
                # 'Other ME activity', 
                # 'Order passively executed in part', 'Order passively executed in full', 
                # 'Order executed in part (other)', 'Order executed in full (other)'
                elif order_msgs.at[i, 'MessageType'] == 'Execution_Report':

                    counter += 1  # Increment event counter

                    # Partial passive execution - (P) for Passive 
                    if order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Partial Fill (P)':
                        order.passive_fill(leaves=msgs.at[i, 'LeavesQty'])
                        msgs.at[i, 'Event'] = 'Order passively executed in part'  # Assign event to message i
                        msgs.at[i, 'EventNum'] = counter  # Populate EventNum for message i
                        msgs.at[i, 'PriceLvl'] = order.me_prc
                        msgs.at[i, 'Categorized'] = True  # Flag message i as categorized

                    # Full passive execution - (P) for Passive 
                    elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Full Fill (P)':
                        order.passive_fill(leaves=np.nan)
                        msgs.at[i, 'Event'] = 'Order passively executed in full'  # Assign event to message i
                        msgs.at[i, 'EventNum'] = counter  # Populate EventNum for message i
                        msgs.at[i, 'PriceLvl'] = order.me_prc
                        msgs.at[i, 'Categorized'] = True  # Flag message i as categorized

                    # Other partial execution
                    elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Partial Fill (Other)':
                        order.update_me(q=msgs.at[i, 'LeavesQty'])
                        msgs.at[i, 'Event'] = 'Order executed in part (other)' # Assign event to message i
                        msgs.at[i, 'EventNum'] = counter  # Populate EventNum for message i
                        msgs.at[i, 'PriceLvl'] = order.me_prc
                        msgs.at[i, 'Categorized'] = True # Flag message i as categorized

                    # Other full execution
                    elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Full Fill (Other)':
                        order.update_me(q=np.nan)
                        msgs.at[i, 'Event'] = 'Order executed in full (other)'  # Assign event to message i
                        msgs.at[i, 'EventNum'] = counter  # Populate EventNum for message i
                        msgs.at[i, 'PriceLvl'] = order.me_prc
                        msgs.at[i, 'Categorized'] = True  # Flag message i as categorized

                    # Other cancel accept (NOTE: For missing GW msg due to packet loss.)
                    elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Cancel Accept':
                        order.update_me(q=msgs.at[i, 'LeavesQty'])
                        msgs.at[i, 'Event'] = 'Other ME activity'
                        msgs.at[i, 'EventNum'] = counter
                        msgs.at[i, 'PrevPriceLvl'] = order.me_prc
                        msgs.at[i, 'Categorized'] = True
                        order_testing_counter['other_me_activity'] += 1 # testing counter

                    # Other cancel/replace accept (For missing GW msg due to packet loss.)
                    elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Cancel/Replace Accept':
                        msgs.at[i, 'PrevPriceLvl'] = order.me_prc
                        order.amend()
                        order.update_me(q=msgs.at[i, 'LeavesQty'])
                        msgs.at[i, 'Event'] = 'Other ME activity'
                        msgs.at[i, 'EventNum'] = counter  # Populate EventNum
                        msgs.at[i, 'Categorized'] = True  # Flag message j as categorized
                        order_testing_counter['other_me_activity'] += 1 # testing counter
                        
                    # ME Order Expire
                    elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Order Expire':
                        order.update_me(q=np.nan)
                        msgs.at[i, 'Event'] = 'Other ME activity'  # Assign event to message i
                        msgs.at[i, 'EventNum'] = counter  # Populate EventNum
                        msgs.at[i, 'PriceLvl'] = order.me_prc
                        msgs.at[i, 'Categorized'] = True  # Flag message j as categorized

                    # Other ME activity
                    else:
                        order.update_me(q=msgs.at[i, 'LeavesQty'])
                        msgs.at[i, 'Event'] = 'Other ME activity'
                        msgs.at[i, 'EventNum'] = counter
                        msgs.at[i, 'Categorized'] = True
                        order_testing_counter['other_me_activity'] += 1 # testing counter

                # Other outbound cases 
                # For other outbound cases where an inbound cannot be found (packet loss), 
                # we classify them into 'Other ME activity' here. If their inbounds could be
                # found, we would have already categorized them in the previous sections.
                # This includes the following Type of outbound: 
                # 'ME: Cancel Reject (TLTC)', 'ME: Cancel Reject (Other), 
                # 'ME: Other Reject',
                # and classifies message i in the following Events:
                # 'Other ME activity' 
                elif order_msgs.at[i, 'MessageType'] in ('Other_Reject', 'Cancel_Reject', 'Other_Outbound'):
                    counter += 1
                    msgs.at[i, 'Event'] = 'Other ME activity'
                    msgs.at[i, 'EventNum'] = counter
                    msgs.at[i, 'Categorized'] = True
                    order_testing_counter['other_me_activity'] += 1

        ## Quotes
        # Note that for data from exchanges without quotes, simply set QuoteRelated to False
        # for all messages and this section will be automatically skipped.
        # 
        # A quote is different from an order:
        #     1. A quote is a two-sided, executable, limit order valid for the day.
        #     2. Only qualified users (market makers) can use quote and each 
        #        user can only have one active quote at a time.
        #     3. When one side of the quote is fully filled, 
        #        the other side will NOT be cancelled automatically.
        # 
        # Some quote related messages can have effect on one or two sides while others can only 
        # have effects on both sides:
        #     1. New_Quote message when there is no active quote from the user: act on both sides only.
        #     2. New_Quote message to modify an existing quote: act on one or two sides.
        #     3. Cancel_Request: act on both sides (one can only cancel both sides of the quote).
        #     4. Rejects: when a New_Quote is rejected, it is rejected on both sides.
        # 
        # Loop over quote related messages on each side
        # Inbound messages only trigger outbound messages on the side (or sides) that they update. 
        # It is possible to see a quote related inbound but there is no outbound on one side
        # because that inbound only has an effect on one side.
        # An outbound on one side is expected if the quote changes the price/quantity of that side
        # or updates from missing. We look for the outbound message only if we expect there is an 
        # outbound on that side. This is because we need to know whether missing an outbound on 
        # one side is normal (because the quote inbound does not affect this side), 
        # or due to packet loss.
        #
        # Simple Quote Example (Common Case) - 'Quote Updated' Event using 'Gateway New Quote' that outbounds 'ME: Cancel/Replace Accept'
        # 
        #     Action: Participant sent Gateway New Order (MessageType == 'New_Quote') with an updated bid/ask quote and the ME 
        #             cancelled the previous quote and replaced it with the new bid/ask quote
        # 
        #     Event Classification process:
        # 
        #         Summary: Loop over messages until we get to our inbound Gateway New Quote message and then we check the 
        #                  subsequent messages until we find an outbound 'ME: Cancel/Replace Accept' message. We assign both messages
        #                  to the same EventNum and label the Event 'Native Quote Updated'
        # 
        #         1. Loop over msgs for the user on Bid and Ask sides until you get to the new quote message
        #         2. Check that MessageType == 'New_Quote' and whether the message has already been categorized
        #         3. Assign an EventNum to the message and set Categorized to True
        #         4. Loop over subsequent messages. For each subsequent message, check if we expect a ME response and whether the 
        #            new message has already been categorized
        #         5. Check new messages' UnifiedMessageType until UnifiedMessageType == 'ME: Cancel/Replace Accept' 
        #         6. Then, assign the inbound quote message EventNum to the new message (they are now part of the same Event),
        #            and set the Event of the inbound message to 'Native Quote Updated'
        #         7. Terminate the second loop, and move on to the next message
        # 
        #       Result: The inbound 'Gateway New Quote' message and the outbound 'ME: Cancel/Replace Accept' message form an Event 
        #               labeled 'Native Quote Updated' with number EventNum.
        # 

        if sum(user_msgs['QuoteRelated'] > 0):

            order_msgs = user_msgs.loc[user_msgs['QuoteRelated']]
            
            # Loop over both Sides as each quote can update one side or both sides.
            # Categorize Bid and Ask events separately
            for S in ['Bid', 'Ask']:

                counter = 0 # Initialize event counter

                quote = Quote()

                for i in order_msgs.index:

                    # Skip previously categorized messages
                    if msgs.at[i, '%sCategorized' % S]:
                        continue

                    # Gateway New Quote
                    # This includes the following Unified Message Types:  'Gateway New Quote'
                    # and classifies message i in the following Events:
                    # 'New quote accepted', 'New quote updated', 
                    # 'New quote aggressively executed in full',
                    # 'New quote aggressively executed in part', 
                    # 'New quote expired', 'New quote failed',
                    # 'New quote suspended', 'New quote no response'
                    elif order_msgs.at[i, 'MessageType'] == 'New_Quote':
                        quote.update(p=msgs.at[i, '%sPrice' % S], q=order_msgs.at[i, '%sSize' % S])
                        quote.update_expectations()
                        counter += 1  # Increment event counter
                        pcounter = counter # counter for passive events
                        msgs.at[i, '%sEventNum' % S] = counter  # Populate EventNum
                        msgs.at[i, 'Prev%sPriceLvl' % S] = quote.cancel_prc
                        msgs.at[i, '%sPriceLvl' % S] = quote.gw_prc
                        msgs.at[i, '%sCategorized' % S] = True # Flag message i as categorized
                        resolved_i = False  # Initialize indicator for loop

                        # Loop over subsequent messages
                        opposite_side_accepts = 0

                        # Skip to next iteration of loop if we do not expect ME message 
                        if not (quote.expect_add or quote.expect_amend):
                            # Testing counter for no reply expected after a quote
                            quote_testing_counter['quote_reply_not_exp'] += 1
                            resolved_i = True
                            continue
                        
                        # Loop over subsequent messages to assign the event to message i based on outbound messages
                        # of the following Unified Message Types:
                        # ME: New Order Accept, ME: Order Reject, 
                        # ME: Partial Fill (P), ME: Partial Fill (A),
                        # ME: Full Fill (P), ME: Full Fill (A), 
                        # ME: Order Expire, ME: Order Suspend,
                        # ME: Other Reject, 'ME: Cancel/Replace Accept'
                        for j in order_msgs.index[order_msgs.index > i]:
                            
                            # Skip if message j has already been categorized
                            if msgs.at[j, '%sCategorized' % S]:
                                continue

                            # If message j is on the opposite side of the current loop
                            # (we loop through Bid and Ask separately), we need to 
                            # decide whether to break, pass, or continue:
                            #     1. If there have been two accepts on the opposite side 
                            #        after the new quote inbound, then this means outbound j 
                            #        is already in the next quote event. The event on the current side
                            #        must be ended due to serial processing of messages. 
                            #        Hence we break from the loop.
                            #     2. Else if message j is a reject, a reject on either side indicates a 
                            #        reject on both sides. So we let the message go through the event
                            #        classification code and it will be categorized as a reject event.
                            #     3. Else, we continue the loop to the next message 
                            #        because we are doing the classification for two sides separately
                            #        and message j is on the opposite side. It is not a reject, so it 
                            #        does not affect the event of the current side of the loop.

                            if order_msgs.at[j, 'Side'] == opposite(S):
                                opposite_side_accepts += order_msgs.at[j, 'UnifiedMessageType'] in ('ME: Cancel/Replace Accept', 'ME: New Order Accept')
                                if opposite_side_accepts > 1:
                                    # Testing counter for cases in which 2 op accept messages were seen 
                                    quote_testing_counter['op_side_break'] += 1
                                    break
                                elif order_msgs.at[j, 'UnifiedMessageType'] in ('ME: Order Reject', 'ME: Other Reject'):
                                    pass
                                else:
                                    continue

                            # Break if next ME message has different ClientOrderID
                            # This assumes that Gateway New Order messages are processed serially in order
                            # of submission. This assumption should be fine within a UserID.
                            # Note that we require the MessageType to be Execution_Report, in addition to 
                            # having a different ClientOrderID to break. The purpose of the additional 
                            # requirement on MessageType is to let cancels go through this check instead of break.
                            # With perfect data, this should not bind.
                            if order_msgs.at[j, 'ClientOrderID'] != order_msgs.at[i, 'ClientOrderID']:
                                # if order_msgs.at[j, 'MessageType'] == 'Execution_Report':
                                break

                            # ME New Order Accept 
                            if (order_msgs.at[j, 'UnifiedMessageType'] == 'ME: New Order Accept'):
                                quote.update_me(q=order_msgs.at[j, 'LeavesQty'], st='Accepted (0)')
                                msgs.at[i, '%sEvent' % S] = 'New quote accepted'  # Assign event to message i
                                msgs.at[j, '%sEventNum' % S] = counter  # Populate EventNum
                                msgs.at[j, '%sPriceLvl' % S] = quote.me_prc
                                msgs.at[j, '%sCategorized' % S] = True  # Flag message j as categorized
                                resolved_i = True  # Update indicator

                            # ME Cancel/Replace Accept
                            elif (order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Cancel/Replace Accept'):
                                quote.update_me(q=order_msgs.at[j, 'LeavesQty'], st='Amended (5)')
                                msgs.at[i, '%sEvent' % S] = 'New quote updated'  # Assign event to message i
                                msgs.at[j, '%sEventNum' % S] = counter  # Populate EventNum
                                msgs.at[j, 'Prev%sPriceLvl' % S] = quote.cancel_prc
                                msgs.at[i, 'Prev%sQty' % S] = quote.cancel_qty
                                msgs.at[j, '%sPriceLvl' % S] = quote.me_prc
                                msgs.at[j, '%sCategorized' % S] = True  # Flag message j as categorized
                                resolved_i = True  # Update indicator

                            # ME Full Fill - Passive
                            # This block allows us to pass over passive fill messages that occur immediately after
                            # Gateway order submission. Otherwise, we would break the loop on observing a passive fill.
                            # The point is to simplify the code. Note that we only categorize message j here, i is not categorized.
                            elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Full Fill (P)':
                                pcounter += 1
                                quote.passive_fill(leaves=np.nan, st='Executed (2)')
                                msgs.at[j, '%sEvent' % S] = 'Quote passively executed in full'
                                msgs.at[j, '%sEventNum' % S] = pcounter
                                msgs.at[j, '%sPriceLvl' % S] = quote.me_prc  # Assume fill at prev ME limit price
                                msgs.at[j, '%sCategorized' % S] = True

                            # ME Full Fill - Aggressive
                            elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Full Fill (A)':
                                quote.update_me(q=msgs.at[j, 'LeavesQty'], st='Executed (2)')
                                msgs.at[i, '%sEvent' % S] = 'New quote aggressively executed in full'  # Assign event to message i
                                msgs.at[i, '%sMinExecPriceLvl' % S] = msgs.at[j, 'ExecutedPrice']
                                msgs.at[i, '%sMaxExecPriceLvl' % S] = msgs.at[j, 'ExecutedPrice']
                                msgs.at[j, '%sEventNum' % S] = counter  # Populate EventNum
                                msgs.at[j, '%sPriceLvl' % S] = quote.me_prc
                                msgs.at[j, '%sCategorized' % S] = True  # Flag message j as categorized
                                resolved_i = True  # Update indicator

                            # ME Partial Fill - Passive
                            # This block allows us to pass over passive fill messages that occur immediately after
                            # Gateway order submission. Otherwise, we break the loop on observing a passive fill.
                            # The point is to simplify the code. Note that we only categorize message j here, i is not categorized.
                            elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Partial Fill (P)':
                                pcounter += 1
                                quote.passive_fill(leaves=msgs.at[j, 'LeavesQty'], st='Executed (1)')
                                msgs.at[j, '%sEvent' % S] = 'Quote passively executed in part'
                                msgs.at[j, '%sEventNum' % S] = pcounter
                                msgs.at[j, '%sPriceLvl' % S] = quote.me_prc  # Assume fill at prev ME limit price
                                msgs.at[j, '%sCategorized' % S] = True

                            # ME Partial Fill - A is Aggressive
                            elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Partial Fill (A)':
                                quote.update_me(q=msgs.at[j, 'LeavesQty'], st='Executed (1)')
                                executed_prices = [msgs.at[j, 'ExecutedPrice']]
                                msgs.at[i, '%sEvent' % S] = 'New quote aggressively executed in part'
                                msgs.at[i, '%sMinExecPriceLvl' % S] = min(executed_prices)
                                msgs.at[i, '%sMaxExecPriceLvl' % S] = max(executed_prices)
                                msgs.at[j, '%sEventNum' % S] = counter  # Populate EventNum
                                msgs.at[j, '%sPriceLvl' % S] = quote.gw_prc
                                msgs.at[j, '%sCategorized' % S] = True  # Flag message j as categorized
                                resolved_j = False  # Initialize indicator for loop

                                # Loop over subsequent messages
                                # This loop either completes the event on the aggressive partial fill or 
                                # checks remaining messages for additional partial fills or full fills
                                for k in order_msgs.index[order_msgs.index > j]:

                                    # Skip if message has already been categorized
                                    if msgs.at[k, '%sCategorized' % S]:
                                        continue
                                    # For messages on the opposite side (see previous comments)
                                    if order_msgs.at[k, 'Side'] == opposite(S):
                                        opposite_side_accepts += order_msgs.at[k, 'UnifiedMessageType'] in ('ME: Cancel/Replace Accept', 'ME: New Order Accept')
                                        if opposite_side_accepts > 1:
                                            quote_testing_counter['op_side_break'] += 1
                                            break
                                        elif order_msgs.at[k, 'UnifiedMessageType'] in ('ME: Order Reject', 'ME: Other Reject'):
                                            pass
                                        else:
                                            continue
                                    # Break if next ME message has different ClientOrderID
                                    # See previous comments for details. 
                                    if order_msgs.at[k, 'ClientOrderID'] != order_msgs.at[i, 'ClientOrderID']:
                                        # if msgs.at[k, 'MessageType'] == 'Execution_Report':
                                        break

                                    # ME Full Fill
                                    if order_msgs.at[k, 'UnifiedMessageType'] == 'ME: Full Fill (A)':
                                        quote.update_me(q=msgs.at[k, 'LeavesQty'], st='Executed (2)')
                                        executed_prices = executed_prices + [msgs.at[k, 'ExecutedPrice']]
                                        msgs.at[i, '%sEvent' % S] = 'New quote aggressively executed in full'  # Assign event to message i
                                        msgs.at[i, '%sMinExecPriceLvl' % S] = min(executed_prices)
                                        msgs.at[i, '%sMaxExecPriceLvl' % S] = max(executed_prices)
                                        msgs.at[k, '%sEventNum' % S] = counter # Populate EventNum
                                        msgs.at[k, '%sPriceLvl' % S] = quote.gw_prc
                                        msgs.at[k, '%sCategorized' % S] = True # Flag message k as categorized
                                        resolved_j, resolved_i = True, True  # Update indicators

                                    # ME Partial Fill
                                    elif order_msgs.at[k, 'UnifiedMessageType'] == 'ME: Partial Fill (A)':
                                        quote.update_me(q=msgs.at[k, 'LeavesQty'], st='Executed (1)')
                                        executed_prices = executed_prices + [msgs.at[k, 'ExecutedPrice']]
                                        msgs.at[i, '%sMinExecPriceLvl' % S] = min(executed_prices)
                                        msgs.at[i, '%sMaxExecPriceLvl' % S] = max(executed_prices)
                                        msgs.at[k, '%sEventNum' % S] = counter  # Populate EventNum
                                        msgs.at[k, '%sPriceLvl' % S] = quote.gw_prc
                                        msgs.at[k, '%sCategorized' % S] = True  # Flag message k as categorized

                                    # Other ME Message
                                    # e.g. Cancel accept and passive fill messages would fall into this category
                                    elif order_msgs.at[k, 'MessageType'] == 'Execution_Report':
                                        quote_testing_counter['partial_other_me'] += 1 # Testing counter
                                        if order_msgs.at[k, 'UnifiedMessageType'] in ('ME: Partial Fill (P)', 'ME: Full Fill (P)'):
                                            quote_testing_counter['partial_me_passive'] += 1 # Testing counter
                                        elif order_msgs.at[k, 'UnifiedMessageType'] == 'ME: Cancel Accept':
                                            quote_testing_counter['partial_me_cancel'] += 1 # Testing counter
                                        break
                                        
                                    if resolved_j:
                                        break

                                # No further ME Response
                                # No further ME response after an aggressive partial fill.
                                # e.g. the quote traded aggressively in part and the rest is post to book.
                                # In LSE, the matching engine will not send a post-to-book confirmation in this case.
                                if not resolved_j:
                                    quote_testing_counter['pf_no_further_reply'] += 1
                                    resolved_i = True

                            # ME Order Reject 
                            # If there is a reject on either side, the entire quote is rejected
                            elif order_msgs.at[j, 'UnifiedMessageType'] in ('ME: Order Reject', 'ME: Other Reject'):
                                quote.update_me(q=np.nan, st='Rejected (8)')
                                msgs.at[i, '%sEvent' % S] = 'New quote failed'  # Assign event to message i
                                msgs.at[j, '%sEventNum' % S] = counter  # Populate EventNum
                                msgs.at[j, '%sPriceLvl' % S] = quote.gw_prc
                                msgs.at[j, '%sCategorized' % S] = True  # Flag message j as categorized
                                resolved_i = True  # Update indicator

                            # ME Order Expire
                            elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Order Expire':
                                quote.update_me(q=np.nan, st='Expired (6)')
                                msgs.at[i, '%sEvent' % S] = 'New quote expired'  # Assign event to message i
                                msgs.at[j, '%sEventNum' % S] = counter  # Populate EventNum
                                msgs.at[j, '%sPriceLvl' % S] = quote.gw_prc
                                msgs.at[j, '%sCategorized' % S] = True  # Flag message j as categorized
                                resolved_i = True  # Update indicator

                            # ME Order Suspend
                            elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Order Suspend':
                                quote.update_me(q=msgs.at[j, 'LeavesQty'], st='Suspended (9)')
                                msgs.at[i, '%sEvent' % S] = 'New quote suspended'  # Assign event to message i
                                msgs.at[j, '%sEventNum' % S] = counter  # Populate EventNum
                                msgs.at[j, '%sPriceLvl' % S] = quote.me_prc
                                msgs.at[j, '%sCategorized' % S] = True  # Flag message j as categorized
                                resolved_i = True  # Update indicator

                            # Other ME Message
                            # e.g. Cancel accept messages would fall into this category 
                            # This should not happen with perfect data since there shouldn't be
                            # a cancel accept following a New_Quote message
                            elif order_msgs.at[j, 'MessageType'] == 'Execution_Report':
                                quote_testing_counter['nqt_other_me'] += 1
                                if order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Cancel Accept':
                                    quote_testing_counter['nqt_me_cancel'] += 1
                                break
                            
                            if resolved_i:
                                break                        

                        # No ME Response
                        # In new orders this category would parallel "New order no response"
                        if not resolved_i:
                            # testing counter for cases in which 2 op messages were seen 
                            quote_testing_counter['expected_never_arrived'] += 1
                            msgs.at[i, '%sEvent' % S] = 'New quote no response'  # Assign event to message i
                            quote.no_me_response()

                        # Update counter
                        # pcounter was incremented by passive events. This ensures we do not 
                        # have the same event numbers for passive and aggressive events
                        counter = pcounter
                        
                    # Gateway Cancel
                    # This includes the following Unified Message Types: 'Gateway Quote Cancel'
                    # and classifies message i in the following Events:
                    # 'Quote cancel accepted', 'Quote cancel failed', 'Quote cancel rejected', 
                    # 'Quote cancel no response'
                    elif order_msgs.at[i, 'MessageType'] == 'Cancel_Request':
                        counter += 1  # Increment event counter 
                        quote.update_expectations()
                        msgs.at[i, '%sEventNum' % S] = counter  # Populate EventNum
                        msgs.at[i, 'Prev%sPriceLvl' % S] = quote.gw_prc  # Assume cancel occurs at price level of last
                        msgs.at[i, '%sCategorized' % S] = True  # Flag message i as categorized
                        resolved_i = False  # Initialize indicator for loop
                        
                        # Break if expect cancel is false
                        if quote.expect_cancel == False:
                            # testing counter for no reply expected after a quote
                            quote_testing_counter['quote_reply_not_exp'] += 1
                            resolved_i = True
                            continue

                        # Loop over subsequent messages to assign the event to message i based on outbound messages
                        # of the following Unified Message Types:
                        # 'ME: Order Reject', 'ME: Protocol Reject', 'ME: Business Reject', 'ME: Cancel Accept',
                        # 'ME: Cancel Reject (TLTC)', 'ME: Cancel Reject (Other)'
                        for j in order_msgs.index[order_msgs.index > i]:
                            
                            # Continue to the next message and skip j if 
                            # message j has already been categorized. 
                            if msgs.at[j, '%sCategorized' % S]:
                                continue

                            # If message j is on the opposite side of the loop:
                            # If message j is a cancel reject, then let message j go through 
                            # the event classification code below. Since a reject will have
                            # an effect on both sides.
                            # Else if message 
                            # we see a new order accept on the opposite side.
                            # Else, continue the loop: skip message j and go to the next one
                            # since j is on the opposite side and has no effect on the current 
                            # side of the loop.
                            if order_msgs.at[j, 'Side'] == opposite(S):
                                if order_msgs.at[j, 'UnifiedMessageType'] in ('ME: Cancel Reject (Other)', 'ME: Cancel Reject (TLTC)', 'ME: Other Reject'):
                                    pass
                                elif order_msgs.at[j, 'UnifiedMessageType'] in  ('ME: Cancel/Replace Accept', 'ME: New Order Accept'):
                                    quote_testing_counter['op_side_break'] += 1
                                    break
                                else:
                                    continue
                            
                            if order_msgs.at[j, 'ClientOrderID'] != order_msgs.at[i, 'ClientOrderID']:
                                # if order_msgs.at[j, 'MessageType'] == 'Execution_Report':
                                break

                            # Break on next ME: New Order Accept 
                            # Because this means we enter the next event of the current side of the loop
                            if order_msgs.at[j, 'UnifiedMessageType'] == 'ME: New Order Accept':
                                break

                            # ME Cancel Accept
                            if order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Cancel Accept':
                                quote.cancel()
                                msgs.at[i, '%sEvent' % S] = 'Quote cancel accepted'  # Assign event to message i
                                msgs.at[j, '%sEventNum' % S] = counter  # Populate EventNum
                                msgs.at[j, 'Prev%sPriceLvl' % S] = quote.cancel_prc
                                msgs.at[i, 'Prev%sQty' % S] = quote.cancel_qty
                                msgs.at[j, '%sCategorized' % S] = True  # Flag message j as categorized
                                resolved_i = True  # Update indicator

                            # ME Cancel Reject (TLTC = To Late to Cancel)
                            # This is the event used for failed cancels in the race detection
                            elif order_msgs.at[j, 'UnifiedMessageType'] == 'ME: Cancel Reject (TLTC)':
                                quote.cancel_reject()
                                msgs.at[i, '%sEvent' % S] = 'Quote cancel rejected'  # Assign event to message i
                                msgs.at[j, '%sEventNum' % S] = counter  # Populate EventNum
                                msgs.at[j, 'Prev%sPriceLvl' % S] = quote.cancel_prc
                                msgs.at[i, 'Prev%sQty' % S] = quote.cancel_qty
                                msgs.at[j, '%sCategorized' % S] = True  # Flag message j as categorized
                                resolved_i = True  # Update indicator

                            # ME Cancel Reject 
                            # If there is a reject on either side, the entire quote is rejected
                            elif order_msgs.at[j, 'UnifiedMessageType'] in ('ME: Cancel Reject (Other)', 'ME: Protocol Reject', 'ME: Business Reject'):
                                quote.cancel_reject()
                                msgs.at[i, '%sEvent' % S] = 'Quote cancel failed'  # Assign event to message i
                                msgs.at[j, '%sEventNum' % S] = counter  # Populate EventNum
                                msgs.at[j, 'Prev%sPriceLvl' % S] = quote.cancel_prc
                                msgs.at[i, 'Prev%sQty' % S] = quote.cancel_qty
                                msgs.at[j, '%sCategorized' % S] = True  # Flag message j as categorized
                                resolved_i = True  # Update indicator

                            # Testing counter for other outbound messages
                            elif order_msgs.at[j, 'MessageType'] == 'Execution_Report' and order_msgs.at[j, 'Side'] == S:
                                quote_testing_counter['cancel_other_me'] += 1 # testing counter
                            
                            if resolved_i:
                                break

                        # No ME Response
                        if not resolved_i:
                            msgs.at[i, '%sEvent' % S] = 'Quote cancel no response'  # Assign event to message i
                            quote.no_me_response()
                            quote_testing_counter['cancel_no_reply'] += 1 # testing counter

                # Execution Report - passive, other fills and reject cases for outbounds
                # These messages refer to passive fills and to outbound messages
                # with missing inbound messages (packet loss)
                # This includes the following Type: 'ME: Partial Fill (P)', 'ME: Full Fill (P)', 'ME: Partial Fill (Other)',
                # 'ME: Full Fill (Other)', 'ME: Cancel Accept', 'ME: Cancel/Replace Accept', 'ME: Order Reject', 'ME: Order Suspend'
                # 'ME: New Order Accept', 'ME: Order Expire'
                # and classifies message i in the following Events:
                # 'Quote passively executed in part', 'Quote passively executed in full', 
                # 'Quote executed in part (other), 'Quote executed in full (other), 'Other ME activity'
                    elif order_msgs.at[i, 'MessageType'] == 'Execution_Report' and order_msgs.at[i, 'Side'] == S:

                        counter += 1  # Increment event counter

                        # Partial passive execution
                        if order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Partial Fill (P)':
                            quote.passive_fill(leaves=msgs.at[i, 'LeavesQty'], st='Executed (1)')
                            msgs.at[i, '%sEvent' % S] = 'Quote passively executed in part' # Assign event to message
                            msgs.at[i, '%sEventNum' % S] = counter  # Populate EventNum
                            msgs.at[i, '%sPriceLvl' % S] = quote.me_prc
                            msgs.at[i, '%sCategorized' % S] = True  # Flag message i as categorized

                        # Full passive execution
                        elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Full Fill (P)':
                            quote.passive_fill(leaves=np.nan, st='Executed (2)')
                            msgs.at[i, '%sEvent' % S] = 'Quote passively executed in full'  # Assign event to message i
                            msgs.at[i, '%sEventNum' % S] = counter  # Populate EventNum
                            msgs.at[i, '%sPriceLvl' % S] = quote.me_prc
                            msgs.at[i, '%sCategorized' % S] = True  # Flag message i as categorized

                        # Other partial execution
                        elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Partial Fill (Other)':
                            quote.update_me(q=msgs.at[i, 'LeavesQty'], st='Executed (1)')
                            msgs.at[i, '%sEvent' % S] = 'Quote executed in part (other)'  # Assign event to message i
                            msgs.at[i, '%sEventNum' % S] = counter  # Populate EventNum
                            msgs.at[i, '%sPriceLvl' % S] = quote.me_prc
                            msgs.at[i, '%sCategorized' % S] = True  # Flag message i as categorized

                        # Other full execution
                        elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Full Fill (Other)':
                            quote.update_me(q=np.nan, st='Executed (2)')
                            msgs.at[i, '%sEvent' % S] = 'Quote executed in full (other)'  # Assign event to message i
                            msgs.at[i, '%sEventNum' % S] = counter  # Populate EventNum
                            msgs.at[i, '%sPriceLvl' % S] = quote.me_prc
                            msgs.at[i, '%sCategorized' % S] = True  # Flag message i as categorized

                        # Other cancel accept (For outbounds whose corresponding inbounds are missing due to packet loss.)
                        elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Cancel Accept':
                            quote.update_me(q=msgs.at[i, 'LeavesQty'], st='Cancelled (4)')
                            msgs.at[i, '%sEvent' % S] = 'Other ME activity'
                            msgs.at[i, '%sEventNum' % S] = counter
                            msgs.at[i, 'Prev%sPriceLvl' % S] = quote.me_prc
                            msgs.at[i, '%sCategorized' % S] = True
                            quote_testing_counter['other_me_activity'] += 1 # testing counter

                        # Other cancel/replace accept (For outbounds whose corresponding inbounds are missing due to packet loss.)
                        elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Cancel/Replace Accept':
                            msgs.at[i, 'Prev%sPriceLvl' % S] = quote.me_prc
                            quote.update_me(st='Amended (5)')
                            msgs.at[i, '%sEvent' % S] = 'Other ME activity'
                            msgs.at[i, '%sEventNum' % S] = counter # Populate EventNum
                            msgs.at[i, '%sCategorized' % S] = True # Flag message j as categorized
                            quote_testing_counter['other_me_activity'] += 1 # testing counter

                        # Other ME activity
                        else:
                            msgs.at[i, '%sEvent' % S] = 'Other ME activity'  # Assign event to message i
                            msgs.at[i, '%sEventNum' % S] = counter  # Populate EventNum
                            msgs.at[i, '%sPriceLvl' % S] = quote.me_prc
                            msgs.at[i, '%sCategorized' % S] = True  # Flag message i as categorized
                            if order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Order Reject':
                                quote.update_me(q=msgs.at[i, 'LeavesQty'], st='Rejected (8)')
                            elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Order Suspend':
                                quote.update_me(q=msgs.at[i, 'LeavesQty'], st='Suspended (9)')
                            elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: Order Expire':
                                quote.update_me(q=msgs.at[i, 'LeavesQty'], st='Expired (6)')
                            elif order_msgs.at[i, 'UnifiedMessageType'] == 'ME: New Order Accept':
                                quote.update_me(q=msgs.at[i, 'LeavesQty'], st='Accepted (0)')
                            quote_testing_counter['other_me_activity'] += 1 # testing counter

                    # Matching Engine Message (other side)
                    elif order_msgs.at[i, 'MessageType'] == 'Execution_Report' and order_msgs.at[i, 'Side'] == opposite(S):
                        msgs.at[i, '%sCategorized' % S] = True  # Flag message i as categorized

                    # This includes the following Type: 
                    # 'ME: Cancel Reject (TLTC)', 'ME: Cancel Reject (Other), 
                    # 'ME: Other Reject',
                    # They might be left uncategorized due to packet loss (missing the inbound).
                    # We classify those messages into the following Events:
                    # 'Other ME activity'
                    elif order_msgs.at[i, 'UnifiedMessageType'] in ('ME: Cancel Reject (TLTC)','ME: Cancel Reject (Other)','ME: Other Reject', 'ME: Other Outbound'):
                        counter += 1  # Increment event counter
                        msgs.at[i, '%sEvent' % S] = 'Other ME activity'  # Assign event to message i
                        msgs.at[i, '%sEventNum' % S] = counter  # Populate EventNum
                        msgs.at[i, '%sCategorized' % S] = True  # Flag message i as categorized
                        
    ### generate some debugging variables in logs and write output to file
    # Event numbers
    # quote events
    bidEvent = msgs[msgs['BidEvent'] != ''].shape[0]
    askEvent = msgs[msgs['AskEvent'] != ''].shape[0]
    total_quote_events = bidEvent + askEvent
    logger.info('Number Quote Events: %s' % str(total_quote_events))

    # order events
    total_order_events = msgs['Event'].value_counts()[1:].sum()
    logger.info('Number Order Events: %s' % str(total_order_events))

    # Log testing counters
    logger.info('Order Counters: ')
    logger.info(order_testing_counter)
    logger.info('Quote Counters: ')
    logger.info(quote_testing_counter)

    ### CONCLUDE ###
    logger.info('Writing to file...')

    # Write to file
    msgs.to_csv(outfile_msgs, compression = 'gzip', index=False)

    # End timer
    timer_end = datetime.datetime.now()

    # Add info to log
    logger.info('Complete.')
    logger.info('Timer End: %s' % str(timer_end))
    logger.info('Time Elapsed: %s' % str(timer_end - timer_st))





