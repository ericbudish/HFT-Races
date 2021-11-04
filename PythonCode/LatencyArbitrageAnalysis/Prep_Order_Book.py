'''
Prep_Order_Book.py

This module reconstructs the order book based on the message data and 
the economic events inside the matching engine.
Please see Section 10.2 of the Code and Data Appendix, as well as the
docstring for prepare_order_book() for implementation details.

Reference:
    Section 10.2 of the Code and Data Appendix.

Input:
    Message data, with inbounds and outbounds grouped into economic events.
    This is the output from Classify_Messages.py.

Output:
    1. Top of Order Book: A dataset with the top-of-book information at the 
        time of each message. The primary data fields include:
        - price at best bid and ask
        - depth at best bid and ask
    2. Depth Info: Dictionary with keys for each side and price that provides 
        the depth at each message where the depth changes for that price and side.
'''
################################################################################################################################

# Import packages
import pandas as pd
import numpy as np
import datetime
import pickle
import os

from .OrderBook.OrderBook import OrderBook
from .utils.Logger import getLogger

def prepare_order_book(runtime, date, sym, args, paths):
    '''
    Function that reads in the message data of the specified sym-date 
    and returns the Top-of-Book and Depth data of that sym-date. 
    
    User note: this is a particularly long function because it goes through all
    the different types of economic events that affect the order book. There are
    more than 30 types of events recognized by this package. Examples include:
        "New order accepted", "New order expired",
        "New order aggressively executed in full",
        "New order passively executed in full", 
        "Order cancel accepted", "Order cancel failed", etc.
    Please refer to Section 10.2 of the Code and Data Appendix for a complete list
    of events. The variety of exchange economic events adds up to the number of 
    lines in this function. We may break this function into several smaller 
    subfunctions in future versions of the code.
    

    Please refer to Section 10.2 of the Code and Data Appendix.

    Params:  
        runtime: str, for log purpose.
        date:    str, symbol-date identifier.
        sym:     str, symbol-date identifier.
        args:    dictionary of arguments, including:
                     - dtypes_msgs: dtype dict for message data with event classification.
                     - price_factor: int. convert price from monetary unit to large integer,
                                          obtained by int(10 ** (max_dec_scale+1)).
                     - ticktable: dataframe. ticksize information.
        paths:   dictionary of file paths, including:
                     - path_temp: path to the temp data file.
                     - path_logs: path to log files.

    Output: 
        Top: A dataframe with top-of-book information at the time of each message. 
            It has the same number of rows as the message dataset.
            Columns of Top :
                'BestBid', 'BestBidQty', 'BestAsk', 'BestAskQty'
                'BestBid_h', 'BestBidQty_h', 'BestAsk_h', 'BestAskQty_h'
                'BestBid_TickSize', 'BestAsk_TickSize', 'Diff_TickSize', 'MidPt_TickSize',
                'Spread', 'Spread_h', 'MidPt', 'MidPt_h'
                'LastValidMidPt', 'LastValidSpread', 
                't_last_chg_BestBid', 't_last_chg_BestAsk', 't_last_chg_MidPt'
                'TradePos', 'EventLastParentMsgID'.            
        Depth:
            Dictionary with keys for each side and price that provides the depth at each message
            where the depth changes for that price and side.
                - E.g., depth_updates['Bid'][25] = [(3143, 40), (3454, 0), (4543, 15)] represents the indices 
                of the messages on which depth changes on the Bid side at price 25 and the total depth of the
                book after the change.  

    Steps: 
        0. Load data
        
        1. Identify Book Updating messages: 
            For Trades, we will remove liquidity as of the earlier of the two
            trade messages. In cases where the aggressive party in the trade posts
            depth to the book, we will add this liquidity when we remove the
            liquidity for the last trade in the event. 
            For non-trades, we will add/remove depth at the outbound message. 
            This section identifies the relevant messages and copies them to the time
            at which they will update the book.

            1.1 Flag the last message of each event. For inbounds with multiple aggressive trades or IOCs that 
                trade and expire we flag the last message. In all other cases, there is only one outbound message.
            1.2 Pair trades.
            1.3 Generate UpdateRelevant fields (copy information on 2nd party of trade to first party's index
                if it has the first flag).
            1.4 Flag as book updating the UpdateRelevant messages if their event changes the book.
                (e.g. the post to book message in a New order accepted event gets flagged as book updating).
        
        2. Update and Correct the Book: Loop over outbound messages, on each iteration of the loop:
            2.1 Add the current best bid/ask to the message in the top dataset and 
                update/cancel the depth for that price and side if the message is book updating.
            2.2 Correct the book on update relevant messages when it is inconsistent with the 
                internal logic of order books or with BBO (a field that flags some messages as 
                joining the best bid/ask).

                Note on Order Book Correction:
                With perfect message data, all market events in the matching engine can be observed
                in the form of combinations of inbounds and outbounds. However, the data may not be 
                perfect in practice. When the data fails to satisfy the following conditions, the 
                order book reconstructed can be off and book correction is necessary:
                    1. In a trade where the aggressive party partially fills and then posts to book, 
                        it is assumed that the non-traded depth is posted to book as of the 1st trade 
                        message observed in the data.
                        (The book could be off for a few messages if it takes time for ME to add the 
                        remaining depth to the book.)
                    2. The sequence of outbound messages is the same as the sequence of events in the 
                        matching engine. That is to say, outbound messages should change the book in 
                        the order they appear in the data.
                        (In reality, messages may overtake each other after leaving the matching engine.)
                    3. The data has all messages. 
                        (This is usually not the case due to packet loss.)
                The order book correction creates a necessary layer of protection against imperfect data.
                Please refer to the code block for implementation details.
        
        3. Clean Data Structure for Final Output: 
            Populate all non-book messages in top of book dataset by forward filling the bbo
            from the most recent book updating message and generate additional stats.
   
    '''
    # Initialize log
    logpath = '%s/%s/' %(paths['path_logs'], 'MessageDataProcessing_'+runtime)
    logfile = 'Step_2_Prep_Order_Book_%s_%s_%s.log' % (runtime, date, sym)
    if not os.path.exists(logpath):
        os.makedirs(logpath)
    logger = getLogger(logpath, logfile, __name__)
    #####################
    ### 0. INITIALIZE ###
    #####################
    logger.info('Processing: %s %s' % (date, sym))

    price_factor = args['price_factor']
    ticktable = args['ticktable']
    dtypes_msgs = args['dtypes_msgs']

    infile_msgs = '%s/ClassifiedMsgData/%s/Classified_Msgs_Data_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym)
    outfile_top = '%s/BBOData/%s/BBO_Data_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym)
    outfile_depth = '%s/DepthData/%s/Depth_%s_%s.pkl' % (paths['path_temp'], date, date, sym)
    
    # Start timer
    timer_st = datetime.datetime.now()

    # Add info to log
    logger.info('Timer Start: %s' % str(timer_st))

    ### LOAD DATA ###
    logger.info('Loading data...')
    
    # Load data
    col_required = [
        'MessageTimestamp', 
        'UniqueOrderID', 'TradeMatchID', 
        'MessageType', 'ExecType', 'UnifiedMessageType', 'TIF', 'QuoteRelated', 'RegularHour',
        'PriceLvl', 'BidPriceLvl', 'AskPriceLvl', 'Side', 'ExecutedPrice', 'PrevPriceLvl', 'PrevBidPriceLvl', 'PrevAskPriceLvl',
        'Event', 'BidEvent', 'AskEvent', 'EventNum', 'OpenAuctionTrade', 'AuctionTrade'
    ]

    msgs = pd.read_csv(infile_msgs, dtype = dtypes_msgs, parse_dates=['MessageTimestamp'])

    missing_cols = set(col_required).difference(set(msgs.columns))
    assert len(missing_cols) == 0, 'Missing Data in Symbol-Date (%s, %s) Raw Message Data: missing fields %s' % (date, sym, missing_cols)

    ########################################
    ## 1. Identify Book Updating Messages ##
    ########################################
    #
    ## Description
    # Determine which messages will update the book. For Trades, we will remove liquidity as of the earlier of the two 
    # trade messages. In cases where the aggressive party in the trade posts depth to the book, we will add 
    # the remaining liquidity when we remove the liquidity in the trade. For non-trades, we will add/remove depth
    # at the outbound message. This section identifies the relevant messages and copies them to the time at
    # which they will update the book:
    #                                             
    #   1. Generate Flag_1: Flag the last message of each order-event pair. For inbounds with multiple aggressive
    #                       trades or IOCs that trade and expire we flag the last message. In all other cases,
    #                       there is only one message
    # 
    #   2. Generate Flag_2 (Trade Pos): Flag the sequence in which the 2 outbound messages for a trade appear
    # 
    #   3. Loop over outbound messages, and add information relevant to updates. If Flag 1 is True:
    #     a. For cases without multiple trades/IOCs that trade and expire Flag 1 is always True
    #     b. When we see a trade's 1st party (TradePos 1), we look for its counter-party in the following messages.
    #        If Flag 1 is True for that message, we copy its information to the first party and fill the _2
    #        fields (e.g. UpdateRelevant2). If TradePos is 2, we skip the message
    #     c. Use event types to flag UpdateRelevant messages as BookUpdEvent1/BookUpdEvent2 or 
    #        BookPrevLvlUpdEvent1/BookPrevLvlUpdEvent2
    # 
    # Please refer to Table 9 and Section 10.2 of the Code and Data Appendix.
    #
    ## Inputs: 
    #         UniqueOrderID: ID assigned to each group of messages referring to the same order 
    #         EventNum / BidEventNum / AskEventNum: number of economic event assigned based on 
    #                                               the combination of matching engine (outbound) 
    #                                               and gateway (inbound) messages
    #         QuoteRelated: Indicator for whether a message is related to a quote
    #         TradeMatchID: An ID that matches the pairs of execution messages in trades
    #         ExecType: for execution reports, describes details of execution    
    #            1. Order_Accepted
    #            2. Order_Cancelled
    #            3. Order_Executed
    #            4. Order_Expired
    #            5. Order_Rejected
    #            6. Order_Replaced
    #            7. Order_Suspended
    #            8. Order_Restated
    #         PriceLvl / BidPriceLvl / AskPriceLvl: bid and ask prices for quotes and prices for orders
    #         PrevPriceLvl / PrevBidPriceLvl / PrevAskPriceLvl: prev state of the order/quote prices
    ## Outputs: 
    #          EventLastMsg: Indicator that the message is the last in an event and potentially update relevant
    #          EventLastMsgType: Notes whether the message is part of a limit order event (limit), 
    #                            bid quote event (bid), or ask quote event (ask)
    #          EventLastParentMsgID: The location (index) of the first message in the event.
    #                                This is labeled in the last message of the event 
    #                                to facilitate finding event information when looping over 
    #                                updating messages later
    #          TradePos: Labels each trade execution message as missing counterparty (0), 
    #                    first message in the trade (1), and second message in the trade (2)
    #          TradeCpMsgID: Labels the counterparty in the trade for each trade message for which 
    #                        we observe the counterparty
    #          UpdateRelevant1, UpdateRelevant1MsgID, 
    #          UpdateRelevant1Type, UpdateRelevant1ParentMsgID, 
    #          UpdateRelevant2, UpdateRelevant2MsgID, 
    #          UpdateRelevant2Type, UpdateRelevant2Type, 
    #          UpdateRelevant2ParentMsgID: These fields are filled for the 1st seen message of a trade and the non-trade update 
    #                                      relevant messages for which only _1 are filled. Trade messages for which we 
    #                                      don't observe the counterparty are treated as non-trade messages
    #
    #          BookUpdEvent1, BookUpdEvent2: These fields determine whether the msg induced a book update. 
    #                                        These messages are those on which we update book
    #          BookPrevLvlUpdEvent1, BookPrevLvlUpdEvent2: These fields determine whether the msg induced a book level cancel.
    #                                                      These messages are those on which we cancel levels in the book
    # 
    # Example: For the first message in a trade UpdateRelevant1 is True if the message is 
    #          the last message of its event and UpdateRelevant2 is True if the counterparty 
    #          message is also the last message of its event. If the first is part of a New order
    #          aggr executed in part and the second is part of an Order passively executed 
    #          in full, then BookUpdEvent1 and BookUpdEvent2 will both be true
    #          
    #          For a New order accept UpdateRelevant1 is True because the ME message is 
    #          the last in the event and UpdateRelevant2 is False as there is no 
    #          second party. BookUpdEvent1 will be true here but not BookUpdEvent2
    # 
    # In a trade with 2 parties there is always a passive fill (which is the last message of its event). 
    # So, all trades (with both parties) have at least one flagged message (either UpdateRelevant1 or 
    # UpdateRelevant2 is populated  as of the first party of the trade)
    # 

    # Initialize flag for book-updating message, associated inbound message
    logger.info('Reconstructing order book...')
    msgs['EventLastMsg'] = False
    msgs['EventLastMsgType'] = None
    msgs['EventLastParentMsgID'] = None
    msgs['TradePos'] = None
    msgs['TradeCpMsgID'] = None

    ## Step 1.1
    # Function to find the last message in each non-quote event. This also 
    # identifies the inbound message that causes that message.
    # This message should be an outbound. 
    # Note that here, Limit refers to non-quotes
    def EventUpdate(event):
        i, j = event.index[0], event.index[-1]
        msgs.at[j, 'EventLastMsg'] = True
        msgs.at[j, 'EventLastMsgType'] = 'Limit'
        msgs.at[j, 'EventLastParentMsgID'] = i

    # Function to flag last message in a quote event on the bid
    # and related information
    def BidEventUpdate(event):
        i, j = event.index[0], event.index[-1]
        msgs.at[j, 'EventLastMsg']= True
        msgs.at[j, 'EventLastMsgType'] = 'Bid'
        msgs.at[j, 'EventLastParentMsgID'] = i
        
    # Function to flag last message in a quote event on the ask 
    # and related information 
    def AskEventUpdate(event):
        i, j = event.index[0], event.index[-1]
        msgs.at[j, 'EventLastMsg']= True
        msgs.at[j, 'EventLastMsgType'] = 'Ask'
        msgs.at[j, 'EventLastParentMsgID'] = i

    ## Step 1.2
    # Function to flag whether a given trade message was the first or second to 
    # be timestamped and to flag the index for the trade counterparty
    def TradeCounterparty(trade):
        if len(trade) == 2:
            i, j = trade.index[0], trade.index[1]
            msgs.at[i, 'TradePos'] = 1.
            msgs.at[j, 'TradePos'] = 2.
            msgs.at[i, 'TradeCpMsgID'] = j
            msgs.at[j, 'TradeCpMsgID'] = i
        elif len(trade) == 1:
            i = trade.index[0]
            msgs.at[i, 'TradePos'] = 0.


    # Find the event last messages using the above functions.
    msgs.loc[~msgs['QuoteRelated']].groupby(['UniqueOrderID', 'EventNum']).apply(EventUpdate)
    msgs.loc[msgs['QuoteRelated']].groupby(['UniqueOrderID', 'BidEventNum']).apply(BidEventUpdate)
    msgs.loc[msgs['QuoteRelated']].groupby(['UniqueOrderID', 'AskEventNum']).apply(AskEventUpdate)
    msgs.groupby(['TradeMatchID']).apply(TradeCounterparty)

    # Initialize the Update Relevant variables 
    msgs['UpdateRelevant1'] = False
    msgs['UpdateRelevant1MsgID'] = None
    msgs['UpdateRelevant1Type'] = None
    msgs['UpdateRelevant1ParentMsgID'] = None
    msgs['UpdateRelevant2'] = False
    msgs['UpdateRelevant2MsgID'] = None
    msgs['UpdateRelevant2Type'] = None
    msgs['UpdateRelevant2ParentMsgID'] = None

    # Initialize the BookUpdEvent message flag
    msgs['BookUpdEvent1'] = False
    msgs['BookUpdEvent2'] = False

    # Initialize the BookPrevLvlUpdEvent message flag
    msgs['BookPrevLvlUpdEvent1'] = False
    msgs['BookPrevLvlUpdEvent2'] = False

    # Initialize testing counters
    book_testing_counter = {}
    book_testing_counter['other_me_no_accept' ] = 0 # new order accept with no inbound
    book_testing_counter['other_me_cr_accept' ] = 0 # cr accept with no inbound
    book_testing_counter['other_me_cancel_accept' ] = 0 # cancel accept with no inbound
    book_testing_counter['other_me_fill' ] = 0 # full/partial fill passive and other
    book_testing_counter['other_me_suspend' ] = 0 # ME order suspend 
    book_testing_counter['other_me_expire' ] = 0 # ME order expired
    book_testing_counter['other_me_restated' ] = 0 # ME order restated 
    book_testing_counter['order_book_crossings'] = 0 # counts of negative spreads
                                                     # [This can be non-zero due to off-hours and auctions]
    ## Step 1.3
    # Loop over all outbound:
    #    a. If the outbound message is a trade execution (ExecType = Order_Executed), then we check whether the message has
    #       a counterparty. If it does and it is an event last message, then we fill the update relevant _1 variables.
    #       If the counterparty message is also an event last message, we fill the _2 variables. We leave as False the variables in 
    #       the counterparty message. If there is no counterparty or the message is non-trade we only fill the _1 variables
    #    b. Then, flag the message as book updating depending on the event of the parent message
    for k in msgs.loc[(msgs['MessageType'] == 'Execution_Report')].index:
        # Implement (a): Assign UpdateRelevant variables
        if msgs.at[k, 'ExecType'] == 'Order_Executed':
            if msgs.at[k, 'TradePos'] == 1.:
                j = int(msgs.at[k, 'TradeCpMsgID'])
                if msgs.at[k, 'EventLastMsg']:
                    msgs.at[k, 'UpdateRelevant1'] = True
                    msgs.at[k, 'UpdateRelevant1MsgID'] = k
                    msgs.at[k, 'UpdateRelevant1Type'] = msgs.at[k, 'EventLastMsgType']
                    msgs.at[k, 'UpdateRelevant1ParentMsgID'] = msgs.at[k, 'EventLastParentMsgID']
                if msgs.at[j, 'EventLastMsg']:
                    msgs.at[k, 'UpdateRelevant2'] = True
                    msgs.at[k, 'UpdateRelevant2MsgID'] = j
                    msgs.at[k, 'UpdateRelevant2Type'] = msgs.at[j, 'EventLastMsgType']
                    msgs.at[k, 'UpdateRelevant2ParentMsgID']= msgs.at[j, 'EventLastParentMsgID']
            elif msgs.at[k, 'TradePos'] == 0.:
                if msgs.at[k, 'EventLastMsg']:
                    msgs.at[k, 'UpdateRelevant1'] = True
                    msgs.at[k, 'UpdateRelevant1MsgID']= k
                    msgs.at[k, 'UpdateRelevant1Type'] = msgs.at[k, 'EventLastMsgType']
                    msgs.at[k, 'UpdateRelevant1ParentMsgID'] = msgs.at[k, 'EventLastParentMsgID']
        elif msgs.at[k, 'EventLastMsg']:
            msgs.at[k, 'UpdateRelevant1'] = True
            msgs.at[k, 'UpdateRelevant1MsgID'] = k
            msgs.at[k, 'UpdateRelevant1Type'] = msgs.at[k, 'EventLastMsgType']
            msgs.at[k, 'UpdateRelevant1ParentMsgID'] = msgs.at[k, 'EventLastParentMsgID']
        
        # Implement (b): Loop over each party in the trade and flagged relevant messages 
        # that could update the order book. Then, fill the BookUpdating/Cancelling 
        # variables depending on the Event of the parent messages
        for n in ['1', '2']:

            if msgs.at[k, 'UpdateRelevant%s' % n]:

                # Get the ID of the book updating message. For the n = 1, j is k.
                # when n = 2, j is the counterparty to k
                j = int(msgs.at[k, 'UpdateRelevant%sMsgID' % n])

                # Look up index of associated inbound messages. This is the parent 
                # of k for n = 1 and the parent of the counterparty
                # of k for n = 2
                i = int(msgs.at[k, 'UpdateRelevant%sParentMsgID' % n])

                # Look up parent message type. Set the valid price
                # indicator, source, unified message type, event and event side.
                # For quotes, use Bid/Ask Event and set the side accordingly.
                # For orders, use Event and set the side to ''.
                # Limit is the category name for non-quotes
                # unified_message_type is the message type for the parent 
                # message of message k, i.e. the first message in the event containing k
                if msgs.at[k, 'UpdateRelevant%sType' % n] == 'Limit':
                    prev_prc_valid = msgs.at[i, 'PrevPriceLvl']>0
                    prc_valid = msgs.at[i, 'PriceLvl'] > 0
                    unified_message_type  = msgs.at[i, 'UnifiedMessageType']
                    event = msgs.at[i, 'Event']
                    event_side = ''
                elif msgs.at[k, 'UpdateRelevant%sType' % n] == 'Bid':
                    prev_prc_valid = msgs.at[i, 'PrevBidPriceLvl'] > 0
                    prc_valid = msgs.at[i, 'BidPriceLvl'] > 0 
                    unified_message_type = msgs.at[i, 'UnifiedMessageType'] 
                    event = msgs.at[i, 'BidEvent']
                    event_side = 'Bid'
                elif msgs.at[k, 'UpdateRelevant%sType' % n] == 'Ask':
                    prev_prc_valid = msgs.at[i, 'PrevAskPriceLvl'] > 0 
                    prc_valid = msgs.at[i, 'AskPriceLvl'] > 0
                    unified_message_type = msgs.at[i, 'UnifiedMessageType'] 
                    event = msgs.at[i, 'AskEvent'] 
                    event_side = 'Ask'
                
                # For each updating case, we set BookUpdEvent1 or 2 to true,
                #   which flags the cases where the book should be updated at this Price-Side.
                # For each cancelling case, we set BookPrevLvlUpdEvent1 or 2 to true,
                #   which flags the cases where the book level should be cancelled at this Price-Side.
                
                # Case 1: New Limit Order
                if unified_message_type == 'Gateway New Order (Limit)' and prc_valid:
                    if event == 'New order accepted':
                        msgs.at[k, 'BookUpdEvent%s' % n] = True
                    elif event == 'New order aggressively executed in part':
                        msgs.at[k, 'BookUpdEvent%s' % n] = True
                
                # Case 2: New Passive-only Limit Order
                elif unified_message_type == 'Gateway New Order (Passive Only)' and prc_valid:
                    if event == 'New order accepted':
                        msgs.at[k, 'BookUpdEvent%s' % n] = True
                        
                # Case 3: New Stop Limit Order
                elif unified_message_type == 'Gateway New Order (Stop Limit)' and prc_valid:
                    if event == 'New order accepted':
                        msgs.at[k, 'BookUpdEvent%s' % n] = True
                    elif event == 'New order aggressively executed in part':
                        msgs.at[k, 'BookUpdEvent%s' % n] = True

                # Case 4: Cancel Request
                elif unified_message_type == 'Gateway Cancel' and prev_prc_valid:
                    if event == 'Cancel request accepted':
                        msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True
                    if event == 'Quote cancel accepted':
                        msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True

                # Case 5: Cancel/Replace Request
                elif unified_message_type == 'Gateway Cancel/Replace' and prc_valid:
                    if event == 'Cancel/replace request accepted':
                        if prev_prc_valid:
                            msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True
                        msgs.at[k, 'BookUpdEvent%s' % n] = True
                    elif event == 'Cancel/replace request aggr executed in part':
                        if prev_prc_valid:
                            msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True
                        msgs.at[k, 'BookUpdEvent%s' % n] = True
                    elif event == 'Cancel/replace request aggr executed in full':
                        if prev_prc_valid:
                            msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True
                    
                # Case 6: Gateway New Quote
                elif unified_message_type == 'Gateway New Quote' and prc_valid:
                    if event == 'New quote accepted':
                        msgs.at[k, 'BookUpdEvent%s' % n] = True
                    elif event == 'New quote updated':
                        if prev_prc_valid:
                            msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True
                        msgs.at[k, 'BookUpdEvent%s' % n] = True
                    elif event == 'New quote aggressively executed in part':
                        if prev_prc_valid:
                            msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True
                        msgs.at[k, 'BookUpdEvent%s' % n] = True
                    elif event == 'New quote aggressively executed in full':
                        if prev_prc_valid:
                            msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True

                # Case 7: Other ME Activity
                # This case catches the passive fill events, other ME activities, 
                # and the packet loss cases. In those cases, the first message of
                # the event is an outbound. 
                elif prc_valid and unified_message_type == 'ME: New Order Accept':
                    book_testing_counter['other_me_no_accept'] += 1 # add counter new order other
                    msgs.at[k, 'BookUpdEvent%s' % n] = True
                elif prc_valid and unified_message_type == 'ME: Cancel/Replace Accept':
                    book_testing_counter['other_me_cr_accept'] += 1 # add counter c/r other
                    if prev_prc_valid:
                        msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True
                    msgs.at[k, 'BookUpdEvent%s' % n] = True
                elif prc_valid and unified_message_type in {'ME: Full Fill (A)', 'ME: Partial Fill (A)', 
                                                            'ME: Full Fill (P)', 'ME: Partial Fill (P)'}:
                    # Note that here we update on aggressive partial fills even though we do not have 
                    # complete information.  It would be wrong to update here if this was an IOC 
                    # that expired or was part of a larger execution in full,
                    # but we would have missed some messages in that case. 
                    # This is consistent with the way that we handle New Orders that fill partially. 
                    # We assume that they post to the book and that we are not missing a message.
                    # This is necessary because the code assumes that new orders do not have a 
                    # post-to-book message when they fill partially. This assumption is consistent 
                    # with the LSE specifications.
                    book_testing_counter['other_me_fill'] += 1 # add counter fill other 
                    msgs.at[k, 'BookUpdEvent%s' % n] = True
                elif prc_valid and unified_message_type in {'ME: Order Suspend'}:
                    book_testing_counter['other_me_suspend'] += 1 # add counter suspend other
                    msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True
                elif prev_prc_valid and unified_message_type in {'ME: Cancel Accept'}:
                    book_testing_counter['other_me_cancel_accept'] += 1 # add counter cancel accept other
                    msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True
                elif prc_valid and unified_message_type in {'ME: Order Expire'}:
                    book_testing_counter['other_me_expire'] += 1 # add counter expire other
                    msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True
                elif unified_message_type in {'ME: Order Restated'}: 
                    book_testing_counter['other_me_restated'] += 1
                    msgs.at[k, 'BookPrevLvlUpdEvent%s' % n] =  True
                    pass
    ######################################
    ## 2. Update and Correct Order Book ##
    ######################################
    #
    ## Description
    # 
    #     0. First, we define the order book data structures. Then,
    # For each outbound message:
    #     1. Update current or previous book level and depth structures given
    #        BookUpdEvent and BookPrevLvlUpdEvent fields. This is done
    #        by maintaining the current state of the book in the Book and
    #        OrderBookLvl objects (in .OrderBook.OrderBook)
    #     2. If message is a trade and its counterparty is book updating, 
    #        then update using the counterparty
    #     3. Given core order book logic correct any book locks and crosses after update relevant messages
    #           (e.g. if a New order accepted to book crosses the spread then remove the levels it crosses)
    #     4. Given BBO field correct any inconsistencies for order accepts
    #           (e.g. if a New order should join the best bid remove any better levels)
    # 
    # Inputs: 
    #       - Inputs for Identify Book Updating Messages section
    #       - Outputs from Identify Book Updating Messages section
    #
    # Outputs:
    #         1. top: A top of book dataframe with the following fields for each message: 
    #                - BestBid/BestAsk: price at best bid and ask
    #                - BestBidQty/BestAskQty: depth at best bid and ask
    #                - Flags for testing and audits
    # 
    #         2. depth_updates: Dictionary with keys for each side and price that provides the depth at each message
    #                        where the depth changes for that price and side
    #                - e.g. depth_updates['Bid'][25] = [(3143, 40), (3454, 0), (4543, 15)] represents the indices 
    #                       of the messages on which depth changes on the Bid side at price 25 and the total depth of the
    #                       book after the change
    # 

    ## Step 2.1
    # Initialize output data structures and Create classes for storing order book information
    # See OrderBook.OrderBook for details.
    # 
    # The class object OrderBook represents the state of the order book at a given outbound message. 
    # We also use it to fill the top dataframe with the top of book information that is one of the outputs 
    # of this script
    # 
    # The OrderBookLvl class is initialized within OrderBook to store the information to update the order
    # book level and the depth data structure for a given price/side

    # top is a dataframe with the top of book (BBO) information 
    # Prices are initialized as infinity and updated later.
    # We do this to separate empty order book due to no updating messages from
    # empty order book due to zero or one-sided market.
    top = pd.DataFrame({'BestBid': np.inf, 'BestBidQty': np.inf,
                        'BestAsk': np.inf, 'BestAskQty': np.inf,
                        'BestBid_h': np.inf, 'BestBidQty_h': np.inf,
                        'BestAsk_h': np.inf, 'BestAskQty_h': np.inf,
                        'Corrections_OrderAccept': 0,'Corrections_Trade': 0,  
                        'Corrections_notA': 0, 
                        'Corrections_OrderAccept_h': 0,'Corrections_Trade_h': 0,  
                        'Corrections_notA_h': 0, 
                        'DepthKilled': 0, 'DepthKilled_h': 0}, index=msgs.index)
    # Initialize dictionaries for records of depth updates
    depth_updates = {}

    # Initialize order book data structure
    Book = OrderBook(msgs, top, depth_updates)

    # Step 2.2, 2.3
    # Populate the order book data structures
    # 
    # Description
    #     Loop over all outbound messages. Within that loop:
    #         1. Check if UpdateRelevant1 is True.
    #         2. If UpdateRelevant1 is True, then get the index of the message and 
    #            the corresponding parent message. From the parent message, 
    #            get the event information (e.g. New Order Accept), the unified message type for the parent, 
    #            and the price levels. Then update/cancel the order book  and order book level using the classes described above. 
    #         3. Do the order book correction at the time of the open auction.
    #         4. Check if UpdateRelevant2 is True (counterparty to trade is Book Updating) 
    #         5. If UpdateRelevant2 is True then repeat step 2 with the information for Message 2 
    #
    # Simple Trade Example - 1st party of the trade has event 'New Order aggressively executed in full', with outbound 'ME: Full Fill (A)'
    #                        2nd party of the trade has event 'New Order passively executed in part', with outbound 'ME: Partial Fill (P)'
    # 
    #     Action: User sent Gateway New Order (MessageType==New_Order) with a bid price and quantity and the ME matched it to a posted order on
    #             the ask side. The passive order is not fully filled by the inbound
    # 
    #     Summary: We loop over exec reports until we see a UpdateRelevant1 = True related to an ME: Full Fill (A). 
    #              Since the order is a full fill, it does not post to book and so we do not update the bid side (BookUpdEvent1 = False).
    #              We update the depth at that price on the ask side where the passive partial fill is book updating. 
    #              Then, we remove any depth at asks strictly below the traded price
    #     Steps:
    #         1. Loop over exec msgs until you get to the 'ME: Full Fill (A)' with UpdateRelevant = True
    #         2. Parent event does not update the book (because it is aggressive and executed in full and BookUpdEvent1= False)
    #         3. Check the counterparty Event's BookUpdEvent2
    #         4. Since BookUpdatingEvent2 is true, update the depth at the ask price to the remaining quantity after the trade
    #         5. Remove any asks that are strictly lower than the executed price and any bids that are weakly higher than the executed price
    #         6. Since the order did not join the book the BBO check does not kick in
    
    # Get the last outbound of open auction trades
    open_auction_detected = sum(msgs['OpenAuctionTrade']) > 0
    open_auction_last_trade_idx = msgs[msgs['OpenAuctionTrade']].index[-1] if open_auction_detected else np.nan
    # Get the index of the first message with the same UniqueOrderID for all msgs
    # so that we know whether a message belongs to a GFA order
    msgs['idx'] = msgs.index
    OrderParentMsgIDs = msgs.groupby(['UniqueOrderID'], as_index=False)['idx'].first().reset_index()
    OrderParentMsgIDs = OrderParentMsgIDs.rename(index=str, columns = {'idx':'idx_inb'})
    msgs = msgs.merge(OrderParentMsgIDs, on=['UniqueOrderID'], how='left')
    ## Loop through all outbound execution reports and perform book updates and correction
    for k in msgs.loc[(msgs['MessageType'] == 'Execution_Report')].index:
        GoodforAuction = msgs.at[msgs.at[k, 'idx_inb'], 'TIF'] == 'GFA'
        if not GoodforAuction: 
            ## Step 2.2
            # Book-Updating Messages
            # Loop over each party in the trade and update the order book according BookUpdEvent1 and 2
            # and BookPrevLvlUpdEvent1 and 2. This is done by calling the functions for the instances of the
            # classes described above
            for n in ['1', '2']:
        
                if msgs.at[k, 'UpdateRelevant%s' % n]:
        
                    # Get the ID of the book updating message. For the n = 1, j is k.
                    # when n = 2, j is the counterparty to k
                    j = int(msgs.at[k, 'UpdateRelevant%sMsgID' % n])
        
                    # Look up index of associated inbound messages
                    # This is the parent of k for n = 1 and the parent of the counterparty
                    # of k for n = 2
                    i = int(msgs.at[k, 'UpdateRelevant%sParentMsgID' % n])
        
                    # Look up parent message type. Set the valid price.
                    # Limit is the category for non-quotes
                    if msgs.at[k, 'UpdateRelevant%sType' % n] == 'Limit':
                        event_side = ''
                    elif msgs.at[k, 'UpdateRelevant%sType' % n] == 'Bid':
                        event_side = 'Bid'
                    elif msgs.at[k, 'UpdateRelevant%sType' % n] == 'Ask':
                        event_side = 'Ask'

                    # We use j for side because, in quotes, j is not populated for the parent message (inbound)
                    if msgs.at[k, 'BookPrevLvlUpdEvent%s' %n]:
                        # for Order Expires we cancel the current price lvl
                        if msgs.at[j, 'UnifiedMessageType'] == 'ME: Order Expire':
                            Book.UpdatePrevLvl(msgs.at[j, 'Side'], msgs.at[i, '%sPriceLvl' % event_side], k, j)
                        
                        # for all other cases we cancel the prev price lvl
                        else:
                            Book.UpdatePrevLvl(msgs.at[j, 'Side'], msgs.at[i, 'Prev%sPriceLvl' % event_side], k, j)
                
                    if msgs.at[k, 'BookUpdEvent%s' %n]:
                        Book.UpdateLvl(msgs.at[j, 'Side'], msgs.at[i, '%sPriceLvl' % event_side], k, j)
        ## Step 2.3 Book Correction
        # Step 2.3.1 Book Correction at the open auction
        # Order book logic correction at the time of the last open auction trade msg
        # At the end of the open auction, the order book should be fully uncrossed. However, it
        # might remain crossed due to packet loss. The purpose of this block is to make sure 
        # the book is uncrossed at the end of the open auction by book correction when packet 
        # loss or other imperfection of data comes about.
        # At the last outbound of the open auction trades, we remove all orders to buy at prices
        # above the auction price and all orders to sell at below auction prices. They are not
        # fully removed in book updating process due to packet loss. For orders at the auction 
        # price, we sum up the quantity on both sides and remove the side with a smaller quantity.
        # This is under the assumption that packet loss should be rare and as a result the side
        # with smaller quantity are more likely to be the product of packet losses.
        #
        # Example:
        # The auction price is 10. At the last outbound of the open auction trades, 
        # the book is still crossed: 
        # Bid  Price | Qty    Ask  Price | Qty
        #          9 |  50            11 |  50
        #         10 | 100            10 |  10
        #         11 |  10             9 |  10
        # After this step, the bid at 11 and ask at 9 (auction price is 10) are removed.
        # For bid and ask at 10, the ask side is removed and the bid side remains unchanged
        # because ask at 10 only has qty 10 while the bid side has qty 100.
        if open_auction_detected and k == open_auction_last_trade_idx:
            open_auction_price = msgs.at[open_auction_last_trade_idx, 'ExecutedPrice']
            if open_auction_price > 0:
                Book.Correctlvl(open_auction_price, 'Bid', 'Trade', True, k)
                Book.Correctlvl(open_auction_price, 'Ask', 'Trade', True, k)
                remaining_bid_qty = Book.lvls[('Bid', open_auction_price)].curr_depth_h if ('Bid', open_auction_price) in Book.lvls.keys() else 0
                remaining_ask_qty = Book.lvls[('Ask', open_auction_price)].curr_depth_h if ('Ask', open_auction_price) in Book.lvls.keys() else 0
                if remaining_bid_qty < remaining_ask_qty:
                    Book.Correctlvl(open_auction_price, 'Bid', 'Trade', False, k)
                elif remaining_bid_qty > remaining_ask_qty:
                    Book.Correctlvl(open_auction_price, 'Ask', 'Trade', False, k)
                else:
                    Book.Correctlvl(open_auction_price, 'Bid', 'Trade', False, k)
                    Book.Correctlvl(open_auction_price, 'Ask', 'Trade', False, k)
        ## Step 2.3.2 book correction during continuous trading
        # 
        # Order book logic correction, only start correction during the regular trading hours
        # 1. For new order accepts on the bid (ask): Remove all price levels on the ask (bid) price 
        #                                            that are weakly greater (less) than the price level 
        #                                            of the accepted order.
        # 2. For full fills: Remove all price levels on the bid that are strictly greater than the 
        #                    executed price and all price levels on the ask that are strictly less than 
        #                    the executed price.
        # 3. For partial fills: Remove the same price levels as for full fills and also the price on the
        #                       opposite side of the book at which the trade executed.
        if (msgs.at[k, 'RegularHour'] and not GoodforAuction):
            for n in ['1', '2']:
                if msgs.at[k, 'UpdateRelevant%s' % n]:
                    j = int(msgs.at[k, 'UpdateRelevant%sMsgID' % n])
                    
                    # Get the event for message k and its counterparty.
                    event  = msgs.at[int(msgs.at[k, 'UpdateRelevant%sParentMsgID' % n]), 'Event']
                    unified_message_type = msgs.at[int(msgs.at[k, 'UpdateRelevant%sParentMsgID' % n]), 'UnifiedMessageType']
                    
                    if msgs.at[k, 'UpdateRelevant%sType' % n] == 'Limit':
                        event_side = ''
                    elif msgs.at[k, 'UpdateRelevant%sType' % n] == 'Bid':
                        event_side = 'Bid'
                    elif msgs.at[k, 'UpdateRelevant%sType' % n] == 'Ask':
                        event_side = 'Ask'
                    
                    # Check orders accepted to the book without trading
                    # Check IOCs that expire without trading (this means no orders can trade against the IOCs,
                    # that order would have been accepted to the book if it were not an IOC but a regular limit order)
                    # True and False in the Book. Correctlvl method means to clear the levels strictly or weakly less
                    # than the price.
                    # Note that 'Gateway New Order (IOC)' includes both IOC and FOK. We want only IOC orders to correct
                    # the order book here. We filter out FOKs by the TIF field.
                    if (msgs.at[j, 'UnifiedMessageType'] in {'ME: New Order Accept', 'ME: Cancel/Replace Accept'}) or\
                       (msgs.at[j, 'UnifiedMessageType'] == 'ME: Order Expire' and event == 'New order expired' and\
                        unified_message_type == 'Gateway New Order (IOC)' and msgs.at[j, 'TIF'] == 'IOC'):
                        if msgs.at[j, 'Side'] == 'Ask':
                            Book.Correctlvl(msgs.at[j, '%sPriceLvl' %event_side], 'Bid', 'OrderAccept', False, k)
                        elif msgs.at[j, 'Side'] == 'Bid':
                            Book.Correctlvl(msgs.at[j, '%sPriceLvl' %event_side], 'Ask', 'OrderAccept', False, k)
                    
                    # Check all fill messages and kill any bids strictly greater than the executed price 
                    # and asks strictly less than the executed price
                    elif msgs.at[j, 'UnifiedMessageType'] in {'ME: Full Fill (A)', 'ME: Full Fill (P)', 
                                                              'ME: Partial Fill (A)','ME: Partial Fill (P)'}:
                        Book.Correctlvl(msgs.at[j, 'ExecutedPrice'], 'Bid', 'Trade', True, k)
                        Book.Correctlvl(msgs.at[j, 'ExecutedPrice'], 'Ask', 'Trade', True, k)
                        
                    # If the messages fully fills on the aggressive side, also kill the executed price on the side
                    # of the aggressive execution. Since this message traded, anything at that executed price
                    # on this level should have traded
                    if msgs.at[j, 'UnifiedMessageType'] == 'ME: Full Fill (A)':
                        Book.Correctlvl(msgs.at[j, 'ExecutedPrice'], msgs.at[j, 'Side'], 'Trade', False, k)
        
                    # For partial fills, remove quantity from executed price on opposite side of the book.
                    # This assumes that when we see a partial fill (A) message that we cannot connect to an inbound 
                    # (or a partial fill other), we post the remainder to the book. This is consistent with how we
                    # treat New Orders that aggressively execute in part.
                    if msgs.at[j, 'UnifiedMessageType'] in {'ME: Partial Fill (P)', 'ME: Partial Fill (A)'}:
                        if msgs.at[j, 'Side'] == 'Ask':
                            Book.Correctlvl(msgs.at[j, 'ExecutedPrice'], 'Bid', 'Trade', False, k)
                        elif msgs.at[j, 'Side'] == 'Bid':
                            Book.Correctlvl(msgs.at[j, 'ExecutedPrice'], 'Ask', 'Trade', False, k)
                            
        
    ################################################
    ## 3.0 CLEAN DATA STRUCTURES FOR FINAL OUTPUT ##
    ################################################
    #
    ## Description
    # Reshapes top and depth data structures adding additional fields like midpoint prices. 
    # We also fill forward top of book info for all messages given book updating messages
    #
    # Inputs: top and depth objects from previous section
    #
    # Outputs: cleaned top and depth objects
    #
    ## Clean top of book data

    logger.info('Creating top-of-book dataset...')

    top = Book.get_top()
    depth_updates = Book.get_depth()
    
    # Regular hours
    reg_hours = msgs['RegularHour']
    
    ## Forward fill
    # Fill values for cases in which the price for the best bid/ask was never
    # filled in by the book update message loop (inbound messages) 
    ffill_cols = ['BestBid', 'BestBidQty', 'BestBid_h', 'BestBidQty_h',
                  'BestAsk', 'BestAskQty', 'BestAsk_h', 'BestAskQty_h']
    for col in ffill_cols:
        ### Notes:
        # Top dataframe is initialized as infinity. 
        # For non-book updating msgs, the value for the 8 variables are still np.inf.
        # For book updating msgs, if they don't have best bid or ask (e.g. one-sided market),
        # the values for the 8 variables are set to np.nan in OrderBook.py 
        # during book updating and correction.
        # We distinguish between 
        # np.nan (truly missing best bid/ask due to one-sided market), and
        # np.inf (missing the values because it is not a book updating message).
        # We need to forward fill all np.inf values but keep the np.nan value as missing.
        # So we first set np.nan to -np.inf and np.inf to np.nan, then forward fill np.nan,
        # then set the -np.inf to np.nan. 
        # At the end, we set the value to np.nan for all non-regular hour messages.
        top.loc[np.isnan(top[col]), col] = -np.inf
        top.loc[top[col] == np.inf, col] = np.nan
        top[col] = top[col].ffill()
        top.loc[top[col] == -np.inf, col] = np.nan
        top.loc[~reg_hours, col] = np.nan

    # Calculate spread and midpoint during regular hours when the market is two sided
    # Note that all prices are multiples of 10 (by price_factor). We use integer division
    # because it preserves the int type and is equivalent in this case to float division
    # Note that if the market is zero or one-sided, MidPt and Spread will be np.nan.
    two_sided = (top['BestBid'].notnull()) & (top['BestAsk'].notnull())
    top.loc[reg_hours & two_sided, 'Spread'] = top['BestAsk'] - top['BestBid']
    top.loc[reg_hours & two_sided, 'MidPt'] = (top['BestBid'] + top['BestAsk']) // 2
    # For MidPt and Spread counting hidden qty
    two_sided_h = (top['BestBid_h'].notnull()) & (top['BestAsk_h'].notnull())
    top.loc[reg_hours & two_sided_h, 'Spread_h'] = top['BestAsk_h'] - top['BestBid_h']
    top.loc[reg_hours & two_sided_h, 'MidPt_h'] = (top['BestBid_h'] + top['BestAsk_h']) // 2

    # Append useful columns from the message data (to help with auditing/debugging)
    cols = ['MessageTimestamp', 'Side', 'UnifiedMessageType', 
            'RegularHour','OpenAuctionTrade','AuctionTrade']
    top = pd.concat([msgs[cols], top], axis = 1)
        
    ### SUPPLEMENTAL CALCULATIONS ###
    logger.info('Supplemental calculations...')
    
    ## Flag bids and asks with different tick sizes.
    # Convert ticktable prices to price factor format.
    ticktable['p_int64'] = (ticktable['p'] * price_factor).astype('Int64') 
    ticktable['tick_int64'] = (ticktable['tick'] * price_factor).astype('Int64')
    all_prices = set(top['BestBid'].dropna().values).union(set(top['BestAsk'].dropna().values), set(top['MidPt'].dropna().values))
    prices_tx = {}
    for p in all_prices:
        prices_tx[p] = ticktable.loc[ticktable['p_int64'] <= p, 'tick_int64'].iloc[-1]
    prices_tx[np.nan] = 0
    top['BestBid_TickSize'] = top['BestBid'].map(prices_tx)
    top['BestAsk_TickSize'] = top['BestAsk'].map(prices_tx)
    top['MidPt_TickSize'] = top['MidPt'].map(prices_tx)
    
    ## Calculate change in midpt. 
    # We ignores all invalid midpoints (<= 0). 
    # Invalid midpoints can happen if the bid or ask is missing
    # We also set the midpoint change in the first message of a session to be NA
    top['Chg_MidPt'] = top['MidPt'].diff()
    top['Prev_MidPt_TickSize'] = top['MidPt_TickSize'].shift(1)
    top['MidPt_TickChange'] = top['MidPt_TickSize'] != top['Prev_MidPt_TickSize']
    top['Chg_MidPt_Tx'] = top['Chg_MidPt'] // top['Prev_MidPt_TickSize']
    invalid_or_new_session = (top['MidPt'].shift(1).isna()) & (top['MidPt'].isna()) | (msgs['SessionID'] != msgs['SessionID'].shift(1))
    top.loc[invalid_or_new_session ,'Chg_MidPt'] = np.nan
    top.loc[invalid_or_new_session ,'Prev_MidPt_TickSize'] = np.nan
    top.loc[invalid_or_new_session ,'Chg_MidPt_Tx'] = np.nan     
    top.loc[invalid_or_new_session ,'MidPt_TickChange'] = False
    
    ## TIME SINCE ORDER BOOK UPDATES
    # Calculate last valid midpoint. 
    # If non-valid midpoint/spread, assign most recent valid midpoint/spread. 
    # If no previous valid midpoint/spread, assign missing
    valid = reg_hours & two_sided & (top['Spread'] > 0)
    top['LastValidMidPt'] = top.loc[valid, 'MidPt']
    top['LastValidSpread'] = top.loc[valid, 'Spread']
    top['LastValidMidPt'] = top['LastValidMidPt'].ffill()
    top['LastValidSpread'] = top['LastValidSpread'].ffill()
    book_testing_counter['order_book_crossings'] = ((top['Spread'] <= 0) & two_sided).sum()

    # Flag any price changes 
    last_chg_BestBid = (top['BestBid'].notnull()) & (top['BestBid'].shift(1) != top['BestBid'])
    last_chg_BestAsk = (top['BestAsk'].notnull()) & (top['BestAsk'].shift(1) != top['BestAsk'])
    last_chg_MidPt = (top['MidPt'].notnull()) & (top['MidPt'].shift(1) != top['MidPt'])

    # For any price change, retrieve time of that change and leave all intermediate
    # t_last_chg as missing
    top.loc[last_chg_BestBid, 't_last_chg_BestBid'] = top.loc[last_chg_BestBid, 'MessageTimestamp']
    top.loc[last_chg_BestAsk, 't_last_chg_BestAsk'] = top.loc[last_chg_BestAsk, 'MessageTimestamp']
    top.loc[last_chg_MidPt, 't_last_chg_MidPt'] = top.loc[last_chg_MidPt, 'MessageTimestamp']

    # Fill down time of price change. For price change messages, the time of the last change
    # is the current time. For all other messages it is the time of the most recent change
    # check that ffill fills from  previous non-na (not most recent)
    top['t_last_chg_BestBid'] = top['t_last_chg_BestBid'].fillna(method='ffill')
    top['t_last_chg_BestAsk'] = top['t_last_chg_BestAsk'].fillna(method='ffill')
    top['t_last_chg_MidPt'] = top['t_last_chg_MidPt'].fillna(method='ffill')

    # Add trade position and parent of book updating message to top dataframe
    top['TradePos'] = msgs['TradePos']
    top['EventLastParentMsgID'] = msgs['EventLastParentMsgID']

    ## Clean Depth data structure
    logger.info('Creating depth info data structure...')

    # Convert from Dictionary to DataFrame (Book is the deepest level with the depth info)
    depth_updates = pd.DataFrame.from_dict(depth_updates, orient = 'index').reset_index()
    depth_updates[['ix', 'S', 'P', 'Book']] = pd.DataFrame(depth_updates['index'].to_list(), index = depth_updates.index)
    depth_updates = depth_updates[['ix', 'S', 'P', 'Book', 0]] # re-order the columns
    depth_updates.columns = ['ix', 'S', 'P', 'Book', 'Value']

    # Initialize tree structure for final depth output. This will convert the original depth dictionary
    # for each side, price, and update message to a dictionary of depths and updating message indices
    # for side, price and depth type (displayed, hidden)
    depth = {'bid' : {}, 'ask': {}, 'bid_h': {}, 'ask_h': {}}

    # Flag messages on the bid, on the ask and with displayed depth
    is_disp = depth_updates['Book'] == 'Disp' 
    is_bid = depth_updates['S'] == 'Bid'
    is_ask =  depth_updates['S'] == 'Ask'

    # Get dataframe of updates to displayed depth on bid and displayed depth on ask
    bids = depth_updates.loc[is_disp & is_bid]
    asks = depth_updates.loc[is_disp & is_ask]
    # For each unique price for displayed depth on the bid (ask), get the index for all updates at that price
    # and the displayed depth at that index and add it to the dictionary with the side and price as keys
    for p in bids['P'].unique():
        bids_p = bids.loc[bids['P'] == p]
        depth['bid'][p] = pd.Series(bids_p['Value'].values, index = bids_p['ix'].values).sort_index()
    for p in asks['P'].unique():
        asks_p = asks.loc[asks['P'] == p]
        depth['ask'][p] = pd.Series(asks_p['Value'].values, index = asks_p['ix'].values).sort_index()

    # Repeat for Total depth
    is_total = depth_updates['Book'] == 'Total'
    is_bid = depth_updates['S'] == 'Bid'
    is_ask = depth_updates['S'] == 'Ask'

    bids = depth_updates.loc[is_total & is_bid] 
    asks = depth_updates.loc[is_total & is_ask]

    for p in bids['P'].unique():
        bids_p = bids.loc[bids['P'] == p]
        depth['bid_h'][p] = pd.Series(bids_p['Value'].values, index = bids_p['ix'].values).sort_index()
    for p in asks['P'].unique():
        asks_p = asks.loc[asks['P'] == p]
        depth['ask_h'][p] = pd.Series(asks_p['Value'].values, index = asks_p['ix'].values).sort_index()

    ### OUTPUT ###

    # Log testing counters
    logger.info('Number of messages: %s' % str(msgs.shape[0]))
    logger.info('Book Counters: ')
    logger.info(book_testing_counter)

    logger.info('Regular Hour Crossing: %s' % str(((top['Spread'] <= 0) & two_sided & reg_hours).sum()))

    # Number of ticksize differences
    logger.info('Number of ticksize differences: %s' % str(((top['BestBid_TickSize'] != top['BestAsk_TickSize']) & two_sided).sum()))

    logger.info('Writing to file...')

    # Save top of book data
    top.to_csv(outfile_top, compression = 'gzip')

    # Save depth of book data
    pickle.dump(depth, open(outfile_depth, 'wb'), protocol=pickle.HIGHEST_PROTOCOL)

    # End timer
    timer_end = datetime.datetime.now()

    # Add info to log
    logger.info('Complete.')
    logger.info('Timer End: %s' % str(timer_end))
    logger.info('Time Elapsed: %s' % str(timer_end - timer_st))


