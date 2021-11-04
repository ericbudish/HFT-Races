'''
dtypes.py

This file stores the Pandas data types for each data field.
Users should refer to Section 3 of the Code and Data Appendix for 
detailed instructions on how to pre-process the exchange message 
data. 
'''

## dtypes for exchange message data after pre-processing
dtypes_raw_msgs = {
      'ClientOrderID':'O', 'UniqueOrderID':'O', 'TradeMatchID': 'O', 
      'UserID':'O', 'FirmID':'O', 'SessionID':'float64',
      'MessageTimestamp':'O', 'MessageType':'O', 'OrderType':'O',
      'ExecType':'O', 'OrderStatus':'O', 'TradeInitiator':'O', 
      'TIF':'O', 'CancelRejectReason': 'O',
      'Side':'O', 'OrderQty':'float64', 'DisplayQty':'float64', 
      'LimitPrice':'float64', 'StopPrice':'float64',
      'ExecutedPrice': 'float64',  'ExecutedQty': 'float64', 'LeavesQty': 'float64',
      'QuoteRelated':'bool',
      'BidPrice':'float64', 'BidSize':'float64', 
      'AskPrice':'float64', 'AskSize':'float64',
      'RegularHour':'bool'} 
      # OpenAuctionTrade and AuctionTrade contain NAs
      # because they are only populated in trade confirmation messages.
      # So we do not specify the dtype when reading them in.
      # We will replace the NAs with False in the program (Classify_Messages.py).

## dtypes for message data after Classify_Messages.py
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
      
## dtypes for top-of-book data constructed in Prep_Order_Book.py
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
