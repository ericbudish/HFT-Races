'''
Prep_Race_Data.py

This module has the function prepare_data().
It is called in Race_Detection_and_Statistics.py to prepare the message
data and the top-of-book data for race detection. The prepare_data() function generates
additional fields in the message data and top-of-book data. These fields 
will be used for race detection.
'''
import numpy as np

############################### 
## Main Functions ##
###############################


def prepare_data(msgs, top):
    '''
    This function prepares the msgs dataset for the race detection loop. 
    In particular, the function identifies the messages that could 
    potentially belong to a race (race relevant) and gathers
    relevant price, quantity and race outcome group information

    Params: 
        msgs: the message dataframe with event classification
        top:  top-of-book dataframe generated in the Order Book code

    Output: msgs with added relevant fields for race detection. 
            In particular:
            - Ask/BidRaceRlvt: whether a message could belong to a race or not
            - Ask/BidRaceRlvtType: whether a message is an attempt to Take or Cancel if it is in a race
            - Ask/BidRaceRlvtPriceLvl: the pricelvl the message is trying to Take or Cancel if it is in a race
            - Ask/BidRaceRlvtPriceLvlSigned: price field with bid race prices*-1. Signed price fields are used to compare
                                            ask and bid race prices with the same logic operators.
                                            e.g. by having negative bids the highest bid is now the smallest signed price.
            - Ask/BidRaceRlvtBestExecPriceLvl: best execution price (for taker) of an attempt to take that trades. This is 
                                            the max execution price for ask orders and the min execution price for bid orders
            - Ask/BidRaceRlvtBestExecPriceLvlSigned: best execution price field with bid race prices*-1. Signed price fields  
                                                    are used to compare ask and bid prices with the same logic operators.
            - Ask/BidRaceRlvtQty: the depth at the pricelvl the message is attempting to take or the depth that it successfully 
                                cancels if it is in a race
            - Ask/BidRaceRlvtOutcomeGroup: describes the category of outcomes in which a possible race message falls 
                                        (fail, sucessess, race price dependent, or unknown) if it appears in a race. 
                                        For example, a new order that sucessfully takes depth  in a race
                                        will have an outcome group of race price dependent. We will later use this 
                                        outcome group to determine the actual outcome of the message when it appears in 
                                        the race
            - Ask/BidRaceRlvtNoResponse: whether the relevant message was an inbound with no response

    This function adds new fields to the msgs dataframe and calls the following helper functions:
        gen_ask_races_fields
        gen_bid_races_fields
        get_processing_time

    Steps:
        1. Identify messages that could potentially be included in a race and flag them as race 
            relevant and as cancel or take attempts. 
            New orders and price-improving cancel-and-replace (c/r) (higher bids, lower asks) are attempts to take.
            Cancels and price-worsening c/r (lower bids, higher asks) are attempts to cancel.
            This is done in gen_ask_races_fields() and gen_bid_races_fields().
        2. Unify price and quantity info for race relevant messages. Generate a single price field
            with race relevant prices for flagged messages (prev prices for cancel attempts and
            current prices for take attempts).
            This is done by in gen_ask_races_fields() and gen_bid_races_fields().
        3. Add fields with information on processing time (time from inbound to 1st outbound).
            Processing time is computed by calling get_processing_time().
        4. Add fields with signed ask and bid prices so that they can be compared with the 
            same logic operators.
            

    '''
    ## Step 1 and 2. Create general fields to flag messages relevant to races on the bid/ask
    # and unify price and quantity info for race relevant messages.
    # Note: Stop, Stop Limit, and Pegged orders are ignored because they are unlikely to appear in races.
    
    # Generate variables for slicing the msg data 
    # Flag quote-related messages
    is_qr = msgs['QuoteRelated'] == True

    # Message Side
    side_ask = msgs['Side'] == 'Ask'
    side_bid = msgs['Side'] == 'Bid'

    # Gateway Cancel (non-Quote Related)
    # Note that quote related cancels are considered independently in the function
    canc = (msgs['UnifiedMessageType'] == 'Gateway Cancel') & ~is_qr

    # Gateway New Order (Limit)
    new_limit = msgs['UnifiedMessageType'] == 'Gateway New Order (Limit)'
   
    # Gateway New Order (IOC/FOK)
    # Note that UnifiedMessageType == 'Gateway New Order (IOC)' includes both IOCs and FOKs.
    # They are treated the same way for race detection and statistics.
    new_ioc = msgs['UnifiedMessageType'] == 'Gateway New Order (IOC)'
   
    # Gateway New Order (Market)
    new_mkt = msgs['UnifiedMessageType'] == 'Gateway New Order (Market)'
   
    # Gateway Cancel/Replace messages
    crep = msgs['UnifiedMessageType'] == 'Gateway Cancel/Replace'
   
    # Gateway New Quote messages
    new_quote = msgs['UnifiedMessageType'] == 'Gateway New Quote'
    
    # Valid price slicers
    # Prices (plural) refers to both prev and current price lvls.
    # Price (singular) only refers to the current price lvl 
    # (or prev if specified) but not both
    valid_prices = (msgs['PrevPriceLvl'].notnull()) & (msgs['PriceLvl'].notnull())
    valid_prev_price = msgs['PrevPriceLvl'].notnull()
    
    valid_ask_price = msgs['AskPriceLvl'].notnull()
    valid_bid_price = msgs['BidPriceLvl'].notnull()
    valid_prev_ask_price = msgs['PrevAskPriceLvl'].notnull()
    valid_prev_bid_price = msgs['PrevBidPriceLvl'].notnull()

    valid_ask_prices = (msgs['PrevAskPriceLvl'].notnull()) & (msgs['AskPriceLvl'].notnull())
    valid_bid_prices = (msgs['PrevBidPriceLvl'].notnull()) & (msgs['BidPriceLvl'].notnull())
                                                                                                       
    ## Generate Ask and Bid Race fields
    # These fields tell us if a given message could be in an Ask/Bid race and 
    # whether the message would attempt a Take or Cancel in a race and what its
    # outcome group would be in a race.
    #
    # In a race the following are considered take attempts:
    #     - New Order (limit/IOC/FOK): Race Price Dependent: Partial/Full fills, 
    #                                  Success: None
    #                                  Fail: Post to book/expired, 
    #                                  Unknown: New order no response
    #     - New Order (Market): Race Price Dependent: Partial/Full fills
    #                           Success: None
    #                           Fail: None
    #                           Unknown: New order no response
    #     - C/R that improve the price: Race Price Dependent: Partial/Full fills
    #                                   Success: None
    #                                   Fail: C/R accepted
    #                                   Unknown: C/R no response
    #     - New quotes that improve the price: Race Price Dependent: Partial/Full fills
    #                                             Success: None
    #                                             Fail: Quote updated, accepted 
    #                                             Unknown: New order no response
    # 
    # In a race the following are considered as cancel attempts:
    #     - Cancels: Race Price Dependent: None
    #                Success: Cancel accepted
    #                Fail: Cancel rejected
    #                Unknown (Cancel no response
    #     - C/R that worsen the price: Race Price Dependent: None
    #                                  Success: C/R accepted
    #                                  Fail: C/R rejected
    #                                  Unknown: C/R no response
    #     - New quote Cancels: Race Price Dependent: None
    #                             Success: Quote Cancel accepted
    #                             Fail: Quote Cancel rejected
    #                             Unknown: New order no response
    #     - New quotes that worsen the price: Race Price Dependent
    #                                            Success: Quote updated 
    #                                            Fail: None 
    #                                            Unknown: Quote accepted, New quote no response
    # 
    # The following functions also unify price and quantity information for race relevant messages.
    # 
    # More detail can be found in Section 10.4 of the Code and Data Appendix
    # 
    # Ask Races
    msgs = gen_ask_races_fields(msgs, top, side_ask, side_bid, canc, new_limit, new_ioc, new_mkt,
                         crep, valid_prices, valid_bid_price, valid_ask_prices, new_quote,
                         valid_prev_price, valid_prev_ask_price)
                        
    # Bid Races
    msgs = gen_bid_races_fields(msgs, top, side_ask, side_bid, canc, new_limit, new_ioc, new_mkt,
                         crep, valid_prices, valid_bid_prices, valid_ask_price, new_quote,
                         valid_prev_price, valid_prev_bid_price)
   
    ## Step 3: Add processing time
    
    # Fill in processing time. This will help determine the time horizon of races.
    # This is defined as the time from an inbound to the first outbound for each order-event pair
    msgs = get_processing_time(msgs, is_qr)

    ## Step 4 Create Signed Prices
    # Signed price fields are used to compare ask and bid prices with the same logic operators.
    # e.g. by having negative bids the highest bid is now the smallest signed price
    msgs['AskRaceRlvtPriceLvlSigned'] = msgs['AskRaceRlvtPriceLvl']
    msgs['BidRaceRlvtPriceLvlSigned'] = -1 * msgs['BidRaceRlvtPriceLvl']
    msgs['AskRaceRlvtBestExecPriceLvlSigned'] = msgs['AskRaceRlvtBestExecPriceLvl']
    msgs['BidRaceRlvtBestExecPriceLvlSigned'] = -1. * msgs['BidRaceRlvtBestExecPriceLvl'] 

    top['BestAskSigned'] = top['BestAsk']
    top['BestBidSigned'] = -1 * top['BestBid']

    # Add flags for 'Fill Events' (simplifies the QtyTraded calculation)
    msgs['AskFillEvent'] = msgs['Event'].isin({'New order aggressively executed in full', 'New order aggressively executed in part'})
    msgs['AskFillEvent'] = msgs['AskFillEvent'] | msgs['Event'].isin({'Cancel/replace aggressively executed in full', 'Cancel/replace aggressively executed in part'})
    msgs['AskFillEvent'] = msgs['AskFillEvent'] | msgs['BidEvent'].isin({'New quote aggressively executed in full', 'New quote aggressively executed in part'})
    msgs['BidFillEvent'] = msgs['Event'].isin({'New order aggressively executed in full', 'New order aggressively executed in part'})
    msgs['BidFillEvent'] = msgs['BidFillEvent'] | msgs['Event'].isin({'Cancel/replace aggressively executed in full', 'Cancel/replace aggressively executed in part'})
    msgs['BidFillEvent'] = msgs['BidFillEvent'] | msgs['AskEvent'].isin({'New quote aggressively executed in full', 'New quote aggressively executed in part'})
    
    # Return updated dataframe
    return (msgs, top)

############################### 
## Helper Functions ##
###############################

def gen_ask_races_fields(msgs, top, side_ask, side_bid, canc, new_limit, new_ioc, new_mkt, crep, \
                        valid_prices, valid_bid_price, valid_ask_prices, new_quote, \
                        valid_prev_price, valid_prev_ask_price):
    '''
    This function is called in prepare_data() and 
    changes msgs by adding race relevant ask fields.
    The fields added to msgs are the following (see the docstring and prepare_data for detail):
        AskRaceRlvt
        AskRaceRlvtType
        AskRaceRlvtPriceLvl
        AskRaceRlvtPriceLvlSigned
        AskRaceRlvtBestExecPriceLvl
        AskRaceRlvtBestExecPriceLvlSigned
        AskRaceRlvtQty
        AskRaceRlvtOutcomeGroup
        AskRaceRlvtNoResponse
    '''
    
    ## Create variables to identify race relevant messages
    # wrs: worsening (moving to a worse price i.e. lower bid, higher ask)
    # impr: improving (moving to a better price i.e. higher bid, lower ask)
    
    # Gateway Cancel (non-Quote Related)
    # Ignore: Cancel request failed
    canc_ask = canc & valid_prev_price & side_ask & msgs['Event'].isin({'Cancel request accepted', 
                                                                        'Cancel request rejected', 
                                                                        'Cancel no response'})
    
    # Gateway Cancel (Quote Related)
    # Cancel message on either side cancels both sides of quote
    # Ignore: New quote cancel failed (non-TLTC)
    canc_qr =  valid_prev_ask_price & msgs['AskEvent'].isin({'Quote cancel accepted', 
                                                             'Quote cancel rejected',
                                                             'Quote cancel no response'}) 

    # Gateway New Order (Limit)
    # Ignore: New order suspended, New order failed (non-TLTC), New order expired
    new_limit_bid = new_limit & side_bid & msgs['Event'].isin({'New order aggressively executed in full',
                                                               'New order aggressively executed in part',
                                                               'New order accepted',
                                                               'New order no response'})
    # Gateway New Order (IOC/FOK)
    # Ignore: New order suspended, New order failed (non-TLTC)
    new_ioc_bid = new_ioc & side_bid & msgs['Event'].isin({'New order aggressively executed in full',
                                                           'New order aggressively executed in part',
                                                           'New order expired',
                                                           'New order no response'})
    # Gateway New Order (Market)
    # Ignore: New order suspended, New order failed (non-TLTC)
    new_mkt_bid = new_mkt & side_bid & msgs['Event'].isin({'New order aggressively executed in full',
                                                           'New order aggressively executed in part',
                                                           'New order no response'})

    # Gateway Cancel/Replace messages
    # Ignore: case in which Gateway C/R increases or decreases size at same price, C/R failed,
    #         price impr C/R that are rejected, price wrs C/R that execute (should be rare)
    crep_wrs_ask = crep & side_ask & valid_prices & (msgs['PrevPriceLvl'] < msgs['PriceLvl']) \
                        & msgs['Event'].isin({'Cancel/replace request accepted',
                                              'Cancel/replace request rejected',
                                              'Cancel/replace no response'})
    crep_impr_bid = crep & side_bid & valid_prices & (msgs['PrevPriceLvl'] < msgs['PriceLvl']) \
                         & msgs['Event'].isin({'Cancel/replace aggressively executed in full',
                                               'Cancel/replace aggressively executed in part',
                                               'Cancel/replace request accepted',
                                               'Cancel/replace no response'})

    # Gateway New Quote messages
    # Ignore: For wrs (cancel case) we only care about updates because 
    # filling at a new price is not clearly a fail or success,
    # filling at an old price could be a fail or could be an 
    # unrelated fill from earlier (all other cases are ignored),
    # New quote failed, ignoring case in which we update the quantity at the same price
    new_quote_wrs_ask = new_quote & valid_ask_prices & (msgs['PrevAskPriceLvl'] < msgs['AskPriceLvl']) \
                                  & msgs['AskEvent'].isin({'New quote updated',
                                                           'New quote accepted',
                                                           'New quote no response'})
    new_quote_impr_bid = (new_quote & valid_bid_price  & ((msgs['PrevBidPriceLvl'].notnull())|(msgs['PrevBidPriceLvl'] < msgs['BidPriceLvl'])) \
                                    & msgs['BidEvent'].isin({'New quote aggressively executed in full',
                                                             'New quote aggressively executed in part',
                                                             'New quote updated',
                                                             'New quote accepted',
                                                             'New quote no response'}))

    ## Generate Ask race relevant fields
    # Initialize indicators
    msgs['AskRaceRlvt'] = False
    msgs['AskRaceRlvtType'] = None
    msgs['AskRaceRlvtPriceLvl'] = np.nan
    msgs['AskRaceRlvtPriceLvlSigned'] = np.nan  
    msgs['AskRaceRlvtBestExecPriceLvl'] = np.nan
    msgs['AskRaceRlvtBestExecPriceLvlSigned'] = np.nan 
    msgs['AskRaceRlvtQty'] = None
    msgs['AskRaceRlvtOutcomeGroup'] = 'Unknown'
    msgs['AskRaceRlvtNoResponse'] =  False

    # Set AskRaceRlvt to True for all considered cases
    ask_race_msgs = (canc_ask | canc_qr | new_limit_bid | new_mkt_bid | crep_wrs_ask | crep_impr_bid |
                     new_ioc_bid | new_quote_wrs_ask | new_quote_impr_bid)
    msgs.loc[ask_race_msgs, 'AskRaceRlvt'] = True
  
    ## Cancel attempts in Ask races
    ask_race_msgs_cancels    = canc_ask | crep_wrs_ask
    ask_race_msgs_cancels_qr = new_quote_wrs_ask | canc_qr
    msgs.loc[ask_race_msgs_cancels | ask_race_msgs_cancels_qr, 'AskRaceRlvtType'] = 'Cancel Attempt'
    
    # Set PriceLvls
    msgs.loc[ask_race_msgs_cancels   , 'AskRaceRlvtPriceLvl'] = msgs.loc[ask_race_msgs_cancels   , 'PrevPriceLvl']
    msgs.loc[ask_race_msgs_cancels_qr, 'AskRaceRlvtPriceLvl'] = msgs.loc[ask_race_msgs_cancels_qr, 'PrevAskPriceLvl']
    
    # Set execution price only for Takes
    
    # Set Qtys
    msgs.loc[ask_race_msgs_cancels   , 'AskRaceRlvtQty'] = msgs.loc[ask_race_msgs_cancels   , 'PrevQty']
    msgs.loc[ask_race_msgs_cancels_qr, 'AskRaceRlvtQty'] = msgs.loc[ask_race_msgs_cancels_qr, 'PrevAskQty']
  
    # Set success
    msgs.loc[(ask_race_msgs_cancels) & \
             (msgs['Event'].isin({'Cancel request accepted','Cancel/replace request accepted'})), 'AskRaceRlvtOutcomeGroup'] = 'Success'
    msgs.loc[(ask_race_msgs_cancels_qr) &\
             (msgs['AskEvent'].isin({'Quote cancel accepted','New quote updated'})), 'AskRaceRlvtOutcomeGroup'] = 'Success'
    # Set Fail
    msgs.loc[(ask_race_msgs_cancels) & \
             (msgs['Event'].isin({'Cancel request rejected','Cancel/replace request rejected'})), 'AskRaceRlvtOutcomeGroup'] = 'Fail'
    msgs.loc[(canc_qr) & (msgs['AskEvent'].isin({'Quote cancel rejected'})), 'AskRaceRlvtOutcomeGroup'] = 'Fail'
                                             
    # Set Unknown                                         
    msgs.loc[(ask_race_msgs_cancels) & \
             (msgs['Event'].isin({'Cancel no response','Cancel/replace no response'})), \
            'AskRaceRlvtOutcomeGroup'] = 'Unknown'
    msgs.loc[(ask_race_msgs_cancels_qr) & \
             (msgs['AskEvent'].isin({'New quote accepted','New quote no response','Quote cancel no response'})),\
            'AskRaceRlvtOutcomeGroup'] = 'Unknown'
                                                                                          
    # Flag no response 
    msgs.loc[ask_race_msgs_cancels,    'AskRaceRlvtNoResponse'] = msgs.loc[ask_race_msgs_cancels, 'Event'].isin({'Cancel no response',
                                                                                                                 'Cancel/replace no response'})
    msgs.loc[ask_race_msgs_cancels_qr, 'AskRaceRlvtNoResponse'] = msgs.loc[ask_race_msgs_cancels_qr, 'AskEvent'].isin({'Quote cancel no response',
                                                                                                                       'New quote no response'})

    ## Take attempts in Ask races
    ask_race_msgs_takes_nmkt = new_limit_bid | new_ioc_bid | crep_impr_bid
    ask_race_msgs_takes      = ask_race_msgs_takes_nmkt | new_mkt_bid
    ask_race_msgs_takes_qr   = new_quote_impr_bid
    msgs.loc[ask_race_msgs_takes | ask_race_msgs_takes_qr, 'AskRaceRlvtType'] = 'Take Attempt'
    
    # Set PriceLvls
    msgs.loc[ask_race_msgs_takes_nmkt, 'AskRaceRlvtPriceLvl'] = msgs.loc[ask_race_msgs_takes_nmkt , 'PriceLvl']
    msgs.loc[new_mkt_bid             , 'AskRaceRlvtPriceLvl'] = top.loc[new_mkt_bid              , 'BestAsk']   # Use BBO as mkt orders don't have PriceLvl
    msgs.loc[ask_race_msgs_takes_qr  , 'AskRaceRlvtPriceLvl'] = msgs.loc[ask_race_msgs_takes_qr, 'BidPriceLvl']
    
    # Set execution Prices
    msgs.loc[ask_race_msgs_takes_nmkt, 'AskRaceRlvtBestExecPriceLvl'] = msgs.loc[ask_race_msgs_takes_nmkt, 'MinExecPriceLvl']
    msgs.loc[new_mkt_bid             , 'AskRaceRlvtBestExecPriceLvl'] = msgs.loc[new_mkt_bid, 'MinExecPriceLvl']
    msgs.loc[ask_race_msgs_takes_qr  , 'AskRaceRlvtBestExecPriceLvl'] = msgs.loc[ask_race_msgs_takes_qr, 'BidMinExecPriceLvl']
    
    
    # Set Qtys
    msgs.loc[ask_race_msgs_takes, 'AskRaceRlvtQty'] = msgs.loc[ask_race_msgs_takes , 'OrderQty']
    msgs.loc[ask_race_msgs_takes_qr, 'AskRaceRlvtQty'] = msgs.loc[ ask_race_msgs_takes_qr, 'BidSize']
  
    # Set Race Price Dependent
    msgs.loc[(ask_race_msgs_takes) & (msgs['Event'].isin({'New order aggressively executed in full',
                                                      'New order aggressively executed in part',
                                                      'Cancel/replace aggressively executed in full',
                                                      'Cancel/replace aggressively executed in part'})), 'AskRaceRlvtOutcomeGroup'] = 'Race Price Dependent'
    msgs.loc[(ask_race_msgs_takes_qr) & (msgs['BidEvent'].isin({'New quote aggressively executed in full',
                                                            'New quote aggressively executed in part'}))  , 'AskRaceRlvtOutcomeGroup'] = 'Race Price Dependent'
    # Set Fails
    msgs.loc[(ask_race_msgs_takes_nmkt) & (msgs['Event'].isin({'New order accepted',
                                                           'New order expired',
                                                           'Cancel/replace request accepted'})), 'AskRaceRlvtOutcomeGroup'] = 'Fail'
    msgs.loc[(ask_race_msgs_takes_qr) & (msgs['BidEvent'].isin({'New quote updated', 
                                                            'New quote accepted'})), 'AskRaceRlvtOutcomeGroup'] = 'Fail'
                                                            
    # Set Unknown                                         
    msgs.loc[(ask_race_msgs_takes_qr) & (msgs['BidEvent'].isin({'New quote no response'})), 'AskRaceRlvtOutcomeGroup'] = 'Unknown'
    msgs.loc[(ask_race_msgs_takes) & (msgs['Event'].isin({'New order no response',
                                                      'Cancel/replace no response'})), 'AskRaceRlvtOutcomeGroup'] = 'Unknown'
                                                           
    # Flag No Responses
    msgs.loc[ask_race_msgs_takes_nmkt, 'AskRaceRlvtNoResponse'] = msgs.loc[ask_race_msgs_takes_nmkt, 'Event'].isin({'New order no response', 
                                                                                                                'Cancel/replace no response'})
    msgs.loc[ask_race_msgs_takes_qr,   'AskRaceRlvtNoResponse'] = msgs.loc[ask_race_msgs_takes_qr  , 'BidEvent'].isin({'New quote no response'})     

    return msgs                                                                                                            
                                
def gen_bid_races_fields(msgs, top, side_ask, side_bid, canc, new_limit, new_ioc, new_mkt, crep, \
                        valid_prices, valid_bid_prices, valid_ask_price, new_quote, \
                        valid_prev_price, valid_prev_bid_price):
    '''
    Function generates and fills in all the relevant fields for bid races
    This function is called in prepare_data() and 
    changes msgs by adding relevant bid race fields.
    The fields added to msgs are the following (see the docstring and prepare_data for detail):
        BidRaceRlvt
        BidRaceRlvtType
        BidRaceRlvtPriceLvl
        BidRaceRlvtPriceLvlSigned
        BidRaceRlvtBestExecPriceLvl
        BidRaceRlvtBestExecPriceLvlSigned
        BidRaceRlvtQty
        BidRaceRlvtOutcomeGroup
        BidRaceRlvtNoResponse
    '''

    ## Create variables to identify race relevant messages
    # wrs: worsening (moving to a worse price i.e. lower bid, higher ask)
    # impr: improving (moving to a better price i.e. higher bid, lower ask)
    
    # Gateway Cancel (non-Quote Related)
    # Ignore: Cancel request failed
    canc_bid = canc & valid_prev_price & side_bid & msgs['Event'].isin({'Cancel request accepted', 
                                                                      'Cancel request rejected', 
                                                                      'Cancel no response'})

    # Gateway Cancel (Quote Related)
    # Cancel message on either side cancels both sides of quote
    # Ignore: New quote cancel failed (non-TLTC)
    canc_qr =  valid_prev_bid_price & msgs['BidEvent'].isin({'Quote cancel accepted',
                                                           'Quote cancel rejected',
                                                           'Quote cancel no response'}) 

    # Gateway New Order (Limit)
    # Ignore: New order suspended, New order failed (non-TLTC), New order expired
    new_limit_ask = new_limit & side_ask & msgs['Event'].isin({'New order aggressively executed in full',
                                                             'New order aggressively executed in part',
                                                             'New order accepted',
                                                             'New order no response'})
    # Gateway New Order (IOC/FOK)
    # Ignore: New order suspended, New order failed (non-TLTC)
    new_ioc_ask = new_ioc & side_ask & msgs['Event'].isin({'New order aggressively executed in full',
                                                         'New order aggressively executed in part',
                                                         'New order expired',
                                                         'New order no response'})
    # Gateway New Order (Market)
    # Ignore: New order suspended, New order failed (non-TLTC)
    new_mkt_ask = new_mkt & side_ask & msgs['Event'].isin({'New order aggressively executed in full',
                                                         'New order aggressively executed in part',
                                                         'New order no response'})
    # Gateway Cancel/Replace messages
    # Ignore: case in which Gateway C/R increases or decreases size at same price, C/R failed,
    #         price impr C/R that are rejected, price wrs C/R that execute (rare)
    crep_impr_ask = crep & side_ask & valid_prices & (msgs['PrevPriceLvl'] > msgs['PriceLvl']) & msgs['Event'].isin({'Cancel/replace aggressively executed in full',
                                                                                                              'Cancel/replace aggressively executed in part',
                                                                                                              'Cancel/replace request accepted',
                                                                                                              'Cancel/replace no response'})
    crep_wrs_bid = crep & side_bid & valid_prices & (msgs['PrevPriceLvl'] > msgs['PriceLvl']) & msgs['Event'].isin({'Cancel/replace request accepted',
                                                                                                              'Cancel/replace request rejected',
                                                                                                              'Cancel/replace no response'})
    # Gateway New Quote messages
    # Ignore: For wrs (cancel case) we only care about updates because filling at a new price is not clearly a fail or success,
    # filling at an old price could be a fail or could be an unrelated fill from earlier (all other cases are ignored),
    # New quote failed, ignoring case in which we update the quantity at the same price
    new_quote_wrs_bid = new_quote & valid_bid_prices & (msgs['PrevBidPriceLvl'] > msgs['BidPriceLvl']) & msgs['BidEvent'].isin({'New quote updated', 
                                                                                                                                  'New quote accepted',
                                                                                                                                  'New quote no response'})
    new_quote_impr_ask = (new_quote & valid_ask_price  & ((msgs['PrevAskPriceLvl'].notnull())|
                                            (msgs['PrevAskPriceLvl'] > msgs['AskPriceLvl'])) & msgs['AskEvent'].isin({'New quote aggressively executed in full',
                                                                                                                'New quote aggressively executed in part',
                                                                                                                'New quote updated',
                                                                                                                'New quote accepted',
                                                                                                                'New quote no response'}))

    ## Bid Race Indicators
    # Flag messages that participate in races on the Bid
    # Initialize indicators
    msgs['BidRaceRlvt'] = False
    msgs['BidRaceRlvtType'] = None
    msgs['BidRaceRlvtPriceLvl'] = np.nan
    msgs['BidRaceRlvtPriceLvlSigned'] = np.nan 
    msgs['BidRaceRlvtBestExecPriceLvl'] = np.nan
    msgs['BidRaceRlvtBestExecPriceLvlSigned'] = np.nan 
    msgs['BidRaceRlvtQty'] = None
    msgs['BidRaceRlvtOutcomeGroup'] = 'Unknown'
    msgs['BidRaceRlvtNoResponse'] =  False

    # Set BidRaceRlvt to True for all considered cases
    bid_race_msgs = (canc_bid | canc_qr | new_limit_ask | new_ioc_ask | new_mkt_ask | crep_wrs_bid |
                      crep_impr_ask | new_quote_wrs_bid | new_quote_impr_ask )
    msgs.loc[bid_race_msgs, 'BidRaceRlvt'] = True

    ## Cancel attempts in Bid races
    bid_race_msgs_cancels    = canc_bid | crep_wrs_bid 
    bid_race_msgs_cancels_qr = new_quote_wrs_bid | canc_qr
    msgs.loc[bid_race_msgs_cancels | bid_race_msgs_cancels_qr, 'BidRaceRlvtType'] = 'Cancel Attempt'
    
    # Set PriceLvls
    msgs.loc[bid_race_msgs_cancels   , 'BidRaceRlvtPriceLvl'] = msgs.loc[bid_race_msgs_cancels   , 'PrevPriceLvl']
    msgs.loc[bid_race_msgs_cancels_qr, 'BidRaceRlvtPriceLvl'] = msgs.loc[bid_race_msgs_cancels_qr, 'PrevBidPriceLvl']
    
    # Only fill execution prices for takes
    
    # Set Qtys
    msgs.loc[bid_race_msgs_cancels   , 'BidRaceRlvtQty'] = msgs.loc[bid_race_msgs_cancels   , 'PrevQty']
    msgs.loc[bid_race_msgs_cancels_qr, 'BidRaceRlvtQty'] = msgs.loc[bid_race_msgs_cancels_qr, 'PrevBidQty']

    # Set success
    msgs.loc[(bid_race_msgs_cancels) & (msgs['Event'].isin({'Cancel request accepted',
                                                        'Cancel/replace request accepted'})) , 'BidRaceRlvtOutcomeGroup'] = 'Success'
    msgs.loc[(bid_race_msgs_cancels_qr) & (msgs['BidEvent'].isin({'Quote cancel accepted',
                                                              'New quote updated'})), 'BidRaceRlvtOutcomeGroup'] = 'Success' 
    
    # Set Fail
    msgs.loc[(bid_race_msgs_cancels) & (msgs['Event']).isin({'Cancel request rejected',
                                                         'Cancel/replace request rejected'}), 'BidRaceRlvtOutcomeGroup'] = 'Fail'
    msgs.loc[(canc_qr) & (msgs['BidEvent'].isin({'Quote cancel rejected'})), 'BidRaceRlvtOutcomeGroup'] = 'Fail'
                                             
                                             
    # Set Unknown                                         
    msgs.loc[(bid_race_msgs_cancels_qr) & (msgs['BidEvent'].isin({'New quote accepted',
                                                              'New quote no response',
                                                              'Quote cancel no response'})), 'AskRaceRlvtOutcomeGroup'] = 'Unknown'
    msgs.loc[(bid_race_msgs_cancels_qr) & (msgs['Event'].isin({'Cancel no response',
                                                           'Cancel/replace no response'})), 'AskRaceRlvtOutcomeGroup'] = 'Unknown'

    # Flag no response
    msgs.loc[bid_race_msgs_cancels, 'BidRaceRlvtNoResponse'] = msgs.loc[bid_race_msgs_cancels, 'Event'].isin({'Cancel no response',
                                                                                                          'Cancel/replace no response'})
    msgs.loc[bid_race_msgs_cancels_qr, 'BidRaceRlvtNoResponse'] = msgs.loc[bid_race_msgs_cancels_qr, 'BidEvent'].isin({'Quote cancel no response',
                                                                                                                   'New quote no response'})                                                                                                      

    ## Take attempts in Bid races
    bid_race_msgs_takes_nmkt = crep_impr_ask | new_limit_ask | new_ioc_ask 
    bid_race_msgs_takes      = bid_race_msgs_takes_nmkt | new_mkt_ask
    bid_race_msgs_takes_qr   = new_quote_impr_ask
    msgs.loc[bid_race_msgs_takes | bid_race_msgs_takes_qr, 'BidRaceRlvtType'] = 'Take Attempt'
    
    # Set PriceLvls
    msgs.loc[bid_race_msgs_takes_nmkt, 'BidRaceRlvtPriceLvl'] = msgs.loc[bid_race_msgs_takes_nmkt , 'PriceLvl']
    msgs.loc[new_mkt_ask             , 'BidRaceRlvtPriceLvl'] = top.loc[new_mkt_ask              , 'BestBid']  # Use BBO as mkt orders don't have PriceLvl
    msgs.loc[bid_race_msgs_takes_qr  , 'BidRaceRlvtPriceLvl'] = msgs.loc[bid_race_msgs_takes_qr, 'AskPriceLvl']
    
    # Set best execution prices
    msgs.loc[bid_race_msgs_takes_nmkt, 'BidRaceRlvtBestExecPriceLvl'] = msgs.loc[bid_race_msgs_takes_nmkt, 'MaxExecPriceLvl']
    msgs.loc[new_mkt_ask             , 'BidRaceRlvtBestExecPriceLvl'] = msgs.loc[new_mkt_ask, 'MaxExecPriceLvl']
    msgs.loc[bid_race_msgs_takes_qr  , 'BidRaceRlvtBestExecPriceLvl'] = msgs.loc[bid_race_msgs_takes_qr, 'AskMaxExecPriceLvl']

    # Set Qtys
    msgs.loc[bid_race_msgs_takes, 'BidRaceRlvtQty'] = msgs.loc[bid_race_msgs_takes, 'OrderQty']
    msgs.loc[bid_race_msgs_takes_qr, 'BidRaceRlvtQty'] = msgs.loc[bid_race_msgs_takes_qr, 'AskSize']

    # Set Race Price Dependent
    msgs.loc[(bid_race_msgs_takes) & (msgs['Event'].isin({'Cancel/replace aggressively executed in full',
                                                      'Cancel/replace aggressively executed in part',
                                                      'New order aggressively executed in full',
                                                      'New order aggressively executed in part'})), 'BidRaceRlvtOutcomeGroup'] = 'Race Price Dependent'
    msgs.loc[(bid_race_msgs_takes_qr) & (msgs['AskEvent'].isin({'New quote aggressively executed in full',
                                                            'New quote aggressively executed in part'}))  , 'BidRaceRlvtOutcomeGroup'] = 'Race Price Dependent'
    # Set Fails
    msgs.loc[(bid_race_msgs_takes_nmkt) & (msgs['Event'].isin({'Cancel/replace request accepted',
                                                           'New order accepted',
                                                           'New order expired'})), 'BidRaceRlvtOutcomeGroup'] = 'Fail'
    msgs.loc[(bid_race_msgs_takes_qr) & (msgs['AskEvent'].isin({'New quote updated' ,
                                                            'New quote accepted'}))  , 'BidRaceRlvtOutcomeGroup'   ] = 'Fail'
                                                            
    # Set Unknown                                         
    msgs.loc[(bid_race_msgs_takes_qr) & (msgs['AskEvent'].isin({'New quote no response'})), 'AskRaceRlvtOutcomeGroup'] = 'Unknown'
    msgs.loc[(bid_race_msgs_takes) & (msgs['Event'].isin({'New order no response',
                                                      'Cancel/replace no response'})), 'AskRaceRlvtOutcomeGroup'] = 'Unknown'
    
    # Flag no response
    msgs.loc[bid_race_msgs_takes_nmkt, 'BidRaceRlvtNoResponse'] = msgs.loc[bid_race_msgs_takes_nmkt, 'Event'].isin({'New order no response', 
                                                                                                                'Cancel/replace no response'})
    msgs.loc[bid_race_msgs_takes_qr, 'BidRaceRlvtNoResponse'] = msgs.loc[bid_race_msgs_takes_qr, 'AskEvent'].isin({'New quote no response'})

    return msgs

def get_processing_time(msgs, is_qr):
    '''
    Function to determine the processing time of each inbound.
    Processing time is defined as the time from an inbound to the first outbound.
    '''
  
    # Set up data for assigning inbound-outbound timestamps for each inbound message. 
    msgs['Inbound_MessageTimestamp'] = np.datetime64('NaT')
    msgs['Outbound_MessageTimestamp'] = np.datetime64('NaT')

    # Loop over orders and then repeat for quote related messages by side
    for qr_side in ['', 'Ask', 'Bid']:
        event_side = '%sEventNum' % qr_side
        if qr_side == '':
            non_qr_msgs = msgs.loc[~is_qr]
        else:
            non_qr_msgs = msgs.loc[is_qr & msgs[event_side].notnull()]
        
        # Find the inbound and outbound times for messages in each event within the order
        for _, sdf in non_qr_msgs.groupby(['UniqueOrderID', event_side]):
            gw_i = 0
            # Loop over each message within the uniqueorderid-event set
            for i in sdf.index:
                # Check if this is the first message for the event
                if gw_i == 0:
                    # Assign an inbound timestamp to the first message if it is an inbound. Every message is 
                    # either inbound or outbound
                    if msgs.at[i, 'Inbound'] == True:
                        gw_i = i
                        msgs.at[i, 'Inbound_MessageTimestamp'] = msgs.at[i, 'MessageTimestamp']
                    else:
                        break
                
                # For the first outbound message in the event following an inbound, copy the timestamp to the corresponding
                # inbound as the outbound timestamp.
                if msgs.at[i, 'Outbound'] == True:
                    msgs.at[gw_i, 'Outbound_MessageTimestamp'] = msgs.at[i, 'MessageTimestamp']
                    break
    
    # Assign the processing time based on the inbound/outbound timestamps. Then forward fill to all messages.
    # This will fill processing times for outbound messages with processing time as well, but
    # they are not used in race detection.
    msgs['ProcessingTime'] = msgs['Outbound_MessageTimestamp'] - msgs['Inbound_MessageTimestamp']
    msgs['ProcessingTime'] = msgs['ProcessingTime'].fillna(method = 'ffill') 
    
    return msgs
