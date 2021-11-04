'''
Race_Detection_Functions.py

This module contains the race detection function find_single_lvl_races()
It is called in Race_Detection_and_Statistics.py to detect races.
'''

import numpy as np
import pandas as pd

from .Race_Msg_Outcome import get_msg_outcome
############################### 
## Main Functions ##
###############################

def find_single_lvl_races(msgs, top, ticktable, race_param): 
    '''
    Identify messages belonging to races on a single price level.
    Use baseline race criteria to check if a sequence of messages is a race.
    Avoid two races at the same price level to overlap.
    Record races in race_recs.

    Param: Please refer to Section 10.3 of the Code and Data Appendix.
            msgs, top: main message dataframe and top of book dataframe with added fields from PrepData()
            ticktable: the symbol-date's tick table info
            race_param: dict of race parameters, including
                min_num_participants: int. min number of unique UserIDs required in a race
                min_num_takes: int. min number of takes required in a race
                min_num_cancels: int. min number of cancels required in a race
                strict_fail: bool. 
                    If strict_fail == False, failed IOC orders, failed cancels, 
                        and plain-vanilla limit orders (including quotes and cancel/replace)
                        that are marketable at the race price but fail to take in the race 
                        count as fails.
                    If strict_fail == True, only failed IOC orders and failed cancels count as fails
                strict_success: bool.
                    If strict_success == False, a success is achieved by any of the following: 
                        a limit order or IOC order that executes a positive quantity at the race price; 
                        a cancel request that cancels at least in part at the race price. 
                    If strict_success == True, we additionally require that a limit order or IOC order 
                        fails to execute at the race price. This implies that the full price level was 
                        cleared in the race by either successful takes or successful cancels. 
                method: 'Info_Horizon' or 'Fixed_Horizon'. Please see Section 4 of the Code and Data Appendix for detail.
                    If method == 'Info_Horizon', the following additional race parameters are required:
                        min_reaction_time: int. Minimum time to respond to a matching engine update in microseconds.
                        info_horizon_upper_bound: int. Upper bound of the information horizon in microseconds.
                    If method == 'Fixed_Horizon', the following additional race parameter is required:
                        len_fixed_hor: int. Length of the fixed horizon in microseconds.

    Output: 
        race_recs 
            Dataframe with fields 'SingleLvlRaceID', 'ix', 'MessageTimestamp', 'Side', 'P_Signed', 'ix_race', and 'race_horizon'
            Each row contains information on a single race at a given price level, and is timed at the starting take or 
            cancel. A given starting message can appear in multiple rows as it can trigger races at multiple price levels. 
            Hence, there can be multiple races that have the same index and MessageTimestamp, but with different price level.
            We do not allow two races at the same price level to overlap in time.

    Steps: For Bid and Ask sides
        1. Loop over relevant messages flagged by Ask/BidRlvtMsg in Prep_Race_Data.py
        2. If a message is a take or cancel attempt at a valid price (potential race start),
            check if the price of a take attempt is at the BBO or better (higher bid, lower ask) or the cancel attempt.
            Get all prices between the order pricelvl and the bbo for take attempts and get the cancel
            pricelvl for cancel attempts 
        3. For each pricelvl in the list from 2, loop over subsequent relevant messages (flagged in Prep_Race_Data.py)
            within the information horizon of the potential race starting message. 
            Flag all messages that attempt to cancel at the price level or take at the price level or better
        4. If the sequence of messages flagged in 3 satisfies the Baseline Race criteria, add a row to race_recs
            with the information of the race and the indices of all the race messages

    The race criteria for a sequence of messages is:
        1. At least min_num_participants unique UserIDs
        2. At least min_num_takes take attempts
        3. At least min_num_cancels cancel attempts
        4. At least one successful message 
        5. At least one failed message 
        6. Additional requirement if strict_success == True: At least a failed take at P.
        7. No additional check for strict_fail because it is already taken into consideration
           when we obtain "%sRaceRlvtMsgOutcome % S" via the get_msg_outcome function.

    '''
    # Load race parameters
    
    method = race_param['method']
    if method == 'Info_Horizon':
        min_reaction_time = np.timedelta64(int(race_param['min_reaction_time']), 'us')
        info_horizon_upper_bound = np.timedelta64(int(race_param['info_hor_upper_bound']), 'us')
    if method == 'Fixed_Horizon':
        len_fixed_hor = np.timedelta64(int(race_param['len_fixed_hor']), 'us')

    # Initialize data structure to store race records
    # The dictionary will have the following form: {(idx, timestamp, side, price, info horizon): index of race msgs}
    race_recs = {}
    
    # First look for races on the bid, then for races on the ask
    for S in {'Bid', 'Ask'}:
        # Slice the relevant msgs and the associated top-of-book (i.e. BBO) information.
        # The relevant messages are those which, if they appear in races, meet the following criteria:
        # When S = 'Ask': race to take the orders to sell.
        #                 Take attempts are msgs to buy, or cancel/replace to buy at higher prices
        #                 Cancel attempts are msgs to cancel orders to sell, or cancel/replace to sell at higher prices
        # When S = 'Bid': race to take the orders to buy.
        #                 Take attempts are msgs to sell, or cancel/replace to sell at lower prices
        #                 Cancel attempts are msgs to cancel orders to buy, or cancel/replace to buy at lower prices
        relevant_msgs = msgs.loc[msgs['%sRaceRlvt' % S]]
        relevant_top = top.loc[msgs['%sRaceRlvt' % S]]

        # Initialize vectors of relevant msg characteristics to make loop faster
        # Prices are 'Signed' so that more aggressive prices are always greater than less aggressive prices
        ix = relevant_msgs.index.values  # Indices of the relevant messages (the row #s)
        ts = relevant_msgs['MessageTimestamp'].values
        pr = relevant_msgs['%sRaceRlvtPriceLvlSigned' % S].values  
        bbo = relevant_top['Best%sSigned' % S].values 
        valid = (relevant_top['Best%s' % S].notnull()).values
        canc_attempt = (relevant_msgs['%sRaceRlvtType' % S] == 'Cancel Attempt').values 
        take_attempt = (relevant_msgs['%sRaceRlvtType' % S] == 'Take Attempt').values 
        proc_time = relevant_msgs['ProcessingTime'].values
        
        # Initialize a data structure to keep track of the time of the previous race at each price level
        # They help us avoid overlapping races at the same price level.
        prev_race_MessageTime = {}
        prev_race_horizon = {}

        # Loop over msgs
        # pr is negative for bid races and positive for ask races.
        # pr has the opposite side price levels for take attempts and the same side price levels for cancel attempts.
        # bbo is negatitve for bid and positive for ask. See the cases below for greater detail
        i, i_stop, j_stop, check_i_for_races = 0, len(ix) - 1, len(ix) - 1, False
        counter_processingT = 0 # Count cases where a null processing time resulted in an adjustment to the output
        while i <= i_stop:
            
            # Case 1: Msg is a cancel attempt 
            # We check for races from cancel attempts. 
            # With the perfect data, cancels can only happen at prices at the BBO or at higher asks/lower bids
            # and we should only count cancels at the BBO.
            # In reality, we can observe a cancel at lower asks/higher bids than BBO because there is 
            # an update latency of our order book construction.
            # This can happen for two reasons.
            #       1. BBOs can only be updated on outbound. So the BBO can be off by several messages.
            #          For example, the market is bid 10 ask 14 and someone sends an order to buy
            #          at 13. He then sends a second message to cancel that order even before the 
            #          order accepted outbound is out from the matching engine. When we see the 
            #          cancel attempt inbound, we think the market is 10-14 while actually it should
            #          13-14 if the order book is updated right away. This is also a reason for 
            #          some races to have negative effective spread.
            #       2. Packet loss. Some messages are missing and that makes the order book inaccurate.
            # 
            # As a result, we don't impose a hard requirement on the price at which the msg tries 
            # to cancel because the order book can be off by several messages (and this can be quite
            # common). These races are valid races provided that we observe >=1 takes, success 
            # and fail at this price level.

            
            if valid[i] and canc_attempt[i]:
                possible_race_prices = [pr[i]] 
                check_i_for_races = True

            # Case 2: Msg is a take attempt at current BBO or a higher bid/lower ask price 
            # (if successful, this message would take depth at the BBO).
            # Ask race case: pr is for a new order, new quote or C/R on the bid and is positive.
            #                So, the condition is satisfied when we are attempting
            #                to take at the best ask or higher.
            # Bid race case: pr is for a new order, new quote or C/R on the ask and is negative, and 
            #                bbo is also negative. So, the condition is satisfied when we are attempting
            #                to take at the best bid or lower (higher in negative signed price)
            elif valid[i] and take_attempt[i] and pr[i] >= bbo[i]:
                possible_race_prices = get_price_lvls(bbo[i], pr[i], ticktable) 
                check_i_for_races = True

            # If Case 1 or Case 2 is True, check for races at the possible race prices
            if check_i_for_races:
                for p in possible_race_prices:

                    # Initialize 'time of the previous race' for the first race at price/side
                    if not p in prev_race_MessageTime.keys():
                        prev_race_MessageTime[p] = np.datetime64('NaT')
                        prev_race_horizon[p]= np.timedelta64(0, 'us')
                        
                    # If no overlapping, check for a new race.
                    # The condition below will always be True if there hasn't been a race on the price and side.
                    # That is, no overlapping restriction for the first race.
                    if not ts[i] <= (prev_race_MessageTime[p] + prev_race_horizon[p]):
                      
                        # Set the race horizon for this race. 
                        # if method == 'Info_Horizon', race_horizon is the sum of the
                        # processing time of the message (outbound - inbound) and the 
                        # min_reaction_time, capped at info_horizon_upper_bound.
                        # elif method == 'Fixed_Horizon', race_horizon is len_fixed_hor.
                        if method == 'Info_Horizon':
                            if pd.isnull(proc_time[i]):
                                race_horizon = info_horizon_upper_bound
                                counter_processingT = counter_processingT + 1
                            else:
                                race_horizon = min(proc_time[i] + min_reaction_time, info_horizon_upper_bound)
                        elif method == 'Fixed_Horizon':
                            race_horizon = len_fixed_hor

                        # First create a list with the indices of all possible race msgs at p within the info horizon from i
                        seq = [i]
                        j =  i + 1
                        while j <= j_stop:
                            if ts[j] > (ts[i] + race_horizon):
                                break
                            else:
                                # Include any cancels at p
                                if canc_attempt[j] and pr[j] == p:
                                    seq = seq + [j]
                                # And any takes at p or a higher bid/lower ask price
                                elif take_attempt[j] and pr[j] >= p:
                                    seq = seq + [j]
                                # If not a take or cancel at p or a higher bid/lower ask we don't add the index
                                j += 1
                        
                        # Slice message data for the possible race messages
                        race_msgs = msgs.loc[ix[seq]]
                        
                        # Get msg outcomes given if the message appears in a race at price p.
                        # This mostly matters for take attempts that are Race Price Dependent which will
                        # now be labeled as Success, Fail or Unknown given the race price p
                        race_msgs['%sRaceRlvtMsgOutcome' % S] = get_msg_outcome(S, p, race_msgs, race_param['strict_fail'])
                        
                        # Then check the sequence for a baseline race
                        # and update the race_recs dictionary if baseline race criteria satisfied.
                        # See function header for baseline race criteria
                        if is_a_race(S, p, race_msgs, race_param):
                            race_recs[(race_msgs.index[0], race_msgs['MessageTimestamp'].iloc[0], 
                                                        S, p, race_horizon)] = [race_msgs.index]
                            prev_race_MessageTime[p] = ts[i]
                            prev_race_horizon[p] = race_horizon

            check_i_for_races = False
            i += 1

    # Convert the data from dict to pd.DataFrame
    if len(race_recs) > 0:
        race_recs = pd.DataFrame.from_dict(race_recs, orient='index').reset_index()
        race_recs.columns = ['UnifiedInfo', 'Race_Msgs_Idx']
        race_recs[['Race_Start_Idx', 'MessageTimestamp', 'Side', 'P_Signed', 'Race_Horizon']] = pd.DataFrame(race_recs['UnifiedInfo'].to_list(), index = race_recs.index)
    
        # Create a single level race identifier, taking sequential values
        race_recs = race_recs.sort_values(['Race_Start_Idx', 'Side', 'P_Signed']).reset_index()
        race_recs['SingleLvlRaceID'] = range(len(race_recs.index))
        race_recs = race_recs[['SingleLvlRaceID', 'Race_Start_Idx', 'MessageTimestamp', 'Side', 'P_Signed', 'Race_Msgs_Idx', 'Race_Horizon']]
    else:
        race_recs = pd.DataFrame(columns=['SingleLvlRaceID', 'Race_Start_Idx', 'MessageTimestamp', 'Side', 'P_Signed', 'Race_Msgs_Idx', 'Race_Horizon'])
    # Return race recs
    return race_recs
    
######################
## Helper Functions ##
######################
    
def is_a_race(S, pr, race_msgs, race_param):
    '''
    This function checks whether seq of messages satisfies the single lvl race logic
    The requirement for a race is:
        1. At least min_num_participants unique UserIDs
        2. At least min_num_takes take attempts
        3. At least min_num_cancels cancel attempts
        4. At least one successful message 
        5. At least one failed message 
        6. Additional requirement if strict_success == True: At least a failed take at P.
        7. No additional check for strict_fail because it is already taken into consideration
           when we obtain "%sRaceRlvtMsgOutcome % S" via the get_msg_outcome function.
    '''
    # Set default
    is_a_race = False
    # Obtain msg info
    num_userIDs = race_msgs['UserID'].nunique()
    is_take = race_msgs['%sRaceRlvtType' % S].to_numpy() == 'Take Attempt'
    is_canc = race_msgs['%sRaceRlvtType' % S].to_numpy() == 'Cancel Attempt'
    is_success = race_msgs['%sRaceRlvtMsgOutcome' % S].to_numpy() == 'Success'
    is_fail = race_msgs['%sRaceRlvtMsgOutcome' % S].to_numpy() == 'Fail'

    # min required number of participants
    if num_userIDs >= race_param['min_num_participants']:
        # min required number of takes
        if is_take.sum() >= race_param['min_num_takes']:
            # min required number of cancels
            if is_canc.sum() >= race_param['min_num_cancels']:
                # at least one success msg
                if is_success.sum() >= 1:
                    # at least one failed msg
                    if is_fail.sum() >= 1:
                        # Set is_a_race to True
                        is_a_race = True
                        
    # Additional requirement if strict_success == True:
    if race_param['strict_success'] == True:
        # Obtain additional msg info (take at P)
        RaceRlvtPriceLvlSigned = race_msgs['%sRaceRlvtPriceLvlSigned' % S].to_numpy()
        is_fail_take_eq_p = is_fail & is_take & (RaceRlvtPriceLvlSigned == pr)
        # We require a failed take at P if strict_success == True.
        # If this is satisfied, don't change the value of is_a_race.
        # Otherwise, set is_a_race to False.
        if is_fail_take_eq_p.sum() >= 1:
            pass
        else:
            is_a_race = False

    # No additional check for strict_fail. 
    # This is because we already changed the definition of a fail 
    # in "%sRaceRlvtMsgOutcome % S" via the get_msg_outcome function
    # if strict_fail == True.

    return is_a_race

def get_price_lvls(p_min, p_max, ticktable):
    '''
    This function returns the list of prices between two prices with tick size increments
    '''
    p, lvls = p_min, []
    while p <= p_max:
        lvls = lvls + [p]
        p += ticktable.loc[ticktable['p_int64'] <= abs(p), 'tick_int64'].iloc[-1].item()
    return (np.array(lvls))
