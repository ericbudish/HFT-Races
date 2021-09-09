'''
This script contains functions to determine the race message outcomes
'''

import pandas as pd

def GetTakeOutcome(S, P_Signed, msg, strict_fail):
    '''
    This function inputs a message and returns whether it is a successful take message in race at price P and side S
    This function uses RaceRlvtOutcome and checks whether the Take was successful at the race price level or better 
    (for the taker). If strict_fail is True then, only expired IOCs are considered fails.
    
    Param:
        S: side 
        P_Signed: signed price 
        msg: pd.Series containing message of interest 
        strict_fail: If strict is True then, only expired IOCs are considered fails.
    
    Return: String representing the outcome of the take message
    Please note that 'Gateway New Order (IOC)' includes both IOC and FOK,
    we use TIF to exclude FOK orders.
    '''
    outcome = msg['%sRaceRlvtOutcomeGroup' % S]
    exec_pr = msg['%sRaceRlvtBestExecPriceLvlSigned' % S]
    msg_type = msg['UnifiedMessageType']
    msg_tif = msg['TIF']
    
    if strict_fail == True:
        if outcome == 'Fail':
            if (msg_type == 'Gateway New Order (IOC)') & (msg_tif == 'IOC'):
                return 'Fail'
            else:
                return 'Unknown'
        elif outcome == 'Race Price Dependent':
            if exec_pr <= P_Signed:
                return 'Success'
            else:
                return 'Unknown'
        else:
            return 'Unknown'
    else:
        if outcome == 'Fail':
            return 'Fail'
        elif outcome == 'Race Price Dependent':
            if exec_pr <= P_Signed:
                return 'Success'
            else:
                return 'Fail'
        else:
            return 'Unknown'

def GetOutcome(S, P_Signed, subset_msgs, strict_fail):
    '''
    This function inputs a set of messages and returns whether those messages are 
    successful/failed Take or Cancel given a race at price P and side S.
    
    Param:
        S: side 
        P_Signed: signed price 
        subset_msgs: pd.DataFrame containing messages of interest 
        strict_fail: If strict is True, then only expired IOCs are considered fails.
        
    Return:
        outcome: pd.Series of strings representing outcomes 
    '''
    outcome = pd.Series('Unknown', index=subset_msgs.index)
    for i in subset_msgs.index:
        if subset_msgs.at[i, '%sRaceRlvtType' % S] == 'Cancel Attempt':
            outcome.at[i] = subset_msgs.at[i, '%sRaceRlvtOutcomeGroup' % S]
        else: 
            outcome.at[i] = GetTakeOutcome(S, P_Signed, subset_msgs.loc[i], strict_fail)
    return (outcome)
