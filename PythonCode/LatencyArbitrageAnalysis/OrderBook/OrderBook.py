'''
OrderBook.py

Defines the orderbook object for book updating.

The class object OrderBook represents the state of the order book at a given outbound message. 
We also use it to fill the top dataframe with the top of book information that is one of the outputs 
of this script

The OrderBookLvl class is initialized within OrderBook to store the information to update the order
book level and the depth data structure for a given price/side
'''

import numpy as np

# Define data structure to represent the order book
class OrderBook(object):

    # Initialization
    def __init__(self, df, top, depth_updates):
        
        # Data
        self._df = df
        self._top = top
        self._depth_updates = depth_updates
        
        # Dictionary to reference active price levels
        self.lvls = {}
        
        # Current bid and ask dict {side:{Price:Qty}} for displayed depth and total depth
        self.curr_disp = {'Bid':{},'Ask':{}}
        self.curr_total = {'Bid':{},'Ask':{}}

        # Current best bid and best ask price
        self.best_bid = np.nan
        self.best_bid_qty = np.nan
        self.best_ask = np.nan
        self.best_ask_qty = np.nan

        # Current best bid and best ask including hidden depth
        self.best_bid_h = np.nan
        self.best_bid_h_qty = np.nan
        self.best_ask_h = np.nan
        self.best_ask_h_qty = np.nan

    def get_top(self):
        return self._top

    def get_depth(self):
        return self._depth_updates

    def clean_book(self, S):
        # Remove levels with zero quantity and nan quantity
        self.curr_disp[S] = {k:v for k,v in self.curr_disp[S].items() if v > 0}
        self.curr_total[S] = {k:v for k,v in self.curr_total[S].items() if v > 0}
    
    def calculate_bbo(self, S):
        # update the bbo on the given side
        if S == 'Bid':
            self.best_bid = np.array(list(self.curr_disp[S].keys())).max() if len(self.curr_disp[S]) > 0 else np.nan
            self.best_bid_qty = self.curr_disp[S].get(self.best_bid, np.nan)
            self.best_bid_h = np.array(list(self.curr_total[S].keys())).max() if len(self.curr_total[S]) > 0 else np.nan
            self.best_bid_h_qty = self.curr_total[S].get(self.best_bid_h, np.nan)
        if S == 'Ask':
            self.best_ask = np.array(list(self.curr_disp[S].keys())).min() if len(self.curr_disp[S]) > 0 else np.nan
            self.best_ask_qty = self.curr_disp[S].get(self.best_ask, np.nan)
            self.best_ask_h = np.array(list(self.curr_total[S].keys())).min() if len(self.curr_total[S]) > 0 else np.nan
            self.best_ask_h_qty = self.curr_total[S].get(self.best_ask_h, np.nan)
            
    def UpdateBBO(self, k, S, P, qty, qty_h):
        # Update current BBO
        # - S is Side, P is PriceLvl for the Event 
        # - k is the book updating message index for the outbound quote message we loop over
        # - qty is displayed number of shares and qty_h is total number of shares
        self.curr_disp[S][P] = qty
        self.curr_total[S][P] = qty_h
        self.clean_book(S)
        self.calculate_bbo(S)

        # Update records of depth updates in depth_updates dictionary
        # Current depth at the book change
        self._depth_updates[(k, S, P, 'Disp')] = qty
        self._depth_updates[(k, S, P, 'Total')] = qty_h

        # Update output top dataframe (BBO)
        self._top.at[k, 'BestBid'] = self.best_bid
        self._top.at[k, 'BestBidQty'] = self.best_bid_qty
        self._top.at[k, 'BestAsk'] = self.best_ask
        self._top.at[k, 'BestAskQty'] = self.best_ask_qty

        # Update BBO for Best Bid/Ask including hidden Qty
        self._top.at[k, 'BestBid_h'] = self.best_bid_h
        self._top.at[k, 'BestBidQty_h'] = self.best_bid_h_qty
        self._top.at[k, 'BestAsk_h'] = self.best_ask_h
        self._top.at[k, 'BestAskQty_h'] = self.best_ask_h_qty

    def UpdateLvl(self, S, P, k, j):
        # Create/updating an order book level using the OrderBookLvl Class 
        # - S is Side, P is PriceLvl for the Event's first message
        # - j is the book updating message index for a given party in the trade (1 or 2)
        #   or the non-trade outbound message
        # - k is the book updating message index for the outbound quote message we loop over
        # - j and k are equal in cases where j is for the index for the first message 
        df = self._df
        if (S, P) not in self.lvls.keys():
            self.lvls[(S, P)] = OrderBookLvl(S, P)
        booklevel = self.lvls[(S, P)]
        unique_order_id = df.at[j, 'UniqueOrderID']
        booklevel.orders[unique_order_id] = df.at[j, 'DisplayQty'] # Add Displayed Quantity to the dictionary
        booklevel.orders_h[unique_order_id] = df.at[j, 'LeavesQty'] # Add Total Quantity to the dictionary
        booklevel.curr_depth = np.array(list(booklevel.orders.values())).sum() # Calculate current displayed depth
        booklevel.curr_depth_h = np.array(list(booklevel.orders_h.values())).sum() # Calculate current total depth
        self.UpdateBBO(k, booklevel.S, booklevel.P, booklevel.curr_depth, booklevel.curr_depth_h)

    def UpdatePrevLvl(self, S, P, k, j):
        # Cancels a given order in the (S, P) level 
        # same variables as update
        # Removes a given order from the depth structure and 
        # updates the book without that order        
        df = self._df
        if (S, P) not in self.lvls.keys():
            self.lvls[(S, P)] = OrderBookLvl(S, P)
        booklevel = self.lvls[(S, P)]
        unique_order_id = df.at[j, 'UniqueOrderID']
        booklevel.orders[unique_order_id] = 0
        booklevel.orders_h[unique_order_id] = 0
        booklevel.curr_depth = np.array(list(booklevel.orders.values())).sum()
        booklevel.curr_depth_h = np.array(list(booklevel.orders_h.values())).sum()
        self.UpdateBBO(k, booklevel.S, booklevel.P, booklevel.curr_depth, booklevel.curr_depth_h)
        
    def Correctlvl(self, P, correct_side, correct_type, strict, k): 
        # Apply order book corrections logic
        curr_disp = list(self.curr_disp[correct_side].keys())
        curr_total = list(self.curr_total[correct_side].keys())
        if correct_side == 'Bid':
            need_to_kill =  (strict and self.best_bid > P) or (not strict and self.best_bid >= P)
            need_to_kill_h = (strict and self.best_bid_h > P) or (not strict and self.best_bid_h >= P)
        if correct_side == 'Ask':
            need_to_kill =  (strict and self.best_ask < P) or (not strict and self.best_ask <= P) 
            need_to_kill_h = (strict and self.best_ask_h < P) or (not strict and self.best_ask_h <= P) 
        # Kill any order need to be killed for displayed qty
        if P > 0 and need_to_kill:
            for plvl in curr_disp:
                if (correct_side == 'Bid' and ((strict and plvl > P) or (not strict and plvl >= P))) or \
                   (correct_side == 'Ask' and ((strict and plvl < P) or (not strict and plvl <= P))):
                    self._top.at[k,'DepthKilled'] = self._top.at[k, 'DepthKilled'] + na_to_zero(self.lvls[(correct_side, plvl)].curr_depth)
                    self._top.at[k,'Corrections_%s' % correct_type] = self._top.at[k, 'Corrections_%s' % correct_type] + 1
                    self.UpdateKillLvl(correct_side, plvl, k)
        # Kill any order need to be killed for total qty
        if P > 0 and need_to_kill_h:
            for plvl in curr_total:
                if (correct_side == 'Bid' and ((strict and plvl > P) or (not strict and plvl >= P))) or \
                   (correct_side == 'Ask' and ((strict and plvl < P) or (not strict and plvl <= P))):
                    self._top.at[k,'DepthKilled_h'] = self._top.at[k, 'DepthKilled_h'] + na_to_zero(self.lvls[(correct_side, plvl)].curr_depth_h)
                    self._top.at[k,'Corrections_%s_h' % correct_type] = self._top.at[k, 'Corrections_%s_h' % correct_type] + 1
                    self.UpdateKillLvl(correct_side, plvl, k)

    def UpdateKillLvl(self, S, P, k):
        # Kill the entire Level (set all depth and volume to empty/-99)
        if (S, P) not in self.lvls.keys():
            self.lvls[(S, P)] = OrderBookLvl(S, P)
        booklevel = self.lvls[(S, P)]
        booklevel.orders = {}
        booklevel.orders_h = {}
        booklevel.curr_depth = np.nan
        booklevel.curr_depth_h = np.nan
        self.UpdateBBO(k, booklevel.S, booklevel.P, np.nan, np.nan)

class OrderBookLvl(object):
    '''
    Define data structure to represent a level of the order book
    Param:
        S: side
        P: price
        orders: Displayed depth of active orders {UniqueOrderID:qty}
        orders_h: Total depth of active orders {UniqueOrderID:qty_h}
        curr_depth: Current displayed depth
        curr_depth_h: Current total depth
    '''
    def __init__(self, S, P):
        self.S = S                  
        self.P = P                  
        self.orders = {}            
        self.orders_h = {}          
        self.curr_depth = np.nan
        self.curr_depth_h = np.nan

def na_to_zero(num):
    if np.isnan(num):
        return 0
    return num
