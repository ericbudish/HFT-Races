'''
Orders.py

Class object to keep track of order price and quantity information
'''

import numpy as np

class Order(object):
    '''
    Class object to keep track of order price and quantity information.
    Amend is used for limit orders in place of cancel replace to shorten method names.
    Amend for quotes can be understood as changing an existing quote
    '''
    def __init__(self):
        self.cancel_prc = np.nan
        self.cancel_qty = np.nan
        self.gw_prc = np.nan
        self.gw_qty = np.nan
        self.me_prc = np.nan
        self.me_qty = np.nan
    def add(self, p = np.nan, q = np.nan):
        '''
        Add price and quantity from current inbound message
        '''
        self.gw_prc = p
        self.gw_qty = q
    def cancel(self):
        '''
        Set cancel price and quantity to previous message information.
        Cancel price and quantity are used as the previous price and quantity
        for cancel messages
        '''
        self.cancel_prc = self.gw_prc
        self.cancel_qty = self.me_qty
        self.me_qty = np.nan
    def cancel_reject(self):
        '''
        This is like cancel, but we do not update the ME quantity.
        Cancel price and quantity are used as the previous price and quantity
        for cancel reject messages
        '''
        self.cancel_prc = self.gw_prc
        self.cancel_qty = self.me_qty
    def amend(self, p = np.nan, q = np.nan):
        '''
        Set cancel price and quantity to previous message information
        and specify current price and quantity,
        Cancel price and quantity are used as the previous price and quantity
        for cancel/replace messages
        '''
        self.cancel_prc = self.gw_prc
        self.cancel_qty = self.me_qty
        self.gw_prc = p
        self.gw_qty = q
    def amend_reject(self, p = np.nan, q = np.nan):
        '''
        This is the same as amend. It is used to be consistent with event classification logic
        Cancel price and quantity are used as the previous price and quantity
        for cancel/replace reject messages
        '''
        self.cancel_prc = self.gw_prc
        self.cancel_qty = self.me_qty
        self.gw_prc = p
        self.gw_qty = q
    def passive_fill(self, leaves = np.nan):
        '''
        Update ME quantity
        '''
        self.me_qty = leaves
    def update_me(self, q = np.nan):
        '''
        Update ME price and quantity
        '''
        self.me_prc = self.gw_prc
        self.me_qty = q

class Quote(object):
    '''
    Class object to keep track of quote price and quantity information
    '''
    def __init__(self):
        self.cancel_prc = np.nan
        self.cancel_qty = np.nan
        self.gw_prc = np.nan
        self.gw_qty = np.nan
        self.me_prc = np.nan
        self.me_qty = np.nan
        self.status = 'None'
        self.expect_add = False
        self.expect_amend = False
        self.expect_cancel = False
    def update(self, p=np.nan, q=np.nan):
        '''
        Set cancel price and quantity to previous message information
        and specify current price and quantity. 
        Cancel price and quantity are used as the previous price and quantity
        for cancel/replace messages
        '''
        self.cancel_prc = self.gw_prc
        self.cancel_qty = self.me_qty
        self.gw_prc = p
        self.gw_qty = q
    def update_expectations(self):
        '''
        Set expectations for ME responses based on the current status of the 
        order (e.g. if there is no order, then we expect to add a new order 
        but not to cancel or amend)
        '''
        self.expect_add = False
        self.expect_amend = False
        self.expect_cancel = False
        # Case 1: No previously accepted quote
        if self.status in ('None'):
            self.expect_add = True
        # Case 2: Last accepted quote was accepted or suspended
        elif self.status in ('Accepted (0)', 'Suspended (9)', 'Amended (5)', 'Executed (1)'):
            self.expect_cancel = True
            if (self.gw_prc != self.cancel_prc) or (self.gw_qty != self.cancel_qty):
                self.expect_amend = True
        # Case 3: Last accepted quote was executed in full or cancelled
        elif self.status in ('Executed (2)', 'Expired (6)', 'Cancelled (4)', 'Rejected (8)'):
            self.expect_add = True
        # Case 4: Last quote did not receive ME response
        elif self.status in ('No ME Response'):
            if (self.gw_prc != self.cancel_prc) or (self.gw_qty != self.cancel_qty):
                self.expect_add, self.expect_amend = True, True
    def update_reject(self, p=np.nan, q=np.nan):
        '''
        This is the same as update. It is included for event classification logic consistency.
        Cancel price and quantity are used as the previous price and quantity
        for cancel/replace reject messages
        '''
        self.cancel_prc = self.gw_prc
        self.cancel_qty = self.me_qty
        self.gw_prc = p
        self.gw_qty = q
    def cancel(self):
        '''
        Set cancel price and quantity to previous message information.
        Cancel price and quantity are used as the previous price and quantity
        for cancel reject messages
        '''
        self.cancel_prc = self.gw_prc
        self.cancel_qty = self.me_qty
        self.me_qty = np.nan
        self.status = 'Cancelled (4)'
    def cancel_reject(self):
        '''
        This is like cancel, but we do not update the ME quantity
        Cancel price and quantity are used as the previous price and quantity
        for cancel reject messages
        '''
        self.cancel_prc = self.gw_prc
        self.cancel_qty = self.me_qty
    def passive_fill(self, leaves=np.nan, st='None'):
        '''
        Update ME quantity
        '''
        self.me_qty = leaves
        self.status = st
    def update_me(self, q=np.nan, st='None'):
        '''
        Update ME price and quantity
        '''
        self.me_prc = self.gw_prc
        self.me_qty = q
        self.status = st
    def no_me_response(self):
        self.status = 'No ME Response'
        