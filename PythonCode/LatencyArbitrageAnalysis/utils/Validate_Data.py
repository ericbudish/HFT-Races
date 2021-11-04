'''
Validate_Data.py

Data validation methods. Details of the data requirement can be found
in Sections 3 of the Code and Data Appendix. Instructions on data validation
can be found in Section 5.3 of the Code and Data Appendix.

Please use /PythonCode/data_validation.py to validate the message data.

'''

import pandas as pd
import numpy as np


class ValidateData(object):
    
    def __init__(self, date, sym, path_data):
        infile_msgs = '%s/%s/Raw_Msg_Data_%s_%s.csv.gz' % (path_data, date, date, sym)
        dtypes_testing = {
              # Identifiers
              'Symbol':'O','Date':'O','SessionID':'O','UserID':'O', 'FirmID':'O', 
              'ClientOrderID':'O', 'MEOrderID':'O', 'UniqueOrderID':'O', 
              # Timestamp
              'MessageTimestamp':'O', 
              # Basic info
              'MessageType':'O', 'Side':'O','QuoteRelated':'O','RegularHour':'O',
              # Additional fields for certain inbounds and outbounds
              'OrderType':'O','TIF':'O', 
              'OrderQty':'float', 'DisplayQty':'float', 
              'LimitPrice':'float', 'StopPrice':'float',
              'BidPrice':'float', 'BidSize':'float', 
              'AskPrice':'float', 'AskSize':'float',
              'OrigClientOrderID':'O',
              'ExecType':'O', 
              'TradeMatchID': 'O', 'TradeInitiator':'O','OrderStatus':'O', 
              'ExecutedPrice': 'float',  'ExecutedQty': 'float', 'LeavesQty': 'float',
              'OpenAuctionTrade':'O', 'AuctionTrade':'O',
              'CancelRejectReason': 'O'}
              
        self.df = pd.read_csv(infile_msgs, 
                              dtype = dtypes_testing, 
                              parse_dates = ['MessageTimestamp'], 
                              skipinitialspace = True, 
                              compression = 'gzip')
        
        self.date = date
        self.sym = sym
        self.path_data = path_data
        

        self.cols_required = [
            'Symbol', 'Date', 'SessionID', 'UserID', 'FirmID', 
            'ClientOrderID', 'MEOrderID', 'UniqueOrderID', 
            'MessageTimestamp', 'MessageType', 'Side', 'QuoteRelated', 'RegularHour', 
            'OrderType', 'TIF', 'OrderQty', 'DisplayQty', 'LimitPrice', 'StopPrice', 
            'BidPrice', 'BidSize', 'AskPrice', 'AskSize', 'OrigClientOrderID',
            'ExecType', 'TradeMatchID', 'TradeInitiator', 'OrderStatus', 
            'ExecutedPrice', 'ExecutedQty', 'LeavesQty', 
            'OpenAuctionTrade', 'AuctionTrade', 'CancelRejectReason'
        ]

        self.no_missing_val_cols = [
            'Symbol', 'Date', 'UserID', 'FirmID', 
            'ClientOrderID', 'UniqueOrderID', 
            'MessageTimestamp', 'MessageType', 'QuoteRelated', 'RegularHour', 
        ]

        self.bool_cols = ['QuoteRelated','RegularHour']
        self.bool_cols_trades = ['OpenAuctionTrade', 'AuctionTrade']

        self.categorical_dict = {
            'MessageType':{'New_Order','New_Quote','Cancel_Request',
                           'Cancel_Replace_Request','Other_Inbound',
                           'Execution_Report','Cancel_Reject',
                           'Other_Reject','Other_Outbound'},
            'Side':{'Bid','Ask'},
            'OrderType':{'Limit','Market','Stop','Stop_Limit','Pegged','Passive_Only'},
            'ExecType':{'Order_Accepted','Order_Cancelled','Order_Executed',
                        'Order_Expired','Order_Rejected',
                        'Order_Replaced','Order_Suspended','Order_Restated'},
            'TIF':{'GoodTill','IOC','FOK','GFA'},
            'OrderStatus':{'Partial_Fill','Full_Fill'},
            'TradeInitiator':{'Aggressive','Passive','Other'},
            'CancelRejectReason':{'TLTC','Other'}
        }
        
        self.failed_test = False
        
    def test_required_cols(self):
        # Whether the data contains the required columns
        df = self.df
        cols_required = self.cols_required
        missing_columns = set(cols_required).difference(set(df.columns))
        if len(missing_columns) > 0:
            print('Missing Required Data Field(s) in Symbol-Date (%s, %s) Raw Message Data: missing fields %s' % (self.date, self.sym, missing_columns))
            print('')
            self.failed_test = True
            
    def test_msg_ordering(self):
        df = self.df
        # If there is any msg preceding the msg before it in its timestamp, 
        # then the msgs are not properly ordered.
        if 'MessageTimestamp' in df.columns:
            if df['MessageTimestamp'].isna().sum() == 0:
                if (df.MessageTimestamp - df.MessageTimestamp.shift(1) < np.timedelta64(0,'s')).sum() > 0:
                    print('Messages not ordered by MessageTimestamp.')
                    print('')
                    self.failed_test = True
    
    def test_missing_values(self):
        df = self.df
        no_missing_val_cols = self.no_missing_val_cols
        for col in no_missing_val_cols:
            if col in df.columns:
                if df[col].isna().sum() > 0:
                    print('Field %s contains missing values. Missing values are not expected in this field. However, if the amount of missing values is small, the package can still be applicable but the results can be less accurate.' % col)
                    print('')
                    self.failed_test = True

    def test_categorical_cols(self):
        df = self.df
        categorical_dict = self.categorical_dict
        for col in categorical_dict:
            if col in df.columns:
                all_values = categorical_dict[col]
                unexpected_values = set(df[col].dropna().unique()).difference(all_values)
                if unexpected_values:
                    print('%s contains unexpected values: %s.' % (col, unexpected_values))
                    print('')
                    self.failed_test = True

    def test_bool_cols(self):
        df = self.df
        bool_cols = self.bool_cols
        for col in bool_cols:
            if col in df.columns:
                if df[col].isna().sum() == 0:
                    all_values = {'True','False'}
                    unexpected_values = set(df[col].dropna().unique()).difference(all_values)
                    if unexpected_values:
                        print('%s contains unexpected values: %s.' % (col, unexpected_values))
                        print('')
                        self.failed_test = True
        bool_cols_trades = self.bool_cols_trades
        if ('MessageType' in df.columns) and ('ExecType' in df.columns):
            df_trades = df[(df['MessageType']=='Execution_Report') & (df['ExecType']=='Order_Executed')]
            for col in bool_cols_trades:
                if col in df.columns:
                    all_values = {'True','False','1.0','0.0','1','0'}
                    unexpected_values = set(df_trades[col].dropna().unique()).difference(all_values)
                    if unexpected_values:
                        print('%s contains unexpected values: %s.' % (col, unexpected_values))
                        print('')
                        self.failed_test = True
        
    def validate(self):
        print('##################################################')
        print('Testing pre-processed message data %s %s ...' % (self.date, self.sym))
        print('')
        self.test_required_cols()
        self.test_missing_values()
        self.test_msg_ordering()
        self.test_categorical_cols()
        self.test_bool_cols()
        if self.failed_test:
            print('The data fails to meet some requirements. Please check the data requirements and be careful to proceed.')
            print('##################################################')
        else:
            print('The data meets all the requirements.')
            print('##################################################')
        return self.failed_test
