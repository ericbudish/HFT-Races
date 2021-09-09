'''
data_validation.py

This script checks whether the pre-processed data meets the requirements of 
the package. We strongly recommend users to validate the pre-processed 
data before applying the package.

Reference: 
Code and Data Appendix for “Quantifying the High-Frequency Trading
‘Arms Race’: A Simple New Methodology and Estimates” 
by Matteo Aquilina, Eric Budish and Peter O’Neill

Please follow the instructions in this file and 
Section 5.3 of the Code and Data Appendix.

'''
###################################################################################
### Load modules
from LatencyArbitrageAnalysis.utils.Validate_Data import ValidateData
###################################################################################
### Specify paths and symbol-date pairs to be checked
# Path to the pre-processed data files
path_data = '/path/to/pre-processed/RawData/'
# Symbol-dates to be checked
# testing_pairs - list of (date, sym) pairs to be checked
# Each item in the list is a python 2-tuple: e.g., 
# [('2000-01-01', 'ABCD'), ('2000-01-01', 'EFGH')].
testing_pairs = [('2000-01-01','ABCD')] 
###################################################################################
### Data Validation
failed = []
for date, sym in testing_pairs:
      test = ValidateData(date, sym, path_data)
      failed.append(test.validate())
if any(failed):
      print('Data failed to pass some validation tests. Please check the data requirements and be careful to proceed.')
else:
      print('Data validated.')