'''
log_monitor.py

Script to monitor the progress of the program run.
The log monitor uses the log files to monitor the 
progress of the program run. Users can use this script 
to see how many sym-dates have been processed while 
a run is still in progress, or to confirm that all
symbol-dates have been processed at the end of a run. 

Please do not remove the log files before a run is finished
because we use the log files to monitor the progress.

Reference: 
    Sections 6.3 and 7.4 of the Code and Data Appendix.

Instructions:
    1. Specify runtime. runtime is a string that 
        identifies the program run. This will be
        printed out when executing the program.
    2. Set file_symdates = 'path/to/All_SymDates.csv.gz', 
        the path to the reference data file with a list 
        of all symbol-dates. This should be the same file
        used in the main Python scripts.
    3. Set path_logs, the path to log files. 
        This should be the same as the path_logs in 
        the main Python scripts.
    4. Run this file interactively or in a console.

'''
###################################################################################
import pandas as pd

from LatencyArbitrageAnalysis.utils.Monitor_Logs import MonitorLogs

###################################################################################
###### Set parameters 
runtime = 'YOUR_RUNTIME'
path_reference_data = '/path/to/ReferenceData/'
file_symdates = '/path/to/All_SymDates.csv.gz'
path_logs = '/path/to/Logs/'

###################################################################################
###### Monitor log files
## Config input
paths = {'path_logs': path_logs}
pairs = pd.read_csv(file_symdates, dtype={'Date':'O','Symbol':'O'})[['Date','Symbol']].dropna().to_records(index=False).tolist()
## Monitor Logs
# A log summary file will be automatically saved to paths['path_logs']+f'/LogSummary/LogSummary_{runtime}.csv'
logs = MonitorLogs(runtime, pairs, paths)
