'''
Monitor_Logs.py

Description:
This module defines a function MonitorLogs() which tracks the progress of the code 
by reading the log files, and finds out sym-dates that have and have not been 
processed. This function is called in the main Python scripts and in
/PythonCode/log_monitor.py to monitor the progress.

Users should expect to see sym-dates that have not been processed in the
middle of a program run. When a program run is finished, it is also possible that 
the program does not successfully process some sym-dates due to errors (system error, 
keyboard interruption, etc.). In this case, users should figure out the reason and 
restart the run.

Output of MonitorLogs():
The MonitorLogs() function returns a dataframe of the status for each sym-date 
(i.e., each row is a sym-date) for each script.
    1. If a script has not been executed for a sym-date, the status colume for 
        that script and that sym-date is "Does not exist log file",
    2. If a script is finished for a sym-date, the status colume is marked as "Done". 
    3. If a script is being executed, the current step being executed is populated
        in the status column. 
    4. If the script is interrupted due to an error, the last step before the error 
        is populated. 
The dataframe recording the status of each sym-date is saved to Path/to/Logs/LogSummary/.

To debug:
If user encounters any error when running the main Python scripts (e.g., a script runs 
through for some sym-dates but not for all sym-dates), she can load the output of 
MonitorLogs() to investigate which sym-dates are completed and which are not. 
The Traceback message of the error is in the log file of the main script. 
User can open the log file directly to see the error message.
'''
from collections import OrderedDict
import pandas as pd
import os
import datetime

def MonitorLogs(runtime, pairs, paths):
    path_logs = paths['path_logs']
    all_files = os.listdir(path_logs) 
    whichScript = ''
    for i in all_files:
        if i[-15:]==runtime:
            whichScript = i[:-16]
            break
    if whichScript == '':
        print('Did not find log files for the specified runtime.')
        return 
    log_prefix = '%s_' % whichScript
    logpath = '%s/%s/' %(path_logs, log_prefix+runtime)
    # Generate a dictionary that flags each log file for each symbol-date as either
    # Not existing, done, in progress, or errored out and the time elapsed if done.
    output = {}
    for pair in pairs:
        # print(pair)
        date, sym = pair[0], pair[1]
        out = OrderedDict()
        out['Date'], out['Symbol'] = date, sym
        
        # If the file does not exist, tag it as such in the dataframe, otherwise
        # read the file. If the last line starts with 'Time Elapsed,' then the file 
        # is done, or it has errored out. If it is done, store the time elapsed and flag
        # it as such. If it is not done, print the end of the last line of the file to et
        # a status update.
        # We use the last line of the log to determine whether this is finished and 
        # what is the time elapsed. If not finished, we copy the last line to see the error msg.
        file_00 = 'Process_Msg_Data_Main_Log_%s_%s_%s.log' % (runtime, date, sym)
        if not os.path.isfile(logpath + file_00):
            out['Encounter_Error'] = 'Does not exist log file'
        else:
            with open(logpath + file_00, 'r') as f:
                text = f.read()
            if len(text) > 0:
                if text.find('ERROR')  == -1: # search for keyword 'ERROR'. =-1 if not find 
                    out['Encounter_Error'] = 'Did not encounter error'
                else:
                    out['Encounter_Error'] = 'Encounter error'

        # Note: The following 4 blocks are essentially the same. 
        # Each monitors the log file for one step in the pipeline,
        # and reports the status and CPU-time spent. 

        # Step 1 Classify Messages
        file_01 = 'Step_1_Classify_Messages_%s_%s_%s.log' % (runtime, date, sym)
        if not os.path.isfile(logpath + file_01):
            out['Status_Classify_Messages'] = 'Does not exist log file'
            out['Time_Classify_Messages'] = None
        else:
            with open(logpath + file_01, 'r') as f:
                text = f.readlines()
            if len(text) > 0:
                text = [x.strip() for x in text]
                last = text[-1][33:] 
                if last[:12] == 'Time Elapsed':
                    out['Status_Classify_Messages'] = 'Done'
                    out['Time_Classify_Messages'] = pd.to_timedelta(last[14:], errors = 'ignore')
                else:
                    out['Status_Classify_Messages'] = last
                    out['Time_Classify_Messages'] = None
        
        # Step 2 Prepare Order Book
        file_02 = 'Step_2_Prep_Order_Book_%s_%s_%s.log' % (runtime, date, sym)
        if not os.path.isfile(logpath + file_02):
            out['Status_Prepare_Order_Book'] = 'Does not exist log file'
            out['Time_Prepare_Order_Book'] = None
        else:
            with open(logpath + file_02, 'r') as f:
                text = f.readlines()
            if len(text) > 0:
                text = [x.strip() for x in text]
                last = text[-1][33:]
                if last[:12] == 'Time Elapsed':
                    out['Status_Prepare_Order_Book'] = 'Done'
                    out['Time_Prepare_Order_Book'] = pd.to_timedelta(last[14:], errors = 'ignore')
                else:
                    out['Status_Prepare_Order_Book'] = last
                    out['Time_Prepare_Order_Book'] = None
        
        # Step 3 Trading and Order Book Stats
        file_03 = 'Step_3_Trading_and_Order_Book_Stats_%s_%s_%s.log' % (runtime, date, sym)
        if not os.path.isfile(logpath + file_03):
            out['Status_Trading_and_Order_Book_Stats'] = 'Does not exist log file'
            out['Time_Trading_and_Order_Book_Stats'] = None
        else:
            with open(logpath + file_03, 'r') as f:
                text = f.readlines()
            if len(text) > 0:
                text = [x.strip() for x in text]
                last = text[-1][33:]
                if last[:12] == 'Time Elapsed':
                    out['Status_Trading_and_Order_Book_Stats'] = 'Done'
                    out['Time_Trading_and_Order_Book_Stats'] = pd.to_timedelta(last[14:], errors = 'ignore')
                else:
                    out['Status_Trading_and_Order_Book_Stats'] = last
                    out['Time_Trading_and_Order_Book_Stats'] = None

        # Step 4 Race detection and statistics
        file_04 = 'Step_4_Detect_Races_%s_%s_%s.log' % (runtime, date, sym)
        if not os.path.isfile(logpath + file_04):
            out['Status_Race_Detection_Statistics'] = 'Does not exist log file'
            out['Time_Race_Detection_Statistics'] = None
        else:
            with open(logpath + file_04, 'r') as f:
                text = f.readlines()
            if len(text) > 0:
                text = [x.strip() for x in text]
                last = text[-1][33:]
                if last[:12] == 'Time Elapsed':
                    out['Status_Race_Detection_Statistics'] = 'Done'
                    out['Time_Race_Detection_Statistics'] = pd.to_timedelta(last[14:], errors = 'ignore')
                else:
                    out['Status_Race_Detection_Statistics'] = last
                    out['Time_Race_Detection_Statistics'] = None
        
        output[pair] = pd.Series(out)
    
    # Output to a dataframe.
    if not os.path.exists(paths['path_logs']+'/LogSummary'):
        os.makedirs(paths['path_logs']+'/LogSummary')
    df = pd.DataFrame.from_dict(output, orient = 'index')
    df = df.sort_values(['Symbol', 'Date'])
    df = df.reset_index(drop = True)
    log_summary_file = '/LogSummary/LogSummary_%s.csv' % runtime
    df.to_csv(paths['path_logs']+log_summary_file)
    
    # calculate running time
    td_01 = df.loc[df['Status_Classify_Messages'] == 'Done', 'Time_Classify_Messages'].sum() if df.loc[df['Status_Classify_Messages'] == 'Done'].shape[0] > 0 else datetime.timedelta(0)
    td_02 = df.loc[df['Status_Prepare_Order_Book'] == 'Done', 'Time_Prepare_Order_Book'].sum() if df.loc[df['Status_Prepare_Order_Book'] == 'Done'].shape[0] > 0 else datetime.timedelta(0)
    td_03 = df.loc[df['Status_Trading_and_Order_Book_Stats'] == 'Done', 'Time_Trading_and_Order_Book_Stats'].sum() if df.loc[df['Status_Trading_and_Order_Book_Stats'] == 'Done'].shape[0] > 0 else datetime.timedelta(0)
    td_04 = df.loc[df['Status_Race_Detection_Statistics'] == 'Done', 'Time_Race_Detection_Statistics'].sum() if df.loc[df['Status_Race_Detection_Statistics'] == 'Done'].shape[0] > 0 else datetime.timedelta(0)
    
    # Print out the log info
    print('#################################')
    print('Monitor Progress')
    print('Runtime: %s' % runtime)
    print('Script: %s' % whichScript)
    print('Total number of sym-dates: %s' % len(pairs))
    print('Encounter error: %s' % str(df[df['Encounter_Error'] == 'Encounter error'].shape[0]))
    if whichScript == 'MessageDataProcessing':
        Num_finished = df[(df['Status_Classify_Messages'] == 'Done') & (df['Status_Prepare_Order_Book'] == 'Done') & (df['Status_Trading_and_Order_Book_Stats'] == 'Done')].shape[0]
    elif whichScript == 'RaceDetection':
        Num_finished = df[(df['Status_Race_Detection_Statistics'] == 'Done')].shape[0]    
    print('Number of finished sym-dates: %s' % str(Num_finished))
    if whichScript == 'MessageDataProcessing':
        print('Time Spent: ' + str(round((td_01.total_seconds() + td_02.total_seconds() + td_03.total_seconds()) /3600, 2)) + ' CPU hours')
    elif whichScript == 'RaceDetection':
        print('Time Spent: ' + str(round((td_04.total_seconds()) /3600, 2)) + ' CPU hours')
    return df
