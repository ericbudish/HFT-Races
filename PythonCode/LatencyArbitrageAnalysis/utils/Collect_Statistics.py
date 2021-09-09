'''
Collect_Statistics.py

Description: 
The package processes each symbol-date in parallel. When the program produces output statistics, 
it first generates a stats file for each symbol-date. Then, the main Python scripts call the 
function CollectStats in this script to combine the symbol-date level files to an aggregate csv.gz file 
and output to the /Outout folder. 
'''
import os
import pandas as pd

def CollectStats(runtime, paths, pairs, collect):
    '''
    This function combines the stats files for each sym-date in the /Temp folder to an aggregate csv file
    and output to the /Output folder

    param: 
        runtime: str. Indicate the run time. 
        paths:   dict. A collection of paths. Please see the doc string and comment in the main Python scripts to see what it includes.
        pairs:   list. A list of two-tuples of strings: [(date, sym)]. 
        collect: str. 'Race_Stats' or 'Symdate_Stats' or 'Trade_Stats'. Specify which type of stats to collect.
    '''
    agg_filename = '%s/%s_%s.csv.gz' % (paths['path_output'], collect, runtime)
    if collect == 'Race_Stats':
        print('Compiling race stats')
        files = ['%s/RaceStats/%s/Race_Stats_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym) for date, sym in pairs]
        combined = pd.concat([pd.read_csv(f) for f in files if os.path.isfile(f)], sort=False)
        combined.to_csv(agg_filename, index=False, compression='gzip')
        missing = [f for f in files if not os.path.isfile(f)]
        print('Saved race stats to /Output folder')
    elif collect == 'SymDate_Stats':
        print('Compiling symdate stats')
        files = ['%s/SymDateStats/%s/SymDate_Stats_%s_%s.pkl' % (paths['path_temp'], date, date, sym) for date, sym in pairs]
        combined = pd.DataFrame([pd.read_pickle(f) for f in files if os.path.isfile(f)])
        combined.to_csv(agg_filename, index=False, compression='gzip')
        missing = [f for f in files if not os.path.isfile(f)]
        print('Saved symdate stats to /Output folder')
    elif collect == 'Trade_Stats':
        print('Compiling trade stats')
        files = ['%s/TradeStats/%s/Trade_Stats_%s_%s.csv.gz' % (paths['path_temp'], date, date, sym) for date, sym in pairs]
        combined = pd.concat([pd.read_csv(f) for f in files if os.path.isfile(f)], sort=False)
        combined.to_csv(agg_filename, index=False, compression='gzip')
        missing = [f for f in files if not os.path.isfile(f)]
        print('Saved trade stats to /Output folder')
    
    # Print symbol-dates without the stats file.
    # A symbol-date can have no race stats or trade stats because it has no race/no trade
    # But it should always have a symdate stats file for each symdate
    if missing:
        print('Number of Symbol-Dates without %s: %s' % (collect, len(missing)))
        
