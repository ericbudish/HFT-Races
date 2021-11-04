########### Sensitivity.R
#' 
#' Scripts that generates the sensitivity tables in Section 5.1 and Appendix C
#' of Aquilina, Budish, and ONeill. Please refer to Section 9 of the Code and 
#' Data Appendix for instructions.
#' 
#' Reference: Section 9 of the Code and Data Appendix.
#' 
#' Dependency: R (>=4.0.0)
#' 
#' Required packages: The following required packages will be installed 
#' automatically when running the R code:
#'    'data.table','R.utils','stargazer',
#'    'Hmisc','ggridges','ggplot2','scales','dplyr','reshape2'
#' 
#' To generate a sensitivity table,
#' Step 1. Run the Python code following Sections 4-7 of the Code and Data 
#'         Appendix for all race runs of interest. 
#' Step 2. Obtain the data related to each race run or race filter in the 
#'         sensitivity table:
#'         
#'         For each race run, specify the input and output file paths in the 
#'         "SET TECHNICAL PARAMETERS" section below and execute the code. The 
#'         input files should be the race-level statistics for the race run and
#'         the symbol-date level statistics. This produces the column in the 
#'         sensitivity table corresponding to the race run.
#'         
#'         For each race filter, specify the input and output file paths in the 
#'         "SET TECHNICAL PARAMETERS" section below, implement the race filter
#'         in the "ADD RACE FILTER(S)" section below, and execute the code. 
#'         The input files should be the race-level statistics for the baseline 
#'         race run and the symbol-date level statistics. 
#'         This produces the column in the sensitivity table 
#'         corresponding to the race run. For race filters, please refer to 
#'         Section 9 and Table 6 of the Code and Data Appendix.
#'         
#' Step 3. Combine the data of the race runs and filters of those runs into a single 
#'         sensitivity table. This can be accomplished by copy-and-paste.

########################################################################
############################ PREPARATION ###############################
########################################################################
rm(list=ls())
################################
### SET TECHNICAL PARAMETERS ###
################################
##### SET DIRECTORIES
### R Code Input directory. This should be the output directory for Python code.
data.dir = '/path/to/Python/Output/'
### Path to the RCode folder
function.dir = '/path/to/RCode/'
### R Code Output directory, i.e., directory for output Tables and Figures
out.dir = '/path/to/Tables_and_Figures/'
##### SET FILE PATHS
### Path to All_SymDates.csv.gz
# This MUST be the same file for message data processing and race detection
file.all.symdates = '/path/to/All_SymDates.csv.gz'
### input file paths
# This should be the race level dataset for the race run and symbol-date level dataset.
infile.symdate.stats = '/path/to/SymDate_Stats_YOUR_RUNTIME.csv.gz'
infile.race.stats =  '/path/to/Race_Stats_YOUR_RUNTIME.csv.gz'
### input name for the race run/filter. This will be the column name for this race run/filter.
name_col = 'NAME_OF_YOUR_RUN_OR_FILTER'

########################
### DATA PREPARATION ###
########################
### Load functions
source(paste(function.dir, "/Functions.R", sep=''))
### Col names
mark.to.market.Ts = c('1ms', '10ms', '100ms','1s', '10s', '30s', '60s', '100s')
race.msgs.Ts = c('50us','100us','200us','500us','1000us','2000us','3000us')
pre.race.T = c('10us','50us','100us', '500us', '1ms')
race.stats.cols = c(
  # Identifiers and Basic Info
  'Symbol', 'Date', 'SingleLvlRaceID', 'Side', 'TickSize', 'Race_Horizon', 'RacePrice',
  # Order Book Info
  'MidPt', sprintf('MidPt_f_%s',mark.to.market.Ts),
  # Race Profits
  sprintf('Race_Profits_DispDepth_%s',mark.to.market.Ts),
  sprintf('Race_Profits_ActiveQty_%s',mark.to.market.Ts),
  sprintf('Race_Profits_PerShare_%s',mark.to.market.Ts),
  sprintf('Race_Profits_PerShare_Tx_%s',mark.to.market.Ts), 
  sprintf('Race_Profits_PerShare_bps_%s',mark.to.market.Ts), 
  sprintf('LossAvoidance_%s',mark.to.market.Ts),
  # Race Timing
  'Time_S1_F1','Time_S1_F1_Max_0',
  # Race Volume
  'Depth_Disp', 'Qty_Traded', 'Qty_Cancelled', 'Qty_Active', 
  'Qty_Remaining_Disp_Pct', 'Value_Traded','Value_Cancelled',
  'Num_Trades',
  # Race Participation within T and Race Msg Counts
  sprintf('M_Within_%s',race.msgs.Ts),
  sprintf('M_Canc_Within_%s',race.msgs.Ts),
  sprintf('M_Take_Within_%s',race.msgs.Ts), 
  sprintf('M_Take_IOC_Within_%s',race.msgs.Ts),
  sprintf('M_Take_Lim_Within_%s',race.msgs.Ts),
  sprintf('N_Within_%s',race.msgs.Ts),
  sprintf('F_Within_%s',race.msgs.Ts),
  'M_All', 'M_Take', 'M_Canc','N_All','F_All',
  'M_Take_IOC','M_Take_Lim','M_Fail_Take_at_P','M_Fail_Take_IOC',
  'S1_FirmID','F1_FirmID','S1_Type','F1_Type',
  # Spread decomposition
  'Eff_Spread_Paid_Race', 'Eff_Spread_PerShare_Race', 
  sprintf('PriceImpact_Paid_Race_%s', mark.to.market.Ts),
  sprintf('PriceImpact_PerShare_Race_%s', mark.to.market.Ts),
  # Stable race quotes
  sprintf('Stable_Prc_RaceBBO_since_%s_PreRace', pre.race.T),
  sprintf('RaceBBO_Improved_since_%s_PreRace', pre.race.T))

symdate.stats.cols = c(
  # Identifiers
  'Symbol', 'Date', 
  # Volume
  'Vol_Sh', 'Vol',
  # Spread and PI 
  'Avg_Half_Spr_Time_Weighted_Tx','Avg_Half_Spr_Time_Weighted_bps',
  'Avg_Half_Spr_Qty_Weighted_Tx', 'Avg_Half_Spr_Qty_Weighted_bps',
  'Eff_Spread_bps',
  sprintf('PriceImpact_bps_%s', mark.to.market.Ts), 
  # N Messages 
  'N_Msgs', 'N_Tr','N_Msgs_Inbound_NBBO')

### Read datasets
race.stats = read.data(infile.race.stats, race.stats.cols)
symdate.stats = read.data(infile.symdate.stats, symdate.stats.cols)

##########################
### ADD RACE FILTER(S) ###
##########################

### Add race filters here, if any
# Please refer to Section 9 and Table 6 of the Code and Data Appendix for detail.
# race.stats = race.stats[, filter(s) here]

######################################################################
### PRODUCE DATA FOR THE RACE RUN/FILTER IN THE SENSITIVITY TABLES ###
######################################################################

### Calculate the sensitivity table column for the race run/filter
all.symdate = read.data(file.all.symdates)
symdate.stats = merge(all.symdate, symdate.stats, by = c('Symbol','Date'), all.x = TRUE)
col = get.sensitivity.summary.column(race.stats, symdate.stats,  name_col)
write.csv(col, file = paste(out.dir, "/SensitivityStats_", name_col, '.csv', sep = ''))

