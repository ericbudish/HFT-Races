########### MainResults.R
#' 
#' This script generates the table and figures reported in Section 4 main results,
#' Appendix B and Appendix D of Aquilina, Budish, and ONeill. Please refer to 
#' Section 9 of the Code and Data Appendix for instructions.
#' 
#' Reference: Section 9 of the Code and Data Appendix.
#' 
#' Dependency: R (>=4.0.0)
#' 
#' Required packages: Required packages are listed in the 
#' "LOAD PACKAGES" section below. Please use the following command to 
#' install all the required packages:
#' 
#'    install.packages(c('data.table','R.utils','stargazer',
#'    'Hmisc','ggridges','ggplot2','scales','dplyr','reshape2'))
#' 
#' To use this script to reproduce the results, 
#' Step 1. Run the Python code following Sections 4-7 of the Code and Data Appendix.
#' Step 2. Set the technical parameters in the "SET TECHNICAL PARAMETERS" section.
#'         This includes paths to input files, total trading time of the exchange,
#'         and paths to output files.
#' Step 2'. If you want to use race filters, implement the race filter(s) in the
#'         "ADD RACE FILTER(S)" section below. Please refer to Section 9 and 
#'         Table 6 of the Code and Data Appendix for detail.
#' Step 3. Execute the code.

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
### Input constant
# Specify the total trading hours in a trading day in hours
time_total_hr = 8.5

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

########################
### DATA AGGREGATION ###
########################
### Get the data.table for all symbol dates 
all.symdate = read.data(file.all.symdates)
# Merge with symdate.stats. This brings in sym-dates with no trades, if any.
symdate.stats = merge(all.symdate, symdate.stats, by = c('Symbol','Date'), all.x = TRUE)
### Aggregation by symbol-date, and then by symbol and by date
# The race.stats data is first aggregated at the symbol-date level,
# and based on that, it is then aggregated at the symbol and the date levels
### Aggregate by symbol-date
symdate = generate.symdate(race.stats, symdate.stats, '10s')
### Aggregate by date 
date = generate.symdate.groupby(symdate, c('Date'))
### Aggregate by symbol
sym = generate.symdate.groupby(symdate, c('Symbol'))

########################################################################
######################### SECTION 4 TABLES #############################
########################################################################

####################
### RACES PER DAY###
####################

### Table 4.1 Panel A Date Level
vars = c('N_Races')
desc = c('Races per day')
cols.precision = rep.int(0,11)
outfile.name = 'Table_RacesPerDay_Date'
write.vars.summary.table(date, vars, desc, cols.precision, outfile.name, out.dir)

### Table 4.1 Panel B Symbol Level
vars = c('Avg_N_Races')
desc = c('Races per symbol')
cols.precision = c(rep.int(2,2), rep.int(0,9))
outfile.name = 'Table_RacesPerDay_Sym'
write.vars.summary.table(sym,  vars, desc, cols.precision, outfile.name, out.dir)

### Appendix Table B.1 Symbol-Date Level
vars = c('N_Races')
desc = c('Races per symbol-date')
cols.precision = c(rep.int(2,2), rep.int(0,9))
outfile.name = 'Table_RacesPerDay_SymDate'
write.vars.summary.table(symdate, vars, desc, cols.precision, outfile.name, out.dir)

#####################
### RACE DURATION ###
#####################

### Figure 4.1 Race Duration Histogram 
## Subsetting data, drop outliers
# The script only plots the race duration within (min.duration, max.duration) 
# in microseconds. In the paper, we plotted race duration within (-100, 500)
# microseconds. Users may want to choose a different window.
min.duration = -100 # Put the lower bound for the histogram here
max.duration = 500 # Put the upper bound for the histogram here
stats.race.duration = race.stats[Time_S1_F1 >= min.duration & Time_S1_F1 <= max.duration, .(Time_S1_F1)]
## Select file name
outfile.name = 'Figure_RaceDuration_Hist'
## Plotting
p = ggplot(data = stats.race.duration, aes(x = Time_S1_F1)) +
      geom_histogram(binwidth = 5, center = 2.5) + 
      scale_x_continuous(breaks = c(-100, 0, 100, 200, 300, 400, 500)) +
      scale_y_continuous(labels = comma) + 
      labs(x = 'Time from Success 1 to Fail 1 (microseconds)', y = 'Count') + 
      theme_gray(base_size = 12.5)
ggsave(file = file.path(out.dir, paste(outfile.name, '.pdf', sep = '')), width = 5, height = 4)
rm(stats.race.duration)

### Appendix Table B.2 Distribution of Time from S1 to F1
vars = c('Time_S1_F1')
desc = c('Race duration')
cols.precision = rep.int(2,11)
outfile.name = 'Table_RaceDuration'
write.vars.summary.table(race.stats, vars, desc, cols.precision, outfile.name, out.dir)

### Appendix Table B.3 Total Time in Races, i.e. Total of max{Time_S1_F1,0}
vars = c('Time_S1_F1_Max_0_s')
desc = c('Total time in races')
cols.precision = rep.int(3,11)
outfile.name = 'Table_TotalTimeInRaces_SymDate'
write.vars.summary.table(symdate, vars, desc, cols.precision, outfile.name, out.dir)

##################################
### TRADES AND VOLUME IN RACES ###
##################################

### Table 4.2 Panel A and Appendix Table B.4 Panel A Volume in Races
vars = c('Pct_Value_Traded_Race')
desc = c('% Qty Traded in Races')
cols.precision = rep.int(2,11)
# Table 4.2 Panel A
outfile.name = 'Table_PctQtyTradedInRaces_Date'
write.vars.summary.table(date, vars, desc, cols.precision, outfile.name, out.dir)
# Appendix Table B.4 Panel A
outfile.name = 'Table_PctQtyTradedInRaces_Sym'
write.vars.summary.table(sym,  vars, desc, cols.precision, outfile.name, out.dir)

### Table 4.2 Panel B and Appendix Table B.4 Panel B Percentage Num Trades in Races
vars = c('Pct_Num_Race_Trades')
desc = c('% Trades in Races')
cols.precision = rep.int(2,11)
# Table 4.2 Panel B
outfile.name = 'Table_PctTradesInRaces_Date'
write.vars.summary.table(date, vars, desc, cols.precision, outfile.name, out.dir)
# Appendix Table B.4 Panel B
outfile.name = 'Table_PctTradesInRaces_Sym'
write.vars.summary.table(sym,  vars, desc, cols.precision, outfile.name, out.dir)

##############################
### NUMBER OF PARTICIPANTS ###
##############################

### Number of users, firms, msgs, cancels and takes in races table
wTs = c('50us', '100us', '200us', '500us', '1000us')
cols.precision = c(rep.int(2,2), rep.int(0,9))

### Table 4.3 Panel A
vars = sprintf('N_Within_%s', wTs)
desc = sprintf('Participants within %s', wTs)
outfile.name = 'Table_NumberOfParticipants'
write.vars.summary.table(race.stats, vars, desc, cols.precision, outfile.name, out.dir)

### Table 4.3 Panel B
vars = sprintf('M_Take_Within_%s', wTs)
desc = sprintf('Takes within %s', wTs)
outfile.name = 'Table_NumberOfTakes'
write.vars.summary.table(race.stats, vars, desc, cols.precision, outfile.name, out.dir)

### Table 4.3 Panel C
vars = sprintf('M_Canc_Within_%s', wTs)
desc = sprintf('Cancels within %s', wTs)
outfile.name = 'Table_NumberOfCancels'
write.vars.summary.table(race.stats, vars, desc, cols.precision, outfile.name, out.dir)

### Appendix Table B.5 Panel A
vars = sprintf('M_Take_IOC_Within_%s', wTs)
desc = sprintf('IOC takes within %s', wTs)
outfile.name = 'Table_NumberOfTakesIOC'
write.vars.summary.table(race.stats, vars, desc, cols.precision, outfile.name, out.dir)

### Appendix Table B.5 Panel B
vars = sprintf('M_Take_Lim_Within_%s', wTs)
desc = sprintf('Limit takes within %s', wTs)
outfile.name = 'Table_NumberOfTakesLim'
write.vars.summary.table(race.stats, vars, desc, cols.precision, outfile.name, out.dir)

### Appendix Table B.5 Panel C
vars = sprintf('M_Within_%s', wTs)
desc = sprintf('Messages within %s', wTs)
outfile.name = 'Table_NumberOfMessages'
write.vars.summary.table(race.stats, vars, desc, cols.precision, outfile.name, out.dir)

### Appendix Table B.5 Panel D
vars = sprintf('F_Within_%s', wTs)
desc = sprintf('Firms within %s', wTs)
outfile.name = 'Table_NumberOfFirms'
write.vars.summary.table(race.stats, vars, desc, cols.precision, outfile.name, out.dir)

#####################################
### PATTERN OF WINNERS AND LOSERS ###
#####################################

### Figure 4.2 and Appendix Figure B.1 
# Note that the figures are specific to the LSE firms. Instead of reproducing 
# the figures, this script produces two .csv files:
#   1. A file with FirmID, # races won (sending S1), and % races won
#   2. A file with FirmID, # races lost (sending F1), and % races lost
# If the raw data does not have firm identifiers, users should set FirmID=UserID
# and the csv files report the # and % races won and lost for each user. 
n_races_total = nrow(race.stats)
## Winners
outfile.name = 'Table_RacesWonByFirm'
winners = race.stats[, .(.N, .N/n_races_total*to_pct), by=S1_FirmID]
names(winners) = c('S1_FirmID','N_Races','Pct_Races')
setorder(winners, -N_Races)
write.csv(winners, file=file.path(out.dir, paste(outfile.name, '.csv', sep = '')))
## Losers
outfile.name = 'Table_RacesLostByFirm'
losers = race.stats[, .(.N, .N/n_races_total*to_pct), by=F1_FirmID]
names(losers)  = c('F1_FirmID','N_Races','Pct_Races')
setorder(losers, -N_Races)
write.csv(losers, file=file.path(out.dir, paste(outfile.name, '.csv', sep = '')))

############################################
### PATTERN OF TAKE CANCEL AND PROVISION ###
############################################

### Figure 4.3 Panel A Races won by takes vs. cancels
outfile.name = 'Figure_Takes_Cancels_Overall'
n_races_total = nrow(race.stats)
won.by = race.stats[, .N, by=S1_Type]
won.by[, Pct_Won_by := N/n_races_total]
text.size = 12
p = ggplot(data=won.by, aes(x=S1_Type, y=Pct_Won_by, fill=S1_Type)) +
    geom_bar(stat="identity", position=position_dodge(), width = 0.125, show.legend = FALSE) + 
    theme(legend.position=c(0.5, 0.95),
          legend.background=element_blank(), 
          legend.key.size = unit(0.4, "cm"), 
          legend.text = element_text(size = text.size),
          axis.text.x = element_text(size = text.size), 
          axis.text.y = element_text(size = text.size), 
          axis.title.x = element_text(size = text.size),
          axis.title.y = element_text(size = text.size)) + 
    scale_x_discrete(name = "Races Won by") +
    scale_y_continuous(name = '', labels = percent_format(), limits=c(0,1), breaks=seq(0,1,0.2)) + 
    scale_fill_manual(values = c('darkgoldenrod2','indianred2')) 
ggsave(file = file.path(out.dir, paste(outfile.name, '.pdf', sep = '')), width = 3, height = 4)

### Figure 4.3 Panel B Analysis by Firm Group
# This is specific to the LSE. 
# It is difficult to generalize it to other exchanges 
# because the firm groups of interest are different. 
# The script does not produce this figure.
# We provide the code we used to generate the data for this figure
# in /Examples/code_snippet_trade_level_data.py
# User of our code needs to adapt these code snippets to their 
# context depending on the specific details of their setting.

### Table 4.4 Taker Provider Matrix by Firm Group
# This is specific to the LSE. 
# It is difficult to generalize it to other exchanges 
# because the firm groups of interest are different. 
# The script does not produce this table.
# We provide the code we used to generate the data for this table
# in /Examples/code_snippet_trade_level_data.py
# User of our code needs to adapt the code snippets to their 
# context depending on the specific details of their setting.

##########################################
### EXPECTED NUMBER OF RACES BY CHANCE ###
##########################################

### Appendix Table B.6 and Table 4.5
# In the tables we report the expected number of potential race event by 
# chance under the average message arrival rate and the busiest 30 min. 
# The busiest 30 min sessions can be exchange specific. 
# This script only produces the Poisson analysis tables using the average
# message arrival rate.
# We provide the code we used to generate the data for the busiest 30 minutes 
# Poisson exercise in /Examples/code_snippet_poisson_busiest30min.py
# User of our code needs to adapt the code snippets to their 
# context depending on the specific details of their setting.

# potential.race.horizons: list of race horizon in us to report the expected  
# number of potential race events.
potential.race.horizons = c(50, 100, 200)
# time_total_hr:  users should set time_total_hr in the preparation section.
cols.precision = rep.int(2,11)
outfile.name = 'Table_PoissonAnalysis'
write.poisson.tables(symdate, potential.race.horizons, time_total_hr, cols.precision, outfile.name, out.dir)

####################
### RACE PROFITS ###
####################
### Table 4.6 and Appendix Table B.8 Race Profits at Mark to Market 10s
# Note that the tables in the paper report data for FTSE 100/250 and the 
# full sample separately. This grouping is specific to the LSE. 
# This script only produces a single table for the full sample.
mark.to.market = '10s'
vars  = c('Race_Profits_PerShare_Tx','Race_Profits_PerShare','Race_Profits_PerShare_bps',
          'Race_Profits_DispDepth','Race_Profits_ActiveQty')
vars = sprintf('%s_%s', vars, mark.to.market)
desc = c('Per-share profits (ticks)', 'Per-share profits','Per-share profits (basis points)',
         'Per-race profits displayed depth','Per-race profits qty trade/cancel')
cols.precision = rep.int(2,11)
outfile.name = sprintf('Table_ProfitsPerRace_%s', mark.to.market)
write.vars.summary.table(race.stats, vars, desc, cols.precision, outfile.name, out.dir)

### Table 4.7 and Appendix Table B.9 Mean Race Profits at different T
# Note that the tables in the paper report data for FTSE 100/250 and the 
# full sample separately. This grouping is specific to the LSE. 
# This script only produces a single table for the full sample.
mark.to.market.Ts = c('1ms', '10ms', '100ms', '1s', '10s', '30s', '60s', '100s')
vars  = c('Race_Profits_PerShare_Tx','Race_Profits_PerShare','Race_Profits_PerShare_bps',
          'Race_Profits_DispDepth','Race_Profits_ActiveQty')
desc = c('Mean per-share profits (ticks)', 'Mean per-share profits','Mean per-share profits (basis points)',
         'Mean per-race profits displayed depth','Mean per-race profits qty trade/cancel')
precision = 2
outfile.name = 'Table_MeanProfitsPerRaceDiffTimeHrzn'
write.diff.T.means.table(race.stats, mark.to.market.Ts, vars, desc, precision, outfile.name, out.dir)

#######################################################
### PER SHARE PROFITS AND PRICE IMPACT DISTRIBUTION ###
#######################################################

### Figure 4.4 Panel A Race Price Impact 
# Set Column names, get data
mark.to.market.Ts = c('1ms','10ms','100ms','1s','10s')
PI.per.share.cols = sprintf('PriceImpact_PerShare_Race_%s', mark.to.market.Ts)
PI.per.share.bps.cols = sprintf('PriceImpact_PerShare_Race_bps_%s', mark.to.market.Ts)
PI.per.share.bps.desc = sprintf('Price Impact at %s', mark.to.market.Ts)
race.stats = race.stats[, (PI.per.share.bps.cols) := lapply(.SD, function(x){to_bps * x/MidPt}), .SDcols=PI.per.share.cols]
stats_PI_bps = race.stats[, ..PI.per.share.bps.cols]
# Set xlim, filter data. Only plot PI within (-cap, cap)
# Set your desired cap value here.
cap = 20 
stats_PI_bps = stats_PI_bps %>% 
  setNames(PI.per.share.bps.desc) %>% 
  melt(measure.vars=PI.per.share.bps.desc) %>% 
  filter(value > -cap, value < cap, value != 0)
# Plot
outfile.name = 'Figure_RacePriceImpact_Distribution'
p = stats_PI_bps %>% ggplot(aes(x=value, y=variable, fill=variable)) + geom_density_ridges(alpha=0.4) +
  xlab('Per-Share Price Impact in Basis Points') +
  ylab('Kernel Density') +
  guides(fill=FALSE, color=FALSE)+
  theme_minimal()
ggsave(file = file.path(out.dir, paste(outfile.name, '.pdf', sep = '')), width = 5, height = 4)

### Figure 4.4 Panel B Race Profits
# Set Column names, get data
mark.to.market.Ts = c('1ms','10ms','100ms','1s','10s')
share.profits.bps.cols = sprintf('Race_Profits_PerShare_bps_%s', mark.to.market.Ts)
share.profits.cols.displayed = sprintf('Profits at %s', mark.to.market.Ts)
stats_share_profits_bps = race.stats[, ..share.profits.bps.cols]
# Set your desired cap value here.
cap = 10 
stats_share_profits_bps = stats_share_profits_bps %>% 
  setNames(share.profits.cols.displayed) %>% 
  melt(measure.vars=share.profits.cols.displayed) %>% 
  filter(value > -cap, value < cap, value != 0)
# Plot
outfile.name = 'Figure_RaceProfits_Distribution'
p = stats_share_profits_bps %>% ggplot(aes(x=value, y=variable, fill=variable)) + geom_density_ridges(alpha=0.4) +
  xlab('Per-Share Profits in Basis Points') +
  ylab('Kernel Density') +
  guides(fill=FALSE, color=FALSE)+
  theme_minimal()
ggsave(file = file.path(out.dir, paste(outfile.name, '.pdf', sep = '')), width = 5, height = 4)

#####################
### DAILY PROFITS ###
#####################

### Table 4.8 Daily race profits
# Table 4.8 Panel A average by symbol
vars = ("Avg_Race_Profits_DispDepth")
desc = ("Average daily profits by symbol")
cols.precision = rep.int(1,11)
outfile.name = 'Table_AvgDailyProfits_Sym'
write.vars.summary.table(sym,  vars, desc, cols.precision, outfile.name, out.dir)
# Table 4.8 Panel B sum by date
vars = ("Race_Profits_DispDepth")
desc = ("Daily profits total")
cols.precision = rep.int(0,11)
outfile.name = 'Table_DailyProfits_Date'
write.vars.summary.table(date, vars, desc, cols.precision, outfile.name, out.dir)

#############################
### LATENCY ARBITRAGE TAX ###
#############################

### Table 4.9 Latency Arbitrage Tax, 2 measures
# Panel A(i) all volume by symbol
vars = ("Avg_Race_Profits_DispDepth_bps")
desc = c("Latency arbitrage tax, all volume by symbol")
cols.precision = rep.int(3,11)
outfile.name = 'Table_LATax_AllVolume_Sym'
write.vars.summary.table(sym,  vars, desc, cols.precision, outfile.name, out.dir)

# Panel A(ii) non-race volume by symbol
vars = ("Avg_Race_Profits_DispDepth_NR_bps")
desc = c("Latency arbitrage tax, non-race volume by symbol")
cols.precision = rep.int(3,11)
outfile.name = 'Table_LATax_NonRaceVolume_Sym'
write.vars.summary.table(sym,  vars, desc, cols.precision, outfile.name, out.dir)

# Panel B(i) all volume by date
vars = ("Race_Profits_DispDepth_bps")
desc = c("Latency arbitrage tax, all volume by date")
cols.precision = rep.int(3,11)
outfile.name = 'Table_LATax_AllVolume_Date'
write.vars.summary.table(date, vars, desc, cols.precision, outfile.name, out.dir)

# Panel B(ii) non-race volume by date
vars = ("Race_Profits_DispDepth_NR_bps")
desc = c("Latency arbitrage tax, non-race volume by date")
cols.precision = rep.int(3,11)
outfile.name = 'Table_LATax_NonRaceVolume_Date'
write.vars.summary.table(date, vars, desc, cols.precision, outfile.name, out.dir)

######################################
### LATENCY ARBITRAGE SCATTERPLOTS ###
######################################

### Figure 4.5 and Appendix Figure B.2
# Figure 4.5 plots daily total race profits against daily total LSE regular-hour
# volume and FTSE 350 index 1-min volatility for the 43 days in our sample.
# Appendix Figure B.2 plots latency arbitrage tax rate against daily total
# LSE volume and FTSE 350 index 1-min volatility for the 43 days in our sample.
# The LSE regular-hour volume is based on our message data (see Table 4.2). 
# The realized 1-min volatility comes from the TRTH data, which is specific to
# FTSE 350. 
# This script only produces Figure 4.5 Panel A and Appendix Figure B.2 Panel A.
# For Figure 4.5 Panel A and Appendix Figure B.2 Panel A, we provide the code 
# below but users will need to obtain their own volatility data and merge it
# into symdate.stats.
# Users may need to change the units and the plotting range according to their results. 

## Figure 4.5 Panel A Profits on volume
outfile.name = 'Figure_LAProfitsVsVolume'
p = ggplot(data = date, aes(x = Vol/1e9, y = Race_Profits_DispDepth/1e3)) +
  geom_point() + labs(x = 'Total Regular-Hour Volume (billions)',
                      y = 'Total Daily Profits (thousands)') +
  ylim(0, 300) + theme_minimal() + theme(axis.line = element_line(colour = "black"))
ggsave(file.path(out.dir, paste(outfile.name, '.pdf', sep = '')), width = 5, height = 4)

## Figure 4.5 Panel B Profits on volatility
## Users need volatility data to run the following block
# outfile.name = 'Figure_LAProfitsVsVolatility'
# p = ggplot(data = date, aes(x = Volatility, y = Race_Profits_DispDepth/1e3)) +
#   geom_point() + labs(x = 'Volatility (%)',
#                       y = 'Total Daily Profits (thousands)') +
#   ylim(0, 300) + theme_minimal() + theme(axis.line = element_line(colour = "black"))
# ggsave(file.path(out.dir, paste(outfile.name, '.pdf', sep = '')), width = 5, height = 4)

## Appendix Figure B.2 Panel A Latency arbitrage tax on volume
outfile.name = 'Figure_LATaxVsVolume'
p = ggplot(data = date, aes(x = Vol/1e9, y = Race_Profits_DispDepth_bps)) +
  geom_point() + labs(x = 'Total Regular-Hour Volume (billions)',
                      y = 'LA Tax (basis points)') +
  ylim(0, 1) + theme_minimal() + theme(axis.line = element_line(colour = "black"))
ggsave(file.path(out.dir, paste(outfile.name, '.pdf', sep = '')), width = 5, height = 4)

## Appendix Figure B.2 Panel B Latency arbitrage tax on volatility
## Users need volatility data to run the following block
# outfile.name = 'Figure_LATaxVsVolatility'
# p = ggplot(data = date, aes(x = Volatility, y = Race_Profits_DispDepth_bps)) +
#   geom_point() + labs(x = 'Volatility (%)',
#                       y = 'LA Tax (basis points)') +
#   ylim(0, 1) + theme_minimal() + theme(axis.line = element_line(colour = "black"))
# ggsave(file.path(out.dir, paste(outfile.name, '.pdf', sep = '')), width = 5, height = 4)

############################
### SPREAD DECOMPOSITION ###
############################

### Table 4.10 and Appendix Table B.12
# Note that the tables in the paper report data for FTSE 100/250 and the 
# full sample separately. This grouping is specific to the LSE. 
# This script only produces a single table for the full sample. 
vars = c('Eff_Spread_bps', 'Eff_Spread_Races_bps', 'Eff_Spread_NotRaces_bps', 
         'PriceImpact_bps', 'PriceImpact_Races_bps', 'PriceImpact_NotRaces_bps',
         'LossAvoidance_bps', 'Realized_Spread_bps', 'Realized_Spread_Races_bps', 'Realized_Spread_NotRaces_bps',
         'PI_R_PI_Total', 'PI_R_ES')
desc = c('Effective spread paid - overall (bps)', 'Effective spread paid - in races (bps)', 'Effective spread paid - not in races (bps)',
         'Price impact - overall (bps)', 'Price impact - in races (bps)','Price impact - not in races (bps)',
         'Loss avoidance (bps)', 'Realized spread - overall (bps)', 'Realized spread - in races (bps)','Realized spread - not in races (bps)',
         'PI in races / PI total (%)', 'PI in races / Effective spread (%)')
cols.precision = rep.int(2,11)

# Table 4.10 and Appendix Table B.12 Panel A Spread decomposition by symbol
# Note that in the paper we dropped symbols with fewer than 100 races in total. 
# This drops no FTSE 100 symbol and about 25% FTSE 250 symbols.
# The FTSE 100/250 grouping is specific to LSE. This script only produces
# a single table for all symbols. 
outfile.name = 'Table_SpreadDecomposition_Sym'
write.vars.summary.table(sym,  vars, desc, cols.precision, outfile.name, out.dir)

# Appendix Table B.12 Panel B Spread decomposition by date
outfile.name = 'Table_SpreadDecomposition_Date'
write.vars.summary.table(date, vars, desc, cols.precision, outfile.name, out.dir)

###################################
### REALIZED SPREAD AND CANCELS ###
###################################

### Table 4.11 realized spread and cancels by firm group
# This is specific to the LSE. 
# It is difficult to generalize it to other exchanges 
# because the firm groups of interest are different. 
# The script does not produce this table.
# We provide the code we used to generate the data for this table
# in /Examples/code_snippet_cancel_activities.py and code_snippet_trade_level_data.py
# User of our code needs to adapt the code snippets to their 
# context depending on the specific details of their setting.

################################
### LIQUIDITY COST REDUCTION ###
################################

### Table 4.12 Reduction in Liquidity Cost = Race Profits / Non-Race Effective Spread 
vars = c('RaceProfits_DD_ES_NotR')
desc = c('Proportional Reduction in Liquidity Cost')
cols.precision = rep.int(2,11)

# Table 4.12 Panel A by symbol
# In the paper, we report FTSE 100/250 and the full sample separately. We only include 
# symbols that have at least 100 races summed over all dates. This drops about one-quarter
# of FTSE 250 symbols and does not drop any FTSE 100 symbols.
# This script only produces a single table for all symbols. 
outfile.name = 'Table_ReductionLiqCost_Sym'
write.vars.summary.table(sym, vars, desc, cols.precision, outfile.name, out.dir)

# Table 4.12 Panel B by date
outfile.name = 'Table_ReductionLiqCost_Date'
write.vars.summary.table(date, vars, desc, cols.precision, outfile.name, out.dir)

########################################################################
##################### OTHER APPENDIX B TABLES ##########################
########################################################################

####################################
### AVERAGE AND EFFECTIVE SPREAD ###
####################################

### Appendix Table B.10 and B.11 spread by date and by symbol
# Note that Eff_Spread is calculated as half spread
vars = c('Avg_Half_Spr_Time_Weighted_Tx','Avg_Half_Spr_Time_Weighted_bps',
         'Avg_Half_Spr_Qty_Weighted_Tx', 'Avg_Half_Spr_Qty_Weighted_bps')
desc = c('Time-weighted half-spread (ticks)','Time-weighted half-spread (bps)',
         'Quantity-weighted half-spread (ticks)','Quantity-weighted half-spread (bps)')
cols.precision = rep.int(2,11)

# Table B.10 spread by date
outfile.name = 'Table_Spread_Date'
write.vars.summary.table(date, vars, desc, cols.precision, outfile.name, out.dir)

# Table B.11 spread by symbol
outfile.name = 'Table_Spread_Sym'
write.vars.summary.table(sym,  vars, desc, cols.precision, outfile.name, out.dir)

##############################################
### SPREAD DECOMPOSITION DIFFERENT HORIZON ###
##############################################

### Appendix Table B.13 and B.14 spread decomposition with 100ms and 1s mark-to-market
vars = c('Eff_Spread_bps', 'Eff_Spread_Races_bps', 'Eff_Spread_NotRaces_bps', 
         'PriceImpact_bps', 'PriceImpact_Races_bps', 'PriceImpact_NotRaces_bps',
         'LossAvoidance_bps', 'Realized_Spread_bps', 'Realized_Spread_Races_bps', 'Realized_Spread_NotRaces_bps',
         'PI_R_PI_Total', 'PI_R_ES')
desc = c('Effective spread paid - overall (bps)', 'Effective spread paid - in races (bps)', 'Effective spread paid - not in races (bps)',
         'Price impact - overall (bps)', 'Price impact - in races (bps)','Price impact - not in races (bps)',
         'Loss avoidance (bps)', 'Realized spread - overall (bps)', 'Realized spread - in races (bps)','Realized spread - not in races (bps)',
         'PI in races / PI total (%)', 'PI in races / Effective spread (%)')
cols.precision = rep.int(2,11)

### Appendix Table B.13 spread decomposition - 100ms
# Aggregate by symbol-date
symdate = generate.symdate(race.stats, symdate.stats, '100ms')
# Aggregate by date 
date = generate.symdate.groupby(symdate, c('Date'))
# Aggregate by symbol
sym = generate.symdate.groupby(symdate, c('Symbol'))
# Appendix Table B.13 Panel A and B by symbol
outfile.name = 'Table_SpreadDecomposition_Sym_100ms'
write.vars.summary.table(sym,  vars, desc, cols.precision, outfile.name, out.dir)
# Appendix Table B.13 Panel C by date
outfile.name = 'Table_SpreadDecomposition_Date_100ms'
write.vars.summary.table(date, vars, desc, cols.precision, outfile.name, out.dir)

### Appendix Table B.14 spread decomposition - 1s
symdate = generate.symdate(race.stats, symdate.stats, '1s')
# Aggregate by date 
date = generate.symdate.groupby(symdate, c('Date'))
# Aggregate by symbol
sym = generate.symdate.groupby(symdate, c('Symbol'))
# Appendix Table B.14 Panel A and B by symbol
outfile.name = 'Table_SpreadDecomposition_Sym_1s'
write.vars.summary.table(sym,  vars, desc, cols.precision, outfile.name, out.dir)
# Appendix Table B.14 Panel C by date
outfile.name = 'Table_SpreadDecomposition_Date_1s'
write.vars.summary.table(date, vars, desc, cols.precision, outfile.name, out.dir)

########################################################################
########################### NUMBERS IN TEXT ############################
########################################################################
### Race Duration Bin Count
# Numbers in Section 4.1 Race Durations
min = -100
max = 500
bin = 5
breaks = c(seq(from=min,to=max,by=bin))
tab = rbind(hist(race.stats[Time_S1_F1 >min & Time_S1_F1 <=max]$Time_S1_F1,breaks = breaks, plot=FALSE)$counts,
            hist(race.stats[Time_S1_F1 >min & Time_S1_F1 <=max]$Time_S1_F1,breaks = breaks, plot=FALSE)$counts/nrow(race.stats) * 100)
rownames(tab) = c('# races', '% of all races')
colnames(tab) = as.list(paste(breaks[1:length(breaks)-1], 'to' ,breaks[2:length(breaks)]))
write.csv(t(tab), file = paste(out.dir, 'RaceDuration_BinCount.csv', sep=''))
### The distribution of Race Horizon
describe(race.stats$Race_Horizon)

########################################################################
################## ADDITIONAL ROBUSTNESS CHECKS ########################
########################################################################

### Appendix Table D.1 signs of race profits and price impact
# Signs of race profits and price impact
mark.to.market.Ts = c('1ms', '10ms', '100ms','1s', '10s')
vars = c(sprintf('Race_Profits_PerShare_%s',mark.to.market.Ts),
         sprintf('PriceImpact_PerShare_Race_%s',mark.to.market.Ts))
tab = get.vars.sign.pct.column(race.stats, vars)
write.csv(tab, file=file.path(out.dir, sprintf('ProfitsPISign.csv')))
# Pct races with race profits always < 0
race.stats[, Race_Profits_Always_lt0_10ms := (Race_Profits_PerShare_1ms < 0) & (Race_Profits_PerShare_10ms < 0)]
race.stats[, Race_Profits_Always_lt0_100ms := (Race_Profits_PerShare_1ms < 0) & (Race_Profits_PerShare_10ms < 0) & (Race_Profits_PerShare_100ms < 0)]
race.stats[, Race_Profits_Always_lt0_1s := (Race_Profits_PerShare_1ms < 0) & (Race_Profits_PerShare_10ms < 0) & (Race_Profits_PerShare_100ms < 0) & (Race_Profits_PerShare_1s < 0)]
race.stats[, Race_Profits_Always_lt0_10s := (Race_Profits_PerShare_1ms < 0) & (Race_Profits_PerShare_10ms < 0) & (Race_Profits_PerShare_100ms < 0) & (Race_Profits_PerShare_1s < 0) & (Race_Profits_PerShare_10s < 0)]
vars = sprintf('Race_Profits_Always_lt0_%s', c('10ms', '100ms','1s', '10s'))
tab = get.vars.true.pct.column(race.stats, vars)
write.csv(tab, file=file.path(out.dir, sprintf('ProfitsAlwaysNegative.csv', t)))

### Appendix Table D.2 races triggered by order book activity
race.stats = race.stats[, Eff_Spread_PerShare_bps := to_bps * Eff_Spread_PerShare_Race/MidPt]
race.stats = race.stats[, Profits.1ms.Positive := Race_Profits_PerShare_1ms > 0]

for (t in c('10us','50us','100us','500us','1ms')) {
  stable.var = sprintf('Stable_Prc_RaceBBO_since_%s_PreRace', t)
  improved.var = sprintf('RaceBBO_Improved_since_%s_PreRace', t)
  
  race.stats.stable = race.stats[get(stable.var)==TRUE]
  race.stats.nonstable = race.stats[get(stable.var)==FALSE]
  race.stats.improved = race.stats[get(stable.var)==FALSE & get(improved.var)==1]
  race.stats.nonimproved = race.stats[get(stable.var)==FALSE & get(improved.var)!=1]
  
  # col.all = get.sensitivity.summary.column(race.stats, symdate.stats, nm)
  col.stable = get.sensitivity.summary.column(race.stats.stable, symdate.stats, sprintf('stable_%s', t))
  col.nonstable = get.sensitivity.summary.column(race.stats.nonstable, symdate.stats, sprintf('nonstable_%s', t))
  col.improved = get.sensitivity.summary.column(race.stats.improved, symdate.stats, sprintf('improved_%s', t))
  col.nonimproved = get.sensitivity.summary.column(race.stats.nonimproved, symdate.stats, sprintf('nonimproved_%s', t))
  
  cols = cbind(col.stable, col.nonstable[2], col.improved[2], col.nonimproved[2])

  tab = rbind(cols,
              c('M_Canc_Within_500us',
                race.stats.stable[, mean(M_Canc_Within_500us, na.rm=TRUE)],
                race.stats.nonstable[, mean(M_Canc_Within_500us, na.rm=TRUE)],
                race.stats.improved[, mean(M_Canc_Within_500us, na.rm=TRUE)],
                race.stats.nonimproved[, mean(M_Canc_Within_500us, na.rm=TRUE)]),
              c('Eff_Spread_PerShare_bps',
                race.stats.stable[, mean(Eff_Spread_PerShare_bps, na.rm=TRUE)],
                race.stats.nonstable[, mean(Eff_Spread_PerShare_bps, na.rm=TRUE)],
                race.stats.improved[, mean(Eff_Spread_PerShare_bps, na.rm=TRUE)],
                race.stats.nonimproved[, mean(Eff_Spread_PerShare_bps, na.rm=TRUE)]),
              c('Pct.Profits.1ms.Positive',
                race.stats.stable[, mean(Profits.1ms.Positive, na.rm=TRUE)],
                race.stats.nonstable[, mean(Profits.1ms.Positive, na.rm=TRUE)],
                race.stats.improved[, mean(Profits.1ms.Positive, na.rm=TRUE)],
                race.stats.nonimproved[, mean(Profits.1ms.Positive, na.rm=TRUE)])
  )
  write.csv(tab, file=file.path(out.dir, sprintf('PreRace_OrderBookActivity_%s.csv', t)))
}
