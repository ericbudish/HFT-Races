########### Functions.R
#' 
#' This file contains all functions used to reproduce the results in
#' Aquilina, Budish, and O’Neill.
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
########################################################################
########################### LOAD PACKAGES ##############################
########################################################################
require('data.table')
require('R.utils')
require('stargazer')
require('Hmisc')
require('ggridges')
require('ggplot2')
require('scales')
require('dplyr')
require('reshape2')
########################################################################
########################### SET CONSTANTS ##############################
########################################################################
to_pct = 100.
to_bps = 10000.
us_to_s = 1/1000000.
########################################################################
#################### DATA PREPARATION FUNCTIONS ########################
########################################################################
read.data = function(infile, cols = NULL) {
  #' @description 
  #' This function reads the stats sets (output from Python)
  #' @param infile file path
  #' @param cols columns to select, NULL to include all columns
  #' @return data DT 
  ### Read data and parse date
  data = fread(infile,stringsAsFactors = FALSE,select=cols,fill=TRUE)[, Date:=as.Date(Date)]
  return(data)
}

generate.symdate = function(race.stats, symdate.stats, t) {
  #' @description 
  #' Function that generates sym-date level aggregate stats.
  #' @param stats stats DT after preparations
  #' @param symdate.stats symbol-date level stats
  #' @param t mark to market horizon for profits and spread related stats
  #' @return symdate DT of sym-date level aggregation
  
  ### Get variable names
  ## Price Impact in races for each race
  PriceImpact_Paid_Race = sprintf('PriceImpact_Paid_Race_%s',t)
  ## Price Impact bps, symbol-date level
  PriceImpact_bps = sprintf('PriceImpact_bps_%s',t)
  ## Per-Race and Per-Share Profits for each race
  Race_Profits_DispDepth = sprintf('Race_Profits_DispDepth_%s', t)
  Race_Profits_PerShare = sprintf('Race_Profits_PerShare_%s',t)
  Race_Profits_PerShare_Tx = sprintf('Race_Profits_PerShare_Tx_%s',t)
  Race_Profits_ActiveQty = sprintf('Race_Profits_ActiveQty_%s',t)
  LossAvoidance = sprintf('LossAvoidance_%s',t)

  ### Flag races for PI and ES calculation
  # A race is flagged if the midpoint price at the race start or at T after that
  # is missing or ES <= 0 (due to packet loss or one sided market).
  # We only use non-flagged races to calculate PI and ES bps in races below.
  MidPt_f_T = sprintf('MidPt_f_%s', t)
  race.stats[, FlaggedRace:= is.na(MidPt) | is.na(get(MidPt_f_T)) | Eff_Spread_PerShare_Race<=0]

  ### Get the stats requiring race stats input (and possibly also symdate stats input)
  # Only include fields that should be zero when there is no race for a symdate
  race.stats = merge(race.stats, symdate.stats, by = c('Date', 'Symbol'), all.x = TRUE)
  symdate = race.stats[, .(
    ## Counts
    N_Races = .N, 
    N_Tr_Races = sum(Num_Trades, na.rm=TRUE),
    ## Race Flags (ES<=0, Missing MidPt_Inb, or Missing MidPt_Forward)
    N_FlaggedRaces = sum(FlaggedRace==TRUE),
    ## Race Timing
    Time_S1_F1_Max_0_s = sum(Time_S1_F1_Max_0, na.rm = TRUE) * us_to_s, # convert to s
    Time_S1_F1_neg = sum(ifelse(Time_S1_F1 < 0, 1., 0.), na.rm = TRUE), 
    ## Race Profits
    Race_Profits_DispDepth = sum(get(Race_Profits_DispDepth), na.rm= TRUE),
    Race_Profits_DispDepth_bps = to_bps * (sum(get(Race_Profits_DispDepth), na.rm = TRUE)/Vol[1]),
    Race_Profits_DispDepth_NR_bps = to_bps * (sum(get(Race_Profits_DispDepth), na.rm= TRUE)/(Vol[1] - sum(Value_Traded, na.rm=TRUE))), 
    Race_Profits_PerShare_Tx = sum(get(Race_Profits_PerShare_Tx), na.rm = TRUE),
    Race_Profits_ActiveQty = sum(get(Race_Profits_ActiveQty), na.rm = TRUE),
    ## Loss Avoidance
    LossAvoidance = sum(get(LossAvoidance), na.rm = TRUE),
    LossAvoidance_bps = to_bps * sum(get(LossAvoidance), na.rm = TRUE)/Vol[1],
    ## Race Volume
    Qty_Traded = sum(Qty_Traded, na.rm = TRUE),
    Qty_Cancelled = sum(Qty_Cancelled, na.rm = TRUE),
    Qty_Active = sum(Qty_Traded, na.rm = TRUE) + sum(Qty_Cancelled, na.rm = TRUE),
    Vol_Races = sum(Value_Traded, na.rm = TRUE),
    Vol_NotRaces = Vol[1] - sum(Value_Traded, na.rm = TRUE),
    Pct_Value_Traded_Race = to_pct * sum(Value_Traded, na.rm = TRUE)/Vol[1]),
    by = c('Symbol', 'Date')]
  ## Calculate the PI ES races in bps using only the non-flagged races
  symdate = merge(symdate, race.stats[FlaggedRace==FALSE, .(
    Eff_Spread_Races_bps = to_bps * sum(Eff_Spread_Paid_Race, na.rm=TRUE)/sum(Value_Traded, na.rm = TRUE),
    PriceImpact_Races_bps = to_bps * (sum(get(PriceImpact_Paid_Race), na.rm = TRUE)/sum(Value_Traded, na.rm = TRUE))),
    by = .(Date, Symbol)], by = c('Symbol', 'Date'), all.x = TRUE)
  ### Merge with symdate stats to get all symdates and create zeros for empty rows
  protected.cols = c(c('Date','Symbol'), colnames(symdate.stats))
  symdate = merge(symdate.stats, symdate, by = c('Date', 'Symbol'), all.x = TRUE)
  symdate[is.na(N_Races), setdiff(colnames(symdate),protected.cols)] = 0
  ### Generate spread decomposition variables
  ## Price impact 
  symdate = symdate[, PriceImpact_Paid := get(PriceImpact_bps)/to_bps * Vol]
  symdate = symdate[, PriceImpact_Paid_Races := PriceImpact_Races_bps/to_bps * Vol_Races]
  symdate = symdate[, PriceImpact_Paid_NotRaces := PriceImpact_Paid - PriceImpact_Paid_Races]
  symdate = symdate[, PriceImpact_bps := get(PriceImpact_bps)] # rename for convenience
  symdate = symdate[, PriceImpact_NotRaces_bps := to_bps * PriceImpact_Paid_NotRaces/Vol_NotRaces]
  ## Effective spread, note that this is already half spread
  symdate = symdate[, Eff_Spread_Paid := Eff_Spread_bps/to_bps * Vol]
  symdate = symdate[, Eff_Spread_Paid_Races := Eff_Spread_Races_bps/to_bps * Vol_Races]
  symdate = symdate[, Eff_Spread_Paid_NotRaces := Eff_Spread_Paid - Eff_Spread_Paid_Races]
  symdate = symdate[, Eff_Spread_NotRaces_bps := to_bps * Eff_Spread_Paid_NotRaces/Vol_NotRaces]
  ## Realized spread 
  symdate = symdate[, Realized_Spread := Eff_Spread_Paid - PriceImpact_Paid]
  symdate = symdate[, Realized_Spread_Races := Eff_Spread_Paid_Races - PriceImpact_Paid_Races]
  symdate = symdate[, Realized_Spread_NotRaces := Eff_Spread_Paid_NotRaces - PriceImpact_Paid_NotRaces]
  symdate = symdate[, Realized_Spread_bps := Eff_Spread_bps - PriceImpact_bps]
  symdate = symdate[, Realized_Spread_Races_bps := Eff_Spread_Races_bps - PriceImpact_Races_bps]
  symdate = symdate[, Realized_Spread_NotRaces_bps := Eff_Spread_NotRaces_bps - PriceImpact_NotRaces_bps]
  ## Spread decomposition
  symdate = symdate[, `:=`(
                      PI_ES = to_pct * (PriceImpact_Paid / Eff_Spread_Paid),
                      PI_R_PI_Total = to_pct * PriceImpact_Paid_Races / PriceImpact_Paid,
                      PI_R_ES = to_pct * PriceImpact_Paid_Races / Eff_Spread_Paid,
                      PI_NotR_ES_NotR = to_pct * PriceImpact_Paid_NotRaces/Eff_Spread_Paid_NotRaces,
                      RaceProfits_DD_ES = to_pct * Race_Profits_DispDepth / Eff_Spread_Paid,
                      RaceProfits_DD_ES_NotR = to_pct * Race_Profits_DispDepth / Eff_Spread_Paid_NotRaces,
                      RaceProfits_Active_ES = to_pct * Race_Profits_ActiveQty / Eff_Spread_Paid,
                      LossA_ES = to_pct * LossAvoidance / Eff_Spread_Paid)]
  
  ### Set NA values in ratios caused by zero division to zero
  symdate[Vol == 0, c('Race_Profits_DispDepth_bps','Pct_Value_Traded_Race',
                      'PriceImpact_bps','Eff_Spread_bps','Realized_Spread_bps')] = 0
  symdate[Vol_Races == 0, c('Realized_Spread_Races_bps','Eff_Spread_Races_bps','PriceImpact_Races_bps')] = 0
  symdate[Vol_NotRaces == 0, c('Race_Profits_DispDepth_NR_bps','Realized_Spread_NotRaces_bps',
                               'Eff_Spread_NotRaces_bps','PriceImpact_NotRaces_bps')] = 0
  symdate[Eff_Spread_Paid == 0, c('PI_ES','PI_R_ES','RaceProfits_DD_ES','RaceProfits_Active_ES','LossA_ES')] = 0
  symdate[Eff_Spread_Paid_NotRaces == 0, c('PI_NotR_ES_NotR','RaceProfits_DD_ES_NotR')] = 0
  symdate[PriceImpact_Paid == 0, c('PI_R_PI_Total')] = 0
  return(symdate)
}

generate.symdate.groupby = function(symdate, by.vars) {
  #' @description 
  #' Function that generates measures at the date/symbol level based on the symdate data.table
  #' @param symdate symdate DT after symdate level aggregation by generate.symdate
  #' @param by.vars groupby vars
  #' @return groupby DT of date/symbol level aggregation based on symdate level aggregation
  
  groupby = symdate[, .(
    ## Counts
    N_Races = sum(N_Races, na.rm = TRUE),
    Avg_N_Races = mean(N_Races, na.rm = TRUE),
    N_Tr = sum(N_Tr),
    N_Tr_Races = sum(N_Tr_Races),
    Pct_Num_Race_Trades = to_pct * sum(N_Tr_Races)/sum(N_Tr),
    ## Race Timing
    Time_S1_F1_Max_0_s = sum(Time_S1_F1_Max_0_s, na.rm = TRUE),
    ## Aggregate Volume
    Qty_Active = sum(Qty_Active, na.rm = TRUE),
    Vol_Sh = sum(Vol_Sh, na.rm = TRUE),
    Vol = sum(Vol, na.rm = TRUE),
    Vol_Races = sum(Vol_Races, na.rm = TRUE),
    Vol_NotRaces = sum(Vol_NotRaces, na.rm = TRUE),
    Pct_Value_Traded_Race = to_pct * sum(Vol_Races, na.rm = TRUE)/sum(Vol, na.rm = TRUE),
    ## Race Profits
    Race_Profits_DispDepth = sum(Race_Profits_DispDepth, na.rm = TRUE),
    Race_Profits_DispDepth_bps = to_bps * (sum(Race_Profits_DispDepth, na.rm = TRUE)/sum(Vol, na.rm = TRUE)),
    Race_Profits_DispDepth_NR_bps = to_bps * (sum(Race_Profits_DispDepth, na.rm = TRUE)/sum(Vol_NotRaces, na.rm = TRUE)),
    ## Simple Average Race Profits across symdate
    Avg_Race_Profits_DispDepth = mean(Race_Profits_DispDepth, na.rm = TRUE),
    Avg_Race_Profits_DispDepth_bps = mean(Race_Profits_DispDepth_bps, na.rm = TRUE),
    Avg_Race_Profits_DispDepth_NR_bps = mean(Race_Profits_DispDepth_NR_bps, na.rm = TRUE),
    ## Simple Average Spread across symdate
    Avg_Half_Spr_Time_Weighted_Tx = mean(Avg_Half_Spr_Time_Weighted_Tx, na.rm = TRUE),
    Avg_Half_Spr_Time_Weighted_bps = mean(Avg_Half_Spr_Time_Weighted_bps, na.rm = TRUE),
    Avg_Half_Spr_Qty_Weighted_Tx = mean(Avg_Half_Spr_Qty_Weighted_Tx, na.rm = TRUE),
    Avg_Half_Spr_Qty_Weighted_bps = mean(Avg_Half_Spr_Qty_Weighted_bps, na.rm = TRUE),
    ## Effective Spread paid
    Eff_Spread_Paid = sum(Eff_Spread_Paid, na.rm = TRUE),
    Eff_Spread_bps = weighted.mean(Eff_Spread_bps, Vol, na.rm = TRUE),
    Eff_Spread_Paid_Races = sum(Eff_Spread_Paid_Races, na.rm = TRUE),
    Eff_Spread_Races_bps = weighted.mean(Eff_Spread_Races_bps, Vol_Races, na.rm = TRUE),
    Eff_Spread_Paid_NotRaces = sum(Eff_Spread_Paid_NotRaces, na.rm = TRUE),
    Eff_Spread_NotRaces_bps = weighted.mean(Eff_Spread_NotRaces_bps, Vol_NotRaces, na.rm = TRUE),
    ## Price Impact
    PriceImpact_Paid = sum(PriceImpact_Paid, na.rm = TRUE),
    PriceImpact_bps = weighted.mean(PriceImpact_bps, Vol, na.rm = TRUE),
    PriceImpact_Paid_Races = sum(PriceImpact_Paid_Races, na.rm = TRUE),
    PriceImpact_Races_bps = weighted.mean(PriceImpact_Races_bps, Vol_Races, na.rm = TRUE),
    PriceImpact_Paid_NotRaces = sum(PriceImpact_Paid_NotRaces, na.rm = TRUE),
    PriceImpact_NotRaces_bps = weighted.mean(PriceImpact_NotRaces_bps, Vol_NotRaces, na.rm = TRUE),
    ## Loss Avoidance bps
    LossAvoidance_bps = weighted.mean(LossAvoidance_bps, Vol),
    ## Cost Reduction
    RaceProfits_DD_ES_NotR =  to_pct * (sum(Race_Profits_DispDepth, na.rm = TRUE) / sum(Eff_Spread_Paid_NotRaces, na.rm = TRUE))),
    by = by.vars]
  groupby[is.na(RaceProfits_DD_ES_NotR), c('RaceProfits_DD_ES_NotR')]=0
  groupby[is.na(Pct_Num_Race_Trades), c('Pct_Num_Race_Trades')] = 0
  groupby[Vol_Races==0, c('Eff_Spread_Races_bps')] = 0
  ## Spread Decomposition
  groupby = groupby[, `:=`(
                           Realized_Spread_bps = Eff_Spread_bps - PriceImpact_bps,
                           Realized_Spread_Races_bps = Eff_Spread_Races_bps - PriceImpact_Races_bps,
                           Realized_Spread_NotRaces_bps = Eff_Spread_NotRaces_bps - PriceImpact_NotRaces_bps,
                           PI_ES = to_pct * PriceImpact_Paid/Eff_Spread_Paid,
                           PI_R_PI_Total = to_pct * PriceImpact_Paid_Races/PriceImpact_Paid,
                           PI_R_ES = to_pct * PriceImpact_Paid_Races/Eff_Spread_Paid,
                           LossA_ES = to_pct * LossAvoidance_bps/Eff_Spread_bps)]
  return (groupby)
}

########################################################################
############### FUNCTIONS FOR SECTION 4 MAIN RESULTS ###################
########################################################################

get.var.percentiles.row = function(data, var, cols.precision=rep.int(3,11)) {
  #' @description 
  #' Function that get the percentiles and Min/Max/Mean/sd for a given variable 
  #' var in dataset data
  #' @param data data.table
  #' @param var variable name
  #' @param cols.precision n digits for each of the 11 numeric columns
  #' @return row of percentiles
  
  row = data[, .(Name   = var, 
                 Mean   = formatC(mean(get(var), na.rm = TRUE),                      digits=cols.precision[1], format='f', big.mark=','), 
                 sd     = formatC(sd(get(var), na.rm = TRUE),                        digits=cols.precision[2], format='f', big.mark=','), 
                 Min    = formatC(min(get(var), na.rm = TRUE),                       digits=cols.precision[3], format='f', big.mark=','),   
                 Pct01  = formatC(quantile(get(var), probs = c(0.01), na.rm = TRUE), digits=cols.precision[4], format='f', big.mark=','), 
                 Pct10  = formatC(quantile(get(var), probs = c(0.10), na.rm = TRUE), digits=cols.precision[5], format='f', big.mark=','), 
                 Pct25  = formatC(quantile(get(var), probs = 0.25, na.rm = TRUE),    digits=cols.precision[6], format='f', big.mark=','), 
                 Median = formatC(median(get(var), na.rm = TRUE),                    digits=cols.precision[7], format='f', big.mark=','), 
                 Pct75  = formatC(quantile(get(var), probs = 0.75, na.rm = TRUE),    digits=cols.precision[8], format='f', big.mark=','), 
                 Pct90  = formatC(quantile(get(var), probs = c(0.90), na.rm = TRUE), digits=cols.precision[9], format='f', big.mark=','), 
                 Pct99  = formatC(quantile(get(var), probs = c(0.99), na.rm = TRUE), digits=cols.precision[10], format='f', big.mark=','), 
                 Max    = formatC(max(get(var), na.rm= TRUE),                        digits=cols.precision[11], format='f', big.mark=','))]
  return(row)
}

write.vars.summary.table = function(data, vars, desc, cols.precision, outfile.name, out.dir) {
  #' @description 
  #' Function that writes a summary table for one or multiple variables to latex.
  #' Each variable in vars takes a row in the summary table.
  #' The columns are the summary stats from get.var.percentiles.row().
  #' @param data data DT.
  #' @param vars list of variables for descriptive stats.
  #' @param desc list of variable descriptions to be included in the table.
  #' @param cols.precision n digits for each column.
  #' @param outfile.name output file name.
  #' @param out.dir output directory.
  #' @return None, output to file.

  rows = list()
  for (i in 1:length(vars)) {
    row = get.var.percentiles.row(data, vars[i], cols.precision)
    rows[[i]] = row
  }
  tab = rbindlist(rows)
  tab = cbind(desc, tab)
  names(tab)[names(tab) == "desc"] ='Description'
  tab = tab[, -c('Name')]
  stargazer(tab, header=FALSE, type='latex',
            out=file.path(out.dir, paste(outfile.name, '.tex', sep ='')),
            float = FALSE, summary = FALSE, rownames = FALSE)
}

write.diff.T.means.table = function(race.stats, mark.to.market.Ts, vars, desc, precision, outfile.name, out.dir) {
  #' @description 
  #' Function that calculates the mean race profits at different mark to market horizons.
  #' @param race.stats processed race stats dataset
  #' @param mark.to.market.Ts list of Ts
  #' @param vars list of variable names (without the '_T' part)
  #' @param desc list of descriptions (text in the table), same length as vars
  #' @param precision n digits for the numbers in the table
  #' @param outfile.name file name of the output, without the extension
  #' @param out.dir output directory
  #' @return None, output to file
  rows = list()
  for (i in 1:length(vars)) {
    rows[[i]] = race.stats[, lapply(.SD, function(x){mean(x, na.rm = TRUE)}), .SDcols = sprintf('%s_%s', vars[i], mark.to.market.Ts)]
  }
  tab = rbindlist(rows, use.names = FALSE)
  colnames(tab) = mark.to.market.Ts
  tab = cbind(desc, tab)
  names(tab)[names(tab) == "desc"] ='Description'
  stargazer(tab, header=FALSE, type='latex', out=file.path(out.dir, paste(outfile.name, '.tex', sep='')),
            float = FALSE, summary = FALSE, rownames = FALSE, digits = precision)
}

write.poisson.tables = function(symdate, potential.race.horizons, time_total_hr, cols.precision, outfile.name, out.dir) {
  #' @description 
  #' Function that generates the Poisson tables for expected number of races
  #' @param symdate symdate aggregation, output from generate.symdate()
  #' @param potential.race.horizons list of race horizons (numeric in us)
  #' @param time_total_hr total time of a trading day in hours
  #' @param cols.precision n digits for each column
  #' @param outfile.name file name prefix of the output, without the extension
  #' @param out.dir output directory
  #' @return None, output to file

  time_total_us = time_total_hr * 3600. * 1000000.
  symdate[, Avg_Arrival_Rate_RaceRlvt_Side := (N_Msgs_Inbound_NBBO/2.)/time_total_us]

  for (t in potential.race.horizons){
    # Get the number of occurrences of T periods of time in a symbol-date
    num_t_increments = time_total_us/as.numeric(t)
    
    # Calculate probability of at least 2 (3) messages within t us
    symdate[[sprintf('prob2_RaceRlvt_Side_%s', t)]] =  1 - ppois(1, lambda = symdate$Avg_Arrival_Rate_RaceRlvt_Side*as.numeric(t))
    symdate[[sprintf('prob3_RaceRlvt_Side_%s', t)]] =  1 - ppois(2, lambda = symdate$Avg_Arrival_Rate_RaceRlvt_Side*as.numeric(t))
    
    # Multiply the probability by the number of periods T in the day to get the expected number of occurrences
    symdate[[sprintf('exp_N2_%s', t)]] = symdate[[sprintf('prob2_RaceRlvt_Side_%s', t)]]*num_t_increments
    symdate[[sprintf('exp_N3_%s', t)]] = symdate[[sprintf('prob3_RaceRlvt_Side_%s', t)]]*num_t_increments
  }

  vars = c(sprintf('exp_N2_%s', potential.race.horizons), sprintf('exp_N3_%s', potential.race.horizons))
  desc = c(sprintf('Expected potential race events with 2+ participants within %s', potential.race.horizons),
           sprintf('Expected potential race events with 3+ participants within %s', potential.race.horizons))
  write.vars.summary.table(symdate, vars, desc, cols.precision, outfile.name, out.dir)
}

########################################################################
############### FUNCTIONS FOR SECTION 5 SENSITIVITY ####################
########################################################################

get.sensitivity.summary.column = function(race.stats, symdate.stats, name) {
  #' @description 
  #' Function to generate one column in the sensitivity table for a race run or a race filter
  #' Returns a column with the measures in the sensitivity table
  #' @param race.stats DT stats 
  #' @param symdate.stats DT symdate.stats 
  #' @param name name of the run or filter
  #' @return tab, a data.frame with two columns: Descriptions and the values of the stats
  
  ### Aggregate by symbol date
  symdate = generate.symdate(race.stats, symdate.stats, '10s')
  ### Aggregate by date 
  date = generate.symdate.groupby(symdate, c('Date'))
  ### Aggregate by symbol
  sym = generate.symdate.groupby(symdate, c('Symbol'))
  
  ### Numbers in the table
  # # Races per day
  races_per_day = mean(symdate$N_Races, na.rm = TRUE)
  # Mean race duration
  race_duration = mean(race.stats$Time_S1_F1, na.rm =TRUE)
  # % wrong winner (race duration < 0)
  pct_wrong_winner = 100. * sum(symdate$Time_S1_F1_neg, na.rm = TRUE) / sum(symdate$N_Races, na.rm = TRUE)
  # % volume in races
  pct_vol = mean(date$Pct_Value_Traded_Race, na.rm = TRUE)
  # # race relevant msgs within 500us per race
  n_m_within_500us = mean(race.stats$M_Within_500us, na.rm = TRUE)
  # Per-share profits in ticks
  pershare_profits_tx = mean(race.stats$Race_Profits_PerShare_Tx_10s, na.rm = TRUE)
  # Per-share profits in monetary terms
  pershare_profits = mean(race.stats$Race_Profits_PerShare_10s, na.rm = TRUE)
  # Per-share profits in bps
  pershare_profits_bps = mean(race.stats$Race_Profits_PerShare_bps_10s, na.rm = TRUE)
  # Per-race profits, displayed depth
  perrace_profits_dd = mean(race.stats$Race_Profits_DispDepth_10s, na.rm = TRUE)
  # Per-race profits, active qty (qty traded + canceled)
  perrace_profits_active = mean(race.stats$Race_Profits_ActiveQty_10s, na.rm = TRUE)
  # Daily race profits per symbol
  dailyprofits_sym = mean(sym$Avg_Race_Profits_DispDepth, na.rm = TRUE)
  # Daily race profits total
  dailyprofits_date = mean(date$Race_Profits_DispDepth, na.rm = TRUE)
  # LA Tax, all volume by date
  latax_meas1_date = mean(date$Race_Profits_DispDepth_bps, na.rm = TRUE)
  # LA Tax, non-race volume by date
  latax_meas2_date = mean(date$Race_Profits_DispDepth_NR_bps, na.rm = TRUE)
  # PI in races / PI total
  pir_pit = mean(date$PI_R_PI_Total, na.rm = TRUE)
  # PI in races / ES total
  pir_es = mean(date$PI_R_ES, na.rm = TRUE)
  # Loss avoidance / ES total
  lossA_es = mean(date$LossA_ES, na.rm = TRUE)
  # Reduction in liquidity costs by symbol
  redux_liq_cost_sym = mean(sym$RaceProfits_DD_ES_NotR, na.rm = TRUE)
  # Reduction in liquidity costs by date
  redux_liq_cost_date = mean(date$RaceProfits_DD_ES_NotR, na.rm = TRUE)
  
  ### Formatting
  values = c(races_per_day, race_duration, pct_wrong_winner, pct_vol, n_m_within_500us, 
             pershare_profits_tx, pershare_profits, pershare_profits_bps, 
             perrace_profits_dd, perrace_profits_active,
             dailyprofits_sym, dailyprofits_date, 
             latax_meas1_date, latax_meas2_date, 
             pir_pit, pir_es, lossA_es, 
             redux_liq_cost_sym, redux_liq_cost_date)
  names = c("Races per day - per symbol", "Mean race duration (us)", "% of races with wrong winner",
            "% of volume in races", "Mean number of messages within 500 us",
            "Per-share profits (ticks)", "Per-share profits (monetary units)", "Per-share profits (bps)",
            "Per-race profits - displayed depth", "Per-race profits - qty trade/cancel", 
            "Daily profits - per symbol", "Daily profits - aggregate",
            "Latency arbitrage tax, all volume (bps)", "Latency arbitrage tax, non-race volume (bps)", 
            "Price impact in races / all price impact (%)", 
            "Price impact in races / effective spread (%)",
            "Loss avoidance / Effective spread (%)",
            "% Reduction in liquidity cost - by symbol", "% Reduction in liquidity cost - by date")
  tab = data.frame(Description = names)
  tab[[name]] = values
  return(tab)
}

########################################################################
############### FUNCTIONS FOR ADDITIONAL ROBUSTNESS ####################
########################################################################

get.vars.sign.pct.column = function(data, vars) {
  #' @description 
  #' Function that produces pct >0/=0/<0 for vars in data
  #' @param data data DT after preparation / aggregation
  #' @param vars variable list to calculate descriptive stats from
  #' @return tab
  
  rows = list()
  for (var in vars) {
    total = data[, sum(!is.na(get(var)))]
    rows[[var]] = data[, .(pos = sum(get(var) > 0,na.rm=TRUE)/total, 
                           neg = sum(get(var) < 0,na.rm=TRUE)/total, 
                           zero = sum(get(var)==0,na.rm=TRUE)/total)]
  }
  tab = rbindlist(rows)
  tab = cbind(vars, tab)
  tab = melt(tab)
  tab = as.data.table(tab)
  tab[, vars:=paste(vars, variable, sep='_')]
  setorder(tab, vars)
  tab[, c('variable')]=NULL
  return(tab)
}

get.vars.true.pct.column = function(data, vars) {
  #' @description 
  #' Function that produces pct TRUE for vars in data
  #' @param data data DT after preparation / aggregation
  #' @param vars variable list to calculate descriptive stats from
  #' @return tab
  
  rows = list()
  for (var in vars) {
    total = data[, sum(!is.na(get(var)))]
    rows[[var]] = data[, .(true = sum(get(var),na.rm=TRUE)/total)]
  }
  tab = rbindlist(rows)
  tab = cbind(vars, tab)
  tab = melt(tab)
  tab = as.data.table(tab)
  tab[, vars:=paste(vars, variable, sep='_')]
  setorder(tab, vars)
  tab[, c('variable')]=NULL
  return(tab)
  
}

