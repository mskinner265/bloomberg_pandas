# -*- coding: utf-8 -*-
"""
Created on Mon Aug 10 14:35:29 2015

@author: mskinner
"""
import matplotlib.pyplot as plt
import pandas as pd
import datetime as dt
import numpy as np
#import sys
#sys.path.insert(0, 'O:/AAA-IndividualFolders/MSkinner/Research/Equities/BalanceSheet/LIB/')
import get_balanceSheet_portfolio as bs
#from math import floor

#def zt(val, percentile=80):
#    np_val = np.array(val)    
#    limit = np.percentile(np_val, percentile, interpolation='lower')
#    trimmed = np_val[ np_val <= limit]
#    trimmed_median = np.median( trimmed )
#    return (val - trimmed_median) / np.std(trimmed)
#
#
#
#def cczc(   df, industry_field='icb_industry_name', 
#                            col_names=['oper_margin','return_com_eqy'], col_weightings=[1/3., 2/3.]):
#    
#    for c in col_names:
#        #df['z_'+c]=df.groupby( industry_field )[c].apply(z_trimmed)
#        
#    
#    z_col_names = ['z_' + s for s in col_names]        
#    z_weights = pd.DataFrame(col_weightings, index=z_col_names)
#    #dfr['z_composite'] = dfr[z_col_names].dot(z_weights)
#    return df[z_col_names].dot(z_weights)
#


debug = 1
include_end_dt = 0
start_dt = dt.datetime(year=2008, month=4,day=1)
#start_dt = dt.datetime(year=1995, month=4,day=1)
#start_dt = dt.datetime(year=2014, month=4,day=1)   # 2 periods  
#start_dt = dt.datetime(year=2014, month=10,day=1)   # 1 period
#start_dt = dt.datetime(year=2011, month=10,day=1)   # 

#end_dt   = dt.datetime.today()
end_dt   = dt.datetime(year=2015, month=9, day=2)
#end_dt   = dt.datetime(year=2009, month=12, day=31)
#end_dt   = dt.datetime(year=2013, month=1, day=31)

# Semi-annual frequency
dt_index = pd.date_range(start_dt,end_dt, freq='2BQ')
if (include_end_dt) and not (end_dt in dt_index):
    dt_index = dt_index.append(pd.DatetimeIndex([dt.datetime(year=end_dt.year, month=end_dt.month, day=end_dt.day)]))

rev_dates = dt_index[::-1]
#dt_index = reversed(dt_index)

# Quarterly Frequency
#dt_index = pd.date_range(start_dt,end_dt, freq='BQ')

#date1 = dt.datetime(year=1994, month=12, day=31)
balance_sheet_dir = """O:\\AAA-IndividualFolders\\MSkinner\\Research\\Equities\\BalanceSheet\\INDEX\\BALANCE_SHEET\\SPX\\"""
#price_dir = """O:\AAA-IndividualFolders\MSkinner\Research\Equities\BalanceSheet\\PRICES\\SPX\\OM_ROE\\"""
price_dir = """O:\\AAA-IndividualFolders\\MSkinner\\Research\\Equities\\BalanceSheet\\INDEX\\PRICES\\SPX\\"""
stock_index = 'SPX INDEX'
#c_names=['oper_margin','return_com_eqy']
#c_weights=[1/3., 2/3.]
c_names = ['lt_debt_to_com_eqy','cur_ratio','5yr_avg_return_on_invested_cptl']
c_weights=[-0.5, -0.3, 0.2]

print "date index:::::"
for i in dt_index:
    print i

for i in range(0,len(dt_index)-1):
#for i in [0]:    
    ### Set start/end dates    
    date1 = dt_index[i]
    date2 = dt_index[i+1]
    
    print "\n\nDATE 1: ", date1, " DATE 2: ", date2
        
    ### Retrieve Data either from file, or from Bloomberg
    df = bs.get_balance_sheet_data(base_dir = balance_sheet_dir, asofdate = date1, index_name = stock_index, delim=',')
                         
    ### Calculate the index and weightings                     
    dfx                = bs.exclude_financials_df(df, 'icb_industry_name', 'Financials')
    dfx['sector_wts']  = bs.calc_maket_cap_col(dfx, mkt_cap_field='historical_market_cap', industry_field='icb_industry_name')    
    df_sect_wts        = bs.calc_maket_cap_df(dfx, mkt_cap_field='historical_market_cap', industry_field='icb_industry_name')
#    dfx['z_composite'] = bs.calc_composite_z_col(
#                                        df, industry_field='icb_industry_name',
#                                        col_names= c_names,
#                                        col_weightings= c_weights)

    dfz = bs.calc_z(dfx, industry_field='icb_industry_name',col_names= c_names, col_weightings= c_weights)


    #g = dfx.groupby('icb_industry_name')[c_names]
    #x = g.apply(bs.z_trimmed)
    #for a,b,c,d in x.itertuples():
    #    print a,",",b,",",c,",",d
  
    # g1 = dfx.groupby('icb_industry_name')['lt_debt_to_com_eqy']
    # val = dfx[ dfx['icb_industry_name'] == 'Technology']['lt_debt_to_com_eqy']
    # limit = np.percentile(val, 80, interpolation='lower')
    # trimmed = val[ val <= limit]
    # trimmed_median = np.median( trimmed )



    if (debug): print "####################", df_sect_wts    
    
    
    ### Windsorize
    dfw = bs.windsorize_df(df = dfz, col='z_composite',cutoff=3)                  

    dfInd = bs.get_index_inclusion_df(df_windsor = dfw, 
                                    df_sector_mkt_cap_wts = df_sect_wts,
                                    ind_col='icb_industry_name', 
                                    wgt_col='weight', 
                                    mkt_cap_col='historical_market_cap')
                                    
    print "LONG : ", len(dfInd[dfInd['long']])
    print "SHORT: ", len(dfInd[dfInd['short']])    
                       
    inclusion_tickers = dfInd[dfInd[['long','short']].any(axis=1)].index      ### index is ticker             
    
    px = bs.get_historical_prices(tickers=inclusion_tickers, index_name=stock_index,
                                  base_dir=price_dir, 
                                  start_date=date1, end_date=date2)
                                  
                                  

    ### get rid of nan index values             
    px[ px == 0 ] = np.nan
    pxn = px[ np.logical_not(pd.isnull(px.index) | (px.index == 'field'))]
    
    ### set the dates to be the index
    pxn['date'] = pd.to_datetime(pxn.index)
    pxn = pxn.set_index('date')
    pxn = pxn.convert_objects(convert_numeric=True)
    pxf = pxn.ffill()
    #pxf = pxn.fillna(value=0)
    
        
    #pxL = pxf.apply(lambda x: x * dfInd['long'], axis=1)
    #pxS = pxf.apply(lambda x: x * dfInd['short'], axis=1)

    pxL = dfInd['long'] * pxf
    pxS = dfInd['short'] * pxf
    
    pxL[ pxL == 0] = np.nan   # elminate prices that got zeroed out by not existing in 'long' or 'short' (dfInd['long'])
    pxS[ pxS == 0] = np.nan


    if (i==0):
        price_per_shareL = 100
        price_per_shareS = 100
    else:
        print "starting with cashL: ", cashL
        print "starting with cashS: ", cashS
        countL = sum(pxL.loc[date1] > 0)
        countS = sum(pxS.loc[date1] > 0)
        
        price_per_shareL = cashL / countL
        price_per_shareS = cashS / countS

        if len(dfInd[ dfInd['long']]) != len(dfInd[ dfInd['long']]):
            print "ERROR: different number of long/short symbols"
            exit


    sharesL = pd.DataFrame(price_per_shareL / pxL.loc[date1]).T
    sharesS = pd.DataFrame(price_per_shareS / pxS.loc[date1]).T

    if (i==0):
        initCashL = (sharesL * pxL).iloc[0].sum()
        initCashS = (sharesS * pxS).iloc[0].sum()
        print "initial cash L: ",initCashL
        print "initial cash S: ",initCashS

    #sharesL_df = pd.DataFrame([shares.loc[date1]]*len(pxf),index=pxf.index)
    sharesL_df = pd.DataFrame([sharesL.loc[date1]]*len(pxL), index=pxL.index)
    sharesS_df = pd.DataFrame([sharesS.loc[date1]]*len(pxS), index=pxS.index)
   
    pfL     = pxL * sharesL_df * dfInd['long']
    pfS     = pxS * sharesS_df * dfInd['short']

   # Find the value of the portfolio 
    valueL  = pfL.sum(axis=1)
    valueS  = pfS.sum(axis=1)



    # Find the value of the porfolio on the last day
    cashL = valueL.loc[date2]
    cashS = valueS.loc[date2]
 
    print "ending with cashL:", cashL 
    print "ending with cashS:", cashS 
   
    if (i==0):
        portfolioL = valueL
        portfolioS = valueS
    else:
        portfolioL = portfolioL.append(valueL)
        portfolioS = portfolioS.append(valueS)

    
    
    
portL = portfolioL.resample('D', how='last').dropna()
portS = portfolioS.resample('D', how='last').dropna()


#    if (i==0):
#        shares = pd.DataFrame(100 / pxf.loc[date1]).T
#        sharesL_df = pd.DataFrame([shares.loc[date1]]*len(pxf),index=pxf.index)
#        sharesS_df = sharesL_df.copy()
#        #sharesS = pd.DataFrame(-100 / pxf.loc[date1]).T * dfInd['short']
#    else:
#        namesL = len(dfInd[dfInd['long']])      
#        namesS = len(dfInd[dfInd['short']])
#        valueL = portfolioL[-1]
#        valueS = portfolioS[-1]
#        sharesL = pd.DataFrame( (valueL/namesL) / pxf.loc[date1]).T
#        sharesS = pd.DataFrame( (valueS/namesS) / pxf.loc[date1]).T
#        sharesL_df = pd.DataFrame([sharesL.loc[date1]]*len(pxf),index=pxf.index)
#        sharesS_df = pd.DataFrame([sharesS.loc[date1]]*len(pxf),index=pxf.index)
    
    #sharesShort = pd.DataFrame([sharesS.loc[date1]]*len(pxf),index=pxf.index)
#        
#    pfL     = pxf * sharesL_df * dfInd['long']
#    pfS     = pxf * sharesS_df * dfInd['short']
#
#    if (i==0):
#        portfolioL = pfL.sum(axis=1)
#        portfolioS = pfS.sum(axis=1)
#    else:
#        portfolioL = portfolioL.append(pfL.sum(axis=1))
#        portfolioS = portfolioS.append(pfS.sum(axis=1))

x1 = portL.index.to_pydatetime()
x2 = portS.index.to_pydatetime()

#%%

# plt.plot(x1, portfolioL,'b-')
# plt.plot(x2, portfolioS,'r-')
# plt.plot(x1,portfolioL-portfolioS,'k-')

#plt.plot(portL.ix['20081201':].index,portL.ix['20081201':]/portS.ix['20081201':],'k-')
#plt.plot(portL.ix['20081201':].index,portL.ix['20081201':],'k-')
#plt.plot(portL.ix['20081201':].index,portS.ix['20081201':],'b-')

print "Long over Short"
plt.plot(portL.index,portL/portS,'k-')

#print "Short over Long"
#plt.plot(portL.index,portS/portL,'b-')


#    if (i>0)
#        pf = bs.rebalance_portfolio(pf)
          


                       
                       
                       
                       
#    ###Sector with fewest names
#    sect_count = dfw.groupby('icb_industry_name').agg('count')['weight']
#    ## create [historical_market_cap, names, ratio of wght to mkt cap]    
#    df_pct = pd.concat([df_sect_wts, pd.DataFrame(sect_count)], axis=1, join='outer')
#    df_pct.rename(columns={'weight':'sym_count', 'historical_market_cap':'weight'}, inplace=True)
#    ### 'pct' is the amount a single name of a sector represents in weighting (by market cap)
#    df_pct['pct'] = df_pct['weight'].div(df_pct['sym_count'])
#    max_rep = max(df_pct['pct'])
#    max_rep_sect = df_pct['pct'].idxmax()
#    ### calculate number of names in the index
#    implied_total = df_pct['sym_count'].loc[max_rep_sect] / df_pct['weight'].loc[max_rep_sect]
#    implied_total = floor(implied_total)    
#    df_pct['include'] = df_pct['weight'] * implied_total
#    df_pct['include'] = (df_pct['weight']* implied_total).apply(floor)
#    dfw['cutoff'] = dfw['icb_industry_name'].map(df_pct['include'])
#    
#    #dfw.loc[:,'z_rank_top'] = 
#    dfw['z_rank_top'] = dfw.groupby('icb_industry_name')['z_composite'].apply(lambda x: x.rank(ascending=False))
#    dfw['z_rank_bot'] = dfw.groupby('icb_industry_name')['z_composite'].apply(lambda x: x.rank(ascending=True))                   
#    
#    dfw['long']  = dfw['z_rank_top'] >= dfw['cutoff']
#    dfw['short'] = dfw['z_rank_bot'] >= dfw['cutoff']   

    


###    determine whether mkt cap is out of bounds
#    ans = pd.concat([pd.DataFrame(sect_count) ,(df_sect_wts * implied_total), df_sect_wts], axis=1, join='outer')
#    ans.columns = ['count','new_count','mkt_cap']
#    ans['out_bounds'] = ans['count'] < ans['new_count']
#    print date1, "ANSWER:", ans

#    v = [date1, sect, num, wgt[0], implied_total]
#    dc = dict(zip(cols,v))
#    da = pd.DataFrame(list(dc.iteritems())).T
#    da.columns = da.iloc[0]    
#    da = da.reindex(da.index.drop(0))
#    answer = pd.concat([answer, da])


###### Calculate Number of companies to include
#num_w = len(dfw.index)
#min_sector_weight = dfw['sector_wts'].min()
#min_wtd_sector = dfw.loc[dfw['sector_wts'].idxmin()]['industry_sector']
#min_wtd_sector_count = len(dfw[dfw['industry_sector'] ==min_wtd_sector ])
#min_wtd_include = round( min_wtd_sector_count / min_sector_weight, 0)
#
#if ( min_wtd_include > (min_wtd_sector_count -2)  ):
#    print "hi"
#
#if (min_wtd_include > (min_wtd_sector_count -2)  ):
#    print "lo"
#    
#syms_per_sect = (min_wtd_include * df_sect_wts['historical_market_cap']).apply(lambda x: round(x,0))
#dfw.loc[:,'syms_per_sect'] = dfw['industry_sector'].map(syms_per_sect)



###### Rank companies within sector
# dfw.loc[:,'z_rank_top'] = dfw.groupby('industry_sector')['z_composite'].apply(lambda x: x.rank(ascending=False))
# dfw.loc[:,'z_rank_bot'] = dfw.groupby('industry_sector')['z_composite'].apply(lambda x: x.rank(ascending=True))                   










                   
                   
#tickers = df.index.tolist()
##base_dir, start_date, end_date, index_name
#directory = """O:\AAA-IndividualFolders\MSkinner\Research\Equities\BalanceSheet\\PRICES\\"""
#date1 = dt.datetime(year=2014, month=7, day= 9)
#date2   = dt.datetime(year=2014, month=8, day= 9)
#prices = bs.get_prices(base_dir=directory, start_date=date1, end_date=date2, ticker_list=tickers, name=stock_index)


#def get_prices( base_dir, start_date, end_date, index_name):



