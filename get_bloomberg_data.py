# -*- coding: utf-8 -*-
"""
Created on Thu Jul 16 17:07:48 2015

@author: bloomberg
"""
from pandas import Series, DataFrame, Panel
from datetime import datetime, date, time
import pandas as pd
import sys
################## User defined imports and parameters
sys.path.insert(0, 'O:/AAA-IndividualFolders/MSkinner/Code/BBG_API/')
import pybbg_k as pybbg
from math import ceil
######################################################################

def get_historic_ref(ticker, fld_list, overrides, override_dates, column_names, field_names=None, set_ind=True):
    if (not field_names):
        field_names=fld_list
    
    bbg = pybbg.Pybbg()
    bbg.service_refData()
    df = bbg.ref_override(ticker, fld_list, 
                          overrides, override_dates, 
                          column_names, field_names
                           )
    ### print "RETURNED DF FROM ref_overrride:" , df
    if (set_ind):
        df.set_index('member', inplace=True)
    return df

def get_yr_qtr(dt):
    if dt.month < 4:
        qtr = 'Q4' 
        yr  = dt.year -1 
    else: 
        qtr = 'Q'+str(int(ceil(dt.month/3.)-1)) # (-1 to get to end of recent quarter)
        yr = dt.year

    return [yr, qtr]

#####################################################
date1 = datetime(year=2013,month=3,day=03)
#####################################################

########################### REQUEST INDEX MEMBERSHIP
#ticker = ['VIG CP EQUITY','CETV CP Equity','CEZ CP Equity','FOREG CP Equity']
tx = ['S5TELS INDEX']
#t = ['A US EQUITY']
#t = ['SPX INDEX']

def get_index(index_ticker, asof_date):
    f1 = ['INDX_MWEIGHT_HIST']
    o = ['END_DATE_OVERRIDE']
    od = [asof_date.strftime('%Y%m%d')]
    cn = ['member','weight']
    f2 = ['Index Member','Percent Weight']
    df = get_historic_ref(ticker=index_ticker, fld_list=f1, overrides=o, override_dates=od, column_names=cn, field_names=f2, set_ind=False)
    return df



### df['member'] += ' Equity'  ### add ' Equity' to each ticker name
### t  = df['member'].tolist() 
### df = df.set_index('member')


############# REQUEST NAME
def get_names(ticker_list, asof_date):
    f1 = ['NAME']
    o = ['END_DATE_OVERRIDE']
    od = [asof_date.strftime('%Y%m%d')]
    cn = ['member','name']
    df1 = get_historic_ref(ticker=ticker_list, fld_list=f1, overrides=o, override_dates=od, column_names=cn)
    return df1

######################## REQUEST SECTOR (BICS)
def get_bics(ticker_list, asof_date):
    f1 = ['INDUSTRY_SECTOR']
    o = ['END_DATE_OVERRIDE']
    od = [asof_date.strftime('%Y%m%d')]
    cn = ['member','industry_sector']
    df2 = get_historic_ref(ticker=ticker_list, fld_list=f1, overrides=o, override_dates=od, column_names=cn)
    return df2
    
##################################
####    FOLLOWING REQUESTS USE
####    THESE overrides and dates
#o = ['EQY_FUND_YEAR','FUND_PER']
#od = get_yr_qtr(date1)
###################################
###################################
#
################## REQUEST CURRENT RATIO    
def get_hist_refdata(ticker_list, asof_date, bbg_field_name):
    #f1 = ['CUR_RATIO']
    #cn  = ['member','cur_ratio']
    f1 = [bbg_field_name]
    o = ['EQY_FUND_YEAR','FUND_PER']
    od = get_yr_qtr(asof_date)
    cn  = ['member',bbg_field_name.lower()]
    df3 = get_historic_ref(ticker=ticker_list, fld_list=f1, overrides=o, override_dates=od, column_names=cn)
    return df3


def get_hist_refAnnual(ticker_list, asof_date, bbg_field_name):
    #f1 = ['CUR_RATIO']
    #cn  = ['member','cur_ratio']
    f1 = [bbg_field_name]
    o = ['EQY_FUND_YEAR']     #,'FUND_PER']
    od = [asof_date.year]    #od = get_yr_qtr(asof_date)
    cn  = ['member',bbg_field_name.lower()]
    df3 = get_historic_ref(ticker=ticker_list, fld_list=f1, overrides=o, override_dates=od, column_names=cn)
    return df3


######################################### REQUEST ROIC
#f1 = ['5YR_AVG_RETURN_ON_INVESTED_CPTL']
#cn = ['member','5yr_avg_return_on_invested_cptl']
#df4 = get_historic_ref(ticker=t, fld_list=f1, overrides=o, override_dates=od, column_names=cn)
#
############################ REQUEST LT D/E
#f1 = ['LT_DEBT_TO_COM_EQY']
#cn = ['member','lt_debt_to_com_eqy']
#df5 = get_historic_ref(ticker=t, fld_list=f1, overrides=o, override_dates=od, column_names=cn)
#
############################### REQUEST LT D/E
#f1 = ['HISTORICAL_MARKET_CAP']
#cn = ['member','historical_market_cap']
#df6 = get_historic_ref(ticker=t, fld_list=f1, overrides=o, override_dates=od, column_names=cn)
#
#
############################### PRINT TO FILE
#dfr=pd.concat([df, df1, df2, df3, df4, df5, df6 ], axis=1, join='outer')
#base_dir = """O:\AAA-IndividualFolders\MSkinner\Research\Equities\BalanceSheet\\"""
#fn = base_dir + str(tx[0]) + '_' + date1.strftime('%Y%m%d') + '.csv'
#dfr.to_csv(fn,sep=',',index_label=date1.strftime('%Y.%m.%d'))





#print "DONE!"


#dfr.to_csv('O:\AAA-IndividualFolders\MSkinner\Research\Equities\BalanceSheet\sp500.csv',sep=',')

########################################
### bdib function
########################################
#ticker = 'KOSPI2 Index'
#fld_list = ['open', 'high', 'low', 'close', 'volume', 'numEvents', 'value']






