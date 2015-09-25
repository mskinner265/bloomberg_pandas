# -*- coding: utf-8 -*-
"""
Created on Fri Aug 07 11:28:56 2015

@author: bloomberg
"""
import datetime as dt
import pandas as pd
import numpy as np
import os.path
import sys
from math import floor


#def z_trimmed_df(df, col, percentile):
#    trimmed_med = np.percentile(df[col].values, percentile, interpolation='lower')
#    # print "trimmed median: ", trimmed_med
#    z = (df[col] - trimmed_med) / np.std(df[col])
#    dfz = pd.DataFrame(z)
#    dfz = dfz.rename(columns = {col: 'z_'+col})
#    print "%%%%%%%%%%%%%%%%%%%%%%%%%%% trimmed median: ", dfz
#    return dfz


def z_trimmed(val, percentile=80):
    np_val = np.array(val)    
    limit = np.percentile(np_val[np.isfinite(np_val)], percentile, interpolation='lower')
    trimmed = np_val[ np_val <= limit]
    trimmed_median = np.median( trimmed )
    return (val - trimmed_median) / np.std(trimmed)



def get_balance_sheet_data(base_dir, asofdate, index_name, delim='|'):
                    #col_list=['oper_margin','return_com_eqy'], 
                    #col_weightings=[1/3., 2/3.],
                    #exclude_financials=1):
    ### !!! base_dir = """O:\AAA-IndividualFolders\MSkinner\Research\Equities\BalanceSheet\\CSV\\"""
    #delim = '|'    
    index_list = [index_name]
    fn = base_dir + str(index_list[0]) + '_' + asofdate.strftime('%Y%m%d') + '.csv'    
    print "Looking for file:", fn
    if (os.path.isfile(fn)):
        ####################################################################################
        ## Read data from file
        print "file exists, reading: ", fn
        dfr = pd.DataFrame.from_csv(fn, index_col=0, sep=delim)
        dfr.index.names = ['member']
        ####################################################################################
    else:
        ####################################################################################
        ### Or get bloomberg data
        ####################################################################################
        sys.path.insert(0, 'O:/AAA-IndividualFolders/MSkinner/Research/Equities/BalanceSheet/LIB/')
        import get_bloomberg_data as gbd
        sys.path.insert(0, 'O:/AAA-IndividualFolders/MSkinner/Code/BBG_API/')
        import pybbg_k as pybbg

        df = gbd.get_index(index_list, asofdate)
        df['member'] += ' Equity'  ### add ' Equity' to each ticker name
        t  = df['member'].tolist() 
        df = df.set_index('member')
        df2 = gbd.get_bics(t, asofdate)
        df3 = gbd.get_hist_refdata(t, asofdate, 'ICB_INDUSTRY_NAME')
        df4 = gbd.get_hist_refdata(t, asofdate, 'HISTORICAL_MARKET_CAP')
        df5 = gbd.get_hist_refdata(t, asofdate, 'OPER_MARGIN')        
        df6 = gbd.get_hist_refdata(t, asofdate, 'RETURN_COM_EQY')        
        #df7 = gbd.get_hist_refdata(t, asofdate, 'BS_CUR_ASSET_REPORT')
        #df8 = gbd.get_hist_refdata(t, asofdate, 'BS_CUR_LIAB')
        df9 = gbd.get_hist_refdata(t, asofdate, 'CUR_RATIO')
        df10 = gbd.get_hist_refdata(t, asofdate, '5YR_AVG_RETURN_ON_INVESTED_CPTL')
        df11 = gbd.get_hist_refdata(t, asofdate, 'LT_DEBT_TO_COM_EQY')
        dfr=pd.concat([df, df2, df3, df4, df5, df6, df9, df10, df11], axis=1, join='outer')    
        ### Write to file when done getting from bbg   
        dfr.to_csv(fn,sep=delim,index_label=asofdate.strftime('%Y.%m.%d'))
        dfr = dfr.convert_objects(convert_numeric=True)
        print "File written to:", fn
    return dfr

#    ### def construct_portfolio(): 
#    ###### Begin Portfolio Calculation   
#    #col_names = ['oper_margin','return_com_eqy']
#    #z_col_names = ['z_oper_margin','z_return_com_eqy']
#    col_names = col_list
#    z_col_names = ['z_' + s for s in col_list]
#    
###### Exclude Financial sector companies
def exclude_financials_df(df, sector_name='icb_industry_name', fin_sector='financials'):
    exclude_sectors = ['financial', 'diversified', 'financials', fin_sector]
    dfx =  df[ df[sector_name].str.lower().apply( lambda x: x not in exclude_sectors )]
    return dfx

###### Calc Index industry weightings by mkt cap by industry
def calc_maket_cap_col(df, mkt_cap_field='historical_market_cap', industry_field='icb_industry_name'):
    tot_mkt_cap = df[ mkt_cap_field ].sum()
    sect_wts = df.groupby( industry_field )[ mkt_cap_field ].apply(lambda x: x.sum() / float(tot_mkt_cap))
    #df_sect_wts = pd.DataFrame(sect_wts)
    #df['sector_wts'] = df['industry_sector'].map(sect_wts)
    return df[ industry_field ].map(sect_wts)

###### Calc Index industry weightings by mkt cap by industry
def calc_maket_cap_df(df, mkt_cap_field='historical_market_cap', industry_field='icb_industry_name'):
    tot_mkt_cap = df[ mkt_cap_field ].sum()
    sect_wts = df.groupby( industry_field )[ mkt_cap_field ].apply(lambda x: x.sum() / float(tot_mkt_cap))
    return pd.DataFrame(sect_wts)

###### Calc Composite Z-score
def calc_composite_z_col(   df, industry_field='icb_industry_name', 
                            col_names=['oper_margin','return_com_eqy'], col_weightings=[1/3., 2/3.]):
    
    for c in col_names:
        df['z_'+c]=df.groupby( industry_field )[c].apply(z_trimmed)
    z_col_names = ['z_' + s for s in col_names]        
    z_weights = pd.DataFrame(col_weightings, index=z_col_names)
    #dfr['z_composite'] = dfr[z_col_names].dot(z_weights)
    return df[z_col_names].dot(z_weights)

###### Calc Composite Z-score
def calc_z(   df_in, industry_field='icb_industry_name', 
                            col_names=['oper_margin','return_com_eqy'], col_weightings=[1/3., 2/3.]):
    
    df = df_in.copy()
    for c in col_names:
        df['z_'+c]=df.groupby( industry_field )[c].apply(z_trimmed)
    z_col_names = ['z_' + s for s in col_names]        
    z_weights = pd.DataFrame(col_weightings, index=z_col_names)
    df['z_composite']=df[z_col_names].dot(z_weights)
    #dfr['z_composite'] = dfr[z_col_names].dot(z_weights)
    return df

    
###### Exclude outliers    
def windsorize_df( df, col='z_composite',cutoff=3):    
    df_temp = df[ abs(df[ col ]) < cutoff]    
    dfw = df_temp.copy()
    return dfw    
    
#    ###### Rank companies within sector
#    dfw.loc[:,'z_rank_top'] = dfw.groupby('industry_sector')['z_composite'].apply(lambda x: x.rank(ascending=False))
#    dfw.loc[:,'z_rank_bot'] = dfw.groupby('industry_sector')['z_composite'].apply(lambda x: x.rank(ascending=True))
   

###### Calculate Number of companies to include
#def calc_syms_per_sect_col(df, df_sector_wts, weights='sector_wts'):
#
#    min_sector_weight = df['sector_wts'].min()
#    min_wtd_sector = df.loc[df['sector_wts'].idxmin()]['industry_sector']
#    min_wtd_sector_count = len(df[df['industry_sector'] ==min_wtd_sector ])
#    sym_count = round( min_wtd_sector_count / min_sector_weight, 0)
#    syms_per_sect = (sym_count * df_sect_wts['historical_market_cap']).apply(lambda x: round(x,0))
#    dfw.loc[:,'syms_per_sect'] = dfw['industry_sector'].map(syms_per_sect)
#    
#    
#    ###### Construct long/short index based on composite z-score ranking
#    dfw.loc[:,'long'] = (dfw['z_rank_top'] <= dfw['syms_per_sect'])
#    dfw.loc[:,'short'] = (dfw['z_rank_bot'] <= dfw['syms_per_sect'])
#
#
#
#    fn = base_dir + str(index_list[0]) + '_zscores_' + asofdate.strftime('%Y%m%d') + '.csv'    
#    dfw.to_csv(fn,sep=',',index_label=asofdate.strftime('%Y.%m.%d'))
#    ### return dfw
#    return dfw[['industry_sector','sector_wts','syms_per_sect','long','short']]


#def get_prices( base_dir, start_date, end_date, ticker_list, name):
#    fn = base_dir + name + '_' + start_date.strftime('%Y%m%d') + '_'+ end_date.strftime('%Y%m%d') + '.csv'    
#    print 'checking for file: ', fn
#    if (os.path.isfile(fn)):
#        ####################################################################################
#        ## Read data from file
#        print 'File exists, loading data...'
#        prices = pd.DataFrame.from_csv(fn, index_col=0, sep=',')
#        prices.index.names = ['ticker']  ### This is actually the 'date column
#        ####################################################################################
#    else:
#        ####################################################################################
#        ### Or get bloomberg data
#        ####################################################################################
#        ###   base_dir="""O:\AAA-IndividualFolders\MSkinner\Research\Equities\BalanceSheet\\PRICES\\"""        
#        print 'File does not exist, loading from bloomberg...'        
#        #import get_bloomberg_data as gbd
#        sys.path.insert(0, 'O:/AAA-IndividualFolders/MSkinner/Code/BBG_API/')
#        #df = gbd.get_index(index_list, start_date)
#        #df['member'] += ' Equity'  ### add ' Equity' to each ticker name
#        #t  = df['member'].tolist() 
#        #df = df.set_index('member')
#        bbg = pybbg.Pybbg()
#        bbg.service_refData()
#        prices = bbg.bdh(ticker_list, ['PX_LAST'], 
#                         start_date=start_date.strftime('%Y%m%d'), 
#                        end_date=end_date.strftime('%Y%m%d'), 
#                        periodselection = 'DAILY')
#        fn = base_dir + name + '_' + start_date.strftime('%Y%m%d') + '_'+ end_date.strftime('%Y%m%d') + '.csv' 
#        prices.to_csv(fn,sep=',') #,index_label=name)
#    
#    
#    return prices 


def get_index_inclusion_df(df_windsor, df_sector_mkt_cap_wts,
                           ind_col='icb_industry_name', wgt_col='weight', mkt_cap_col='historical_market_cap'):
                    
    df_windsorized = df_windsor.copy()                    
    ### sym count by industry
    sect_count = df_windsorized.groupby(ind_col).agg('count')[wgt_col]
    
    ### create [historical_market_cap, names, ratio of wght to mkt cap]    
    df_pct = pd.concat([df_sector_mkt_cap_wts, pd.DataFrame(sect_count)], axis=1, join='outer')
    df_pct.rename(columns={wgt_col:'sym_count', mkt_cap_col:'weight'}, inplace=True)
    
    ### 'pct' is the amount a single name of a sector represents in weighting (by market cap)
    df_pct['pct'] = df_pct[ wgt_col ].div(df_pct['sym_count'])
    print "#################### Sector,Count"
    for k,v in df_pct['sym_count'].iteritems():
        print k,",",v
    
    max_rep_sect = df_pct['pct'].idxmax()
    print "max_rep_sect:",max_rep_sect
    
    ### calculate number of names in the index
    implied_total = df_pct['sym_count'].loc[max_rep_sect] / df_pct['weight'].loc[max_rep_sect]
    implied_total = floor(implied_total)    
    print "implied_total:", implied_total
    df_pct['include'] = df_pct['weight'] * implied_total
    df_pct['include'] = (df_pct['weight']* implied_total).apply(floor)
    df_windsorized['cutoff'] = df_windsorized['icb_industry_name'].map(df_pct['include'])
    
    #dfw.loc[:,'z_rank_top'] = 
    df_windsorized['z_rank_top'] = df_windsorized.groupby('icb_industry_name')['z_composite'].apply(lambda x: x.rank(ascending=False))
    df_windsorized['z_rank_bot'] = df_windsorized.groupby('icb_industry_name')['z_composite'].apply(lambda x: x.rank(ascending=True))                   
    
    df_windsorized['long']  = df_windsorized['z_rank_top'] <= df_windsorized['cutoff']
    df_windsorized['short'] = df_windsorized['z_rank_bot'] <= df_windsorized['cutoff']     
    
    #for k,icb,zc,zt,zb,c,lg,sh in df_windsorized[['icb_industry_name','z_composite','z_rank_top','z_rank_bot','cutoff','long','short']].itertuples():
    #    print ',' . join(map ( str, [k,icb,zc,zt,zb,c,lg,sh]))
    
    return df_windsorized    
    


def get_historical_prices(tickers, index_name, base_dir, start_date, end_date, delim='|'):
    sdt = start_date.strftime('%Y%m%d')
    edt = end_date.strftime('%Y%m%d')
    fn = base_dir + index_name + '_' + sdt + '_'+ edt + '.csv' 
    if (os.path.isfile(fn)):
        ####################################################################################
        ## Read data from file
        print 'Prices File exists, loading data...', fn
        prices = pd.DataFrame.from_csv(fn, index_col=0, sep=delim)
        prices.index.names = [index_name]
        ####################################################################################
    else:    
        sys.path.insert(0, 'O:/AAA-IndividualFolders/MSkinner/Code/BBG_API/')
        import pybbg_k as pybbg

        print 'Requesting prices from Bloomberg'
        bbg = pybbg.Pybbg()
        bbg.service_refData()
        prices = bbg.bdh(tickers, ['PX_LAST'], start_date=sdt, end_date=edt, periodselection = 'DAILY')
        #prices.to_csv(fn,sep=',',index_label=index_name)
        prices.to_csv(fn,sep=delim)
        print 'Prices file written:', fn
    return prices



def get_historical_index_members(base_dir, asofdate, index_name, delim='|'):
    #delim = '|'    
    index_list = [index_name]
    fn = base_dir + str(index_list[0]) + '_' + asofdate.strftime('%Y%m%d') + '.csv'    
    
    if (os.path.isfile(fn)):
        ####################################################################################
        ## Read data from file
        print "file exists, reading: ", fn
        dfr = pd.DataFrame.from_csv(fn, index_col=0, sep=delim)
        dfr.index.names = ['member']
        ####################################################################################
    else:
        ####################################################################################
        ### Or get bloomberg data
        ####################################################################################
        sys.path.insert(0, 'O:/AAA-IndividualFolders/MSkinner/Research/Equities/BalanceSheet/LIB/')
        import get_bloomberg_data as gbd
        sys.path.insert(0, 'O:/AAA-IndividualFolders/MSkinner/Code/BBG_API/')
        import pybbg_k as pybbg

        df = gbd.get_index(index_list, asofdate)
        df['member'] += ' Equity'  ### add ' Equity' to each ticker name
        t  = df['member'].tolist() 
        dfr = df.set_index('member')

    dfr.to_csv(fn, sep=delim)
    return dfr

# bd = dt.datetime(year=2001, month=1,day=1)
# fd = dt.datetime(year=2001, month=4,day=1)
# sd = dt.datetime(year=2001, month=6,day=30)
# ed = dt.datetime.today()
#pd.date_range('1/1/2011', periods=3, freq='A')
