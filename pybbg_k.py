# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 13:47:12 2014
@author: kian
"""

import blpapi
from collections import defaultdict
from pandas import DataFrame
from datetime import datetime, date, time
import pandas as pd
import numpy as np

SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")
INDEX_MEM = blpapi.Name("Index Member")
PCT_WEIGHT = blpapi.Name("Percent Weight")


class Pybbg():
    def __init__(self, host='localhost', port=8194):
        """
        Starting bloomberg API session
        close with session.close()
        """
        
        # Fill SessionOptions
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost('localhost')
        sessionOptions.setServerPort(8194)
    
        # Create a Session
        self.session = blpapi.Session(sessionOptions)
    
        # Start a Session
        if not self.session.start():
            print "Failed to start session."
        
        self.session.nextEvent()
        
        
    def service_refData(self):
        """
        init service for refData
        """
        print "-----BEGIN----!"
        # Open service to get historical data from
        if not self.session.openService("//blp/refdata"):
            print "Failed to open //blp/refdata"
        
        self.session.nextEvent()
        
        # Obtain previously opened service
        self.refDataService = self.session.getService("//blp/refdata")
        
        self.session.nextEvent() 
    
    def bdh(self, ticker_list, fld_list, start_date, end_date=date.today().strftime('%Y%m%d'), periodselection = 'DAILY'):
        """
        Get ticker_list and field_list
        return pandas multi level columns dataframe
        """
        # Create and fill the request for the historical data
        request = self.refDataService.createRequest("HistoricalDataRequest")
        for t in ticker_list:
            request.getElement("securities").appendValue(t)       
        for f in fld_list :
            request.getElement("fields").appendValue(f)
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", periodselection)
        request.set("startDate", start_date)
        request.set("endDate", end_date)
        
        print "Sending Request:", request
        # Send the request
        self.session.sendRequest(request)
        # defaultdict - later convert to pandas
        data = defaultdict(dict)
        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                ticker = msg.getElement('securityData').getElement('security').getValue()
                fieldData = msg.getElement('securityData').getElement('fieldData')
                for i in range(fieldData.numValues()) :
                    for j in range(1,fieldData.getValue(i).numElements()) :
                        data[(ticker, fld_list[j-1])][fieldData.getValue(i).getElement(0).getValue()] = fieldData.getValue(i).getElement(j).getValue()
        
            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
        data = DataFrame(data)
        data.columns = pd.MultiIndex.from_tuples(data, names=['ticker', 'field'])
        data.index = pd.to_datetime(data.index)
        return data

    def bdib(self, ticker, fld_list, startDateTime, endDateTime, eventType='TRADE', interval = 1):
        """
        Get one ticker (Only one ticker available per call); eventType (TRADE, BID, ASK,..etc); interval (in minutes)
                ; fld_list (Only [open, high, low, close, volumne, numEvents] availalbe)
        return pandas dataframe with return Data
        """
        # Create and fill the request for the historical data
        request = self.refDataService.createRequest("IntradayBarRequest")
        request.set("security", ticker)
        request.set("eventType", eventType)
        request.set("interval", interval)  # bar interval in minutes        
        request.set("startDateTime", startDateTime)
        request.set("endDateTime", endDateTime)
        
        print "Sending Request:", request
        # Send the request
        self.session.sendRequest(request)
        # defaultdict - later convert to pandas
        data = defaultdict(dict)
        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                barTickData = msg.getElement('barData').getElement('barTickData')
                for i in range(barTickData.numValues()) :
                    for j in range(len(fld_list)) :
                        data[(fld_list[j])][barTickData.getValue(i).getElement(0).getValue()] = barTickData.getValue(i).getElement(fld_list[j]).getValue()
        
            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
        data = DataFrame(data)
        data.index = pd.to_datetime(data.index)
        return data
        
    def stop(self):
        self.session.stop()
        
        
###############################################################################        
    def processMessage(self, msg, column_names, field_names):
        print "qqqqqqqqqqqqqqqq"
        #c1 = ['member','weight']
        #c2 = 'weight'
        df = pd.DataFrame(columns=column_names)
        securityDataArray = msg.getElement(SECURITY_DATA)
        for securityData in securityDataArray.values():
            print securityData.getElementAsString(SECURITY)
            fieldData = securityData.getElement(FIELD_DATA)
            for field in fieldData.elements():
                if not field.isValid():
                    print field.name(), "is NULL."
                elif field.isArray():
                    # The following illustrates how to iterate over complex
                    # data returns.
                    for i, row in enumerate(field.values()):
                        bbg_fields   = [blpapi.Name(x) for x in field_names]
                        element_list = [row.getElement(x) for x in bbg_fields ]
                        values_list  = [x.getValueAsString() for x in element_list]                        
                        #print "VALUES LIST : ", values_list, " LEN: ", len(values_list), " TYPE: ", type(values_list)
                        #print "COLUMN NAMES: ", column_names, "LEN: ", len(column_names), " TYPE: ", type(column_names)
                        df = pd.DataFrame([values_list], columns=column_names).append(df,ignore_index=True)
                                               
#                        for j, field_val in enumerate(field_names)
#                            print                         
#                        
#                        
#                            print "Row %d: %s" % (i, row)
#                            member = row.getElement(INDEX_MEM)
#                            weight = row.getElement(PCT_WEIGHT)
#                            print "mem: ",member, " wgt: ", weight
#                            m = member.getValueAsString()                        
#                            w = weight.getValueAsFloat()                        
#                            print "m: ", m
#                            print "w: ", w
#                            #df = pd.DataFrame([[m,w]],columns=[c1,c2]).append(df,ignore_index=True)
#                            df = pd.DataFrame([[m,w]],columns=column_names).append(df,ignore_index=True)
#                            print "df===",df
                        #TODO: df.append(member=m,weight=w)
                        #TODO: create DF in line 143 with column names
                        #TODO: append to DF in 160 by column name
                        #
                        #print "type row, ", type(row)
                        #member = row.getElement(INDEX_MEM)
                        #weight = row.getElement(PCT_WEIGHT)
                        #print "type member, ", type(member)
                        #print "type weight, ", type(weight)
                        #print "member: ", member, " pct wgt: ", weight

                else:
                    print "%s =!= %s" % (field.name(), field.getValueAsString())
                    sym = securityData.getElementAsString(SECURITY)
                    val = field.getValueAsString()
                    df = pd.DataFrame([[sym,val]], columns=column_names).append(df,ignore_index=True)                    
                    #r = Series(field.getValueAsFloat, index=field.name())                    
                    #df.append()
    
            fieldExceptionArray = securityData.getElement(FIELD_EXCEPTIONS)
            for fieldException in fieldExceptionArray.values():
                errorInfo = fieldException.getElement(ERROR_INFO)
                print "%s: %s" % (errorInfo.getElementAsString("category"),
                                  fieldException.getElementAsString(FIELD_ID))
        
        return df
        
  ##################################
  ### Begin Experimental Functions
  ##################################       
    def ref_override(self, ticker_list, fld_list, override_list, override_date_list, column_names, field_names):
        print "aaaaaaaaaaaaaaaa"
        refDataService = self.session.getService("//blp/refdata")
        request = refDataService.createRequest("ReferenceDataRequest")
        for t in ticker_list:
            request.append("securities",t)
        for f in fld_list :
            request.append("fields",f)
        
        overrides = request.getElement("overrides")
        override_params = []        
        for i in range(len(override_list)):
            override_params.append(overrides.appendElement())
            override_params[i].setElement("fieldId",override_list[i])
            override_params[i].setElement("value",override_date_list[i])
        
        #override1 = overrides.appendElement()
        #override1.setElement("fieldId", override)
        #override1.setElement("value", override_date)        

        #override2 = overrides.appendElement()
        #override2.setElement("fieldId", o2)
        #override2.setElement("value", o2_d)        
        
        
        
        
        ret_val = pd.DataFrame()
        print "Sending Request:", request
        # Send the request
        ### self.session.sendRequest(request)
        cid = self.session.sendRequest(request)
        print "============"                    
        #response
        try:
            print "************"
            # Process received events
            while(True):
                # We provide timeout to give the chance to Ctrl+C handling:
                print "^^^^^^^^^^^^^^"
                ev = self.session.nextEvent(500)
                for msg in ev:
                    print "+++++++++++++"
                    if cid in msg.correlationIds():
                        # Process the response generically.
                        ret_val = ret_val.append( self.processMessage(msg, column_names, field_names))
                    print msg

                    #ticker = msg.getElement('').getElement('security').getValue()
                    #ticker = msg.getElement('securityData').getElement('security').getValue()
                    #fieldData = msg.getElement('securityData').getElement('fieldData')
                # Response completly received, so we could exit
                if ev.eventType() == blpapi.Event.RESPONSE:
                    break
        finally:
            # Stop the session
            self.session.stop()    
        #return response
        return ret_val
    
 #ReferenceDataResponse = {
#    securityData[] = {
#        securityData = {
#            security = "SPX INDEX"
#            eidData[] = {
#            }
#            fieldExceptions[] = {
#            }
#            sequenceNumber = 0
#            fieldData = {
#                INDX_MWEIGHT_HIST[] = {
#                    INDX_MWEIGHT_HIST = {
#                        Index Member = "A UN"
#                        Percent Weight = -0.000000
#                    }
#                    INDX_MWEIGHT_HIST = {
#                        Index Member = "AA UN"
#                        Percent Weight = -0.000000        
        
        
#        # defaultdict - later convert to pandas
#        data = defaultdict(dict)
#        # Process received events
#        while(True):
#            # We provide timeout to give the chance for Ctrl+C handling:
#            ev = self.session.nextEvent(500)
#            for msg in ev:
#                print "msg:"+ str(msg)                
#                #ticker = msg.getElement('securityData').getElement('security').getValue()
#                #fieldData = msg.getElement('securityData').getElement('fieldData')
#                #for i in range(fieldData.numValues()) :
#                #    for j in range(1,fieldData.getValue(i).numElements()) :
#                #        data[(ticker, fld_list[j-1])][fieldData.getValue(i).getElement(0).getValue()] = fieldData.getValue(i).getElement(j).getValue()
#        
#            if ev.eventType() == blpapi.Event.RESPONSE:
#                # Response completly received, so we could exit
#                break
#        data = DataFrame(data)
#        data.columns = pd.MultiIndex.from_tuples(data, names=['ticker', 'field'])
#        data.index = pd.to_datetime(data.index)
#        return data
        
#### sample
#    request = refDataService.createRequest("ReferenceDataRequest")
#    # append securities to request
#    request.append("securities", "SPX Index")
#    # append fields to request
#    request.append("fields", "INDX_MWEIGHT_HIST")
#
#    # add overrides
#    overrides = request.getElement("overrides")
#    override1 = overrides.appendElement()
#    override1.setElement("fieldId", "END_DATE_OVERRIDE")
#    override1.setElement("value", "20150719")
#
#    print "Sending Request:", request
#    session.sendRequest(request)
#
#    try:
#        # Process received events
#        while(True):
#            # We provide timeout to give the chance to Ctrl+C handling:
#            ev = session.nextEvent(500)
#            for msg in ev:
#                print msg
#            # Response completly received, so we could exit
#            if ev.eventType() == blpapi.Event.RESPONSE:
#                break
#    finally:
#        # Stop the session
#        session.stop()