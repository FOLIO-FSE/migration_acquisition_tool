import backup_restore as br
import migration_report as mr
import functions_AcqErm as mf
from report_blurbs import Blurbs
import notes_class as notes
import datetime
import warnings
import dataframe_class as pd
import json
import uuid
import os
import os.path
import requests
import io
import math
import csv
import time
import random
import logging
import validator
import ast
#from tabulate import tabulate
#import tkinter as tk
#from tkinter import filedialog, messagebox, ttk
import yaml
import shutil
from tqdm import tqdm
        
################################
##ORDERS CLASS
################################
class compositePurchaseorders():
    def __init__(self,client,path_dir):
        try:
            self.migrationreport_a=mr.MigrationReport()
            self.migrationreport_a.add_general_statistics("Alex test")
            self.customerName=client
            self.customerName=pd.dataframe()    
            self.getidfile=False
            dt = datetime.datetime.now()
            self.dt=dt.strftime('%Y%m%d-%H-%M')    
            
            self.path_dir=path_dir
            #os.mkdir(f"{path_dir}\\results")
            self.path_results=f"{path_dir}\\results"
            #os.mkdir(f"{path_dir}\\data")
            self.path_data=f"{path_dir}\\data"
            #os.mkdir(f"{path_dir}\\logs")
            self.path_logs=f"{path_dir}\\logs"
            #os.mkdir(f"{path_dir}\\refdata")
            self.path_refdata=f"{path_dir}\\refdata"
            self.path_mapping_files=f"{path_dir}\\mapping_files"
            logging.basicConfig(filename=f"{self.path_logs}\\composite_purchaseorders-{self.dt}.log", encoding='utf-8', level=logging.INFO,format='%(message)s')
            logging.basicConfig(filename=f"{self.path_logs}\\composite_purchaseorders-DEBUG-{self.dt}.log", encoding='utf-8', level=logging.DEBUG,format='%(message)s')
            mappingfile=self.path_mapping_files+"\\composite_purchase_order_mapping.json"
            if os.path.exists(mappingfile):  
                with open(mappingfile) as json_mappingfile:
                    self.mappingdata = json.load(json_mappingfile)
                field="compositePoLines[0].instanceId"
                valueinstanceid={}
                valueinstanceid=self.readcompositepurchaseorderMapping(folio_field=field)
                if valueinstanceid:
                    value=valueinstanceid.get("value")
                    if value!="":
                        filetoload=f"{self.path_results}\{value}"
                        #self.localinstanceId=self.customerName.importDataFrame(filetoload,sheetName="localinstaceId", dfname="localinstanceId")
                        self.localinstanceId=self.customerName.importDataFrame(filetoload,dfname="LOCAL InstanceId",distinct=['legacy_id_sierra'])
                        print(f"INFO local Instance Id file found, do you want to use this file?")
                        logging.info(f"INFO local Instance Id file found, do you want to use this file?")
                        self.getidfile=True
                field="id"
                valueinstanceid={}
                valueinstanceid=self.readcompositepurchaseorderMapping(folio_field=field)
                if valueinstanceid:
                    value=valueinstanceid.get("value")
                    if value!="":
                        filetoload=f"{self.path_results}\{value}"
                        #self.localinstanceId=self.customerName.importDataFrame(filetoload,sheetName="localinstaceId", dfname="localinstanceId")
                        print(f"INFO Reading UUIDs from file {filetoload}, do you want to use this file as PO ID / poLines?")
                        self.localinstanceId=self.customerName.importDataFrame(filetoload,dfname="purchaseOrdersId")
                        logging.info(f"INFO local Instance Id file found, do you want to use this file?")
                        self.getidfile=True
                
            else:
                print(f"ERROR the {self.path_mapping_files}\composite_purchase_order_mapping.json")
                self.flag=False
            self.productidsDictionary={"REPORT NUMBER":"37b65e79-0392-450d-adc6-e2a1f47de452","ISBN":"8261054f-be78-422d-bd51-4ed9f33c3422","ISSN":"913300b2-03ed-469a-8179-c1092c991227"}
            self.po_countworse=0
            self.counternotes=0
            self.po_count=0
            self.count=0
            countpol=0
            countpolerror=0
            self.countvendorerror=0
            self.countfoundrerror=0 
            self.po_count_new_instance=0

        except Exception as ee:
            print(f"ERROR: Orders Class {ee}")
            
             
           
    def checkingparameters(self, **kwargs):
        try:
            flag=True
            swpolines=kwargs['swpolines']
            schematosearch=kwargs['schematosearch']
            print(f"#########    {schematosearch}   ########")
            logging.info(f"#########    {schematosearch}   ########")
            sw=True
            tupla_order=[]
            tupla_mapping=[]        
            temp=[]    
            tempmap={}
            recordnotfound=[]
            dictem=[]
            if 'fieldtosearch' in kwargs:
                fieldtosearch=kwargs['fieldtosearch']
                if fieldtosearch in self.orders.columns:
                    schematosearch=kwargs['schematosearch']
                    print(f"{schematosearch} | counter")
                    logging.info(f"{schematosearch} | counter ") 
                    if swpolines:
                        #print(self.dfPolines[fieldtosearch].value_counts())
                        #print(f"{schematosearch} | count ")
                        for idx, name in enumerate(self.dfPolines[fieldtosearch].value_counts().index.tolist()):
                            countfield=self.dfPolines[fieldtosearch].value_counts()[idx]
                            print(f"{name} => {countfield}")
                            logging.info(f"{name} => {countfield}")
                        tupla_order = self.dfPolines[fieldtosearch].unique()                           
                    else:
                        for idx, name in enumerate(self.orders[fieldtosearch].value_counts().index.tolist()):
                            countfield=self.orders[fieldtosearch].value_counts()[idx]
                            if name=="":
                                name="blank"
                            print(f"{name} => {countfield}")
                            logging.info(f"{name} => {countfield}") 
                        tupla_order = self.orders[fieldtosearch].unique()
                    logging.info(f"{schematosearch}")
                    logging.info(f"{schematosearch} | counter")
                    logging.info(tupla_order)
                    if 'dfmapping' in kwargs:
                        dfmapping=kwargs['dfmapping']
                        rangetqdm=len(dfmapping)
                    else:
                        dfmapping=self.customerName.createDataFrame(columns=[])
                    resulttqdm = 0
                    if len(tupla_order)>0:
                        for i in tupla_order:
                            toSearch=str(i).strip()
                            if dfmapping.empty:
                                newvalue=toSearch
                            else:
                                newvalue=self.replace(dfmapping,toSearch)
                            if schematosearch=="organizations":
                                toSearchnewvalue=str(newvalue).strip()
                                newvalue=self.searchdata_dataframe(self.dforg,"code","id",toSearchnewvalue) 
                                #newvalue=self.replace(self.dforg,toSearchnewvalue)
                            elif schematosearch=="locations":
                                toSearchnewvalue=str(newvalue).strip()
                                newvalue=self.searchdata_dataframe(self.dflocations,"code","id",toSearchnewvalue) 
                            elif schematosearch=="funds":
                                toSearchnewvalue=str(newvalue).strip()
                                newvalue=self.searchdata_dataframe(self.dffunds,"code","id",toSearchnewvalue)
                            elif schematosearch=="fundsExpenseClass":
                                toSearchnewvalue=newvalue
                                newvalue=self.searchdata_dataframe(self.dfexpense,"code","id",toSearchnewvalue)
                            elif schematosearch=="mtype":
                                toSearchnewvalue=str(toSearch).strip()
                                newvalue=self.searchdata_dataframe(self.dfmtype,"name","id",toSearchnewvalue)
                            elif schematosearch=="localinstanceId":
                                toSearchnewvalue=str(toSearch).strip()
                                newvalue=self.searchdata_dataframe(self.localinstanceId,"legacy_id_sierra","folio_id",toSearchnewvalue)
                            if newvalue is None:
                                if toSearch!="":
                                    if schematosearch=="localinstanceId":
                                        mf.write_file(ruta=f"{self.path_logs}\\titlesNotFounds.log",contenido=f"{recordnotfound}")
                                        recordnotfound=[]
                                    else:
                                        recordnotfound.append(i)
                            else:
                                if swpolines:
                                    self.dfPolines[fieldtosearch] = self.dfPolines[fieldtosearch].replace([i],newvalue)
                                else:
                                    self.orders[fieldtosearch] = self.orders[fieldtosearch].replace([i],newvalue)
                                    tempmap[i]=newvalue
                                        
                    if len(recordnotfound)>0:
                            print(f"INFO {self.client} critical Error the following {schematosearch} does not exist  {recordnotfound}")
                            logging.info(f"INFO {self.client} critical Error the following {schematosearch} does not exist  {recordnotfound}")
                            flag=False

                                
                    else:
                        dt = datetime.datetime.now()
                        self.dt=dt.strftime('%Y%m%d-%H-%M')  
                        print(f"------------------------------------")
                        logging.info(f"{schematosearch}")
                        print(f"{self.dt} the following codes were replaced  for: {schematosearch}")
                        print(f"{schematosearch} | counter")
                        logging.info(f"{schematosearch}")
                        logging.info(f"{schematosearch} | counter")
                        for k,v in tempmap.items(): 
                            print(f"{k} => {v}")                        
                            logging.info(f"{k} => {v}")
                        if swpolines:
                            print(f"{self.dt} the following codes were replaced  for: {schematosearch}")
                            print(f"{schematosearch} | counter")
                            logging.info(f"{schematosearch}")
                            logging.info(f"{schematosearch} | counter")
                            for idx, name in enumerate(self.dfPolines[fieldtosearch].value_counts().index.tolist()):
                                countfield=self.dfPolines[fieldtosearch].value_counts()[idx]
                                print(f"{name} => {countfield}")
                                logging.info(f"{name} => {countfield}") 
                            #print(self.dfPolines[fieldtosearch].value_counts())
                            #logging.info(self.dfPolines[fieldtosearch].value_counts()) 
                        else:
                            print(f"{self.dt} the following codes were replaced  for: {schematosearch}")
                            print(f"{schematosearch} | counter")
                            logging.info(f"{schematosearch}")
                            logging.info(f"{schematosearch} | counter")
                            for idx, name in enumerate(self.orders[fieldtosearch].value_counts().index.tolist()):
                                countfield=self.orders[fieldtosearch].value_counts()[idx]
                                print(f"{name} => {countfield}")
                                logging.info(f"{name} => {countfield}") 
                            #print(self.orders[fieldtosearch].value_counts())
                            #logging.info(self.orders[fieldtosearch].value_counts()) 
            else:
                print(f"field no exist, not mappend")
                if schematosearch=='organizations':
                   flag=False
                   
            return flag
        except Exception as ee:
            print(f"ERROR: Checking {schematosearch} // {ee}")                  
            return flag                        
                            
                           
                                   
    def readcompositepurchaseorderMapping(self, **kwargs):
        try:
            valuesfield={}
            mapfile={}
            fieldToserch=str(kwargs['folio_field'])
            for i in self.mappingdata['data']:
                    if i['folio_field']==fieldToserch:
                            valuesfield['value']=str(i['value']).strip()
                            valuesfield['description']=str(i['description']).strip()
                            valuesfield['legacy_field']=str(i['legacy_field']).strip()
            return valuesfield
        except Exception as ee:
            print(f"ERROR: Orders Class {ee}")
                
    def readMappingfile(self):
        try:
            filetoload=self.path_mapping_files+f"\\acquisitionMapping.xlsx"            
            if os.path.exists(filetoload):
                myobj = datetime.datetime.now()
                self.dobj=myobj.strftime('%T')
                logging.info(f"{self.dobj} INFO Acquisition Mapping spreadsheet OK")
                print(f"{self.dobj} INFO Acquisition Mapping spreadsheet found")
                print(f"{self.dobj} INFO Reading Mapping {filetoload}")
                logging.info(f"{self.dobj} INFO Reading Mapping {filetoload}")
                self.acquisitionMethod=self.customerName.importDataFrame(filetoload,sheetName="acquisitionMethod", dfname="Acquisition Method")
                tuplaacquisitionMethod=["Approval Plan","Demand Driven Acquisitions (DDA)","Depository","Evidence Based Acquisitions (EBA)","Exchange","Gift","Purchase At Vendor System","Purchase","Technical"]                #print("Dataframe: Order Format")
                self.dfacquisitionMethod=self.customerName.importupla(tupla=tuplaacquisitionMethod,dfname="FOLIO Acquisition Method",columns=["name"])                
                self.orderFormat=self.customerName.importDataFrame(filetoload,sheetName="orderFormat", dfname="Order format")
                tuplaorderFormat=["Electronic Resource","P/E Mix","Physical Resource","Other"]
                self.dforderFormat=self.customerName.importupla(tupla=tuplaorderFormat,dfname="FOLIO Order format",columns=["name"])
                #print("Dataframe: Order Type")
                self.orderType=self.customerName.importDataFrame(filetoload,sheetName="orderType", dfname="Order type")
                self.dfordertype=self.customerName.importupla(tupla=["One-Time","Ongoing"],dfname="FOLIO Order type",columns=["name"])
                #tuplaordertype=
                #print("Dataframe: Payment Status")
                self.paymentStatus=self.customerName.importDataFrame(filetoload,sheetName="paymentStatus", dfname="Payment Status",columns=["name"])
                tuplapaymentstatus=["Awaiting Payment", "Cancelled","Fully Paid","Partially Paid","Payment Not Required","Pending","Ongoing"]
                self.dfpaymentStatus=self.customerName.importupla(tupla=tuplapaymentstatus,dfname="FOLIO Payment Status",columns=["name"])
                #print("Dataframe: Receipt Status")
                self.receiptStatus=self.customerName.importDataFrame(filetoload,sheetName="receiptStatus", dfname="Receipt Status")
                tuplareceiptStatus=["Awaiting Receipt", "Cancelled","Fully Received","Partially Received","Pending","Receipt Not Required","Ongoing"]
                self.dflocations=self.customerName.importupla(tupla=tuplareceiptStatus,dfname="FOLIO receiptStatus",columns=["name"])
                #print("Dataframe: WorkFlowStatus")
                self.workflowStatus=self.customerName.importDataFrame(filetoload,sheetName="workflowStatus", dfname="Workflow Status")
                self.workflowStatusEnum=["Pending","Open","Closed"]
                self.dfpaymentStatus=self.customerName.importupla(tupla=self.workflowStatusEnum,dfname="FOLIO Work Flow Status",columns=["name"])
                #print("Dataframe: Locations")
                self.locations=self.customerName.importDataFrame(filetoload,sheetName="locations", dfname="locations")
                self.dflocations=self.customerName.importupla(tupla=mf.jsontotupla(json_file=self.path_refdata+f"\\{self.client}_locations.json",schema="locations"),dfname="locations",columns=["id", "code","name","value","json"])
                #print("Dataframe: Funds/Expenses")
                self.fundsExpenseClass=self.customerName.importDataFrame(filetoload,sheetName="fundsExpenseClass", dfname="Expense Class",columns=["id", "code","name","value","json"])
                #print("Dataframe: Funds")
                self.funds=self.customerName.importDataFrame(filetoload,sheetName="funds",dfname="Funds")
                self.dffunds=self.customerName.importupla(tupla=mf.jsontotupla(json_file=self.path_refdata+f"\\{self.client}_funds.json",schema="funds"),dfname="funds",columns=["id", "code","name","value","json"])
                tupladistributionType=["amount","percentage"]
                self.dfdistributionType=self.customerName.importupla(tupla=tupladistributionType,dfname="FOLIO Fund Distribution Type",columns=["name"])
                #print("Dataframe: Organization code to Change - optional")
                self.organizationCodeToChange=self.customerName.importDataFrame(filetoload,sheetName="organizationCodeToChange", dfname="Organization Change Codes")
                #print(self.organizationCodeToChange)
                self.dforg=self.customerName.importupla(tupla=mf.jsontotupla(json_file=self.path_refdata+f"\\{self.client}_organizations.json",schema="organizations"),dfname="Organizations",columns=["id", "code","name","value","json"])
                self.dfmtype=self.customerName.importupla(tupla=mf.jsontotupla(json_file=self.path_refdata+f"\\{self.client}_mtypes.json",schema="mtypes"),dfname="Material Type",columns=["id", "code","name","value","json"])
                self.dfexpense=self.customerName.importupla(tupla=mf.jsontotupla(json_file=self.path_refdata+f"\\{self.client}_expenseClasses.json",schema="expenseClasses"),dfname="Expense Classes",columns=["id", "code","name","value","json"])
                #self.acqUnits=self.customerName.importupla(tupla=mf.jsontotupla(json_file=self.path_refdata+f"\\{self.client}_acquisitionsUnits.json",schema="acquisitionsUnits"),dfname="acquisitionsUnits",columns=["id", "code","name","json"])
                #self.billtoshipto=self.customerName.importupla(tupla=mf.jsontotupla(json_file=self.path_refdata+f"\\{self.client}_tenant.addresses.json",schema="configs"),dfname="tenant addresses",columns=["id", "code","name","value","json"])
            else:
                logging.info(f"ERROR Acquisition Mapping spreadsheet does not exist: {filetoload} check")
                print(f"ERROR Acquisition Mapping spreadsheet does not exist: {filetoload}")
                self.flag=False
                #return self.flag
            return self.flag
        except Exception as ee:
            print(f"ERROR: Critical please check that already exit the {filetoload} file {ee}")        
            self.flag=False
            return self.flag
            
    def readorders(self, client, **kwargs):
        countpol=0
        self.noprint=True
        self.client=client
        self.flag=True
        totalnotestoprocess=0
        self.polcont=0
        countpolerror=0
        self.flag=self.readMappingfile()
        if self.flag:
            logging.info(f"INFO READING ORDERS")
            print(f"INFO READING ORDERS")
            if 'dfOrders' in kwargs:      
                self.orders=kwargs['dfOrders']
                logging.info(f"INFO Dataframe Purchase Orders OK")
                print(f"INFO Dataframe Purchase Orders OK")
                if 'dfPolines' in kwargs: 
                    self.dfPolines=kwargs['dfPolines']
                    logging.info(f"INFO Dataframe poLines OK")
                    print(f"INFO Dataframe poLines OK")
                else:
                    self.dfPolines=kwargs['dfOrders']
                    logging.info(f"INFO Dataframe poLines from Purchase Orders Ok")
                    print(f"INFO Dataframe poLines from Purchase Orders OK")
                
                #print("\n"+"INFO Cheking Instance"+"\n")   
                #self.gettinginstance() 
                #PO
                print("\n"+"INFO Cheking DATA VS SPREADSHEET"+"\n")
#                if self.organizationCodeToChange.empty:
#                    if self.flag: self.flag=self.checkingparameters(schematosearch="organizations", fieldtosearch="vendor",dfmapping=self.organizationCodeToChange,swpolines=False)
                    #if self.flag: self.flag=self.checkingparameters(schematosearch="organizations", fieldtosearch="vendor",dfmapping=self.dforg,swpolines=False)
                    #if self.flag: self.flag=self.checkingparameters(schematosearch="organizations", fieldtosearch="compositePoLines[0].eresource.accessProvider",dfmapping=self.dforg,swpolines=False)
                    #if self.flag: self.flag=self.checkingparameters(schematosearch="organizations", fieldtosearch="compositePoLines[0].physical.materialSupplier",dfmapping=self.dforg,swpolines=False)     
#                else:
                if self.flag: self.flag=self.checkingparameters(schematosearch="organizations", fieldtosearch="vendor",dfmapping=self.organizationCodeToChange,swpolines=False)
                if self.flag: self.flag=self.checkingparameters(schematosearch="organizations", fieldtosearch="compositePoLines[0].eresource.accessProvider",dfmapping=self.organizationCodeToChange,swpolines=False)
                if self.flag: self.flag=self.checkingparameters(schematosearch="organizations", fieldtosearch="compositePoLines[0].physical.materialSupplier",dfmapping=self.organizationCodeToChange,swpolines=False)                
                if self.flag: self.flag=self.checkingparameters(schematosearch="workflowStatus", fieldtosearch="workflowStatus",dfmapping=self.workflowStatus,swpolines=False)
                if self.flag: self.flag=self.checkingparameters(schematosearch="orderType", fieldtosearch="orderType",dfmapping=self.orderType,swpolines=False)                
                #if self.flag: self.flag=self.checkingparameters(schematosearch="acqUnits", fieldtosearch="id",dfmapping=self.acqUnits,swpolines=False)                
                #POLINES
                if self.flag: self.flag=self.checkingparameters(schematosearch="mtype", fieldtosearch="compositePoLines[0].eresource.materialType",dfmapping=self.dfmtype,swpolines=True)   
                if self.flag: self.flag=self.checkingparameters(schematosearch="mtype", fieldtosearch="compositePoLines[0].physical.materialType",dfmapping=self.dfmtype,swpolines=True)                
                if self.flag: self.flag=self.checkingparameters(schematosearch="funds", fieldtosearch="compositePoLines[0].fundDistribution[0].code",dfmapping=self.funds,swpolines=True)
                if self.flag: self.flag=self.checkingparameters(schematosearch="funds", fieldtosearch="compositePoLines[0].fundDistribution[1].code",dfmapping=self.funds,swpolines=True)
                if self.flag: self.flag=self.checkingparameters(schematosearch="funds", fieldtosearch="compositePoLines[0].fundDistribution[2].code",dfmapping=self.funds,swpolines=True)
                if self.flag: self.flag=self.checkingparameters(schematosearch="funds", fieldtosearch="compositePoLines[0].fundDistribution[3].code",dfmapping=self.funds,swpolines=True)
                if self.flag: self.flag=self.checkingparameters(schematosearch="fundsExpenseClass", fieldtosearch="compositePoLines[0].fundDistribution[0].expenseClassId",dfmapping=self.fundsExpenseClass,swpolines=True)
                if self.flag: self.flag=self.checkingparameters(schematosearch="locations", fieldtosearch="compositePoLines[0].locations[0].locationId",dfmapping=self.locations,swpolines=True)
                if self.flag: self.flag=self.checkingparameters(schematosearch="locations", fieldtosearch="compositePoLines[0].locations[1].locationId",dfmapping=self.locations,swpolines=True)
                if self.flag: self.flag=self.checkingparameters(schematosearch="locations", fieldtosearch="compositePoLines[0].locations[2].locationId",dfmapping=self.locations,swpolines=True)
                if self.flag: self.flag=self.checkingparameters(schematosearch="orderFormat", fieldtosearch="compositePoLines[0].orderFormat",dfmapping=self.orderFormat,swpolines=True)
                if self.flag: self.flag=self.checkingparameters(schematosearch="paymentStatus", fieldtosearch="compositePoLines[0].paymentStatus",dfmapping=self.paymentStatus,swpolines=True)
                if self.flag: self.flag=self.checkingparameters(schematosearch="receiptStatus", fieldtosearch="compositePoLines[0].receiptStatus",dfmapping=self.receiptStatus,swpolines=True)
                if self.flag: self.flag=self.checkingparameters(schematosearch="acquisitionMethod", fieldtosearch="compositePoLines[0].acquisitionMethod",dfmapping=self.acquisitionMethod,swpolines=True)
                if self.getidfile:
                    if self.flag: self.flag=self.checkingparameters(schematosearch="localinstanceId", fieldtosearch="compositePoLines[0].instanceId",swpolines=True)
            totalnotestoprocess=0   
            if self.flag:
                if 'dfnotes' in kwargs:
                    self.dfnotes=kwargs['dfnotes']
                    #print(dfnotes)
                    self.customerName=notes.notes(client,self.path_dir,dataframe=self.dfnotes)
                    self.swnotes=True
                    totalnotestoprocess=len(self.dfnotes)
                else:
                    self.swnotes=False      

                #poLines=dfPolines        
                self.noprint=True
                countnote=0
                orderList=[]
                orderDictionary={}      
                list={}
                #customerName=kwargs['client']
                changeVendor={}
                sivendor=0
                novendor=0
                cont=0
                sw=0
                store={}
                purchase=[]
                purchaseOrders={}
                notesprint={}
                notesapp=[]
                orderList=[]      
                orderDictionary={}      
                list={}
                self.totalrows=len(self.orders)
                logging.info(f"Orders Total Rows {self.totalrows}")
                print(f"INFO ORDERS Total: {self.totalrows}") 
                for i, row in self.orders.iterrows():
                    try:
                        tini = time.perf_counter()
                        self.noprint=True
                        printpoline=True
                        self.nointance=False

                        Order={}
                        tic = time.perf_counter()
                        self.count+=1
                        if self.count==15:
                            a=1
                        #Order Number
                        poNumberSuffix=""
                        poNumberPrefix=""
                        poNumber=""
                        field="poNumberPrefix"
                        if field in self.orders.columns:
                            if row[field]:
                                poNumberPrefix=str(row[field])
                                Order["poNumberPrefix"]=poNumberPrefix.strip()
                        field="poNumberSuffix"
                        if field in self.orders.columns:
                            if row[field]:
                                poNumberSuffix=str(row[field])
                                Order["poNumberSuffix"]=poNumberSuffix.strip()
                        #if row['PO number'] in self.orders.columns:
                        field="poNumber"                
                        if field in self.orders.columns:
                            if row[field]:
                                masterPo=str(row[field]).strip()
                                po=self.check_poNumber(masterPo,self.path_results)
                                poNumber=str(po)
                            else:
                                randompoNumber=str(round(random.randint(100, 1000)))
                                poNumber=str(randompoNumber)
                                mf.write_file(ruta=self.path_logs+"\\oldNew_ordersID.log",contenido=poNumber)

                        po=str(poNumber)
                        #CHECKING DUPLICATED PO number    
                        countlist = orderList.count(str(po))
                        if countlist>0:
                            poNumber=str(po)+str(countlist)
                        Order["poNumber"]= str(poNumber)
                        orderList.append(str(po))
                        #print(orderList)                
                        #print(f"INFO RECORD: {self.count}  poNumber:  {poNumber}")
                        #idOrder
                        orderId=""
                        field="id"
                        if field in self.orders.columns: orderId=str(row[field]).strip()#str(uuid.uuid4())
                        else: orderId=str(uuid.uuid4())
                        
                        Order["id"]=orderId
                    
                        field="dateOrdered"
                        if field in self.orders.columns:
                            if row[field]:
                                dateorder=row[field]
                                try:
                                    dateordered=dateorder.strftime("%Y-%m-%dT%H:%M:%S.000+00:00") 
                                except Exception as ee:
                                    M=dateorder[0:2]
                                    D=dateorder[3:5] 
                                    Y=dateorder[6:10]
                                    dateorder=f"{Y}-{M}-{D}"   
                                    dateordered=f"{Y}-{M}-{D}T00:00:00.000+00:00"
                                
                                Order['dateOrdered']=dateordered
                                #Order["approvedById"]=""
                                #Order["approvalDate"]= ""
                                #Order["closeReason"]=dic(reason="",note="")
                        field="manualPo"
                        Order["manualPo"]= False
                        #PURCHASE ORDER NOTES
                        notea=[]
                        iter=0
                        sw=True
                        while sw:
                            field="notes["+str(iter)+"]"
                            if field in self.orders.columns:
                                if row[field]:
                                    valuenote={}
                                    valuenote=self.readcompositepurchaseorderMapping(folio_field=field)
                                    if valuenote:
                                        valuenote=valuenote.get("legacy_field")
                                        notespo=f"{valuenote}: "+str(row[field]).strip()
                                    else:
                                        notespo=str(row[field]).strip()
                                    notea.append(notespo)
                            else:
                                sw=False
                            iter+=1
                            
                        Order["notes"]=notea
                        if poNumber=="o1484302x":
                            a=1
                        #IS SUSCRIPTION FALSE/TRUE
                        Order_type=""
                        Order_type="One-Time"
                        isSubscription= False
                        isSuscriptiontem=""
                        interval=365
                        renewalDate=""
                        reviewPeriod=""
                        ongoingNote=""
                        if 'orderType' in self.orders.columns:
                            result=str(row['orderType']).strip()
                            if result is not None:
                                if result=="ongoing" or result=="Ongoing":
                                    Order_type="Ongoing"
                                    isongoing=mf.dic(isSubscription=False, manualRenewal=True) 
                                    if 'ongoing.reviewPeriod' in self.orders.columns:
                                        if row['ongoing.reviewPeriod']:
                                            reviewPeriod=int(row['ongoing.reviewPeriod'])
                                    if 'ongoing.interval' in self.orders.columns:
                                        if row['ongoing.interval']: interval=int(row['ongoing.interval'])
                                    field="ongoing.renewalDate"
                                    if field in self.orders.columns:    
                                        if row[field]:
                                            if row[field]!="":
                                                dater=""
                                                dater=row[field]                               
                                                renewalDate=dater.strftime("%Y-%m-%dT%H:%M:%S.000+00:00") 
                                    if 'ongoing.isSubscription' in self.orders.columns:
                                        if str(row['ongoing.isSubscription']).upper()=="TRUE":
                                            isongoing=mf.dic(interval=interval, isSubscription=True, manualRenewal=True, 
                                                    reviewPeriod=reviewPeriod, renewalDate=renewalDate)
                                    Order["ongoing"]=isongoing                        
                        Order["orderType"]=Order_type 
                    
                        ######################
                        shipTo=""
                        billTo=""
                        field="billTo"
                        if field in self.orders.columns:
                        #    toSearch=self.billtoshipto()
                        #    billTotemp=self.searchdata_dataframe(self.billtoshipto,"name","id",toSearch)
                            #if field is not None:
                            billTo=str(row[field])
                            Order["billTo"]=billTo
                            
                        field="shipTo"    
                        if field in self.orders.columns:
                            #toSearch=self.billtoshipto()
                            #shipTotemp=self.searchdata_dataframe(self.billtoshipto,"name","id",toSearch)
                            #if field is not None:
                            shipTo=str(row[field])
                            Order["shipTo"]=shipTo

                        OrganizationUUID=""            
                        #file=self.customerName+"_organizations.json"
                    
                        #OrganizationUUID=mf.readJsonfile(self.path_refdata,client+"_organizations.json","organizations","undefined","code")
                        #if OrganizationUUID is not None:
                        organizationID=""
                        if 'vendor' in self.orders.columns:
                            if row['vendor']:
                                organizationID=str(row['vendor']).strip()
                        Order["vendor"]=organizationID

                        workflowStatus="Pending"
                        approvedStatus= False
                        field="workflowStatus"
                        #row[0]
                        if field in self.orders.columns:
                            if row[field]:
                                workflowStatus=str(row[field]).strip()                 
                                if workflowStatus is not None:
                                    approvedStatus=True
                            
                        Order["approved"]= approvedStatus
                        Order["workflowStatus"]= workflowStatus

                        #Reencumber
                        reEncumber=False
                        field="needReEncumber"
                        if field in self.orders.columns:
                            if row[field]:
                                reencumbertem=str(row[field]).strip()
                                reencumbertem=reencumbertem.upper()
                                if reencumbertem=="YES":
                                    reEncumber=True
                    
                        Order["needReEncumber"]= reEncumber
                        #PURCHASE ORDERS LINES
                        compositePo=[]
                        linkid=""
                        compositePo=""
                        self.returnnotes=0
                        
                        self.nointance="-"
                        self.printstatus="No instance"
                        #COMPOSITE_PO_LINES
                        if printpoline:
                            compositePo=self.compositePoLines(self.dfPolines,organizationID,masterPo,poNumber,client)
                            if compositePo is not None: 
                                Order["compositePoLines"]=compositePo
                                countpol+=1
                                self.polcont= self.polcont + self.poLineTotal
                                
                            else: 
                                Order["compositePoLines"]=[]
                                countpolerror+=1
                        acqunituuid=[]
                        field="acqUnitIds"
                        if field in self.orders.columns:
                            if row[field]:
                                acqunituuid=row[field]
                                acqunituuid.append(adqvaluetemp)
                                #tosearch in dataframe
                        else:
                            adqvalue=self.readcompositepurchaseorderMapping(folio_field=field)    
                            adqvaluetemp=adqvalue.get("value")
                            if adqvaluetemp:
                                acqunituuid.append(adqvaluetemp)
                            
                        Order["acqUnitIds"]=acqunituuid
                               
                        #PRINT NOTES
                        Worder=Order
                        instanceOrder=Order
                        if self.noprint: 
                            if self.nointance:
                                mf.printObject(instanceOrder,self.path_results,self.count,f"{client}_purchaseOrderbyline_with_new_instance_{self.dt}",False)
                                self.printstatus="New instance"
                                self.po_count_new_instance+=1
                            else:
                                mf.printObject(Order,self.path_results,self.count,f"{client}_purchaseOrderbyline_{self.dt}",False)
                                purchase.append(Order)
                                self.printstatus="Instance linked"
                                self.po_count+=1
                        else:
                            mf.printObject(Worder,self.path_results,self.count,f"{client}_worse_purchaseOrderbyline_{self.dt}",False)
                            self.printstatus="Worse"
                            self.po_countworse+=1
                        myobj = datetime.datetime.now()
                        self.dobj=myobj.strftime('%T')
                        tend = time.perf_counter()
                        totaltime=round((tend - tini))
                        if self.returnnotes>0:
                            countnote=self.returnnotes
                            self.counternotes+=countnote
                        else:
                            countnote=self.returnnotes
                                
                        print(f"{self.dobj} RECORD # {self.count}/{self.totalrows} created | printStatus: {self.printstatus} | Instance:{self.nointance} | poNumber:{poNumber} poLines:{self.poLineTotal} | Note: {countnote} | (Time:{totaltime} sec.)") 
                        logging.info(f"{self.dobj} RECORD # {self.count}/{self.totalrows} created | printStatus: {self.printstatus} | Instance:{self.nointance} | poNumber:{poNumber} poLines:{self.poLineTotal} | Note: {countnote} | (Time:{totaltime} sec.)")
                        ordersidmapping={}
                        #ordersidmapping["po_"]=mf.dic(legacy_id=poNumber, folio_id=orderId,folio_poLines=folio_poLines.append(mf.dic(compositePo['poLineNumber'])))
                        compositeOrder=[]
                        ordersidmapping["po_Number"]=poNumber
                        ordersidmapping["folio_id"]=orderId
                        for xl in compositePo:
                            compoId=xl["id"]
                            compositeOrder.append(compoId)    
                        ordersidmapping["compositePoLines"]=compositeOrder
                            
                        mf.printObject(ordersidmapping,self.path_results,self.count,f"{client}_legacyId{self.dt}",False)
                    except Exception as ee:
                        Worder=Order
                        mf.printObject(Worder,self.path_results,self.count,f"{client}_purchaseOrderbyline_worse_{self.dt}",False)
                        print(f"ERROR: {ee}")
                        self.noprint=False
                    
                    purchaseOrders['purchaseOrders']=purchase
                    report=[]
                    #report=reports(df=orders,plog=path_logs,pdata=path_results,file_report=f"{customerName}_purchaseself.orders.json",schema="purchaseOrders",dfFieldtoCompare=poLineNumberfield)
            else:
                print(f"ERROR critical does not exist {self.path_refdata}\\acquisitionMapping.xlsx file")
                logging.info(f"ERROR critical does not exist {self.path_refdata}\\acquisitionMapping.xlsx file")
        if self.flag:
            mf.printObject(purchaseOrders,self.path_results,self.count,f"{client}_purchaseOrders_{self.dt}",True)
        self.migrationreport_a.set(Blurbs.GeneralStatistics,"Record processed",self.count)
        
        print(f"============REPORT======================")
        print(f"Record processed {self.count} Orders")
        logging.info(f"Record processed {self.count} Orders")
        print(f"RESULTS purchase orders {self.po_count} / {self.count} ")
        logging.info(f"RESULTS purchase orders {self.po_count} / {self.count}")        
        print(f"RESULTS purchase orders lines {self.polcont} / {self.polcont} ")
        logging.info(f"RESULTS purchase orders lines {self.polcont} / {self.polcont}")        
        print(f"RESULTS purchase orders with new instance {self.po_count_new_instance} / {self.count}")
        logging.info(f"RESULTS purchase orders with new instance {self.po_count_new_instance}")
        print(f"RESULTS Notes  {self.counternotes} / {totalnotestoprocess}")
        logging.info(f"RESULTS Notes  {self.counternotes} / {totalnotestoprocess}")        
        print(f"RESULTS worse purchase  {self.po_countworse} / {self.count}")
        logging.info(f"RESULTS worse purchase  {self.po_countworse} / {self.count}")
        print(f"RESULTS poLines with errors: {countpolerror} / {self.count}")
        logging.info(f"RESULTS poLines with errors: {countpolerror}/ {self.count}")
        
        with open(f"{self.path_results}/purchaseOrders_migration_report.md", "w+") as report_file:
            self.migrationreport_a.write_migration_report(report_file)
        
#########################################
#POLINES FUNCTION             
#########################################
    '''def compositePoLines(self,poLines,notesapp1,notesapp1Pofield,
                     notesapp2,notesapp2Pofield,istherenotesApp,
                     vendors,masterPo,poLineNumber,customerName,path_results,path_refdata,path_logs):                 '''
        
    def compositePoLines(self,poLines,vendors,masterPo,poLineNumber,client):
        try:
            self.printnote=True
            cpList=[]
            self.po_LineNumber=""
            self.poLineTotal=0
            poCount=1
             
            poLines = poLines[poLines['poNumber']== masterPo]
            linkId=""
            #print(f"INFO POLINES founds for the record: {masterPo} # poLines:",len(poLines))
            self.poLineTotal=len(poLines)
            for c, cprow in poLines.iterrows():
                cp={}
                if 'UUIDPOLINES' in poLines.columns:
                    if 'UUIDPOLINES' in poLines: linkId=cprow['UUIDPOLINES']#str(uuid.uuid4())
                else: linkId=str(uuid.uuid4())
                cp["id"]=linkId
                self.po_LineNumber=f"{poLineNumber}-{poCount}"
                cp["poLineNumber"]=self.po_LineNumber
                
                field="compositePoLines[0].publisher"
                if field in poLines.columns: 
                    if cprow[field]:
                        cp["publisher"]=cprow[field]
                #cp["purchaseOrderId"]=""
                #cp["id"]=""
                #cp["edition"]=""rece
 
                #cp["instanceId"]=""
                #cp["agreementId"]= ""
                field="compositePoLines[0].acquisitionMethod"
                acquisitionMethod="Purchase"
                if field in poLines.columns:
                    if cprow[field]:
                        result=str(cprow[field]).strip()
                        if result is not None:
                            acquisitionMethod=result
                cp["acquisitionMethod"]= acquisitionMethod
                
                field="compositePoLines[0].cancellationRestriction"
                cancellationRestriction=False
                if field in poLines.columns:
                    if cprow[field]:
                        cancelr=cprow[field]
                        if cancelr.upper()=="TRUE":
                            cancellationRestriction=True
                cp["cancellationRestriction"]= False
                #cp["alerts"]=dic(alert="Receipt overdue",id="9a665b22-9fe5-4c95-b4ee-837a5433c95d")
                

                #cp["claims"]=dic()Fa""
                collection=False
                field="compositePoLines[0].collection"
                if field in poLines.columns:
                    if cprow[field]:
                        collection=str(cprow[field]).strip()
                        collection=collection.upper()
                        if collection.upper()=="TRUE":
                            collection=True
                cp["collection"]=collection

                
                rush=False
                field="compositePoLines[0].rush"
                if field in poLines.columns:
                    if cprow[field]:
                        rush=str(cprow[field]).strip()
                        if rush.upper()=="YES":
                            rush=True
                cp["rush"]=rush
                contributorpoLines={}
                field="compositePoLines[0].contributors[0].contributor"
                if field in poLines.columns:
                    if cprow[field]:
                        contributorName=cprow[field]
                        #contributorpoLines=contributorName[]
                        field="compositePoLines[0].contributors[0].contributorNameTypeId"
                        if cprow[field]:
                            contributortype=cprow[field]
                            cp["contributors"]=contributorpoLines
                #ORDER FORMAT
                orderFormat="Other"
                field="compositePoLines[0].orderFormat"
                if field in poLines.columns:
                    if cprow[field]:
                        orderFormat=str(cprow[field]).strip()
                        
                costquantityPhysical=0.0
                field="compositePoLines[0].cost.quantityPhysical"
                if field in poLines.columns:
                    if cprow[field]:
                        costquantityPhysical=float(self.lup(cprow[field]))
                        

                costquantityElectronic= 0.0
                field="compositePoLines[0].cost.quantityElectronic"
                if field in poLines.columns:
                    if cprow[field]:#if cprow['QUANTITY'] NOT MIGRATED:
                        costquantityElectronic=float(self.lup(cprow[field]))
                        
                quantityElectronic=0
                quantityPhysical=0
                ###
                #LOCATIONS(ORDER)
                ################################
                locationstoadd=[]
                locsw=True
                loca=[]
                iter=0
                locationId=[]
                sw=True
                while sw:
                    field=f"compositePoLines[0].locations[{iter}].locationId"
                    if field in poLines.columns:                
                        if cprow[field]:
                            locationtemp=str(cprow[field]).strip()
                            field=f"compositePoLines[0].locations[{iter}].quantityElectronic"
                            if field in poLines.columns:
                                quantityElectronic=1
                                if cprow[field]:#if cprow['QUANTITY'] NOT MIGRATED:
                                    quantityElectronic=int(cprow[field])
                                
                            field=f"compositePoLines[0].locations[{iter}].quantityPhysical"
                            if field in poLines.columns:
                                quantityPhysical=1 
                                if cprow[field]:#if cprow['QUANTITY'] NOT MIGRATED:
                                    quantityPhysical=int(cprow[field])

                            locationstoadd.append([locationtemp,quantityElectronic,quantityPhysical])
                            #locationId.append([locationtemp,quantityElectronic,quantityPhysical])
                            #print(locationId)

                    else:
                        sw = False
                    iter+=1
                lta=len(locationstoadd)
                pos=0
                quantityPhysicalcost=0
                quantityElectroniccost=0
                quantitycost=0
                if lta>1:
                    for loc in locationstoadd:
                        locsw=False
                        locId=""
                        locId=loc[0]
                        quantityElectronic=int(loc[1])
                        quantityPhysical=int(loc[2])
                        if orderFormat=="Physical Resource":
                            loca.append({"locationId":locId,"quantity":quantityPhysical, "quantityPhysical":quantityPhysical}) 
                            quantityPhysicalcost=quantityPhysicalcost+quantityPhysical
                        elif orderFormat=="Electronic Resource":
                            loca.append({"locationId":locId,"quantity":quantityElectronic, "quantityElectronic":quantityElectronic}) 
                            quantityElectroniccost=quantityElectroniccost+quantityElectronic
                        elif orderFormat=="P/E Mix":
                            quantitycosttemp=quantitycost+quantityElectronic+quantityPhysical
                            loca.append({"locationId":locId,"quantity":quantitycosttemp, "quantityElectronic":quantityElectronic,"quantityPhysical":quantityPhysical})
                            quantitypemix=quantitycost+quantitycosttemp
                            #locationId=locationId[0],quantity=2, quantityElectronic=quantityElectronic,quantityPhysical=quantityPhysical
                        else:
                            loca.append({"locationId":locId,"quantity":quantityPhysical, "quantityPhysical":quantityPhysical}) 
                            quantityPhysicalcost=quantityPhysical+1
                    
                elif lta==1:
                    locationId.append(locationstoadd[0][0])
                    quantityPhysical=int(locationstoadd[0][2])
                    quantityElectronic=int(locationstoadd[0][1])
                    quantityPhysicalcost=quantityPhysical
                    quantityElectroniccost=quantityElectronic
                    quantitypemix=quantityPhysical
                else:
                    locationId.append("")
                    quantityPhysical=0
                    quantityElectronic=0


                    
                ispackage=False
                field="compositePoLines[0].isPackage"
                if field in poLines.columns:
                    if cprow[field]:
                        ispackagetem=str(cprow[field]).strip()
                        if ispackagetem.upper()=="YES" or ispackagetem==True:
                            ispackage=True
                cp["isPackage"]=ispackage
                #TITLE
                titlepoLine="No Title"
                enum=["Instance, Holding, Item","Instance, Holding","Instance","None"]
                instance_holdings_items=enum[3]
                field="compositePoLines[0].physical.createInventory"
                if field in poLines.columns:
                    if cprow[field]:
                        instholitem=str(cprow[field]).strip()
                        if instholitem=="Instance, Holding, Item":  
                            instance_holdings_items=enum[0]
                        elif instholitem=="Instance, Holding":
                            instance_holdings_items=enum[1]
                        elif instholitem=="Instance":
                            instance_holdings_items=enum[2]
                        else: 
                            instance_holdings_items=enum[3]
                
                field="compositePoLines[0].titleOrPackage"
                newinstanceid=""
                if field in poLines.columns:
                    if cprow[field]:
                        titlepoLine=str(cprow[field]).strip()
                cp["titleOrPackage"]=titlepoLine
                idnoexit=True
                field="compositePoLines[0].instanceId"
                titleUUID=""
                if field in poLines.columns:
                    if cprow[field]:
                        titleUUID=str(cprow[field]).strip()
                        if self.getidfile:
                            ordertitleUUID=self.get_title(client,element="instances",searchValue=titleUUID,query=f"/")
                        else:
                            ordertitleUUID=self.get_title(client,element="instances",searchValue=titleUUID,query=f"?query=(identifiers=")
                        if ordertitleUUID is None:
                            print(f"INFO Title: {titlepoLine}")
                            newinstanceid=self.createinstance(client,titlepoLine,titleUUID,linkId,self.po_LineNumber)
                            cp["instanceId"]=str(newinstanceid)
                            self.nointance=True
                        else: 
                            cp["instanceId"]=str(ordertitleUUID[0])
                            cp["titleOrPackage"]=str(ordertitleUUID[1])
                            cp["isPackage"]=False
                            self.nointance=False
                            self.printnote=True
                            
                else:
                    self.nointance=True
                    
                if self.nointance:
                    print(f"INFO Title: {titlepoLine}")
                    cp["titleOrPackage"]=titlepoLine
                    cp["isPackage"]=False
                    self.nointance=True
                    self.printnote=False
                    mf.write_file(ruta=f"{self.path_logs}\\titlesNotFounds.log",contenido=f"{self.po_LineNumber}    {titleUUID} {titlepoLine}   instance ID:    {newinstanceid}")
                    

                    

                
                ################################
                ### ORDER FORMAT
                ################################            
                
                materialType=""
                accessproviderUUID=""
                eresourceactivated=False
                field="compositePoLines[0].eresource.activated" 
                if field in poLines.columns:
                    if cprow[field]:
                        eresourceactivated=cprow[field]
                        cp['activated']=eresourceactivated
                
                #ACCESS PROVIDERS
                accessProvider=""
                accessProvider=vendors
                field="compositePoLines[0].eresource.accessProvider" 
                if field in poLines.columns:
                    if cprow[field]:
                        accessProvider=str(cprow[field]).strip()
                
                
                
                #MATERIAL SUPPLIER 
                materialSupplier=vendors
                field="compositePoLines[0].physical.materialSupplier"       
                if field in poLines.columns:
                    if cprow[field]:
                        materialSupplier=str(cprow[field]).strip()
                #PRICE
                listUnitPrice=0.0
                field="compositePoLines[0].cost.listUnitPrice"
                if field in poLines.columns:
                    if cprow[field]:
                        if cprow[field]!="":
                            listUnitPrice=float(self.lup(cprow[field]))
                
                field="compositePoLines[0].cost.listUnitPriceElectronic"
                
                #Currency
                currency="USD"
                field="compositePoLines[0].cost.currency"
                if field in poLines.columns:
                    if cprow[field]:
                        currency=str(cprow[field]).upper()
                    else:
                        curvalue=""
                        curvalue=self.readcompositepurchaseorderMapping(folio_field=field)
                        if curvalue:
                            curvalue=curvalue.get("value")
                            if curvalue:
                                currency=curvalue
                else:
                    curvalue=""
                    curvalue=self.readcompositepurchaseorderMapping(folio_field=field)
                    if curvalue:
                        curvalue=curvalue.get("value")
                        if curvalue:
                            currency=curvalue
                #Locations for print/mixed resources
                physicalmaterialType=""
                field="compositePoLines[0].physical.materialType"
                if field in poLines.columns:
                    if cprow[field]:
                        physicalmaterialType=str(cprow[field]).strip()
                else:
                    instance_holdings_items=enum[3]
                
                eresourcematerialType=""   
                field="compositePoLines[0].eresource.materialType"
                if field in poLines.columns:
                    if cprow[field]:
                        eresourcematerialType=str(cprow[field]).strip()
                


                        
                if orderFormat.upper() == "PHYSICAL RESOURCE":
                        cp["orderFormat"]="Physical Resource"
                        volumes=[]
                        field="compositePoLines[0].physical.volumes[0]"        
                        if field in poLines.columns:
                            if cprow[field]:
                                if cprow[field]!="0":
                                    vol=str(cprow[field]).strip()
                                    volumes.append(vol)
                        #COST COMPOSITE_PO_LINES
                        cp["cost"]=mf.dic(currency=currency,listUnitPrice=listUnitPrice, quantityPhysical=quantityPhysicalcost, poLineEstimatedPrice=listUnitPrice, discountType="percentage")
                        if physicalmaterialType: cp["physical"]=mf.dic(createInventory=instance_holdings_items,volumes=volumes,materialSupplier=materialSupplier, materialType=physicalmaterialType)
                        else: cp["physical"]=mf.dic(createInventory=instance_holdings_items,volumes=volumes,materialSupplier=materialSupplier)
                        if accessProvider: cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                        else: cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False)
                        if locsw:
                            if len(locationId)>0:
                                cp["locations"]=[mf.dic(locationId=locationId[0],quantity=quantityPhysical, quantityPhysical=quantityPhysical)]
                            else: 
                                cp["locations"]=[mf.dic(quantity=quantityPhysical, quantityPhysical=quantityPhysical)]
                        else:
                            cp["locations"]=loca
                        
                elif orderFormat.upper()=="ELECTRONIC RESOURCE":
                        cp["orderFormat"]="Electronic Resource"
                        #COST COMPOSITE_PO_LINES
                        cp["cost"]=mf.dic(listUnitPriceElectronic=listUnitPrice,currency=currency, quantityElectronic=quantityElectroniccost, poLineEstimatedPrice=listUnitPrice,discountType="percentage")
                        if eresourcematerialType:
                            if accessProvider:
                                cp["eresource"]=mf.dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider,materialType=eresourcematerialType)
                            else:
                                cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False)
                        else: 
                            if accessProvider:
                                cp["eresource"]=mf.dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                            else:
                                cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False)

                        if locationId: 
                            if locsw:
                                if len(locationId)>0: cp["locations"]=[mf.dic(locationId=locationId[0],quantity=quantityElectronic, quantityElectronic=quantityElectronic)]
                                else: cp["locations"]=[mf.dic(quantity=quantityElectronic, quantityElectronic=quantityElectronic)]
                            else:
                                cp["locations"]=loca 

                elif orderFormat.upper()=="P/E MIX":
                    cp["orderFormat"]="P/E Mix"
                    cp["cost"]=mf.dic(currency=currency,listUnitPrice=listUnitPrice,listUnitPriceElectronic=listUnitPrice, quantityPhysical=quantitypemix, quantityElectronic=quantityElectronic, poLineEstimatedPrice=listUnitPrice,discountType="percentage")
                    if accessProvider: 
                        if eresourcematerialType: 
                            cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider,materialType=eresourcematerialType)
                        else:
                            cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                    else: 
                        cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False)
                    #cp["locations"]=[mf.dic(locationId=locationId[0],quantity=2, quantityElectronic=1,quantityPhysical=quantityPhysical)]
                    if physicalmaterialType: cp["physical"]=mf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier,
                                       expectedReceiptDate="",receiptDue="",materialType=physicalmaterialType)
                    else: cp["physical"]=mf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier,
                                       expectedReceiptDate="",receiptDue="")
                    if locationId: 
                        if locsw:
                            if len(locationId)>0: cp["locations"]=[mf.dic(locationId=locationId[0],quantity=quantitypemix, quantityElectronic=quantityElectronic,quantityPhysical=quantityPhysical)]
                            else: cp["locations"]=[mf.dic(quantity=quantitypemix, quantityElectronic=quantityElectronic,quantityPhysical=quantityPhysical)]
                        else:
                            cp["locations"]=loca
                else:   
                    cp["orderFormat"]="Other"
                    cp["cost"]=mf.dic(currency=currency,listUnitPrice=listUnitPrice, quantityPhysical=quantityPhysicalcost, poLineEstimatedPrice=listUnitPrice, discountType="percentage")
                    if physicalmaterialType: cp["physical"]=mf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier, materialType=physicalmaterialType)
                    else: cp["physical"]=mf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier)
                    if accessProvider: cp["eresource"]=mf.dic(activated=False,createInventory="None",trial=False,accessProvider=accessProvider)
                    else: cp["eresource"]=mf.dic(activated=False,createInventory="None",trial=False)
                    if locationId: 
                        if locsw:
                            if len(locationId)>0:
                                cp["locations"]=[mf.dic(locationId=locationId[0],quantity=1, quantityElectronic=quantityElectronic)]
                            else:
                                cp["locations"]=[mf.dic(quantity=1, quantityElectronic=quantityElectronic)]
                        else:
                            cp["locations"]=loca 

                #FUNDS DISTRIBUTIONS
                fundDistribution=[]                
                #EXPENSES CLASES
                #FUND DISTRIBUTION BY RESOURCE
                sw=True
                iter=0
                fundsdistributionids=[]
                while sw:
                    field=f"compositePoLines[0].fundDistribution[{iter}].code"
                    if field in poLines.columns:
                        if cprow[field]:
                            fundId=str(cprow[field]).strip()
                            fundcode=""
                            fundcode=self.searchdata_dataframe(self.dffunds,"id","code",fundId)            
                            if fundcode is not None:
                                code=fundcode
                                field=f"compositePoLines[0].fundDistribution[{iter}].value"
                                valuefund=100.0
                                if field in poLines.columns:
                                    if cprow[field]:
                                        fundDistributionvaluetemp=str(cprow[field]).strip()
                                        fundDistributionvaluetemp=fundDistributionvaluetemp.replace("%","")
                                        fundDistributionvaluetemp=fundDistributionvaluetemp.replace("0.","")
                                        valuefund=float(fundDistributionvaluetemp)
                                    else: 
                                        valuefund=100.0
                                expenseClassId=""
                                field=f"compositePoLines[0].fundDistribution[{iter}].expenseClassId"
                                if field in poLines.columns:
                                    if cprow[field]:
                                        expenseClassId=str(cprow[field]).strip()
                                        fundDistribution.append(mf.dic(code=code,fundId=fundId,expenseClassId=expenseClassId,distributionType="percentage",value=valuefund))
                                else:
                                    
                                    fundDistribution.append(mf.dic(code=fundcode,fundId=fundId,distributionType="percentage",value=valuefund))
                    else:
                        sw=False
                    iter+=1
                cp["fundDistribution"]=fundDistribution

                #Ongoing
                receiptDate=""
                
                field="compositePoLines[0].receiptDate"
                if field in poLines.columns:
                    if cprow[field]:
                        if cprow[field]!="  -  -  ":
                            if client!="michstate_prod":
                                dater=""
                                dater=cprow[field]                               
                                receiptDate=dater.strftime("%Y-%m-%dT%H:%M:%S.000+00:00") 
                            else:
                                receitdate=cprow[field]
                                M=receitdate[0:2]
                                D=receitdate[3:5] 
                                Y=receitdate[6:10]
                                dater=f"{Y}-{M}-{D}"   
                                receiptDate=f"{Y}-{M}-{D}T00:00:00.000+00:00"
                            

                cp['receiptDate']=receiptDate
                
                receivingNote=""    
                field="compositePoLines[0].details.receivingNote"
                if field in poLines.columns:
                    receivingNote=str(cprow[field]).strip()
                    
                subscriptionFrom=""
                subscriptionTo=""
                subscriptionInterval=""
                dater=""
                field="compositePoLines[0].details.subscriptionFrom"
                if field in poLines.columns:                    
                    if cprow[field]:
                        if cprow[field]!="  -  -  ":
                            try:
                                if client!="massey":
                                    dater=""
                                    dater=cprow[field]                               
                                    subscriptionFrom=dater.strftime("%Y-%m-%dT%H:%M:%S.000+00:00") 
                                else:
                                    subscriptionFrom=cprow[field]
                                    D=subscriptionFrom[0:2]
                                    M=subscriptionFrom[3:5] 
                                    Y=subscriptionFrom[6:]
                                    
                                    if (Y.find("9"))!=-1:
                                        Y=f"19{Y}"
                                    else:
                                        Y=subscriptionFrom[6:10]

                                    subscriptionFrom=f"{Y}-{M}-{D}T00:00:00.000+00:00"
                            except Exception as ee:
                                dater=mf.timeStampString(dater)
                                subscriptionFrom=dater.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
                field="compositePoLines[0].details.subscriptionTo"
                if field in poLines.columns:
                    if cprow[field]:
                        if cprow[field]!="  -  -  ":
                            dater="" 
                            dater=cprow[field]
                            subscriptionTo=dater.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
                field="compositePoLines[0].details.subscriptionInterval"
                if field in poLines.columns:
                    if cprow[field]: subscriptionInterval=int(cprow[field])
                
                productIds=[]
                iter=0
                sw=True
                while sw:
                    field=f"compositePoLines[{iter}].details.productIds[{iter}].productId"
                    if field in poLines.columns:
                        prodId={}
                        qualifier=""
                        productIdType="8e3dd25e-db82-4b06-8311-90d41998c109"
                        valueprod=""
                        if cprow[field]:
                            prodId['productId']=str(cprow[field])
                            field=f"compositePoLines[0].details.productIds[0].productIdType"                        
                            valueprod=self.readcompositepurchaseorderMapping(folio_field=field)
                            if valueprod:
                                valueprod=valueprod.get("value")
                                pidtype=str(valueprod).strip()
                                pidtype=pidtype.upper()
                            
                            valor = self.productidsDictionary.get(pidtype)
                            if valor is not None:
                                productIdType=str(valor)
                            prodId['productIdType']=productIdType
                            valor=""
                            field=f"compositePoLines[{iter}].details.productIds[{iter}].qualifier"
                            valueprod=self.readcompositepurchaseorderMapping(folio_field=field)
                            if valueprod:
                                valueprod=valueprod.get("value")
                                cdpq=str(valueprod).strip()
                            #valor = self.productidsDictionary.get(cdpq)
                                if cdpq is not None:
                                    qualifier=cdpq
                                prodId['qualifier']=qualifier
                            productIds.append(prodId)                      

                    else:
                        sw=False
                    iter+=1
                cp["details"]=mf.dic(receivingNote=receivingNote,productIds=productIds,subscriptionFrom=subscriptionFrom,
                                                           subscriptionInterval=subscriptionInterval, subscriptionTo=subscriptionTo)
                description=""
                field="compositePoLines[0].donor"
                if field in poLines.columns:
                    if cprow[field]:
                        donor=cprow[field]
                    cp["donor"]=donor
                
                paymentStatus="Pending"
                field="compositePoLines[0].paymentStatus"
                if field in poLines.columns:
                    if cprow[field]:
                        paymentStatus=str(cprow[field]).strip()
                cp['paymentStatus']=paymentStatus
                
                descriptionnote=""
                field="compositePoLines[0].description"
                if field in poLines.columns:
                    if cprow[field]:
                        descriptionnote=str(cprow[field]).strip()
                        cp["description"]=descriptionnote
                
                description=""
                field="compositePoLines[0].poLineDescription"
                if field in poLines.columns:
                    if cprow[field]:
                        description=str(cprow[field]).strip()
                        cp["poLineDescription"]=description

                #cp["publicationDate"]=""
                receiptStatus="Awaiting Receipt"
                receiptDate=""                                    
                field="compositePoLines[0].receiptStatus"
                if field in poLines.columns:
                    if cprow[field]:
                        receiptStatus=str(cprow[field]).strip()
                    
                cp["receiptStatus"]=receiptStatus
                #cp["reportingCodes"]=dic(code="",id="",description="")
                #Check items Manually True / False
                field="compositePoLines[0].checkinItems"
                checkinItems= False
                if field in poLines.columns:
                    if cprow[field]:
                        checkinItemstem=str(cprow[field]).strip()
                else:
                    chekvalue=self.readcompositepurchaseorderMapping(folio_field=field)
                    if chekvalue:
                        if receiptStatus=="Partially Received" and  instance_holdings_items=="None": #or receiptStatus=="Pending" or receiptStatus=="Awaiting Receipt":
                            checkinItems=chekvalue.get("value")
                            
                cp["checkinItems"]=checkinItems
                Requester=""
                field="compositePoLines[0].requester"
                if field in poLines.columns:
                    if cprow[field]:
                        Requester=str(cprow[field])
                        cp["requester"]=Requester
                
                selector=""
                field="compositePoLines[0].selector"
                if field in poLines.columns:
                    if cprow[field]:
                        cp['selector']=str(cprow[field])

                cp["source"]="User"
                vendorAccount=""
                
                instrVendor=""
                vendetails={}
                field=f"compositePoLines[0].vendorDetail.referenceNumbers" 
                if field in poLines.columns:
                    field="compositePoLines[0].vendorDetail.instructions"
                    if field in poLines.columns:
                        if cprow[field]:
                            instrVendor=str(cprow[field]).strip()
                    vendetails['instructions']=instrVendor
                        
                    field="compositePoLines[0].vendorDetail.vendorAccount"
                    if field in poLines.columns:
                        if cprow[field]:
                            vendorAccount=str(cprow[field]).strip()
                            vendetails['vendorAccount']=vendorAccount
                
                    iter=0
                    sw=True
                    referenceNumbers=[]
                    refnum={}
                    while sw:
                        field=f"compositePoLines[{iter}].vendorDetail.referenceNumbers"                    
                        if field in poLines.columns:
                            if cprow[field]:
                                refNumber=str(cprow[field]).strip()
                                refnum['refNumber']=refNumber
                                refnum['refNumberType']="Vendor order reference number"
                            referenceNumbers.append(refnum)
                        else:
                            sw=False
                        iter+=1
                    vendetails['referenceNumbers']=referenceNumbers
                
                    cp["vendorDetail"]=vendetails
                self.returnnotes=0       
                if self.swnotes:      #dataframe,toSearch,linkId):
                    if client=="washcoll" or client=="michstate_prod":
                        masterPo="o"+masterPo
                    #if client=="massey":
                    #    masterPo=masterPo[:-1]
                    if self.printnote:                        
                        self.returnnotes=self.customerName.readnotes(client,dataframe=self.dfnotes,toSearch=masterPo,linkId=linkId,filenamenotes="_notes")                        
                    else:
                        self.returnnotes=self.customerName.readnotes(client,dataframe=self.dfnotes,toSearch=masterPo,linkId=linkId,filenamenotes="_notes_with_new_instance")
                cpList.append(cp)
                #cpList.append(linkid)
                poCount+=1
            return cpList    
        except Exception as ee:
            self.migrationreport_a.add(Blurbs.PolinesErrors,f"ERROR POLINE:{masterPo} | {self.po_LineNumber} | {field} {ee}")
            print(ee)
            print(self.po_LineNumber)
            mf.write_file(ruta=self.path_logs+"\\poLinesErrors.log",contenido=f"Order:{masterPo} {field} {ee}")
            print(f"ERROR POLINE:{masterPo} | {self.po_LineNumber} | {field} {ee}")   
            logging.info(f"ERROR POLINE:{masterPo} | {self.po_LineNumber} | {field} {ee}")   
        except ValueError:
            print(self.po_LineNumber)
            mf.write_file(ruta=self.path_logs+"\\poLinesErrors.log",contenido=f"Order:{masterPo} {ee}")
            print(f"General Error on GET: {self.req.text} {self.req.status_code}")
            logging.info(f"General Error on GET:{masterPo} | {self.po_LineNumber} | {ee}")   


    '''def replace(self,dftoSearch,toSearch):
        temp=str(toSearch)
        tempstring=str(toSearch)
        dataToreturn=None
        multifunds=toSearch.split("),")        
        if len(multifunds)>1:
            for p in multifunds:
                thereparentesis=p.find("(")
                if thereparentesis!=-1:
                    toSearch=str(p[:thereparentesis]).strip()
                    dataToreturn=self.replace_error(dftoSearch,toSearch)
                    tempstring=tempstring.replace(toSearch,dataToreturn)
            dataToreturn=str(tempstring).strip()
        else:
            multifunds=toSearch.split(",")
            if len(multifunds)>1:
                for p in multifunds:
                        toSearch=str(p).strip()
                        dataToreturn=self.replace_error(dftoSearch,toSearch)
                        tempstring=tempstring.replace(toSearch,dataToreturn)
                dataToreturn=str(tempstring).strip()            
            else:
                multifunds=toSearch.split("(")
                if len(multifunds)>1:
                    for p in multifunds:
                        toSearch=str(p).strip()
                        dataToreturn=self.replace_error(dftoSearch,toSearch)
                        tempstring=tempstring.replace(toSearch,dataToreturn)
                    dataToreturn=str(tempstring).strip()  
                else:
                    dataToreturn=self.replace_error(dftoSearch,toSearch)
        return dataToreturn'''
    def searchdata_dataframe(self,dftosearchtemp,fieldtosearch,fieldtoreturn,toSearch):
        try:
            dataToreturn=None
            #print(dftosearchtemp)
            tempo = dftosearchtemp[dftosearchtemp[fieldtosearch]== toSearch]
            #fieldtoreturn=fieldtoreturn
            #print("Mapping found: ",len(temp))
            if len(tempo)>0:
                for x, cptemp in tempo.iterrows():
                    dataToreturn=str(cptemp[fieldtoreturn]).strip()
            else:
                dataToreturn=None
                if fieldtosearch=="legacy_id_sierra":
                    toSearch=f".{toSearch}"
                    tempo = dftosearchtemp[dftosearchtemp[fieldtosearch]== toSearch]
                    if len(tempo)>0:
                        for x, cptemp in tempo.iterrows():
                            dataToreturn=str(cptemp[fieldtoreturn]).strip()
            return dataToreturn
        except Exception as ee:
            print(f"ERROR: mapping {ee} {dataToreturn}")
    
    def replace(self,dftoSearch,toSearch):
        #print(dftoSearch)
        if 'LEGACY SYSTEM' in dftoSearch:
            temp = dftoSearch[dftoSearch['LEGACY SYSTEM']== toSearch]
            fieldtoreturn="FOLIO"
        elif 'code' in dftoSearch:
            temp = dftoSearch[dftoSearch['code']== toSearch]
            fieldtoreturn="id"
        elif 'id' in dftoSearch:
            temp = dftoSearch[dftoSearch['id']== toSearch]
            fieldtoreturn="code"
          
        #print("Mapping found: ",len(temp))
        error_dataToreturn=None
        if len(temp)>0:
            for x, cptemp in temp.iterrows():
                error_dataToreturn=str(cptemp[fieldtoreturn]).strip()
        return error_dataToreturn
    

    def mapping(self,dftoSearch,toSearch):
        try:
            #print(dftoSearch)                    
            dataToreturn=""
            temp = dftoSearch[dftoSearch['LEGACY SYSTEM']== toSearch]
            #print("Mapping found: ",len(temp))
            if len(temp)>0:
                for x, cptemp in temp.iterrows():
                    dataToreturn=str(cptemp['FOLIO']).strip()
            else:
                    mf.write_file(ruta=self.path_logs+"\\workflowNotfound.log",contenido=f"{toSearch}")
                    self.noprint=False
                    dataToreturn=None
            return dataToreturn
        
        except Exception as ee:
            print(f"ERROR: mapping {ee}")
            
    def check_poNumber(self,value, path):
        try:
            #define input string
            input_string = value
            final_string = "" #define string for ouput
            for character in input_string:
                if(character.isalnum()):
                # if character is alphanumeric concat to final_string
                    final_string = final_string + character
                    value=final_string

            valuefix=""
            Newmpol=""
            keepit=False
            #sp_chars = [';', ':', '!', "*","<","/","_","-","(",")","|"," ","@","¿","?","=","#","!"] 
            #valuefix = filter(lambda i: i not in sp_chars, value)
            if value.find(" ")!=-1: value=value.replace(" ","")
            if value.find("#")!=-1: value=value.replace("#","")
            if value.find(">")!=-1: value=value.replace(">","")
            if value.find("<")!=-1: value=value.replace("<","")
            if value.find("/")!=-1: value=value.replace("/","")
            if value.find(":")!=-1: value=value.replace(":","")
            if value.find("-")!=-1: value=value.replace("-","")
            if value.find("_")!=-1: value=value.replace("_","")
            if value.find("(")!=-1: value=value.replace("(","")
            if value.find(")")!=-1: value=value.replace(")","")
            if value.find("&")!=-1: value=value.replace("&","")
            if value.find(".")!=-1: value=value.replace(".","")
            if value.find("'")!=-1: value=value.replace("'","")
            if value.find(",")!=-1: value=value.replace(",","")
            if value.find("|")!=-1: value=value.replace("|","")
            if value.find("!")!=-1: value=value.replace("!","")
            if value.find("=")!=-1: value=value.replace("=","")
            if value.find("@")!=-1: value=value.replace("@","")
            if value.find("?")!=-1: value=value.replace("?","")
            if value.find("¿")!=-1: value=value.replace("¿","")
            if value.find("*")!=-1: value=value.replace("*","")
            if len(value)>18: 
                value=""
                keepit=True
                for i in range(2):
                    Newmpol=str(random.randint(100, 1000))
                    with open(path+"\oldNew_ordersID.txt", "a") as clean:
                        clean.write(str(value)+"/"+str(Newmpol)+"\n")
                    logging.info(f"NEW PO NUMBER {value} | {Newmpol}")   
                    value=Newmpol
            return value
        except Exception as ee:
            print(f"INFO check_poNumber function failed {ee}")
            

        

    def get_title(self,client,**kwargs):
        try:
            pathPattern1=mf.okapiPath(kwargs['element'])
            element=kwargs['element']
            pathPattern=pathPattern1[0]
            searchValue=kwargs['searchValue']
            client=mf.SearchClient(client)
            okapi_url=str(client.get('x_okapi_url'))
            okapi_tenant=str(client.get('x_okapi_tenant'))
            okapi_token=str(client.get('x_okapi_token'))
            dic={}
            #pathPattern="/instance-storage/instances" #?limit=9999&query=code="
            #https://okapi-ua.folio.ebsco.com/instance-storage/instances?query=hrid=="264227"
            pathPattern="/instance-storage/instances" #?limit=9999&query=code="
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            length="1"
            start="1"
            #element="instances"
            #https://okapi-trinitycollegelibrarycambridge.folio.ebsco.com/instance-storage/instances?query=(identifiers any ".b10290242")
            query=""
            query=kwargs['query']                
            #query=f"?query=(identifiers="
            #query=f"query=hrid=="
            #/finance/funds?query=name==UMPROQ
            if query.find("identifiers")!=-1:
                search='"'+searchValue+'")'
            else:
                search=searchValue
  
            #.b10290242
            #paging_q = f"?{query}"+search
            paging_q = f"{query}{search}"
            path = pathPattern+paging_q
            #data=json.dumps(payload)
            url = okapi_url + path
            req = requests.get(url, headers=okapi_headers)
            idhrid=[]
            #print(req.text)
            if req.status_code != 201:
                json_str = json.loads(req.text)
                if 'totalRecords' in json_str:
                    total_recs = int(json_str["totalRecords"])
                    if (total_recs!=0):
                        rec=json_str[element]
                        #print(rec)
                        l=rec[0]
                        if 'id' in l:
                            idhrid.append(l['id'])
                            idhrid.append(l['title'])
                else:
                    l=json_str
                    if 'id' in l:
                        idhrid.append(l['id'])
                        idhrid.append(l['title'])    
            if len(idhrid)==0:
                return None
            elif len(idhrid)>0:
                return idhrid            
            else:
                return None
        except Exception as ee:
            print(f"ERROR: GET TITLE {ee}")
            logging.info(f"ERROR: GET TITLE {ee}")    
            
            
    def createinstance(self, client,titleOrPackage,titleUUID,linkId,poNumbertitle):
        try:
            idinstance=""
            self.createinstanid=True
            print(f"INFO Creating instance for title {titleOrPackage} {titleUUID}")
            idinstance=str(uuid.uuid4())
            instance= {
                                        "id": idinstance,
                                        "_version": 1,
                                        "source": "FOLIO",
                                        "title": titleOrPackage,
                                        "alternativeTitles": [],
                                        "editions": [],
                                        "series": [],
                                        "identifiers": [
                                            {"value": str(titleUUID),
                                            "identifierTypeId": "5e1c71c5-c4ce-4585-a057-88eb3675f353"}
                                            ],
                                        "contributors": [],
                                        "subjects": [],
                                        "classifications": [],
                                        "publication": [],
                                        "publicationFrequency": [],
                                        "publicationRange": [],
                                        "electronicAccess": [],
                                        "instanceTypeId": "30fffe0e-e985-4144-b2e2-1e8179bdb41f",
                                        "instanceFormatIds": [],
                                        "instanceFormats": [],
                                        "physicalDescriptions": [],
                                        "languages": [],
                                        "notes": [],
                                        "discoverySuppress": False,
                                        "statisticalCodeIds": [],
                                        "statusId": "26f5208e-110a-4394-be29-1569a8c84a65",
                                        "tags": {"tagList": []},
                                        "holdingsRecords2": [],
                                        "natureOfContentTermIds": []
                                        }
            
            logging.info(f"INFO: Title created {titleOrPackage} | {idinstance}") 
            mf.printObject(instance,self.path_results,0,f"{client}_instances_{self.dt}",False)
            self.createordertitle(client,idinstance,titleOrPackage,linkId,poNumbertitle)
            return idinstance
        except Exception as ee:
            print(f"ERROR: GET TITLE {ee}")
            
    def createordertitle(self, client,idinstance,titleOrPackage,linkId,poNumbertitle):
        try:
            self.createordertit=True
            idtitleorder=str(uuid.uuid4())        
            ordertitle={
                    "id": idtitleorder,
                    "title": titleOrPackage,
                    "poLineId": linkId,
                    "instanceId": idinstance,
                    "contributors": [],
                    "isAcknowledged": True,
                    "productIds": [],
                    "poLineNumber": poNumbertitle
                    }
            mf.printObject(ordertitle,self.path_results,0,f"{client}_orders-storage-titles_{self.dt}",False)
            logging.info(f"INFO: Title created {titleOrPackage} | {idinstance}") 
        except Exception as ee:
            print(f"ERROR: GET TITLE {ee}")        
            
            
    def lup(self,value):
        lup=""
        lup=str(value).strip()
        lup=lup.replace(",","")
        lup=lup.replace("$","")
        lup=lup.replace("E","")
        return lup
        
        
    # def locationtoSearch(self, **kwargs):
    #     locationtoSearch=str(cprow[field]).strip()
    #     if locationtoSearch.find(",")==-1:
    #         locsw=True
    #         locationtoSearch=locationtoSearch.replace(" ","")
    #         result=""
    #         result=self.mapping(self.locations,str(cprow[field]).strip())
    #         if result is not None:
    #             locationId=mf.readJsonfile(self.path_refdata,f"{client}_locations.json","locations",result,"code")
    #             if locationId is None:
    #                 locationId="None"
    #                 mf.write_file(ruta=self.path_logs+"\\locationsNotFounds.log",contenido=locationtoSearch)                            
    #         else:
    #             loca=[]
    #             x = locationtoSearch.split(",")
    #             locsw=False
    #             lc=0
    #             locationIdA=""
    #             for i in x:
    #                 locationtoSearch=i
    #                 par=0
    #                 par=i.find("(")
    #                 if par>1:
    #                     locationtoSearch=i[:par]
    #                     qP=i[par+1]
    #                 else:
    #                     qP=1
    #                 result=self.mapping(self.locations,locationtoSearch)
    #                 if result is not None:
    #                     locid=mf.readJsonfile(self.path_refdata,f"{client}_locations.json","locations",result,"code")
    #                     if locid is None:
    #                         locid="None"
    #                         mf.write_file(ruta=self.path_logs+"\\locationsNotFounds.log",contenido=locationtoSearch)
    #                 #loca1.append([str(locid[0]),qP])
    #                 if orderFormat.upper()=="PHYSICAL RESOURCE":
    #                     loca.append({"locationId":locid[0],"quantity":int(qP), "quantityPhysical":int(qP)}) 
    #                 elif orderFormat.upper()=="ELECTRONIC RESOURCE":
    #                     loca.append({"locationId":locid[0],"quantity":int(qP), "quantityElectronic":int(qP)})
    #                 else:
    #                     loca.append({"locationId":locid[0],"quantity":int(qP), "quantityPhysical":int(qP)}) 
    #                 locid=[]
    #                 qP=""
    #  #locationIdA=""
    #             ##TITLE
    
    # def fundistribution()
    # fundTosearch=fundTosearch.replace(" ","")
    #                 if fundTosearch=="none":
    #                     fundTosearch=""
    #                 if fundTosearch:
    #                     occurencias=int(fundTosearch.count("%"))
    #                     if occurencias==0:
    #                         searchExpensesValue=""
    #                         expenseClassId=""
    #                         field="compositePoLines[0].fundDistribution[0].expenseClassId"
    #                         if field in poLines.columns:
    #                             if cprow[field]:
    #                                 expsearchtoValue=str(cprow[field]).strip()
    #                                 expenseClassId=mf.readJsonfile(self.path_refdata,f"{client}_expenseClasses.json","expenseClasses",expsearchtoValue,"code")
    #                         #expenseClassId=get_Id(customerName,element="expenseClasses",searchValue=searchtoValue)
    #                         #get_funId(searchValue,orderFormat,client):
    #                         result=""
    #                         result=self.mapping(self.funds,fundTosearch)
    #                         if result is not None:
    #                             fundcodeTosearch=result
    #                         fundId=mf.readJsonfile_fund(self.path_refdata,f"{client}_funds.json","funds",fundcodeTosearch,"code")
    #                         #fundId=get_funId(codeTosearch,orderFormat,customerName)
    #                         if fundId is not None:
    #                             code=fundId[1]
    #                             fundId=fundId[0]
    #                             valuefund=100.0
    #                             if expenseClassId:
    #                                 fundDistribution.append(mf.dic(code=code,fundId=fundId,expenseClassId=expenseClassId,distributionType="percentage",value=valuefund))
    #                             else:
    #                                 fundDistribution.append(mf.dic(code=code,fundId=fundId,distributionType="percentage",value=valuefund))
    #                         else:
    #                             mf.write_file(ruta=self.path_logs+"\\fundsNotfounds.log",contenido=f"{self.po_LineNumber} {fundcodeTosearch}")
                                
    #                     else:
    #                         fundlist=[]
    #                         fundDistribution=[]
    #                         fundlist = codeTosearch.split(",")
    #                         i=0
    #                         for fundin in fundlist:
    #                             cadena=fundin
    #                             codeTosearch=cadena[:5]
    #                             valuefund=cadena[6:10]
    #                             searchExpensesValue=""
    #                             expenseClassId=""
    #                             field="compositePoLines[0].fundDistribution[0].expenseClassId"
    #                             if field in poLines.columns:
    #                                 if cprow[field]:
    #                                     expsearchtoValue=str(cprow[field]).strip()
    #                                     expenseClassId=mf.readJsonfile(self.path_refdata,f"{client}_expenseClasses.json","expenseClasses",expsearchtoValue,"code")
    #                                     if expenseClassId is None:
    #                                         mf.write_file(ruta=self.path_logs+"\\expensesNotfounds.log",contenido=f"{self.po_LineNumber} {expsearchtoValue}")
    #                                     #expenseClassId=get_Id(customerName,element="expenseClasses",searchValue=searchtoValue)
    #                                 #get_funId(searchValue,orderFormat,client):
    #                             fundId=mf.readJsonfile_fund(self.path_refdata,f"{client}_funds.json","funds",codeTosearch,"code")
    #                             #fundId=get_funId(codeTosearch,orderFormat,customerName)
    #                             if fundId is not None:
    #                                 code=fundId[1]
    #                                 fundId=fundId[0]
    #                                 if expenseClassId:
    #                                     fundDistribution.append(mf.dic(code=code,fundId=fundId,expenseClassId=expenseClassId,distributionType="percentage",value=valuefund))
    '''    def gettinginstance(self,client):
        instancebibexit=[]
        instancebibnoexit=[]
        tupla_order = self.orders['compositePoLines[0].instanceId'].unique()
        instancefile=self.path_refdata+"\\instanceid.json"
        if os.path.exists(instancefile):
            pass:
        else:
            for bibid in tupla_order:
                titleUUID=str(bibid).strip()
                ordertitleUUID=self.get_title(client,element="instances",searchValue=titleUUID)
                if ordertitleUUID is None:
                    print(f"INFO Title: {bibid}")
                    #instancebibnoexit=[]
                    mf.write_file(ruta=f"{self.path_logs}\\titlesNotFoundsbyidentifier.log",contenido=f"{bibid}")
                else: 
                    instancebibexist.append([str(bibid),str(ordertitleUUID[0]),str(ordertitleUUID[1])])
                    
                self.dfinstance=self.customerName.importupla(tupla=instancebibexist,dfname="instancesmapping",columns=["legacy_code", "instance_id","instance_title"])'''
