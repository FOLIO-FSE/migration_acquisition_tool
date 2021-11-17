import backup_restore as br
import functions_AcqErm as mf
import notes_class as notes
import datetime
import warnings
from datetime import datetime
from datetime import datetime
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
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import time
import yaml
import shutil
        
################################
##ORDERS FUNCTION
################################

class compositePurchaseorders():
    def __init__(self,client,path_dir):
        try:    
            self.customerName=client
            self.path_dir=path_dir
            #os.mkdir(f"{path_dir}\\results")
            self.path_results=f"{path_dir}\\results"
            #os.mkdir(f"{path_dir}\\data")
            self.path_data=f"{path_dir}\\data"
            #os.mkdir(f"{path_dir}\\logs")
            self.path_logs=f"{path_dir}\\logs"
            #os.mkdir(f"{path_dir}\\refdata")
            self.path_refdata=f"{path_dir}\\refdata"

            with open(self.path_refdata+"\\composite_purchase_order_mapping.json") as json_mappingfile:
                self.mappingdata = json.load(json_mappingfile)
                for i in self.mappingdata['data']:
                    if i['folio_field']=='poNumber':
                        self.descpoNumber=str(i['description']).strip()
                        return            
        except Exception as ee:
            print(f"ERROR: Orders Class {ee}")
            
    def readMappingfile(self):
        self.customerName=pd.dataframe()
        filetoload=self.path_refdata+f"\\acquisitionMapping.xlsx"
        print("INFO Reading Mapping file")
        self.acquisitionMethod=self.customerName.importDataFrame(filetoload,sheetName="acquisitionMethod", dfname="Acquisition Method")
        #print("Dataframe: Order Format")
        self.orderFormat=self.customerName.importDataFrame(filetoload,sheetName="orderFormat", dfname="Order format")
        #print("Dataframe: Order Type")
        self.orderType=self.customerName.importDataFrame(filetoload,sheetName="orderType", dfname="Order type")
        #print("Dataframe: Payment Status")
        self.paymentStatus=self.customerName.importDataFrame(filetoload,sheetName="paymentStatus", dfname="Payment Status")
        #print("Dataframe: Receipt Status")
        self.receiptStatus=self.customerName.importDataFrame(filetoload,sheetName="receiptStatus", dfname="Receipt Status")
        #print("Dataframe: WorkFlowStatus")
        self.workflowStatus=self.customerName.importDataFrame(filetoload,sheetName="workflowStatus", dfname="Workflow Status")
        self.workflowStatusEnum=["Pending","Open","Closed"]
        #print("Dataframe: Locations")
        self.locations=self.customerName.importDataFrame(filetoload,sheetName="locations", dfname="locations")
        #print("Dataframe: Funds/Expenses")
        self.fundsExpenseClass=self.customerName.importDataFrame(filetoload,sheetName="fundsExpenseClass", dfname="Expense Class")
        #print("Dataframe: Funds")
        self.funds=self.customerName.importDataFrame(filetoload,sheetName="funds",dfname="Funds")
        #print("Dataframe: Organization code to Change - optional")
        self.organizationCodeToChange=self.customerName.importDataFrame(filetoload,sheetName="organizationCodeToChange", dfname="Organization Change Codes")
        
        with open(self.path_refdata+"\\composite_purchase_order_mapping.json") as json_mappingfile:
            self.mappingdata = json.load(json_mappingfile)
    
    def readorders(self, client, **kwargs):
        noprint=True
        self.readMappingfile()
        if 'dfnotes' in kwargs:
            dfnote=kwargs['dfnotes']
            #print(dfnotes)
            self.customerName=notes.notes(client,self.path_dir,dataframe=dfnote)
            self.swnotes=True
        else:
            self.swnotes=False
            
        orderList=[]
        if 'dfOrders' in kwargs:      
            orders=kwargs['dfOrders']
        if 'dfOrders' in kwargs: 
            dfPolines=kwargs['dfPolines']
        else:
            dfPolines=kwargs['dfOrders'] 
            noprint=True
        #poLines=dfPolines        
        orderDictionary={}      
        list={}
        #customerName=kwargs['client']
        changeVendor={}
        sivendor=0
        novendor=0
        cont=0
        sw=0
        self.count=0
        countlist=0
        store={}
        purchase=[]
        purchaseOrders={}
        notesprint={}
        notesapp=[]
        orderList=[]      
        orderDictionary={}      
        countvendorerror=0
        countpolerror=0
        list={}
        countpol=0
        for i, row in orders.iterrows():
            try:
                noprint=True
                printpoline=True
                self.nointance=False
                Order={}
                tic = time.perf_counter()
                self.count+=1
                #Order Number
                poNumberSuffix=""
                poNumberPrefix=""
                poNumber=""
                field="poNumberPrefix"
                if field in orders.columns:
                    if row[field]:
                        poNumberPrefix=str(row[field])
                        Order["poNumberPrefix"]=poNumberPrefix.strip()
                field="poNumberSuffix"
                if field in orders.columns:
                    if row[field]:
                       poNumberSuffix=str(row[field])
                       Order["poNumberSuffix"]=poNumberSuffix.strip()
                #if row['PO number'] in orders.columns:
                field="poNumber"                
                if field in orders.columns:
                    if row[field]:
                        masterPo=str(row[field]).strip()
                        po=self.check_poNumber(masterPo,self.path_results)
                        poNumber=po
                else:
                    randompoNumber=str(round(random.randint(100, 1000)))
                    poNumber=str(randompoNumber)
                    mf.write_file(ruta=self.path_logs+"\\oldNew_ordersID.log",contenido=poNumber)

                    po=poNumber
                #CHECKING DUPLICATED PO number    
                countlist = orderList.count(str(po))
                if countlist>0:
                    poNumber=str(po)+str(countlist)
                Order["poNumber"]= poNumber
                orderList.append(str(po))
                #print(orderList)                
                print(f"INFO RECORD: {self.count}  poNumber:  {poNumber} {self.descpoNumber}")
                #idOrder
                orderId=""
                if 'id' in orders.columns: orderId=str(row['UUID']).strip()#str(uuid.uuid4())
                else: orderId=str(uuid.uuid4())
                Order["id"]=orderId
                
                field="dateOrdered"
                if field in orders.columns:
                    if row[field]:
                        dateorder=row[field]
                        Order['dateOrdered']=mf.timeStamp(dateorder)
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
                    noteField="notes["+str(iter)+"]"
                    if noteField in orders.columns:
                        if row[noteField]:
                            notea.append(str(row[noteField]).strip())
                    else:
                        sw=False
                    iter+=1
                        
                Order["notes"]=notea
                if poNumber=="327472x":
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
                if 'orderType' in orders.columns:
                    ot=str(row['orderType']).strip()
                    result=""
                    result=self.mapping(self.orderType,ot)
                    if result is not None:
                        if result=="ongoing" or result=="Ongoing":
                            Order_type="Ongoing"
                            isongoing=mf.dic(isSubscription=False, manualRenewal=True) 
                            if 'ongoing.reviewPeriod' in orders.columns:
                                if row['ongoing.reviewPeriod']:
                                    reviewPeriod=int(row['ongoing.reviewPeriod'])
                            if 'ongoing.interval' in orders.columns:
                                if row['ongoing.interval']: interval=int(row['ongoing.interval'])
                            if 'ongoing.renewalDate' in orders.columns:    
                                if row['ongoing.renewalDate']: 
                                    renewalDate=mf.timeStamp(row['ongoing.renewalDate'])#f"2022-06-30T00:00:00.00+00:00"
                                    isongoing=mf.dic(interval=interval, isSubscription=True, manualRenewal=True, 
                                                   reviewPeriod=reviewPeriod, renewalDate=renewalDate)
                            Order["ongoing"]=isongoing                        
                Order["orderType"]=Order_type 
                
                ######################
                shipTo=""
                billTo=""
                if 'billTo' in orders.columns:
                    Order["billTo"]="f1f0acff-8d64-41ad-afdd-f9b923b2b3aa"
                if 'shipTo' in orders.columns:
                    Order["shipTo"]="f1f0acff-8d64-41ad-afdd-f9b923b2b3aa"
                OrganizationUUID=""            
                #file=self.customerName+"_organizations.json"
                
                #OrganizationUUID=mf.readJsonfile(self.path_refdata,client+"_organizations.json","organizations","undefined","code")
                #if OrganizationUUID is not None:
                organizationID=""
                if 'vendor' in orders.columns:
                    if row['vendor']:
                        vendorToSearch=str(row['vendor']).strip()
                        if vendorToSearch!="none" and vendorToSearch!="train":
                            vendorToSearch=int(vendorToSearch)
                        result=""
                        result=self.mapping(self.organizationCodeToChange,vendorToSearch)
                        if result is not None:
                            vendorToSearch=result
                            OrganizationUUID=mf.readJsonfile(self.path_refdata,client+"_organizations.json","organizations",vendorToSearch,"code")
                            if OrganizationUUID is None:
                                mf.write_file(ruta=self.path_logs+"\\vendorsNotFounds.log",contenido=f"{vendorToSearch}")
                                countvendorerror+=1
                                printpoline=False
                                noprint=False
                            else:
                                organizationID=OrganizationUUID[0]
                        else:
                            mf.write_file(ruta=self.path_logs+"\\vendorsNotFounds.log",contenido=f"{vendorToSearch}")
                            countvendorerror+=1
                            printpoline=False
                            noprint=False

                else:
                    print(f"ERROR Organization id must be present ")
                    noprint=False
                    printpoline=False

                Order["vendor"]=organizationID

                workflowStatus="Pending"
                approvedStatus= False
                field="workflowStatus"
                #row[0]
                if field in orders.columns:
                    if row[field]:
                        toSearch=str(row[field]).strip()
                        workflowStatus=self.mapping(self.workflowStatus,toSearch)                        
                        if workflowStatus is not None:
                            approvedStatus=True
                        
                Order["approved"]= approvedStatus
                Order["workflowStatus"]= workflowStatus

                #Reencumber
                reEncumber=False
                field="needReEncumber"
                if field in orders.columns:
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
                #COMPOSITE_PO_LINES
                if printpoline:
                    compositePo=self.compositePoLines(dfPolines,organizationID,masterPo,poNumber,client)
                    if compositePo is not None: 
                        Order["compositePoLines"]=compositePo
                        countpol+=1
                    else: 
                        Order["compositePoLines"]=[]
                        countpolerror+=1
                        

                    acqunituuid=[]
                    field="acqUnitIds"
                    if field in orders.columns:
                        if row[field]:
                            Acquisitionstemp=str(row[field]).strip()
                            acqunituuid.append(mf.readJsonfile(self.path_refdata,client+"_AcquisitionsUnits.json","AcquisitionsUnits",Acquisitionstemp,"name"))
                            if len(acqunituuid)==0:
                                mf.write_file(ruta=self.path_logs+"/AdqNotFound.log",contenido=f"{Acquisitionstemp}")
                        Order["acqUnitIds"]=acqunituuid
                    #PRINT NOTES
                    
                    Worder=Order
                    instanceOrder=Order
                    if noprint: 
                        if self.nointance:
                            mf.printObject(instanceOrder,self.path_results,self.count,f"{client}_purchaseOrderbyline_with_new_instance",False)
                            self.nointance=False
                        else:
                            mf.printObject(Order,self.path_results,self.count,f"{client}_purchaseOrderbyline",False)
                            purchase.append(Order)
                            noprint=True
                    else:
                        mf.printObject(Worder,self.path_results,self.count,f"{client}_worse_purchaseOrderbyline",False)
                        noprint=True

                    
            except Exception as ee:
                mf.printObject(Order,self.path_results,self.count,f"{client}_purchaseOrderbyline_worse",False)
                print(f"ERROR: {ee}")
                noprint=False
        
        purchaseOrders['purchaseOrders']=purchase    
        mf.printObject(purchaseOrders,self.path_results,self.count,f"{client}_purchaseOrders",True)
        print(f"============REPORT======================")
        report=[]
        #report=reports(df=orders,plog=path_logs,pdata=path_results,file_report=f"{customerName}_purchaseOrders.json",schema="purchaseOrders",dfFieldtoCompare=poLineNumberfield)
        print(f"RESULTS Record processed {self.count}")
        print(f"RESULTS poLines {countpol}")
        print(f"RESULTS poLines with errors: {countpolerror}")
        print(f"RESULTS vendor with errors: {countvendorerror}")
        print(f"RESULTS end")
    
#########################################
#POLINES FUNCTION             
#########################################
    '''def compositePoLines(self,poLines,notesapp1,notesapp1Pofield,
                     notesapp2,notesapp2Pofield,istherenotesApp,
                     vendors,masterPo,poLineNumber,customerName,path_results,path_refdata,path_logs):                 '''
        
    def compositePoLines(self,poLines,vendors,masterPo,poLineNumber,client):
        try:
            cpList=[]
            poCount=1 
            poLines = poLines[poLines['poNumber']== masterPo]
            linkId=""
            print(f"INFO POLINES founds for the record: {masterPo} # poLines:",len(poLines))
            for c, cprow in poLines.iterrows():
                cp={}
                if 'UUIDPOLINES' in poLines.columns:
                    if 'UUIDPOLINES' in poLines: linkId=cprow['UUIDPOLINES']#str(uuid.uuid4())
                else: linkId=str(uuid.uuid4())
                cp["id"]=linkId
                po_LineNumber=f"{poLineNumber}-{poCount}"
                cp["poLineNumber"]=po_LineNumber
                
                field="compositePoLines[0].publisher"
                if field in poLines.columns: 
                    if cprow[field]:
                        cp["publisher"]=cprow[field]
                #cp["purchaseOrderId"]=""
                #cp["id"]=""
                #cp["edition"]=""
                field="compositePoLines[0].checkinItems"
                checkinItems= False
                if field in poLines.columns:
                    if cprow[field]:
                        checkinItemstem=str(cprow[field]).strip()
                        if checkinItemstem.upper()=="YES":
                            checkinItems= True
                cp["checkinItems"]=checkinItems
                #cp["instanceId"]=""
                #cp["agreementId"]= ""
                field="compositePoLines[0].acquisitionMethod"
                acquisitionMethod="Purchase"
                if field in poLines.columns:
                    if cprow[field]:
                        result=self.mapping(self.acquisitionMethod,str(cprow[field]).strip())
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

                quantityPhysical=0
                field="compositePoLines[0].cost.quantityPhysical"
                if field in poLines.columns:
                    if cprow[field]:
                        quantityPhysical=int(cprow[field])

                quantityElectronic=0
                field="compositePoLines[0].cost.quantityElectronic"
                if field in poLines.columns:
                    if cprow[field]:#if cprow['QUANTITY'] NOT MIGRATED:
                        quantityElectronic=int(cprow[field])
                        
                orderFormat="Other"
                field="compositePoLines[0].orderFormat"     
                if field in poLines.columns:
                    if cprow[field]:
                        result=""
                        result=self.mapping(self.orderFormat,str(cprow[field]).strip())
                        if result is not None:
                            orderFormat=result
                ###
                #LOCATIONS(ORDER)
                ################################
                locationId=[]
                locsw=True
                field="compositePoLines[0].locations[0].locationId"
                if field in poLines.columns:
                    if cprow[field]:
                        locationtoSearch=str(cprow[field]).strip()
                        if locationtoSearch.find(",")==-1:
                            locsw=True
                            locationtoSearch=locationtoSearch.replace(" ","")
                            result=""
                            result=self.mapping(self.locations,str(cprow[field]).strip())
                            if result is not None:
                                locationId=mf.readJsonfile(self.path_refdata,f"{client}_locations.json","locations",result,"code")
                                if locationId is None:
                                    locationId="None"
                                    mf.write_file(ruta=self.path_logs+"\\locationsNotFounds.log",contenido=locationtoSearch)                            
                        else:
                            loca=[]
                            x = locationtoSearch.split(",")
                            locsw=False
                            lc=0
                            locationIdA=""
                            for i in x:
                                locationtoSearch=i
                                par=0
                                par=i.find("(")
                                if par>1:
                                    locationtoSearch=i[:par]
                                    qP=i[par+1]
                                else:
                                    qP=1
                                result=self.mapping(self.locations,locationtoSearch)
                                if result is not None:
                                    locid=mf.readJsonfile(self.path_refdata,f"{client}_locations.json","locations",result,"code")
                                    if locid is None:
                                        locid="None"
                                        mf.write_file(ruta=self.path_logs+"\\locationsNotFounds.log",contenido=locationtoSearch)
                                #loca1.append([str(locid[0]),qP])
                                if orderFormat.upper()=="PHYSICAL RESOURCE":
                                    loca.append({"locationId":locid[0],"quantity":int(qP), "quantityPhysical":int(qP)}) 
                                elif orderFormat.upper()=="ELECTRONIC RESOURCE":
                                    loca.append({"locationId":locid[0],"quantity":int(qP), "quantityElectronic":int(qP)})
                                else:
                                    loca.append({"locationId":locid[0],"quantity":int(qP), "quantityPhysical":int(qP)}) 
                                locid=[]
                                qP=""

                            #locationIdA=""
                ##TITLE
                
                ispackage=False
                field="compositePoLines[0].isPackage"
                if field in poLines.columns:
                    if cprow[field]:
                        ispackagetem=str(cprow[field]).strip()
                        if ispackagetem.upper()=="YES" or ispackagetem==True:
                            ispackage=True
                cp["isPackage"]=ispackage

                
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
                if field in poLines.columns:
                    if cprow[field]:
                        titlepoLine=str(cprow[field]).strip()
                        
                field="compositePoLines[0].instanceId"
                if field in poLines.columns:
                    if cprow[field]:
                        titleOrPackage=str(cprow[field]).strip()
                        titleUUID=str(cprow[field]).strip()
                        ordertitleUUID=self.get_title(client,element="instances",searchValue=titleUUID)
                        if len(ordertitleUUID)!=0:# is None:
                            print(f"INFO InstanceId / Title: {ordertitleUUID}")
                            cp["instanceId"]=str(ordertitleUUID[0])
                            cp["titleOrPackage"]=str(ordertitleUUID[1])
                            cp["isPackage"]=False
                        else: 
                            print(f"INFO Title: {titlepoLine}")
                            cp["titleOrPackage"]=titlepoLine
                            mf.write_file(ruta=f"{self.path_logs}\\titlesNotFounds.log",contenido=f"{po_LineNumber}  {titleUUID} {titlepoLine}")
                            self.createinstance(client,titlepoLine,titleUUID)
                            self.nointance=True
                else:
                    cp["titleOrPackage"]=titlepoLine
                    

                
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
                
                #MATERIAL PROVIDERS
                accessProvider=""
                accessProvider=vendors
                accessProvidertosearch="" 
                field="compositePoLines[0].eresource.accessProvider" 
                if field in poLines.columns:
                    if cprow[field]:
                        vendorToSearch=str(cprow[field]).strip()
                        if vendorToSearch!="none" and vendorToSearch!="train":
                            vendorToSearch=int(vendorToSearch)
                        result=""
                        result=self.mapping(self.organizationCodeToChange,vendorToSearch)
                        if result is not None:
                            accessProvidertosearch=result
                            accessproviderUUID=mf.readJsonfile(self.path_refdata,f"{client}_organizations.json","organizations",accessProvidertosearch,"code")
                            if accessproviderUUID is None:
                                mf.write_file(ruta=self.path_logs+"\\providerNotFounds.log",contenido=f"{accessProvidertosearch}")
                            else:
                                accessProvider=accessproviderUUID[0]
                        else:
                            mf.write_file(ruta=self.path_logs+"\\providerNotFounds.log",contenido=f"{accessProvidertosearch}")
                #MATERIAL SUPPLIER 
                materialSupplier=vendors
                materialSuppliertoSearch=""
                field="compositePoLines[0].physical.materialSupplier"       
                if field in poLines.columns:
                    if cprow[field]:
                        vendorToSearch=str(cprow[field]).strip()
                        if vendorToSearch!="none" and vendorToSearch!="train":
                            vendorToSearch=int(vendorToSearch)
                        result=""
                        result=self.mapping(self.organizationCodeToChange,vendorToSearch)
                        if result is not None:
                            materialSuppliertoSearch=result
                            materialproviderUUID=mf.readJsonfile(self.path_refdata,f"{client}_organizations.json","organizations",materialSuppliertoSearch,"code")
                            if materialproviderUUID is None:
                                mf.write_file(ruta=self.path_logs+"\\materialProviderNotFounds.log",contenido=f"{materialproviderUUID}")
                            else:
                                materialSupplier=materialproviderUUID[0]  
                        else:
                            mf.write_file(ruta=self.path_logs+"\\materialProviderNotFounds.log",contenido=f"{accessProvidertosearch}")
                #PRICE
                listUnitPrice=0.0
                field="compositePoLines[0].cost.listUnitPrice"
                if field in poLines.columns:
                    if cprow[field]:
                        lup=str(cprow[field]).strip()
                        lup=lup.replace(",","")
                        lup=lup.replace("$","")
                        lup=lup.replace("E","")
                        listUnitPrice=float(lup)
                currency="USD"
                field="compositePoLines[0].cost.currency"          
                if field in poLines.columns:
                    if cprow[field]:
                        currency=cprow[field]
                #Locations for print/mixed resources
                materialType=""
                field="compositePoLines[0].physical.materialType"
                if orderFormat.upper()  =="PHYSICAL RESOURCE":
                        #Material Type physical
                        #mtypestosearch="unespecified"
                        if field in poLines.columns:
                            if cprow[field]:
                                mtypestosearch=str(cprow[field]).strip()
                                materialType=mf.readJsonfile(self.path_refdata,f"{client}_mtypes.json","mtypes",mtypestosearch,"name")
                                #materialType=get_matId(mtypestosearch,customerName)
                                if materialType is None:
                                    mf.write_file(ruta=self.path_logs+"\\materialTypeNotFounds.log",contenido=f"{po_LineNumber} {mtypestosearch}")
                        #else:
                            
                        #    materialType=mf.readJsonfile(self.path_refdata,f"{client}_mtypes.json","mtypes","unspecified","name")
                        #    ruta=self.path_logs+"\\materialTypeNotFounds.log"
                        #    mf.write_file(ruta=ruta,contenido=f"{poLineNumber} {mtypestosearch}")
                        #    if materialType is None:
                        #        pass
                                
                        cp["orderFormat"]="Physical Resource"
                        cp["cost"]=mf.dic(currency=currency,listUnitPrice=listUnitPrice, quantityPhysical=quantityPhysical, poLineEstimatedPrice=listUnitPrice, discountType="percentage")
                        if materialType: cp["physical"]=mf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier, materialType=materialType)
                        else: cp["physical"]=mf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier)
                        if accessProvider: cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                        else: cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False)
                        if locsw:
                            if len(locationId)>0:cp["locations"]=[mf.dic(locationId=locationId[0],quantity=quantityPhysical, quantityPhysical=quantityPhysical)]
                            else: cp["locations"]=[mf.dic(quantity=quantityPhysical, quantityPhysical=quantityPhysical)]
                        else:
                            cp["locations"]=loca
                        
                elif orderFormat.upper()=="ELECTRONIC RESOURCE":
                        cp["orderFormat"]="Electronic Resource"
                        materialType=""
                        if 'compositePoLines[0].physical.materialType' in poLines.columns:
                            if cprow['compositePoLines[0].physical.materialType']:
                                mtypestosearch=""
                                mtypestosearch=str(cprow['compositePoLines[0].physical.materialType']).strip()
                                materialType=mf.readJsonfile(self.path_refdata,self.path_logs+"\\_mtypes.json","mtypes",mtypestosearch,"name")
                            #materialType=get_matId(mtypestosearch,customerName)
                            if materialType is None:
                                mf.write_file(ruta=self.path_logs+"\\materialTypeNotFounds.txt",contenido=f"{po_LineNumber} {mtypestosearch}")
                                materialType=mf.readJsonfile(self.path_refdata,f"{client}_mtypes.json","mtypes","unspecified","name")

                        cp["cost"]=mf.dic(listUnitPriceElectronic=listUnitPrice,currency=currency, quantityElectronic=quantityElectronic, poLineEstimatedPrice=listUnitPrice,discountType="percentage")
                        if materialType:
                            if accessProvider:
                                cp["eresource"]=mf.dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider,materialType=materialType)
                            else:
                                cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False)
                        else: 
                            if accessProvider:
                                cp["eresource"]=mf.dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                            else:
                                cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False)

                        if locationId: 
                            if locsw:
                                if len(locationId)>0: cp["locations"]=[mf.dic(locationId=locationId[0],quantity=1, quantityElectronic=quantityElectronic)]
                                else: cp["locations"]=[mf.dic(quantity=1, quantityElectronic=quantityElectronic)]
                            else:
                                cp["locations"]=loca 

                elif orderFormat.upper()=="P/E MIX":
                    cp["orderFormat"]="P/E Mix"
                    cp["cost"]=mf.dic(currency=currency,listUnitPrice=listUnitPrice,listUnitPriceElectronic=listUnitPrice, quantityPhysical=quantityPhysical, quantityElectronic=1, poLineEstimatedPrice=listUnitPrice,discountType="percentage")
                    if accessProvider: cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                    else: cp["eresource"]=mf.dic(activated=False,createInventory=instance_holdings_items,trial=False)
                    cp["locations"]=[mf.dic(locationId=locationId[0],quantity=2, quantityElectronic=1,quantityPhysical=quantityPhysical)]
                    if materialType: cp["physical"]=mf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier,
                                       expectedReceiptDate="",receiptDue="",materialType=materialType)
                    else: cp["physical"]=mf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier,
                                       expectedReceiptDate="",receiptDue="")
                else:   
                    cp["orderFormat"]="Other"
                    cp["cost"]=mf.dic(currency=currency,listUnitPrice=listUnitPrice, quantityPhysical=1, poLineEstimatedPrice=listUnitPrice, discountType="percentage")
                    if materialType: cp["physical"]=mf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier, materialType=materialType)
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
                field="compositePoLines[0].fundDistribution[0].code"
                if field in poLines.columns:
                    fundTosearch=str(cprow[field]).strip()
                    fundTosearch=fundTosearch.replace(" ","")
                    if fundTosearch=="none":
                        fundTosearch=""
                    if fundTosearch:
                        occurencias=int(fundTosearch.count("%"))
                        if occurencias==0:
                            searchExpensesValue=""
                            expenseClassId=""
                            field="compositePoLines[0].fundDistribution[0].expenseClassId"
                            if field in poLines.columns:
                                if cprow[field]:
                                    expsearchtoValue=str(cprow[field]).strip()
                                    expenseClassId=mf.readJsonfile(self.path_refdata,f"{client}_expenseClasses.json","expenseClasses",expsearchtoValue,"code")
                            #expenseClassId=get_Id(customerName,element="expenseClasses",searchValue=searchtoValue)
                            #get_funId(searchValue,orderFormat,client):
                            result=""
                            result=self.mapping(self.funds,fundTosearch)
                            if result is not None:
                                fundcodeTosearch=result
                            fundId=mf.readJsonfile_fund(self.path_refdata,f"{client}_funds.json","funds",fundcodeTosearch,"code")
                            #fundId=get_funId(codeTosearch,orderFormat,customerName)
                            if fundId is not None:
                                code=fundId[1]
                                fundId=fundId[0]
                                valuefund=100.0
                                if expenseClassId:
                                    cp["fundDistribution"]=[mf.dic(code=code,fundId=fundId,expenseClassId=expenseClassId,distributionType="percentage",value=valuefund)]
                                else:
                                    cp["fundDistribution"]=[mf.dic(code=code,fundId=fundId,distributionType="percentage",value=valuefund)]
                            else:
                                mf.write_file(ruta=self.path_logs+"\\fundsNotfounds.log",contenido=f"{po_LineNumber} {fundcodeTosearch}")
                                
                        else:
                            fundlist=[]
                            fundDistribution=[]
                            fundlist = codeTosearch.split(",")
                            i=0
                            for fundin in fundlist:
                                cadena=fundin
                                codeTosearch=cadena[:5]
                                valuefund=cadena[6:10]
                                searchExpensesValue=""
                                expenseClassId=""
                                field="compositePoLines[0].fundDistribution[0].expenseClassId"
                                if field in poLines.columns:
                                    if cprow[field]:
                                        expsearchtoValue=str(cprow[field]).strip()
                                        expenseClassId=mf.readJsonfile(self.path_refdata,f"{client}_expenseClasses.json","expenseClasses",expsearchtoValue,"code")
                                        if expenseClassId is None:
                                            mf.write_file(ruta=self.path_logs+"\\expensesNotfounds.log",contenido=f"{po_LineNumber} {expsearchtoValue}")
                                        #expenseClassId=get_Id(customerName,element="expenseClasses",searchValue=searchtoValue)
                                    #get_funId(searchValue,orderFormat,client):
                                fundId=mf.readJsonfile_fund(self.path_refdata,f"{client}_funds.json","funds",codeTosearch,"code")
                                #fundId=get_funId(codeTosearch,orderFormat,customerName)
                                if fundId is not None:
                                    code=fundId[1]
                                    fundId=fundId[0]
                                    if expenseClassId:
                                        fundDistribution.append(mf.dic(code=code,fundId=fundId,expenseClassId=expenseClassId,distributionType="percentage",value=valuefund))

                cp["fundDistribution"]=fundDistribution

                #Ongoing
                receiptDate=""
                
                field="compositePoLines[0].receiptDate"
                if field in poLines.columns:
                    if cprow[field]:
                        if cprow[field]!="  -  -  ":
                            dater=cprow[field]
                            receiptDate=mf.timeStamp(str(cprow[field]))  
                cp['receiptDate']=receiptDate
                
                receivingNote=""    
                field="compositePoLines[0].details.receivingNote"
                if field in poLines.columns:
                    receivingNote=str(cprow[field]).strip()
                    
                subscriptionFrom=""
                subscriptionTo=""
                subscriptionInterval=""
                field="compositePoLines[0].details.subscriptionFrom"
                if field in poLines.columns:                    
                    if cprow[field]: 
                        subscriptionFrom=mf.timeStamp(str(cprow[field]))
                        
                field="compositePoLines[0].details.subscriptionTo"
                if field in poLines.columns:
                    if cprow[field]: subscriptionTo=mf.timeStamp(cprow[field])
                field="compositePoLines[0].details.subscriptionInterval"
                if field in poLines.columns:
                    if cprow[field]: subscriptionInterval=int(cprow[field])
                productIds=[]
                cp["details"]=mf.dic(receivingNote=receivingNote,productIds=productIds,subscriptionFrom=subscriptionFrom,
                                                           subscriptionInterval=subscriptionInterval, subscriptionTo=subscriptionTo)
                description=""
                field="compositePoLines[0].donor"
                if field in poLines.columns:
                    if cprow['field']:
                        donor=cprow['field']
                    cp["donor"]=donor
                
                paymentStatus="Pending"
                field="compositePoLines[0].paymentStatus"
                if field in poLines.columns:
                    if cprow[field]:
                        result=""
                        result=self.mapping(self.paymentStatus,str(cprow[field]).strip())
                        if result is not None:
                            paymentStatus=result
                cp['paymentStatus']=paymentStatus
                
                description=""
                field="compositePoLines[0].description"
                if field in poLines.columns:
                    if cprow[field]:
                        description=str(cprow[field]).strip()
                        cp["description"]=description
                
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
                    receiptStatus=cprow[field]
                    result=""
                    result=self.mapping(self.receiptStatus,str(cprow[field]).strip())
                    if result is not None:
                       receiptStatus=result
                        
                cp["receiptStatus"]=receiptStatus
                #cp["reportingCodes"]=dic(code="",id="",description="")
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
                           
                if self.swnotes:      #dataframe,toSearch,linkId):
                        self.customerName.readnotes(client,dataframe=self.dfnotes,toSearch=masterPo,linkId=linkId)
                cpList.append(cp)
                #cpList.append(linkid)
                poCount+=1
            return cpList    
        except Exception as ee:
            print(ee)
            print(poLineNumber)
            mf.write_file(ruta=self.path_logs+"\\poLinesErrors.log",contenido=f"Order:{masterPo} {ee}")  

    def mapping(self,dftoSearch,toSearch):
        try:
            #print(dftoSearch)                    
            dataToreturn=""
            temp = dftoSearch[dftoSearch['LEGACY SYSTEM']== toSearch]
            #print("Mapping found: ",len(temp))
            if len(temp)>0:
                for x, cptemp in temp.iterrows():
                    dataToreturn=cptemp['FOLIO']
            else:
                mf.write_file(ruta=self.path_logs+"\\workflowNotfound.log",contenido=f"{toSearch}")
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
                    value=Newmpol            
            return value
        except Exception as ee:
            print(f"INFO check_poNumber function failed {ee} field {field}")
            
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
            query=f"?query=(identifiers="
            #query=f"query=hrid=="
            #/finance/funds?query=name==UMPROQ
            search='"'+searchValue+'")'
            #.b10290242
            #paging_q = f"?{query}"+search
            paging_q = f"{query} "+search
            path = pathPattern+paging_q
            #data=json.dumps(payload)
            url = okapi_url + path
            req = requests.get(url, headers=okapi_headers)
            idhrid=[]
            if req.status_code != 201:
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                if (total_recs!=0):
                    rec=json_str[element]
                    #print(rec)
                    l=rec[0]
                    if 'id' in l:
                        idhrid.append(l['id'])
                        idhrid.append(l['title'])            
            return idhrid
        except Exception as ee:
            print(f"INFO get_title function failed {ee}")
            
            
    def createinstance(self, client,titleOrPackage,titleUUID):
        print(f"INFO Creating instance for title {titleOrPackage} {titleUUID}")
        instance= {
                                    "id": str(uuid.uuid4()),
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
        mf.printObject(instance,self.path_results,0,f"{client}_instances",False)