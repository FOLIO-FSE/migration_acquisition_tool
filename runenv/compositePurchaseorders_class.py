import datetime
import warnings
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
import backup_restore as br
import main_functions as mf
import time
from datetime import datetime
import yaml
import shutil


class compositePurchaseorders():
    def __init__(self,client,path_dir):
        try:    
            self.customerName=client
            #os.mkdir(f"{path_dir}\\results")
            self.path_results=f"{path_dir}\\results"
            #os.mkdir(f"{path_dir}\\data")
            self.path_data=f"{path_dir}\\data"
            #os.mkdir(f"{path_dir}\\logs")
            self.path_logs=f"{path_dir}\\logs"
            #os.mkdir(f"{path_dir}\\refdata")
            self.path_refdata=f"{path_dir}\\refdata"
            self.ordersbyline=self.path_logs+"\\"+self.customerName+"_ordersbyline.json"
            print(self.ordersbyline)
            self.ordersbyline=open(self.ordersbyline, 'w') 
        except Exception as ee:
            print(f"ERROR: {ee}")
            
    def readMappingfile(self):
        self.customerName=pd.dataframe()
        filetoload=self.path_refdata+f"\\acquisitionMapping.xlsx"
        print("INFO Reading Mapping file")
        self.acquisitionMethod=self.customerName.importDataFrame(filetoload,sheetName="acquisitionMethod")
        #print("Dataframe: Order Format")
        self.orderFormat=self.customerName.importDataFrame(filetoload,sheetName="orderFormat")
        #print("Dataframe: Order Type")
        self.orderType=self.customerName.importDataFrame(filetoload,sheetName="orderType")
        #print("Dataframe: Payment Status")
        self.paymentStatus=self.customerName.importDataFrame(filetoload,sheetName="paymentStatus")
        #print("Dataframe: Receipt Status")
        self.receiptStatus=self.customerName.importDataFrame(filetoload,sheetName="receiptStatus")
        #print("Dataframe: WorkFlowStatus")
        self.workflowStatus=self.customerName.importDataFrame(filetoload,sheetName="workflowStatus")
        #print("Dataframe: Locations")
        self.locations=self.customerName.importDataFrame(filetoload,sheetName="locations")
        #print("Dataframe: Funds/Expenses")
        self.fundsExpenseClass=self.customerName.importDataFrame(filetoload,sheetName="fundsExpenseClass")
        #print("Dataframe: Funds")
        self.funds=self.customerName.importDataFrame(filetoload,sheetName="funds")
        #print("Dataframe: Organization code to Change - optional")
        self.organizationCodeToChange=self.customerName.importDataFrame(filetoload,sheetName="organizationCodeToChange")
        with open(self.path_refdata+"\\composite_purchase_order_mapping.json") as json_mappingfile:
            self.mappingdata = json.load(json_mappingfile)
        
        #print(self.acquisitionMethod)
################################
##ORDERS FUNCTION
################################
#def readorders(path,file_name,sheetName,customerName,spread):
#        try:
    def readorders(self, client,dfOrders, dfPolines):
        
        self.readMappingfile()
        
        orderList=[]      
        orders=dfOrders
        #poLines=dfPolines        
        orderDictionary={}      
        list={}
        #customerName=kwargs['client']
        changeVendor={}
        sivendor=0
        novendor=0
        cont=0
        sw=0
        count=0
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
                printpoline=True
                Order={}
                tic = time.perf_counter()
                count+=1
                #Order Number
                poNumberSuffix=""
                poNumberPrefix=""
                poNumber=""
                if 'poNumberPrefix' in orders.columns:
                    if row['poNumberPrefix']:
                        poNumberPrefix=str(row['Prefix'])
                        Order["poNumberPrefix"]=poNumberPrefix.strip()
                if 'poNumberSuffix' in orders.columns:
                    if row['poNumberSuffix']:
                       poNumberSuffix=str(row['Suffix'])
                       Order["poNumberSuffix"]=poNumberSuffix.strip()
                #if row['PO number'] in orders.columns:
                if 'poNumber' in orders.columns:
                    if row['poNumber']:
                        masterPo=str(row['poNumber']).strip()
                        po=self.check_poNumber(masterPo,self.path_results)
                        #print(po)
                        #poNumber=po[1:]
                        poNumber=po
                        #print(po)
                        Order["poNumber"]= poNumber
                else:
                    randompoNumber=str(round(random.randint(100, 1000)))
                    poNumber=str(randompoNumber)
                    mf.write_file(ruta=self.path_logs+"\\oldNew_ordersID.log",contenido=poNumber)

                    po=poNumber
                #CHECKING DUPLICATED PO number    
                countlist = orderList.count(str(po))
                if countlist>0:
                    poNumber=str(po)+str(countlist)

                orderList.append(str(po))
                #print(orderList)                
                print("INFO RECORD: "+str(count)+"    poNumber:  "+poNumber)
                #idOrder

                if 'id' in orders.columns: Order["id"]=str(row['UUID'])#str(uuid.uuid4())
                else: Order["id"]=str(uuid.uuid4())
                #Order["approvedById"]=""
                #Order["approvalDate"]= ""
                #Order["closeReason"]=dic(reason="",note="")
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
                if poNumber=="XY117489":
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
                    result=""
                    result=self.mapping(self.orderType,str(row['orderType']).strip())
                    if result is not None:
                        if result=="ongoing" or result=="Ongoing":
                            Order_type="Ongoing"
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
                
                OrganizationUUID=mf.readJsonfile(self.path_refdata,client+"_organizations.json","organizations","undefined","code")
                if OrganizationUUID is not None:
                    if 'vendor' in orders.columns:
                        if row['vendor']:
                            vendorToSearch=str(row['vendor']).strip()
                            OrganizationUUID=mf.readJsonfile(self.path_refdata,client+"_organizations.json","organizations",vendorToSearch,"code")
                            if OrganizationUUID is None:
                                mf.write_file(ruta=self.path_logs+"\\vendorsNotFounds.log",contenido=f"{vendorToSearch}")
                                countvendorerror+=1
                                printpoline=False
                    else:
                        print(f"ERROR {client}_organizations.json not exit in {client}/refdata folder")
                        return None

                Order["vendor"]=OrganizationUUID[0]

                workflowStatus="Pending"
                approvedStatus= False
                if 'workflowStatus' in orders.columns:
                    if row['workflowStatus']:
                        toSearch=str(row['workflowStatus']).strip()
                        workflowStatus=self.mapping(self.workflowStatus,toSearch)
                        if workflowStatus is not None:
                            approvedStatus=True
                Order["approved"]= approvedStatus
                Order["workflowStatus"]= workflowStatus

                #Reencumber
                reEncumber=False
                if 'needReEncumber' in orders.columns:
                    if row['needReEncumber']:
                        reencumbertem=str(row['needReEncumber']).strip()
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
                    compositePo=self.compositePoLines(dfPolines,OrganizationUUID,masterPo,poNumber,client)
                    if compositePo is not None: 
                        Order["compositePoLines"]=compositePo
                        countpol+=1
                    else: 
                        Order["compositePoLines"]=[]
                        countpolerror+=1

                    acqunituuid=[]
                    if 'acqUnitIds' in orders.columns:
                        if row['acqUnitIds']:
                            Acquisitionstemp=str(row['acqUnitIds']).strip()
                            acqunituuid.append(mf.readJsonfile(path_refdata,f"{customerName}_AcquisitionsUnits.json","AcquisitionsUnits",Acquisitionstemp,"name"))
                            if len(acqunituuid)==0:
                                mf.write_file(ruta=self.path_logs+"/AdqNotFound.log",contenido=f"{Acquisitionstemp}")
                        Order["acqUnitIds"]=acqunituuid

                    mf.printObject(Order,self.path_results,count,f"{client}_purchaseOrderbyline.json",False)
                    purchase.append(Order)

                    
            except Exception as ee:
                mf.printObject(Order,self.path_results,count,f"{client}_purchaseOrderbyline_worse.json",False)
                print(f"ERROR: {ee}")
        
        purchaseOrders['purchaseOrders']=purchase    
        mf.printObject(purchaseOrders,self.path_results,count,f"{client}_purchaseOrders",True)
        print(f"============REPORT======================")
        report=[]
        #report=reports(df=orders,plog=path_logs,pdata=path_results,file_report=f"{customerName}_purchaseOrders.json",schema="purchaseOrders",dfFieldtoCompare=poLineNumberfield)
        print(f"RESULTS Record processed {count}")
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
           #locationMapping={"af":"mnaf","aq":"mnaq","br":"mnbr","ca":"mnca","cv":"mncv","dv":"mndv","ir":"mnir","ns":"mnns","ov":"mnov","ss":"mnss"}
            cpList=[]
            count=1 
            poLines = poLines[poLines['poNumber']== masterPo]
            print("poLines founds records: ",len(poLines))
            for c, cprow in poLines.iterrows():
                cp={}
                if 'UUIDPOLINES' in poLines.columns:
                    if 'UUIDPOLINES' in poLines: linkid=cprow['UUIDPOLINES']#str(uuid.uuid4())
                else: linkid=str(uuid.uuid4())
                cp["id"]=linkid
                cp["poLineNumber"]=str(poLineNumber)+"-"+str(count)
                if 'publisher' in poLines.columns: 
                    if cprow['publisher']:
                        cp["publisher"]=cprow['publisher']
                #cp["purchaseOrderId"]=""
                #cp["id"]=""
                #cp["edition"]=""
                checkinItems= False
                if 'checkinItems' in poLines.columns:
                    if cprow['checkinItems']:
                        checkinItemstem=str(cprow['checkinItems']).strip()
                        checkinItemstem=checkinItemstem.upper()
                        if checkinItemstem=="YES":
                            checkinItems= True
                cp["checkinItems"]=checkinItems
                #cp["instanceId"]=""
                #cp["agreementId"]= ""
                acquisitionMethod="Purchase"
                if 'acquisitionMethod' in poLines.columns:
                    if cprow['acquisitionMethod']:
                        result=self.mapping(self.acquisitionMethod,str(cprow['acquisitionMethod']).strip())
                        if result is not None:
                            acquisitionMethod=result
                
                cp["acquisitionMethod"]= acquisitionMethod   
                #cp["alerts"]=dic(alert="Receipt overdue",id="9a665b22-9fe5-4c95-b4ee-837a5433c95d")
                cp["cancellationRestriction"]= False
                #cp["cancellationRestrictionNote"]=""
                #cp["claims"]=dic()Fa""
                collection=False
                if 'collection' in poLines.columns:
                    if cprow["collection"]:
                        collection=str(cprow["collection"]).strip()
                        collection=collection.upper()
                        if collection=="YES":
                            collection=True
                cp["collection"]=collection

                rush=False
                if 'rush' in poLines.columns:
                    if cprow["rush"]:
                        rush=str(cprow["rush"]).strip()
                        rush=rush.upper()
                        if rush=="YES":
                            rush=True
                cp["rush"]=rush

                #cp["contributors"]=[dic()]

                quantityPhysical=1
                if 'quantityPhysical' in poLines.columns:
                    if cprow['quantityPhysical']:
                        quantityPhysical=int(cprow['quantityPhysical'])

                quantityElectronic=1
                if 'quantityElectronic' in poLines.columns:
                    if cprow['quantityElectronic']:#if cprow['QUANTITY'] NOT MIGRATED:
                        quantityElectronic=int(cprow['quantityElectronic'])
                        
                orderFormat=""     
                if 'orderFormat' in poLines.columns:
                    if cprow['orderFormat']:
                        result=""
                        result=self.mapping(self.orderFormat,str(cprow['orderFormat']).strip())
                        if result is not None:
                            orderFormat=result      
                ###
                #LOCATIONS(ORDER)
                ################################
                locationId=""
                locsw=True
                if 'locationId' in poLines.columns:
                    if cprow['locationId']:
                        locationtoSearch=str(cprow['locationId']).strip()
                        if locationtoSearch.find(",")==-1:
                            locsw=True
                            locationtoSearch=locationtoSearch.replace(" ","")
                            result=""
                            result=self.mapping(self.locations,str(cprow['locationId']).strip())
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
                                if orderFormat=="Physical Resource":
                                    loca.append({"locationId":locid[0],"quantity":int(qP), "quantityPhysical":int(qP)}) 
                                elif orderFormat=="Electronic Resource":
                                    loca.append({"locationId":locid[0],"quantity":int(qP), "quantityElectronic":int(qP)})
                                else:
                                    loca.append({"locationId":locid[0],"quantity":int(qP), "quantityPhysical":int(qP)}) 
                                locid=[]
                                qP=""

                            #locationIdA=""
                ##TITLE
                ispackage=False
                if 'isPackage' in poLines.columns:
                    if cprow['isPackage']:
                        ispackagetem=str(cprow['isPackage']).strip()
                        ispackagetem=ispackagetem.upper()
                        if ispackagetem=="YES" or ispackagetem==True:
                            ispackage=True
                cp["isPackage"]=ispackage

                titleOrPackage="No Title"
                enum=["Instance, Holding, Item","Instance, Holding","Instance","None"]
                instance_holdings_items=enum[3]
                if 'physical.createInventory' in poLines.columns:
                    if cprow['physical.createInventory']:
                        instholitem=str(cprow['physical.createInventory']).strip()
                        if instholitem=="Instance, Holding, Item":  
                            instance_holdings_items=enum[0]
                        elif instholitem=="Instance, Holding":
                            instance_holdings_items=enum[1]
                        elif instholitem=="Instance":
                            instance_holdings_items=enum[2]
                        else: 
                            instance_holdings_items=enum[3]
                
                if 'titleOrPackage' in poLines.columns:
                    if cprow['titleOrPackage']:
                        if ispackage:
                            titleOrPackage=str(cprow['titleOrPackage']).strip()
                            cp["titleOrPackage"]=titleOrPackage 
                        else: 
                            #ispackage==False:
                            titleUUID=str(cprow['titleOrPackage']).strip()
                            ordertitleUUID=self.get_title(client,element="instances",searchValue=titleUUID)
                        #ordertitleUUID=readJsonfile_identifier(path_refdata,f"{customerName}_instances.json","instances",titleUUID)
                            if len(ordertitleUUID)!=0:# is None:
                            #instance_holdings_items="None"
                                print(ordertitleUUID)
                                cp["instanceId"]=str(ordertitleUUID[0])
                                cp["titleOrPackage"]=str(ordertitleUUID[1])
                                cp["isPackage"]=False
                            else: 
                                titleOrPackage=cprow['titleOrPackage']
                                cp["titleOrPackage"]=titleOrPackage
                                #instance_holdings_items="None"
                                #instance_holdings_items="Instance"
                                mf.write_file(path=f"{path_logs}\\titlesNotFounds.log",contenido=f"{poLineNumber}  {titleUUID} {titleOrPackage}")
                                self.createinstance(client,titleOrPackage,titleUUID)
                else:
                    cp["titleOrPackage"]=titleOrPackage
                ################################
                ### ORDER FORMAT
                ################################            
                
                materialType=""
                accessProvider=""
                accessproviderUUID=""
                if 'eresource.activated' in poLines.columns:
                    if cprow['eresource.activated']:
                        eresourceactivated=cprow['eresource.activated']
                        cp["eresource.activated"]=eresourceactivated
                    
                
                if 'eresource.accessProvider' in poLines.columns:
                    if cprow['eresource.accessProvider']:
                        accessProvidertosearch=str(cprow['eresource.accessProvider']).strip()
                        accessproviderUUID=mf.readJsonfile(self.path_refdata,f"{client}_organizations.json","organizations",accessProvidertosearch,"code")
                        if accessproviderUUID is None:
                            mf.write_file(ruta=self.path_logs+"\\providerNotFounds.log",contenido=f"{accessProvidertosearch}")
                        else:
                            accessProvider=accessproviderUUID

                materialSupplier=vendors[0]        
                if 'physical.materialSupplier' in poLines.columns:
                    if cprow['physical.materialSupplier']:
                        materialaccessProvidertosearch=str(cprow['physical.materialSupplier']).strip()
                        accessproviderUUID=mf.readJsonfile(self.path_refdata,f"{client}_organizations.json","organizations",materialaccessProvidertosearch,"code")
                        if accessproviderUUID is None:
                            mf.write_file(ruta=self.path_logs+"\\materialProviderNotFounds.log",contenido=f"{materialaccessProvidertosearch}")
                        else:
                            materialSupplier=accessproviderUUID            


                listUnitPrice=0.00
                if 'cost.listUnitPrice' in poLines.columns:
                    if cprow['cost.listUnitPrice']:
                        try:
                            listUnitPrice=float(cprow['cost.listUnitPrice'])
                        except Exception as ee:
                            print(ee)
                            lP=str(cprow['cost.listUnitPrice'])
                            lP=lP.replace("NAD$","")
                            lP=lP.replace(",","")
                            listUnitPrice=float(lP)
                
                currency="NAD"
                if 'cost.currency' in poLines.columns:
                    currency=cprow['cost.currency']
                        
                #Locations for print/mixed resources
                materialType=""
                if orderFormat=="Physical Resource" or orderFormat=="P/E Mix":
                        #Material Type physical
                        #mtypestosearch="unespecified"
                        if 'physical.materialType' in poLines.columns:
                            if cprow['physical.materialType']:
                                mtypestosearch=str(cprow['physical.materialType']).strip()
                                materialType=mf.readJsonfile(self.path_refdata,f"{customerName}_mtypes.json","mtypes",mtypestosearch,"name")
                                #materialType=get_matId(mtypestosearch,customerName)
                                if materialType is None:
                                    mf.write_file(ruta=self.path_logs+"\\materialTypeNotFounds.log",contenido=f"{poLineNumber} {mtypestosearch}")
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
                            cp["locations"]=[mf.dic(locationId=locationId[0],quantity=quantityPhysical, quantityPhysical=quantityPhysical)]
                        else:
                            cp["locations"]=loca

                elif orderFormat=="Electronic Resource":
                        cp["orderFormat"]="Electronic Resource"
                        materialType=""
                        if 'eresource.materialType' in poLines.columns:
                            if cprow['eresource.materialType']:
                                mtypestosearch=""
                                mtypestosearch=str(cprow['eresource.materialType']).strip()
                                materialType=mf.readJsonfile(self.path_refdata,self.path_logs+"\\_mtypes.json","mtypes",mtypestosearch,"name")
                            #materialType=get_matId(mtypestosearch,customerName)
                            if materialType is None:
                                mf.write_file(ruta=self.path_logs+"\\materialTypeNotFounds.txt",contenido=f"{poLineNumber} {mtypestosearch}")
                                materialType=mf.readJsonfile(self.path_refdata,f"{client}_mtypes.json","mtypes","unspecified","name")

                        cp["cost"]=mf.dic(currency=currency,listUnitPrice=listUnitPrice,listUnitPriceElectronic=listUnitPrice, quantityElectronic=quantityElectronic, poLineEstimatedPrice=listUnitPrice,discountType="percentage")
                        if materialType: 
                            cp["eresource"]=mf.dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider,materialType=materialType)
                        else: 
                            cp["eresource"]=mf.dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)

                        if locationId: 
                            if locsw:
                                cp["locations"]=[mf.dic(locationId=locationId[0],quantity=1, quantityElectronic=quantityElectronic)]
                            else:
                                cp["locations"]=loca 

                elif orderFormat=="P/E Mix":
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
                            cp["locations"]=[mf.dic(locationId=locationId[0],quantity=1, quantityElectronic=quantityElectronic)]
                        else:
                            cp["locations"]=loca 

                #FUNDS DISTRIBUTIONS
                fundDistribution=[]
                
                #EXPENSES CLASES
                #FUND DISTRIBUTION BY RESOURCE
                if 'fundDistribution[0].fundId' in poLines.columns:
                    codeTosearch=str(cprow['fundDistribution[0].fundId']).strip()
                    codeTosearch=codeTosearch.replace(" ","")
                    if codeTosearch=="none":
                        codeTosearch=""
                    if codeTosearch:
                        occurencias=int(codeTosearch.count("%"))
                        if occurencias==0:
                            searchExpensesValue=""
                            expenseClassId=""
                            if 'fundDistribution[0].expenseClassId' in poLines.columns:
                                if cprow['fundDistribution[0].expenseClassId']:
                                    searchtoValue=str(cprow['fundDistribution[0].expenseClassId']).strip()
                                    expenseClassId=mf.readJsonfile(self.path_refdata,f"{client}_expenseClasses.json","expenseClasses",searchtoValue,"code")
                            #expenseClassId=get_Id(customerName,element="expenseClasses",searchValue=searchtoValue)
                        #get_funId(searchValue,orderFormat,client):
                        fundId=mf.readJsonfile_fund(self.path_refdata,f"{client}_funds.json","funds",codeTosearch,"code")
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
                            mf.write_file(path=path_logs+"\\fundsNotfounds.log",contenido=f"{poLineNumber} {codeTosearch}")
                            cp["fundDistribution"]=[]
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
                            if 'fundDistribution[0].expenseClassId' in poLines.columns:
                                if cprow['fundDistribution[0].expenseClassId']:
                                    searchtoValue=str(cprow['fundDistribution[0].expenseClassId']).strip()
                                    expenseClassId=mf.readJsonfile(self.path_refdata,f"{client}_expenseClasses.json","expenseClasses",searchtoValue,"code")
                                    if expenseClassId is None:
                                        mf.write_file(path=path_logs+"\\expensesNotfounds.log",contenido=f"{poLineNumber} {searchtoValue}")
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
                receivingNote=""
                if 'receiptDate' in poLines.columns:
                    if cprow['receiptDate']: 
                        receivingNote=str(cprow['receiptDate'])
                if 'details.receivingNote' in poLines.columns:
                    receivingNote=str(cprow['details.receivingNote']).strip()
                subscriptionFrom=""
                subscriptionTo=""
                subscriptionInterval=""
                if 'details.subscriptionFrom' in poLines.columns:
                    if cprow['details.subscriptionFrom']: subscriptionFrom=mf.timeStamp(cprow['details.subscriptionFrom'])
                if 'details.subscriptionInterval' in poLines.columns:
                    if cprow['details.subscriptionTo']: subscriptionTo=mf.timeStamp(cprow['details.subscriptionTo'])
                if 'details.subscriptionInterval' in poLines.columns:
                    if cprow['details.subscriptionInterval']: subscriptionInterval=int(cprow['details.subscriptionInterval'])
                productIds=[]
                #if cprow['CODE1']:
                #    if cprow['CODE1']!="-":
                #        if cprow['CODE1']!="NONE":
                #            productIds.append(dic(productId=str(cprow['CODE1']).strip(), productIdType="8e3dd25e-db82-4b06-8311-90d41998c109"))
                #if cprow['CODE3']:
                #    if cprow['CODE3']!="-":
                #        productIds.append(dic(productId=str(cprow['CODE3']).strip(), productIdType="8e3dd25e-db82-4b06-8311-90d41998c109"))
                #if cprow['RECORD #(BIBLIO)']:
                #    reportNumberdata="."+str(cprow['RECORD #(BIBLIO)']).strip()
                #    productIds.append(dic(productId=reportNumberdata, productIdType="37b65e79-0392-450d-adc6-e2a1f47de452"))

                cp["details"]=mf.dic(receivingNote=receivingNote,productIds=productIds,subscriptionFrom=subscriptionFrom,
                                                           subscriptionInterval=subscriptionInterval, subscriptionTo=subscriptionTo)

                #cp["donor"]=""
                
                paymentStatus="Pending"
                if 'paymentStatus' in poLines.columns:
                    if cprow['paymentStatus']:
                        result=""
                        result=self.mapping(self.paymentStatus,str(cprow["paymentStatus"]).strip())
                        if result is not None:
                            paymentStatus=result
                cp['paymentStatus']=paymentStatus
                
                description=""
                if 'description' in poLines.columns:
                    if cprow['description']:
                        description=str(cprow['description']).strip()
                        cp["description"]=description

                #cp["publicationDate"]=""
                receiptStatus="Awaiting Receipt"
                if 'receiptDate' in poLines.columns:
                    cp["receiptDate"]=""
                    if cprow['receiptDate']:
                        receitdate=str(cprow['receiptDate'])
                        M=receitdate[0:2]
                        D=receitdate[3:5] 
                        Y=receitdate[6:10]
                    if Y=="96" or Y=="97" or Y=="98" or Y=="99":
                        Y=f"19{Y}"
                    cp["receiptDate"]=f"{Y}-{M}-{D}T00:00:00.00+00:00"
                    
                if 'receiptStatus' in poLines.columns:
                    receiptStatus=cprow['receiptStatus']
                    result=""
                    result=self.mapping(self.receiptStatus,str(cprow["receiptStatus"]).strip())
                    if result is not None:
                       receiptStatus=result
                        
                cp["receiptStatus"]=receiptStatus
                #cp["reportingCodes"]=dic(code="",id="",description="")
                Requester=""
                if 'requester' in poLines.columns:
                    if cprow['requester']:
                        Requester=str(cprow['requester'])
                        cp["requester"]=Requester
                
                selector=""
                if 'selector' in poLines.columns:
                    if cprow['selector']:
                        cp['selector']=str(cprow['selector'])

                cp["source"]="User"
                vendorAccount=""
                referenceNumbers=[]
                #referenceNumbers.append(dic(refNumber=refNumber, refNumberType="Vendor title number"))
                #if cprow['RECORD #(ORDER)']:
                #    refNumber=str(cprow['RECORD #(ORDER)']).strip()
                #    referenceNumbers.append(dic(refNumber=refNumber, refNumberType="Vendor order reference number"))

                #instrVendor=""   
                #lw=False
                #notesapp2 = notesapp2[notesapp2['RECORD #(ORDER)']== idSearch]
                #print("Instruction vendors were founds: ",len(notesapp2))
                #for a, nrow in notesapp2.iterrows():
                #    if nrow['VEN. NOTE']:
                #        instrVendor=nrow['VEN. NOTE']
                #        lw=True

                #cp["vendorDetail"]=dic(instructions=instrVendor, referenceNumbers=referenceNumbers, vendorAccount="")
                #NOTES GRAL
                #for nt in istherenotesApp:
                #    notes_single_line(linkid,masterPo, nt,notesapp1Pofield,"PAID","Orders note", path_results,path_refdata,count)


                cpList.append(cp)
                #cpList.append(linkid)
                count=count+1
            return cpList    
        except Exception as ee:
            print(ee)
            mf.write_file(path=self.path_logs+"\\poLinesErrors.log",contenido=f"Order:{masterPo} {ee}")  

    def mapping(self,dftoSearch,toSearch):
        try:                    
            dataToreturn=""
            temp = dftoSearch[dftoSearch['LEGACY SYSTEM']== toSearch]
            #print("poLines founds records: ",len(temp))
            if len(temp)>0:
                for x, cptemp in temp.iterrows():
                    dataToreturn=cptemp['FOLIO']
            else:
                mf.write_file(ruta=self.path_logs+"\\workflowNotfound.log",contenido=f"{toSearch}")
                dataToreturn=None
            return dataToreturn
        
        except Exception as ee:
            print(f"ERROR: {ee}")
            
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
        instance= {
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