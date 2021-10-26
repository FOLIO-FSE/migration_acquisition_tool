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
import main_functions as AcqErm

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
            
    def readacquisitionMapping(self):
        self.customerName=pd.dataframe()
        filetoload=self.path_refdata+f"\\acquisitionMapping.xlsx"
        self.acquisitionMethod=self.customerName.importDataFrame(filetoload,sheetName="acquisitionMethod")
        self.orderFormat=self.customerName.importDataFrame(filetoload,sheetName="orderFormat")
        self.orderType=self.customerName.importDataFrame(filetoload,sheetName="orderType")
        self.paymentStatus=self.customerName.importDataFrame(filetoload,sheetName="paymentStatus")
        self.receiptStatus=self.customerName.importDataFrame(filetoload,sheetName="receiptStatus")
        self.workflowStatus=self.customerName.importDataFrame(filetoload,sheetName="workflowStatus")
        self.locations=self.customerName.importDataFrame(filetoload,sheetName="locations")
        self.fundsExpenseClass=self.customerName.importDataFrame(filetoload,sheetName="fundsExpenseClass")
        self.funds=self.customerName.importDataFrame(filetoload,sheetName="funds")
        self.organizationCodeToChange=self.customerName.importDataFrame(filetoload,sheetName="organizationCodeToChange")
        
        #print(self.acquisitionMethod)
################################
##ORDERS FUNCTION
################################
#def readorders(path,file_name,sheetName,customerName,spread):
#        try:
    def readorders(self, client,dfOrders, dfPolines):
        
        self.readacquisitionMapping()
        
        orderList=[]      
        orders=dfOrders
        poLines=dfPolines        
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
        list={}
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
                    mf.write_file(path=self.path_logs+"\\oldNew_ordersID.log",contenido=poNumber)

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
                if 'notes' in orders.columns:
                    if row['notes']!="":
                        notea.append(str(row['notes']).strip())
                Order["notes"]=notea

                #IS SUSCRIPTION FALSE/TRUE
                Order_type=""
                Order_type="One-Time"
                isongoing=mf.dic(isSubscription=False)
                isSubscription= False
                isSuscriptiontem=""
                interval=365
                renewalDate=""
                reviewPeriod=""
                ongoingNote=""
                if 'orderType' in orders.columns:
                    Otype=str(row['orderType']).strip()
                    self.temp = self.orderType[self.orderType['LEGACY SYSTEM CODE (A,B,C)']== Otype]
                    #print("poLines founds records: ",len(poLines))
                    for c, cprow in self.temp.iterrows():
                        try:
                            Order_type=cprow['FOLIO']
                        except Exception as ee:
                            print(f"ERROR: {ee}")
                
                    if Order_type=="One-time" or Order_type=="One-Time" :
                        isongoing=mf.dic(isSubscription=False)
                    else:
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
                Order["orderType"]=Order_type 
                Order["ongoing"]=isongoing
                ######################
                shipTo=""
                billTo=""
                if 'billTo' in orders.columns:
                    Order["billTo"]="5b1f5f52-7ca8-4690-ac78-2d1bf9e410c0"
                if 'shipTo' in orders.columns:
                    Order["shipTo"]="5b1f5f52-7ca8-4690-ac78-2d1bf9e410c0"
                OrganizationUUID=""            
                #file=self.customerName+"_organizations.json"
                
                OrganizationUUID=mf.readJsonfile(self.path_refdata,client+"_organizations.json","organizations","undefined","code")
                if OrganizationUUID is not None:
                    if 'vendor' in orders.columns:
                        if row['vendor']:
                            vendorToSearch=str(row['vendor']).strip()
                            OrganizationUUID=mf.readJsonfile(self.path_refdata,client+"_organizations.json","organizations",vendorToSearch,"code")
                            if OrganizationUUID is None:
                                mf.write_file(path=self.path_logs+"\\vendorsNotFounds.log",contenido=f"{vendorToSearch}")
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
                        workflow=str(row['workflowStatus']).strip()
                        toSearch=str(row['workflowStatus']).strip()
                        self.temp = workflowStatus[workflowStatus['LEGACY SYSTEM CODE (A,B,C)']== toSearch]
                        #print("poLines founds records: ",len(poLines))
                        if len(self.temp)>0:
                            for c, cprow in self.temp.iterrows():
                                try:
                                    Order_type=cprow['FOLIO']
                                except Exception as ee:
                                    print(f"ERROR: {ee}")
                        else:
                            mf.write_file(path=self.path_logs+"\\workflowNotfound.log",contenido=f"{workflow}")
                    
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
                    compositePo=compositePoLines(poLines,poLineNumberfield, notesapp1,notesapp1Pofield, notesapp2, notesapp2Pofield, istherenotesApp, OrganizationUUID,masterPo,poNumber,customerName,path_results,path_refdata,path_logs)
                    if compositePo is not None: 
                        Order["compositePoLines"]=compositePo
                        countpol+=1
                    else: 
                        Order["compositePoLines"]=[]
                        countpolerror+=1

                    OrganizationUUID=[]
                    if row['ORDACQUNIT']:
                        Acquisitionstemp=str(row['ORDACQUNIT']).strip()
                        OrganizationUUID.append(readJsonfile(path_refdata,f"{customerName}_AcquisitionsUnits.json","AcquisitionsUnits",Acquisitionstemp,"name"))
                        if len(OrganizationUUID)==0:
                            write_file(path=f"{path_logs}/AdqNotFound.log",contenido=f"{Acquisitionstemp}")
                    Order["acqUnitIds"]=OrganizationUUID

                    mf.printObject(Order,path_results,count,f"{customerName}_purchaseOrderbyline.json",False)
                    purchase.append(Order)
            except Exception as ee:
                print(f"ERROR: {ee}")
        purchaseOrders['purchaseOrders']=purchase    
        mf.printObject(purchaseOrders,path_results,count,f"{customerName}_purchaseOrders",True)
        print(f"============REPORT======================")
        report=[]
        print(poLineNumberfield)
        report=reports(df=orders,plog=path_logs,pdata=path_results,file_report=f"{customerName}_purchaseOrders.json",schema="purchaseOrders",dfFieldtoCompare=poLineNumberfield)
        print(f"RESULTS Record processed {count}")
        print(f"RESULTS poLines {countpol}")
        print(f"RESULTS poLines with errors: {countpolerror}")
        print(f"RESULTS vendor with errors: {countvendorerror}")
        print(f"RESULTS end")
    
#########################################
#POLINES FUNCTION             
#########################################
    def compositePoLines(self,poLines,poLineNumberfield,
                     notesapp1,notesapp1Pofield,
                     notesapp2,notesapp2Pofield,istherenotesApp,
                     vendors,masterPo,poLineNumber,customerName,path_results,path_refdata,path_logs):                    
        try:
           #locationMapping={"af":"mnaf","aq":"mnaq","br":"mnbr","ca":"mnca","cv":"mncv","dv":"mndv","ir":"mnir","ns":"mnns","ov":"mnov","ss":"mnss"}
            cpList=[]
            count=1 
            poLines = poLines[poLines[poLineNumberfield]== masterPo]
            print("poLines founds records: ",len(poLines))
            for c, cprow in poLines.iterrows():
                cp={}
                if 'UUIDPOLINES' in poLines: linkid=cprow['UUIDPOLINES']#str(uuid.uuid4())
                else: linkid=str(uuid.uuid4())
                cp["id"]=linkid
                cp["poLineNumber"]=str(poLineNumber)+"-"+str(count)
                if cprow['Publisher']:
                    cp["publisher"]=cprow['Publisher']
                #cp["purchaseOrderId"]=""
                #cp["id"]=""
                #cp["edition"]=""
                checkinItems= False
                if cprow['checkinItems']:
                    checkinItemstem=str(cprow['checkinItems']).strip()
                    checkinItemstem=checkinItemstem.upper()
                    if checkinItemstem=="YES":
                        checkinItems= True
                cp["checkinItems"]=checkinItems
                #cp["instanceId"]=""
                #cp["agreementId"]= ""
                acquisitionMethod="Purchase"
                if cprow['Acquisition Method']:
                    acquisitionMethod=str(cprow['Acquisition Method']).strip()
                    if acquisitionMethod=="Approval plan":    acquisitionMethod="Approval Plan"
                    elif acquisitionMethod=="DDA":            acquisitionMethod="Demand Driven Acquisitions (DDA)"
                    elif acquisitionMethod=="EBA":            acquisitionMethod="Evidence Based Acquisitions (EBA)"
                    elif acquisitionMethod=="Exchange":       acquisitionMethod="Exchange"
                    elif acquisitionMethod=="Membership":     acquisitionMethod="Technical"
                    elif acquisitionMethod=="Gift":           acquisitionMethod="Gift"
                    elif acquisitionMethod=="Purchase at vendor system":  acquisitionMethod="Purchase At Vendor System"
                    elif acquisitionMethod=="Purchase":       acquisitionMethod="Purchase"
                    elif acquisitionMethod=="Depository":     acquisitionMethod="Depository"
                    else: acquisitionMethod="Purchase At Vendor System"
                cp["acquisitionMethod"]= acquisitionMethod   
                #cp["alerts"]=dic(alert="Receipt overdue",id="9a665b22-9fe5-4c95-b4ee-837a5433c95d")
                cp["cancellationRestriction"]: False
                #cp["cancellationRestrictionNote"]=""
                #cp["claims"]=dic()Fa""
                collection=False
                if cprow["collection"]:
                    collection=str(cprow["collection"]).strip()
                    collection=collection.upper()
                    if collection=="YES":
                        collection=True
                cp["collection"]=collection

                rush=False
                if cprow["rush"]:
                    rush=str(cprow["rush"]).strip()
                    rush=rush.upper()
                    if rush=="YES":
                        rush=True
                cp["rush"]=rush

                #cp["contributors"]=[dic()]

                quantityPhysical=1
                if cprow['Quantity Physical']:
                    quantityPhysical=int(cprow['Quantity Physical'])

                quantityElectronic=1
                if cprow['Quantity electronic']:#if cprow['QUANTITY'] NOT MIGRATED:
                    quantityElectronic=int(cprow['Quantity electronic'])    
                ###
                #LOCATIONS(ORDER)
                ################################
                locationId=""
                locsw=True    
                if cprow['LOCATION']:
                    locationtoSearch=str(cprow['LOCATION']).strip()
                    if locationtoSearch.find(",")==-1:
                        locsw=True
                        locationtoSearch=locationtoSearch.replace(" ","")
                        locationId=readJsonfile(path_refdata,f"{customerName}_locations.json","locations",locationtoSearch,"code")
                        #locationId=get_locId(locationtoSearch, customerName)
                        #if locationId is None:
                        #    locationtoSearch=str(cprow['LOCATION']).strip()
                        #    vendorrealToSearch=searchKeysByVal(locationMapping,locationtoSearch)
                        #    locationId=readJsonfile(path_refdata,f"{customerName}_locations.json","locations",vendorrealToSearch,"code")
                        if locationId is None:
                            locationId="None"
                            write_file(path=f"{path_logs}\\locationsNotFounds.log",contenido=locationtoSearch)                            
                    else:
                        loca=[]
                        x = locationtoSearch.split(",")
                        locsw=False
                        lc=0
                        locationIdA=""
                        for i in x:
                            locationtoSearch=i
                            locationId=readJsonfile(path_refdata,"{customerName}_locations.json","locations",locationtoSearch,"code")
                                #locationId=get_locId(locationtoSearch, customerName)
                            locationIdA=locationId
                            #if locationIdA is None:
                            #    vendorrealToSearch=searchKeysByVal(locationMapping,locationtoSearch)
                            #    locationId=readJsonfile(path_refdata,"{customerName}_locations.json","locations",vendorrealToSearch,"code")
                                    #locationIdA=get_locId(vendorrealToSearch, customerName)
                            if locationIdA is None:
                                locationIdA="None"
                                write_file(path=f"{path_logs}\\locationsNotFounds.txt",contenido=f" {poLineNumber} {locationtoSearch} undefined locations")

                            lc+=1
                            if cprow['Order format']:
                                orderFormat=str(cprow['Order format']).strip()
                                #Locations for print/mixed resources
                                if orderFormat=="Physical":
                                    loca.append({"locationId":locationIdA,"quantity":(quantityPhysical-1), "quantityPhysical":(quantityPhysical-1)}) 
                                elif orderFormat=="Electronic Resource":
                                    loca.append({"locationId":locationIdA,"quantity":(quantityElectronic-1), "quantityElectronic":(quantityElectronic-1)})
                                else:
                                    loca.append({"locationId":locationIdA,"quantity":(quantityPhysical-1), "quantityPhysical":(quantityPhysical-1)}) 
                            locationIdA=""
                ##TITLE
                ispackage=False
                if cprow['ispackage']:
                    ispackagetem=str(cprow['ispackage']).strip()
                    ispackagetem=ispackagetem.upper()
                    if ispackagetem=="YES":
                        ispackage=True
                cp["isPackage"]=ispackage

                titleOrPackage="No Title"
                enum=["Instance, Holding, Item","Instance, Holding","Instance","None"]
                instance_holdings_items=enum[3]
                if cprow['Create inventory']:
                    instholitem=str(cprow['Create inventory']).strip()
                    if instholitem=="Instance, Holding, Item":  
                        instance_holdings_items=enum[0]
                    elif instholitem=="Instance, Holding":
                        instance_holdings_items=enum[1]
                    elif instholitem=="Instance":
                        instance_holdings_items=enum[2]
                    else: 
                        instance_holdings_items=enum[3]

                if cprow['TITLE']:
                    if ispackage:
                        titleOrPackage=str(cprow['TITLE']).strip()
                        cp["titleOrPackage"]=titleOrPackage 
                    else: 
                        #ispackage==False:
                        titleUUID=str(cprow['TITLE']).strip()
                        ordertitleUUID=get_title(customerName,element="instances",searchValue=titleUUID)
                        #ordertitleUUID=readJsonfile_identifier(path_refdata,f"{customerName}_instances.json","instances",titleUUID)
                        if len(ordertitleUUID)!=0:# is None:
                            #instance_holdings_items="None"
                            print(ordertitleUUID)
                            cp["instanceId"]=str(ordertitleUUID[0])
                            cp["titleOrPackage"]=str(ordertitleUUID[1])
                            cp["isPackage"]=False
                        else: 
                            titleOrPackage=cprow['TITLE']
                            cp["titleOrPackage"]=titleOrPackage
                            #instance_holdings_items="None"
                            #instance_holdings_items="Instance"
                            write_file(path=f"{path_logs}\\titlesNotFounds.log",contenido=f"{poLineNumber}  {titleUUID} {titleOrPackage}")
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
                            printObject(instance,path_results,0,f"{customerName}_instances",False)
                else:
                    cp["titleOrPackage"]=titleOrPackage
                ################################
                ### ORDER FORMAT
                ################################            
                orderFormat=""
                materialType=""
                accessProvider=vendors
                accessproviderUUID=""
                if cprow['Access provider']:
                    accessProvidertosearch=str(cprow['Access provider']).strip()
                    accessproviderUUID=readJsonfile(path_refdata,f"{customerName}_organizations.json","organizations",accessProvidertosearch,"code")
                    if accessproviderUUID is None:
                        write_file(path=f"{path_logs}\\providerNotFounds.log",contenido=f"{accessProvidertosearch}")
                    else:
                        accessProvider=accessproviderUUID

                materialSupplier=vendors        
                if cprow['(Physical Resource) Material supplier']:
                    materialaccessProvidertosearch=str(cprow['(Physical Resource) Material supplier']).strip()
                    accessproviderUUID=readJsonfile(path_refdata,f"{customerName}_organizations.json","organizations",materialaccessProvidertosearch,"code")
                    if accessproviderUUID is None:
                        write_file(path=f"{path_logs}\\materialProviderNotFounds.log",contenido=f"{materialaccessProvidertosearch}")
                    else:
                        materialSupplier=accessproviderUUID            


                listUnitPrice=0.00
                if cprow['UNIT PRICE']:
                    listUnitPrice=float(cprow['UNIT PRICE'])

                if cprow['Order format']:
                    orderFormat=str(cprow['Order format']).strip()
                    orderFormat=orderFormat.upper()
                    #Locations for print/mixed resources
                    if orderFormat=="PHYSICIAL" or orderFormat=="PHYSICAL" or orderFormat=="PHYSICAL RESOURCE" or orderFormat=="MIXED P/E":
                        #Material Type physical
                        materialType=""
                        if cprow['(Physical Resource)Material type']:
                            mtypestosearch=str(cprow['(Physical Resource)Material type']).strip()
                            materialType=readJsonfile(path_refdata,f"{customerName}_mtypes.json","mtypes",mtypestosearch,"name")
                            #materialType=get_matId(mtypestosearch,customerName)
                            if materialType is None:
                                materialType=get_matId(mtypestosearch,customerName)
                                write_file(path=f"{path_logs}\\materialTypeNotFounds.log",contenido=f"{poLineNumber} {mtypestosearch}")
                                if materialType is None:
                                    write_file(path=f"{path_logs}\\materialTypeNotFounds.log",contenido=f"{poLineNumber} {mtypestosearch}")
                                    materialType="materialtypeUndefined"


    
                        cp["orderFormat"]="Physical Resource"
                        cp["cost"]=dic(currency="USD",listUnitPrice=listUnitPrice, quantityPhysical=quantityPhysical, poLineEstimatedPrice=listUnitPrice, discountType="percentage")
                        if materialType: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier, materialType=materialType)
                        else: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier)
                        if accessProvider: cp["eresource"]=dic(activated=False,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                        else: cp["eresource"]=dic(activated=False,createInventory=instance_holdings_items,trial=False)
                        if locsw: 
                            cp["locations"]=[dic(locationId=locationId,quantity=quantityPhysical, quantityPhysical=quantityPhysical)]
                        else:
                            cp["locations"]=loca

                    elif orderFormat=="ELECTRONIC" or orderFormat=="ELECTRONIC RESOURCE":
                        cp["orderFormat"]="Electronic Resource"
                        materialType=""
                        if cprow['Material type']:
                            mtypestosearch=""
                            mtypestosearch=str(cprow['Material type']).strip()
                            materialType=readJsonfile(path_refdata,f"{path_logs}\\_mtypes.json","mtypes",mtypestosearch,"name")
                            #materialType=get_matId(mtypestosearch,customerName)
                            if materialType is None:
                                materialType=get_matId(mtypestosearch,customerName)
                                write_file(path=f"{path_logs}\\materialTypeNotFounds.txt",contenido=f"{poLineNumber} {mtypestosearch}")
                                if materialType is None:
                                    write_file(path=f"{path_logs}\\materialTypeNotFounds.txt",contenido=f"{poLineNumber} {mtypestosearch}")
                                    materialType="materialtypeUndefined"

                        cp["cost"]=dic(currency="USD",listUnitPrice=listUnitPrice,listUnitPriceElectronic=listUnitPrice, quantityElectronic=quantityElectronic, poLineEstimatedPrice=listUnitPrice,discountType="percentage")
                        if materialType: 
                            cp["eresource"]=dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider,materialType=materialType)
                        else: 
                            cp["eresource"]=dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)

                        if locationId: 
                            if locsw:
                                cp["locations"]=[dic(locationId=locationId,quantity=1, quantityElectronic=quantityElectronic)]
                            else:
                                cp["locations"]=loca 

                    elif orderFormat=="Mixed P/E":
                        cp["orderFormat"]="P/E Mix"
                        cp["cost"]=dic(currency="USD",listUnitPrice=listUnitPrice,listUnitPriceElectronic=listUnitPrice, quantityPhysical=quantityPhysical, quantityElectronic=1, poLineEstimatedPrice=listUnitPrice,discountType="percentage")
                        if accessProvider: cp["eresource"]=dic(activated=False,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                        else: cp["eresource"]=dic(activated=False,createInventory=instance_holdings_items,trial=False)
                        cp["locations"]=[dic(locationId=locationId,quantity=2, quantityElectronic=1,quantityPhysical=quantityPhysical)]
                        if materialType: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier,
                                       expectedReceiptDate="",receiptDue="",materialType=materialType)
                        else: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier,
                                       expectedReceiptDate="",receiptDue="")
                    else:   
                        cp["orderFormat"]="Other"
                        cp["cost"]=dic(currency="USD",listUnitPrice=listUnitPrice, quantityPhysical=1, poLineEstimatedPrice=listUnitPrice, discountType="percentage")
                        if materialType: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier, materialType=materialType)
                        else: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier)
                        if accessProvider: cp["eresource"]=dic(activated=False,createInventory="None",trial=False,accessProvider=accessProvider)
                        else: cp["eresource"]=dic(activated=False,createInventory="None",trial=False)
                        if locationId: 
                            if locsw:
                                cp["locations"]=[dic(locationId=locationId,quantity=1, quantityElectronic=quantityElectronic)]
                            else:
                                cp["locations"]=loca 

                #FUNDS DISTRIBUTIONS
                cp["fundDistribution"]=[]
                #EXPENSES CLASES
                #FUND DISTRIBUTION BY RESOURCE
                codeTosearch=str(cprow['FUND']).strip()
                codeTosearch=codeTosearch.replace(" ","")
                if codeTosearch=="none":
                    codeTosearch=""
                if codeTosearch:
                    occurencias=int(codeTosearch.count("%"))
                    if occurencias==0:
                        searchExpensesValue=""
                        expenseClassId=""
                        if cprow['Expense Class']:
                            searchtoValue=str(cprow['Expense Class']).strip()
                            expenseClassId=readJsonfile(path_refdata,f"{customerName}_expenseClasses.json","expenseClasses",searchtoValue,"code")
                            #expenseClassId=get_Id(customerName,element="expenseClasses",searchValue=searchtoValue)
                        #get_funId(searchValue,orderFormat,client):
                        fundId=readJsonfile_fund(path_refdata,f"{customerName}_funds.json","funds",codeTosearch,"code")
                        #fundId=get_funId(codeTosearch,orderFormat,customerName)
                        if fundId is not None:
                            code=fundId[1]
                            fundId=fundId[0]
                            valuefund=100.0
                            if expenseClassId:
                                cp["fundDistribution"]=[dic(code=code,fundId=fundId,expenseClassId=expenseClassId,distributionType="percentage",value=valuefund)]
                            else:
                                cp["fundDistribution"]=[dic(code=code,fundId=fundId,distributionType="percentage",value=valuefund)]
                        else:
                            write_file(path=f"{path_logs}\\fundsNotfounds.log",contenido=f"{poLineNumber} {codeTosearch}")
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

                            if cprow['Expense Class']:
                                searchtoValue=str(cprow['Expense Class']).strip()
                                expenseClassId=readJsonfile(path_refdata,f"{customerName}_expenseClasses.json","expenseClasses",searchtoValue,"code")
                                if expenseClassId is None:
                                    write_file(path=f"{path_logs}\\expensesNotfounds.log",contenido=f"{poLineNumber} {searchtoValue}")
                                #expenseClassId=get_Id(customerName,element="expenseClasses",searchValue=searchtoValue)
                            #get_funId(searchValue,orderFormat,client):
                            fundId=readJsonfile_fund(path_refdata,f"{customerName}_funds.json","funds",codeTosearch,"code")
                            #fundId=get_funId(codeTosearch,orderFormat,customerName)
                            if fundId is not None:
                                code=fundId[1]
                                fundId=fundId[0]
                                if expenseClassId:
                                    fundDistribution.append(dic(code=code,fundId=fundId,expenseClassId=expenseClassId,distributionType="percentage",value=valuefund))

                        cp["fundDistribution"]=fundDistribution

                #Ongoing
                receivingNote=""
                if cprow['receivingNote']: receivingNote=str(cprow['receivingNote'])
                subscriptionFrom=""
                subscriptionTo=""
                subscriptionInterval=""
                if cprow['Subscription from']: subscriptionFrom=timeStamp(cprow['Subscription from'])
                if cprow['Subscription to']: subscriptionTo=timeStamp(cprow['Subscription to'])
                if cprow['Subscription interval']: subscriptionInterval=int(cprow['Subscription interval'])
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

                cp["details"]=dic(receivingNote=receivingNote,productIds=productIds,subscriptionFrom=subscriptionFrom,
                                                           subscriptionInterval=subscriptionInterval, subscriptionTo=subscriptionTo)

                #cp["donor"]=""
                paymentStatus="Ongoing"
                if cprow["paymentStatus"]:
                    cp["paymentStatus"]=cprow["paymentStatus"]
                description=""

                if cprow['Internal Note']:
                    description=str(cprow['Internal Note']).strip()
                cp["description"]=description

                #cp["publicationDate"]=""
                receiptStatus="Awaiting Receipt"
                cp["receiptDate"]=""
                if cprow['receiptDate']:
                    receitdate=str(cprow['receiptDate'])
                    M=receitdate[0:2]
                    D=receitdate[3:5] 
                    Y=receitdate[6:10]
                    if Y=="96" or Y=="97" or Y=="98" or Y=="99":
                        Y=f"19{Y}"
                    cp["receiptDate"]=f"{Y}-{M}-{D}T00:00:00.00+00:00"
                cp["receiptStatus"]=receiptStatus
                #cp["reportingCodes"]=dic(code="",id="",description="")
                Requester=""
                if cprow['Requester']:
                    Requester=str(cprow['Requester'])
                cp["requester"]=Requester
                if cprow['Selector']:
                    cp['selector']=str(cprow['Selector'])



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
                for nt in istherenotesApp:
                    notes_single_line(linkid,masterPo, nt,notesapp1Pofield,"PAID","Orders note", path_results,path_refdata,count)


                cpList.append(cp)
                #cpList.append(linkid)
                count=count+1
            return cpList    
        except Exception as ee:
            print(ee)
            write_file(path=f"{path_logs}\\poLinesErrors.log",contenido=f"Order:{masterPo} {ee}")  
            
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