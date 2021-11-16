import datetime
from typing import Collection
import warnings
from datetime import datetime
import dataframe_class as pd
import backup_restore as br
import main_functions as mf
import notes_class as notes
import json
import uuid
import os
import os.path
import requests
import io
import math
import csv
import random
import logging
import validator
import ast
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import time
from datetime import datetime
import yaml
import shutil


class organizations():
    def __init__(self,client,path_dir):
        try:
            self.path_dir=path_dir
            self.customerName=client
            #os.mkdir(f"{path_dir}\\results")
            self.path_results=f"{path_dir}\\results"
            #os.mkdir(f"{path_dir}\\data")
            self.path_data=f"{path_dir}\\data"
            #os.mkdir(f"{path_dir}\\logs")
            self.path_logs=f"{path_dir}\\logs"
            #os.mkdir(f"{path_dir}\\refdata")
            self.path_refdata=f"{path_dir}\\refdata"
            self.organizationbyline=self.path_logs+"\\"+self.customerName+"_organizationbyline.json"
            #print(self.organizationbyline)
            self.organizationbyline=open(self.organizationbyline, 'w')            
        except Exception as ee:
            print(f"ERROR: {ee}")
            
    def readMappingfile(self):
        try:       
            self.customerName=pd.dataframe()
            filetoload=self.path_refdata+f"\\acquisitionMapping.xlsx"
            print("INFO Reading mapping file")
            self.paymentMethodAccount=self.customerName.importDataFrame(filetoload,sheetName="paymentMethodAccount",dfname="Payment Method")
            self.categories=self.customerName.importDataFrame(filetoload,sheetName="categories", dfname="Categories")
            with open(self.path_refdata+"\\organization_mapping.json") as json_mappingfile:
                self.mappingdata = json.load(json_mappingfile)
            self.paymentMethod= ["Cash","Credit Card","EFT","Deposit Account","Physical Check","Bank Draft","Internal Transfer","Other"]
        except Exception as ee:
            print(f"ERROR: {ee}")    
###########################
#ORGANIZATIONS
###########################
        
    #def readOrganizations(self, client, dforganizations, dfcontacts, dfinterfaces):
    def readOrganizations(self, client, **kwargs):#dforganizations, dfcontacts, dfinterfaces)
        try:
            start_time = time.perf_counter()
            self.readMappingfile()
            vendors=kwargs['dforganizations']
            self.countcred=0
            if 'dfnotes' in kwargs:
                dfnote=kwargs['dfnotes']
                #print(dfnotes)
                self.customerName=notes.notes(client,self.path_dir,dataframe=dfnote)
                swnotes=True
            else:
                swnotes=False
            if 'dfinterfaces' in kwargs:
                dfinterfaces=kwargs['dfinterfaces'] 
                if dfinterfaces is None:
                    dfinterfaces=vendors
                else:
                    dfinterfaces=kwargs['dfinterfaces']


            if 'dfcontacts' in kwargs:
                dfcontacts=kwargs['dfcontacts'] 
                if dfcontacts is None:
                    dfcontacts=vendors
                else:
                    dfcontacts=kwargs['dfcontacts']
                
            #print(vendors)
            org={}
            list={}
            codeorg=[]
            count=0
            org={}
            contact_Id=""
            interface_Id=""
            org_erpCode=""
            nextorg=""
            orga=[]
            orgFull={}
            for i, row in vendors.iterrows():
                try:
                    orgId=""
                    count+=1
                    
                    swname=False
                    swcode=False
                    tic = time.perf_counter()
                    if 'id' in vendors.columns: orgId=str(row['id']).strip()
                    else: orgId=str(uuid.uuid4())
                    org['id']=orgId
                    isVendor= True
                    if 'isVendor' in vendors.columns:
                        if row['isVendor']:
                            isVendor=row['isVendor']
                    org["isVendor"]= isVendor
                    org['status']="Active"         
                    exportToAccounting=False
                    if 'exportToAccounting' in vendors.columns:
                        exportToAccounting=True
                    org['exportToAccounting']=exportToAccounting
                    vencode=""
                    if 'code' in vendors.columns:
                        if row['code']: 
                            vencode=str(row['code']).strip()
                            codeorg.append(vencode)
                            swcode=True
                    org['code']=vencode
                    #ORG NAME
                    org_name=""
                    if 'name' in vendors.columns: 
                        if row['name']: 
                            org_name=str(row['name']).strip()
                            swname=True
                            print(f"INFO Processing organization record # {count} Org-Name: {org_name}")
                        else:
                            print(f"WARNING Processing organization record # {count} Org-Name: NOT NAME. it is a field requeried ")
                    org['name']=org_name
                    #ORG DESCRIPTION
                    orgdescription=""
                    if 'description' in vendors.columns:
                        if row['description']: orgdescription=str(row['description']).strip()
                        org['description']=orgdescription
                    accountingcode=""
                    if 'sanCode' in vendors.columns:
                        if row['sanCode']: accountingcode=row['sanCode']
                        org['sanCode']=accountingcode
                    #ORG ALIASES#################
                    aliases=[]
                    iter=0
                    sw=True
                    while sw:
                        field=f"aliases[{iter}].value"
                        if field in vendors.columns:
                            if row[field]:
                                aliases.append(str(row[field]).strip())
                        else:
                            sw=False
                        iter+=1
                    if len(aliases)!=0:
                        org["aliases"]=aliases
                        
                    venlanguages=""
                    if 'language' in vendors.columns:
                        if row['language']: 
                            venlanguages=mf.org_languages(value=str(row['language']).strip(),type=1)
                    org['language']=venlanguages
                    #Addresses
                    orgaddresses=[]
                    addAdd= True
                    #addresses[0].addressLine1
                    addresses=[]
                    iter=0
                    sw=True

                    while sw:
                        categories=[]
                        field=f"addresses[{iter}].addressLine1"
                        isPrimary=False
                        addressdata1=""
                        if field in vendors.columns:
                            if row[field]:
                               addressdata1= row[field]
                            if iter==0:
                                isPrimary=True

                            field=f"addresses[{iter}].addressLine2"
                            addressdata2=""
                            if field in vendors.columns:
                                addressdata2= row[field]
                             
                            field=f"addresses[{iter}].city"
                            city=""
                            if field in vendors.columns:
                               if row[field]:
                                   city=row[field]
                            #addresses[0].stateRegion
                            field=f"addresses[{iter}].stateRegion"
                            stateRegion=""
                            if field in vendors.columns:
                               if row[field]:
                                   stateRegion=row[field]
                            #addresses[0].zipCode
                            field=f"addresses[{iter}].zipCode"
                            zipCode=""
                            if field in vendors.columns:
                               if row[field]:
                                   zipCode=row[field]
                                   
                            field=f"addresses[{iter}].categories[{iter}]"
                            cate=""
                            if field in vendors.columns:
                               if row[field]:
                                    toSearch=str(row[field]).strip()
                                    cate=mf.readJsonfile(self.path_refdata,client+"_categories.json","categories",toSearch,"value")
                                    if cate is None:
                                        mf.write_file(ruta=self.path_logs+"\\categoriesNotFounds.log",contenido=f"{toSearch}")
                                    else:                                         
                                        categories.append(cate[0])
                            field=f"addresses[{iter}].language"
                            lan=""
                            language=""
                            if field in vendors.columns:
                                if row[field]:
                                    lan=row[field]
                                    language=mf.org_languages(value=lan,type=2)
                            field=f"addresses[{iter}].country"
                            country=""
                            if field in vendors.columns:
                               if row[field]:
                                   country=row[field]
                            
                            orgaddresses.append(mf.dic(addressLine1= addressdata1,addressLine2=addressdata2,
                                                    city=city,stateRegion= stateRegion,
                                                    zipCode=zipCode,country=country,
                                                    isPrimary=isPrimary,categories=categories,language=language))
                            
                        else:
                            sw=False    
                        iter+=1
                    if len(orgaddresses)!=0:
                        org["addresses"]=orgaddresses
                    #phoneNumbers
                    addpho=""
                    orgphonNumbers=[]
                    #categories=[]
                    cate=[]
                    sw=True
                    iter=0
                    phonNumbers=""
                    #phoneNumbers[0].categories[0]
                    while sw:
                        categories=[]
                        field=f"phoneNumbers[{iter}].phoneNumber"
                        isPrimary=False
                        addressdata1=""
                        if field in vendors.columns:
                            if row[field]:
                                phonNumbers=row[field]
                                if iter==0:
                                    isPrimary=True
                                field=f"phoneNumbers[{iter}].type"
                                phonenumbertype=""
                                if field in vendors.columns:
                                    phonentype=row[field]
                                    phonetypelist=["Office","Mobile","Fax","Other"]
                                    countlist = phonetypelist.count(str(phonentype))
                                    if countlist>0:
                                        phonenumbertype=phonentype
                                    else:
                                        phonenumbertype="Other" 
                                else:
                                    phonenumbertype="Other"
                                    #phoneNumbers[0].categories[0]
                                    field=f"phoneNumbers[{iter}].language"
                                    lan=""
                                    language=""
                                    if field in vendors.columns:
                                        if row[field]:
                                            lan=row[field]
                                            language=mf.org_languages(value=lan,type=2)
                                    
                                    field=f"phoneNumbers[{iter}].categories[{iter}]"
                                    if field in vendors.columns:
                                        if row[field]:
                                            toSearch=str(row[field]).strip()
                                            cate=mf.readJsonfile(self.path_refdata,client+"_categories.json","categories",toSearch,"value")
                                            if cate is None:
                                                mf.write_file(ruta=self.path_logs+"\\categoriesNotFounds.log",contenido=f"{toSearch}")
                                            else:                                         
                                                categories.append(cate)
                                orgphonNumbers.append(mf.dic(phoneNumber= phonNumbers,type=phonenumbertype, isPrimary= isPrimary, language=language,categories=categories))
                        else:
                            sw=False
                        iter+=1 
                        
                    if len(orgphonNumbers)!=0:
                        org['phoneNumbers']=orgphonNumbers
                
                    #emails
                    orgemails=[]
                    cate=[] 
                    sw=True
                    iter=0
                    #phoneNumbers[0].categories[0]
                    while sw:
                        categories=[]
                        field=f"emails[{iter}].value"
                        isPrimary=False
                        email=""
                        if field in vendors.columns:
                            if row[field]:
                                email=row[field]
                                if iter==0:
                                    isPrimary=True
                                #emails[0].description
                                field=f"emails[{iter}].description"
                                desc=""
                                if field in vendors.columns:
                                    if row[field]:
                                        desc=row[field]
                                
                                field=f"emails[{iter}].language"
                                lan=""
                                language=""
                                if field in vendors.columns:
                                    if row[field]:
                                        lan=row[field]
                                        language=mf.org_languages(value=lan,type=2)
                                    
                                field=f"emails[{iter}].categories[{iter}]"
                                if field in vendors.columns:
                                    if row[field]:
                                        toSearch=str(row[field]).strip()
                                        cate=mf.readJsonfile(self.path_refdata,client+"_categories.json","categories",toSearch,"value")
                                        if cate is None:
                                            mf.write_file(ruta=self.path_logs+"\\categoriesNotFounds.log",contenido=f"{toSearch}")
                                        else:                                         
                                            categories.append(cate)
                                orgemails.append(mf.dic(value=email,description=desc,language=language,isPrimary= isPrimary, categories=categories))
                        else:
                            sw=False
                        iter+=1

                    if len(orgemails)!=0:
                        org['emails']=orgemails#orgemails=faf.org_emails(organizations.loc[i],6)
                    #vendorCurrencies
                    sw=True
                    iter=0
                    #phoneNumbers[0].categories[0]
                    currency=[]
                    while sw:
                        field=f"vendorCurrencies[{iter}]" 
                        if field in vendors.columns:
                            if row[field]:
                                currency.append(row[field])
                        else:
                            sw=False
                        iter+=1
                    if len(currency)!=0:
                        org['vendorCurrencies']=currency
                    #urlsOrg
                    orgurls=[]
                    caturl=[]
                    categories=[] 
                    sw=True
                    iter=0
                    #phoneNumbers[0].categories[0]
                    while sw:
                        categories=[]
                        field=f"urls[{iter}].value"
                        isPrimary=False
                        if field in vendors.columns:
                            if row[field]:
                                url=row[field]
                                #emails[0].description
                                field=f"urls[{iter}].description"
                                desc=""
                                if field in vendors.columns:
                                    if row[field]:
                                        desc=row[field]
                                field=f"urls[{iter}].note"
                                note=""
                                if field in vendors.columns:
                                    if row[field]:
                                        note=row[field]
                                
                                field=f"urls[{iter}].language"
                                lan=""
                                language=""
                                if field in vendors.columns:
                                    if row[field]:
                                        lan=row[field]
                                        language=mf.org_languages(value=lan,type=2)   
                                                                
                                field=f"urls[{iter}].categories[{iter}]"
                                if field in vendors.columns:
                                    if row[field]:
                                        toSearch=str(row[field]).strip()
                                        cate=mf.readJsonfile(self.path_refdata,client+"_categories.json","categories",toSearch,"value")
                                        if cate is None:
                                            mf.write_file(ruta=self.path_logs+"\\categoriesNotFounds.log",contenido=f"{toSearch}")
                                        else:                                         
                                            categories.append(cate)
                            
                                orgurls.append(mf.dic(value=url, description=desc,note=note,language=language,categories=categories))                
                        else:
                            sw=False
                        iter+=1
                    if len(orgurls)!=0:
                        org['urls']=orgurls
                    #accounts
                    accounts=[]
                    addacc=True
                    nameaccount=""
                    accountnum=""
                    paymentMethod=""
                    libraryCode=""
                    libraryEdiCode=""
                
                 #Acquisition Unit
                    acqUnitIds= []
                    addacc=True
                    if 'Acquisitions units' in vendors.columns:
                        org['acqUnitIds']=row['Acquisitions units']
                    ##Accounts
                    iter=0
                    sw=True
                    accounts=[]
                    while sw:
                        acc={}
                        field=f"accounts[{iter}].accountNo"
                        if field in vendors.columns:
                            if row[field]:
                                acc['accountNo']=str(row[field]).strip()
                                field=f"accounts[{iter}].accountStatus"
                                if field in vendors.columns:
                                    if row[field]:
                                        acc['accountStatus']=row[field]
                                else:
                                    acc['accountStatus']="Active"
                                #accounts[0].description
                                field=f"accounts[{iter}].description"
                                if field in vendors.columns:
                                    if row[field]:
                                        acc['description']=str(row[field]).strip()
                                #accounts[0].contactInfo
                                field=f"accounts[{iter}].contactInfo"
                                if field in vendors.columns:
                                    if row[field]:
                                        acc['contactInfo']=str(row[field]).strip()
                                field=f"accounts[{iter}].name"
                                accName="No Name"
                                if field in vendors.columns:
                                    if row[field]:
                                        accName=str(row[field]).strip()
                                acc['name']=accName
                                #accounts[0].paymentMethod
                                field=f"accounts[{iter}].paymentMethod"
                                paymentMethod="Other"
                                if field in vendors.columns:
                                    if row[field]:
                                        payM=str(row[field]).strip()
                                        countlist = self.paymentMethod.count(str(payM))
                                        if countlist>0:
                                            paymentMethod=payM
                                acc['paymentMethod']=paymentMethod
                                accounts.append(acc)                                  
                        else:
                            sw=False
                        iter+=1    
                    org['accounts']=accounts
                    org['interfaces']=self.readInterfaces(kwargs['dfinterfaces'], vencode)
                    org['contacts']=self.readContacts(client,kwargs['dfcontacts'], vencode) 
                    if swname and swcode:               
                        mf.printObject(org,self.path_results,count,"organization_byLine.json",False)
                        orga.append(org)
                    else:
                        mf.printObject(org,self.path_results,count,"worse_organization_byLine.json",False)
                    print(f"INFO Organization record: {count} has been created")
                    
                    if swnotes:
                        self.customerName.readnotes(client,toSearch=vencode,linkId=orgId)                      
                    
                    org={}
                    #addnoteapp=False
                    interface_Id=[]
                    #old_org=org_code
                    contact_Id=[]
                    org_erpCode=""
                    toc = time.perf_counter()
                except Exception as ee:
                    print(f"ERROR: {ee}")
            orgFull['organizations']=orga
            mf.printObject(orgFull,self.path_results,count,"organization",True)
            end_time = time.perf_counter()
            print(f"INFO Organization Execution Time : {end_time - start_time:0.2f}" )
            print(f"Interfaces processed : {self.countcred}")
        except Exception as ee:
            print(f"ERROR: {ee}")



    def readInterfaces(self, dfinterfaces, toSearch):
        try:
            count=0
            dfinter = dfinterfaces[dfinterfaces['code']== toSearch]
            iter=0
            interfacesId=[]
            for c, cprow in dfinter.iterrows():
                inter={}
                cred={}
                print(f"INFO Processing interfaces for the {toSearch} organization")
                intername=" "
                interId=""
                field=f"interfaces[0].name"
                if field in dfinter.columns:
                    if cprow[field]:
                        intername=str(cprow[field])
                        inter['name']=intername
                interId=str(uuid.uuid4())
                inter['id']=interId                        
                uri=""
                field=f"interfaces[0].uri"
                if field in dfinter.columns:
                    if cprow[field]:
                        uri=cprow[field]
                        if mf.checkURL(uri): inter['uri']=uri
                        else: inter['uri']="http://"+uri
                field=f"interfaces[0].notes"
                if field in dfinter.columns:
                    if cprow[field]:
                        iternotes=str(cprow[field])
                        inter['notes']=iternotes
                interava=True
                iava=""
                field=f"interfaces[0].available"
                if field in dfinter.columns: 
                    if cprow[field]:
                        iava=cprow[field]
                        if iava=="FALSE":
                            interava=False
                inter['available']=interava
                
                interfacetype=[]
                enuminterfacetype=["Admin","End user","Reports","Orders","Invoices","Other"]
                field=f"interfaces[0].type"
                if field in dfinter.columns:
                    itype=str(cprow[field]).strip()
                    x=itype.split(",")
                    for cadenatype in x:                            
                        countlist = enuminterfacetype.count(str(cadenatype))
                        if countlist>0: 
                            interfacetype.append(cadenatype)

                inter['type']=interfacetype
                
                deliverM="Online"
                deliverMethodvalue=["Online", "FTP", "Email", "Other"]
                field=f"interfaces[0].deliveryMethod"
                if field in dfinter.columns:
                    deliverMethodtosearch=cprow[field]
                    countlist = deliverMethodvalue.count(str(deliverMethodtosearch))
                    if countlist>0: inter['deliveryMethod']=deliverMethodtosearch
                    else: inter['deliveryMethod']=deliverM
                
                field=f"interfaces[0].statisticsFormat"
                if field in dfinter.columns:
                    if cprow[field]:
                        inter['statisticsFormat']=str(cprow[field]).strip()
                onlineLocation=""
                
                field=f"interfaces[0].onlineLocation"
                if field in dfinter.columns:
                    if cprow[field]:
                        onlineLocation=str(cprow[field]).strip()
                        inter['onlineLocation']=cprow[field]
                statisticsNotes=""
                field=f"interfaces[0].statisticsNotes"
                if field in dfinter.columns:
                    if cprow[field]:
                        statisticsNotes=cprow[field]
                        inter['statisticsNotes']=statisticsNotes
                username=""        
                field=f"interfaces[0].username"
                if field in dfinter.columns:
                    if cprow[field]:
                        username=str(cprow[field]).strip()
                        cred['username']=username
                        siuser=True
                        self.countcred+=1
                password=""
                field=f"interfaces[0].password"
                if field in dfinter.columns:
                    if cprow[field]:
                        password=str(cprow[field]).strip()
                        cred['password']=password
                        sicre=True 
                if siuser:
                    cred['id']=str(uuid.uuid4())
                    cred['interfaceId']=interId
                    mf.printObject(cred,self.path_results,self.countcred,"credentials",False)
                    mf.printObject(inter,self.path_results,self.countcred,"interfaces",False)
                iter+=1   
                interfacesId.append(interId)
            return interfacesId
        except Exception as ee:
            print(f"ERROR: Interfaces schema:{ee}")



    def readContacts(self, client,dfcontacts, toSearch):
        #Organizations
        rowc=""
        contactsId=[]
        conID=""
        FN=""
        LN=""
        contcategories=""
        contact = dfcontacts[dfcontacts['code']== toSearch]
        
        iter=0
        for c, rowc in contact.iterrows():
            try:
                
                con={}
                conId=""
                #print(contact.columns)
                field=f"contacts[{iter}].lastName"
                if field in contact.columns:
                    if rowc[field]:
                        print(f"INFO processing Contacts for {toSearch}: ",len(contact))
                        con['lastName']=rowc[field]
                        conId=str(uuid.uuid4())
                        con['id']=conId
                        field=f"contacts[{iter}].prefix"
                        if field in contact.columns:
                            if rowc[field]:
                                con['prefix']=rowc[field]
                        field=f"contacts[{iter}].firstName"
                        if field in contact.columns:
                            if rowc[field]:
                                con['firstName']=rowc[field]
                            else:
                                con['firstName']=" "
                        field=f"contacts[{iter}].notes"
                        if field in contact.columns:
                            if rowc[field]:
                                con['notes']=rowc[field]
                        #field=f"contacts.inactive[{iter}]"
                        con['inactive']=False
                        field=f"contacts[{iter}].notes"
                        if field in contact.columns:
                            if rowc[field]:
                                con['notes']=rowc[field]
                        conphonNumbers=[]
                        sw=True
                        iter=0
                        while sw:
                            categories=[]
                            field=f"contacts[{iter}].phoneNumbers"
                            isPrimary=False
                            addressdata1=""
                            if field in contact.columns:
                                phonNumbers=rowc[field]
                                if iter==0:
                                    isPrimary=True
                                #phoneNumbers[0].type
                                field=f"contacts[{iter}].phoneNumbers.type"
                                phonenumbertype=""
                                if field in contact.columns:
                                    if rowc[field]:
                                        phonentype=rowc[field]
                                        phonetypelist=["Office","Mobile","Fax","Other"]
                                        countlist = phonetypelist.count(str(phonentype))
                                        if countlist>0:
                                            phonenumbertype=phonentype
                                        else:
                                            phonenumbertype="Other" 
                                field=f"contacts[{iter}].phoneNumbers.categories"
                                if field in contact.columns:
                                    if rowc[field]:
                                        toSearch=str(rowc[field]).strip()
                                        cate=mf.readJsonfile(self.path_refdata,client+"_categories.json","categories",toSearch,"value")
                                        if cate is None:
                                            mf.write_file(ruta=self.path_logs+"\\categoriesNotFounds.log",contenido=f"{toSearch}")
                                        else:                                         
                                            categories.append(cate)
                                conphonNumbers.append(mf.dic(phoneNumber= phonNumbers,type=phonenumbertype, isPrimary= isPrimary, language="eng-us",categories=categories))
                            else:
                                sw=False
                            iter+=1 
                        
                            if len(conphonNumbers)!=0:
                                con['phoneNumbers']=conphonNumbers         
                        iter=0
                        sw=True
                        conaddresses=[] 
                        while sw:
                            categories=[]
                            field=f"contact[{iter}].addresses1"
                            isPrimary=False
                            addressdata1=""
                            if field in contact.columns:
                                if rowc[field]:
                                    addressdata1= rowc[field]
                                if iter==0:
                                    isPrimary=True
                                field=f"contact[{iter}].addressLine2"
                                addressdata2=""
                                if field in contact.columns:
                                    addressdata2= rowc[field]
                                field=f"contact[{iter}].addresses.city"
                                city=""
                                if field in contact.columns:
                                    if rowc[field]:
                                        city=rowc[field]
                                #addresses[0].stateRegion
                                field=f"contact.[{iter}]addresses.stateRegion"
                                stateRegion=""
                                if field in contact.columns:
                                    if rowc[field]:
                                        stateRegion=rowc[field]
                                #addresses[0].zipCode
                                field=f"contact[{iter}].addresses.zipCode"
                                zipCode=""
                                if field in contact.columns:
                                    if rowc[field]:
                                        zipCode=rowc[field]
                                   
                                field=f"contact.[{iter}]addresses.categories[{iter}]"
                                country=""
                                if field in contact.columns:
                                   if rowc[field]:
                                    if rowc[field]:
                                        toSearch=str(rowc[field]).strip()
                                        cate=mf.readJsonfile(self.path_refdata,client+"_categories.json","categories",toSearch,"value")
                                        if cate is None:
                                            mf.write_file(ruta=self.path_logs+"\\categoriesNotFounds.log",contenido=f"{toSearch}")
                                        else:                                         
                                            categories.append(cate)
                            
                            #addresses[0].categories[0]
                            
                            
                            #"addresses[0].country"
                                field=f"contact.[{iter}]addresses.country"
                                country=""
                                if field in contact.columns:
                                    if rowc[field]:
                                        country=rowc[field]
                            
                                conaddresses.append(mf.dic(addressLine1= addressdata1,addressLine2=addressdata2,
                                                    city=city,stateRegion= stateRegion,
                                                    zipCode=zipCode,country=country,
                                                    isPrimary=isPrimary,categories=categories,language="eng-us"))
                            
                            else:
                                sw=False    
                            iter+=1
                        if len(conaddresses)!=0:
                            con["addresses"]=conaddresses                                
                            
                        conurls=[]   
                        iter=0
                        sw=True        
                        while sw:
                            categories=[]
                            field=f"contact[{iter}].urls.value"
                            isPrimary=False
                            if field in contact.columns:
                                url=rowc[field]
                                #emails[0].description
                                field=f"contact.[{iter}]urls.description"
                                desc=""
                                if field in contact.columns:
                                    desc=rowc[field]
                          
                                field=f"contact[{iter}].urls.note"
                                note=""
                                if field in contact.columns:
                                    note=rowc[field]
                                
                                field=f"contact[{iter}].urls.categories[{iter}]"
                                if field in contact.columns:
                                   if rowc[field]:
                                    if rowc[field]:
                                        toSearch=str(rowc[field]).strip()
                                        cate=mf.readJsonfile(self.path_refdata,client+"_categories.json","categories",toSearch,"value")
                                        if cate is None:
                                            mf.write_file(ruta=self.path_logs+"\\categoriesNotFounds.log",contenido=f"{toSearch}")
                                        else:                                         
                                            categories.append(cate)
                                conurls.append(mf.dic(value=url, description=desc,note=note,language="eng-us",categories=categories))                
                            else:
                                sw=False
                            iter+=1
                        if len(conurls)!=0:
                            con['urls']=conurls                                
                                
                        sw=True
                        iter=0
                        conemails=[]
                        #phoneNumbers[0].categories[0]
                        while sw:
                            categories=[]
                            field=f"contact[{iter}].emails.value"
                            isPrimary=False
                            email=""
                            if field in contact.columns:
                                email=rowc[field]
                                if iter==0:
                                    isPrimary=True
                                #emails[0].description
                                field=f"contact.[{iter}]emails.description"
                                desc=""
                                if field in contact.columns:
                                    desc=str(rowc[field])
                                field=f"contact.[{iter}]emails.categories[{iter}]"
                                if field in contact.columns:
                                    if rowc[field]:
                                        toSearch=str(rowc[field]).strip()
                                        cate=mf.readJsonfile(self.path_refdata,client+"_categories.json","categories",toSearch,"value")
                                        if cate is None:
                                            mf.write_file(ruta=self.path_logs+"\\categoriesNotFounds.log",contenido=f"{toSearch}")
                                        else:                                         
                                            categories.append(cate)
                                conemails.append(mf.dic(value=email,description=desc,language="eng-us",isPrimary= True, categories=categories))
                            else:
                                sw=False
                            iter+=1
                        if len(conemails)!=0:
                            con['emails']=conemails#orgemails=faf.org_emails(organizations.loc[i],6)
                        mf.printObject(con,self.path_results,c,"contacts",False)                                                 
                        contactsId.append(conId)                   
            except Exception as ee:
                print(f"ERROR: Contacts schema:{ee}")                
        return contactsId