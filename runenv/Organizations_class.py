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

class organizations():
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
            self.ordersbyline=self.path_logs+"\\"+self.customerName+"_organizationbyline.json"
            print(self.ordersbyline)
            self.ordersbyline=open(self.ordersbyline, 'w') 
        except Exception as ee:
            print(f"ERROR: {ee}")
            
    def readacquisitionMapping(self):
        self.customerName=pd.dataframe()
        filetoload=self.path_refdata+f"\\acquisitionMapping.xlsx"
        print("Dataframe: Acquisition Method")
        self.paymentMethodAccount=self.customerName.importDataFrame(filetoload,sheetName="paymentMethodAccount")
        self.categories=self.customerName.importDataFrame(filetoload,sheetName="categories")
        
###########################
#ORGANIZATIONS
###########################
        
    def readOrganizations(self, client,dforganizations, dfcontacts, dfintefaces):
        try:
            vendors=dforganizations
            contacts=dfcontacts
            interfaces=dfinterfaces
            #paymentMethodAccount={"Cash":"","PCard":"Credit Card","EFT":"","Deposit Account":"","Check":"Physical Check","Bank Draft":"","Internal Transfer":""}
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
                    count+=1
                    tic = time.perf_counter()
                    if 'id' in vendors.columns: org['id']=str(row['id']).strip()
                    else: org['id']=str(uuid.uuid4())
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
                    org['code']=vencode
                    #ORG NAME
                    org_name=""
                    if 'name' in vendors.columns: 
                        if row['name']: org_name=str(row['name']).strip()
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
                    #org_erpCode=str(row['ACCTNUM']).strip()
                    #ORG ALIASES#################
                    aliases=[]
                    iter=0
                    sw=True
                    while sw:
                        altnames="aliases["+str(iter)+"].value"
                        if altnames in vendors.columns:
                            if row[altnames]:
                                aliases.append(str(row[altnames]).strip())
                        else:
                            sw=False
                        iter+=1
                    Order["aliases"]=aliases

                    if 'language' in vendors.columns:
                        if row['language']: venlanguages=self.org_languages(str(row['language']).strip())
                        org['language']=venlanguages
                    
                    #Categories nn=blank
                    orgCategoria="nn"
                    #Addresses
                    orgaddresses=[]
                    addAdd= True
                    #addresses[0].addressLine1
                    addresses=[]
                    iter=0
                    sw=True
                    while sw:
                        categories=[]
                        adresses1="addresses["+str(iter)+"].addressLine"
                        isPrimary=False
                        addressdata1=""
                        if adresses1 in vendors.columns:
                            if row[adresses1]:
                               addressdata1= row[adresses1]
                               
                            if iter==0:
                                isPrimary=True
                                
                            adresses2="addresses["+str(iter)+"].addressLine2"
                            addressdata2=""
                            if adresses2 in vendors.columns:
                                addressdata2= row[adresses2]
                             
                            addcity="addresses["+str(iter)+"].city"
                            city=""
                            if addcity in vendors.columns:
                               if row[addcity]:
                                   city=row[addcity]
                            #addresses[0].stateRegion
                            addstateRegion="addresses["+str(iter)+"].stateRegion"
                            stateRegion=""
                            if addstateRegion in vendors.columns:
                               if row[addstateRegion]:
                                   stateRegion=row[addstateRegion]
                            #addresses[0].zipCode
                            addzipCode="addresses["+str(iter)+"].zipCode"
                            zipCode=""
                            if addzipCode in vendors.columns:
                               if row[addzipCode]:
                                   zipCode=row[addzipCode]
                                   
                            addcategorie="addresses["+str(iter)+"].categories["+str(iter)+"]"
                            country=""
                            if addcategorie in vendors.columns:
                               if row[addcategorie]:
                                    toSearch=str(row[addcategorie]).strip()
                                    cate=self.mapping(self.categories,toSearch)
                                    if cate is not None:
                                        categories.append(cate)
                            
                            #addresses[0].categories[0]
                            
                            
                            #"addresses[0].country"
                            addcountry="addresses["+str(iter)+"].country"
                            country=""
                            if 'addcountry' in vendors.columns:
                               if row[addcountry]:
                                   country=row['addcountry']

                            orgaddresses.append(mf.dic(addressLine1= addressdata1,addressLine2=addressdata2,
                                                    city=city,stateRegion= stateRegion,
                                                    zipCode=zipCode,country=country,
                                                    isPrimary=isPrimary,categories=categories,language="eng-us"))
                            
                        else:
                            sw=False    
                        iter+1

                    org["addresses"]=orgaddresses
                    #phoneNumbers
                    addpho=""
                    orgphonNumbers=[]
                    #categories=[]
                    cate=[]
                    sw=True
                    iter=0
                    #phoneNumbers[0].categories[0]
                    while sw:
                        categories=[]
                        phone="phoneNumbers["+str(iter)+"].phoneNumber"
                        isPrimary=False
                        addressdata1=""
                        if phone in vendors.columns:
                            phonNumbers=row[phone]
                        #phoneNumbers[0].type
                        phoneNum="phoneNumbers["+str(iter)+"].type"
                        phonenumbertype=""
                        if phoneNum in vendors.columns:
                            phonenumbertype=row[phoneNum]
                        
                        #phoneNumbers[0].categories[0]
                        addcategorie="phoneNumbers["+str(iter)+"].categories["+str(iter)+"]"
                        if addcategorie in vendors.columns:
                               if row[addcategorie]:
                                    toSearch=str(row['addcategorie']).strip()
                                    cate=self.mapping(self.categories,toSearch)
                                    if cate is not None:
                                        categories.append(cate)
                            
                            
                        else:
                            sw=False
                        iter+=1 
                        orgphonNumbers.append(mf.dic(phoneNumber= phonNumbers,type=phonenumbertype, isPrimary= False, language="eng-us",categories=categories))
                        
                    org['phoneNumbers']=orgphonNumbers
                
                    #emails
                    orgemails=[]
                    cate=[] 
                    sw=True
                    iter=0
                    #phoneNumbers[0].categories[0]
                    while sw:
                        categories=[]
                        mail="emails["+str(iter)+"].value"
                        isPrimary=False
                        email=""
                        if mail in vendors.columns:
                            email=row[mail]
                        #emails[0].description
                        description="emails["+str(iter)+"].description"
                        desc=""
                        if description in vendors.columns:
                            desc=row[description]
                        
                        #phoneNumbers[0].categories[0]
                        addcategorie="emails["+str(iter)+"].categories["+str(iter)+"]"
                        if addcategorie in vendors.columns:
                               if row[addcategorie]:
                                    toSearch=str(row['addcategorie']).strip()
                                    cate=self.mapping(self.categories,toSearch)
                                    if cate is not None:
                                        categories.append(cate)
                            
                        else:
                            sw=False
                        iter+=1
                        orgemails.append(mf.dic(value=email,description=desc,language="eng-us",isPrimary= True, categories=categories))
                    org['emails']=orgemails#orgemails=faf.org_emails(vendors.loc[i],6)
                    #vendorCurrencies
                    sw=True
                    iter=0
                    #phoneNumbers[0].categories[0]
                    currency=[]
                    while sw:
                        vendorCurrency="vendorCurrencies["+str(iter)+"]" 
                        if vendorCurrency in vendors.columns:
                            currency.append(row[vendorCurrency])
                        else:
                            sw=False
                        iter+=1
                            
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
                        url="urls["+str(iter)+"].value"
                        isPrimary=False
                        email=""
                        if mail in vendors.columns:
                            email=row[mail]
                        #emails[0].description
                        description="urls["+str(iter)+"].description"
                        desc=""
                        if description in vendors.columns:
                            desc=row[description]
                        
                        #phoneNumbers[0].categories[0]
                        addcategorie="urls["+str(iter)+"].categories["+str(iter)+"]"
                        if addcategorie in vendors.columns:
                               if row[addcategorie]:
                                    toSearch=str(row['addcategorie']).strip()
                                    cate=self.mapping(self.categories,toSearch)
                                    if cate is not None:
                                        categories.append(cate)
                            
                        else:
                            sw=False
                        iter+=1
                        orgurls.append(mf.dic(value=url, description=desc,language="eng-us",categories=categorie))
                        #print(orgurls)
                        #orgurls=faf.org_urls(vendors.loc[i],7)
                org['urls']=orgurls
                 #accounts
                accounts=[]
                addacc=True
                nameaccount=""
                accountnum=""
                paymentMethod=""
                libraryCode=""
                libraryEdiCode=""
                
                #accounts[0].libraryEdiCode"
                
                
                '''if 'Account1number' in vendors.columns:
                    if row['Account1number']:
                        nameaccount=row['Account1Name']
                        if row['Account1number']: accountnum=row['Account1number']
                        if row['Account1Paymentmethod']:                        
                            paymentMethodtemp=row['Account1Paymentmethod']
                            if paymentMethodtemp=="PCard": paymentMethod="Credit Card"
                            elif paymentMethodtemp=="Check": paymentMethod="Physical Check"
                            elif paymentMethodtemp=="Check;PCard": paymentMethod="Physical Check"
                            elif paymentMethodtemp=="WALDO": paymentMethod="Other"
                            else: paymentMethod="Other"
                            
                        if row['Account1Librarycode']: libraryCode= "999"
                        if row['Account1LibraryEDIcode']: libraryEdiCode="999"
                        accounts.append(mf.dic(name=nameaccount,accountNo=accountnum,accountStatus="Active",paymentMethod=paymentMethod,libraryCode=libraryCode,libraryEdiCode=libraryEdiCode))
                    if 'Account2number' in vendors.columns:
                        if row['Account2number']:
                            nameaccount=row['Account2Name']
                        if row['Account2number']: accountnum=row['Account2number']
                        if row['Account2Paymentmethod']: paymentMethod=row['Account2Payment method']
                        if row['Account2Librarycode']: libraryCode= "999"
                        if row['Account2LibraryEDI code']: libraryEdiCode="999"
                        accounts.append(faf.dic(name=nameaccount,accountNo=accountnum,accountStatus="Active",paymentMethod=paymentMethod,libraryCode=libraryCode,libraryEdiCode=libraryEdiCode))
                org['accounts']=accounts
                    #accounts=faf.org_account(vendors.loc[i],0)'''
                 #Acquisition Unit
                acqUnitIds= []
                addacc=True
                if 'Acquisitions units' in vendors.columns:
                    org['acqUnitIds']=[]
                    #acqUnitIds=faf.org_acqunit(vendors.loc[i],0)
                
                '''interface_Id=[]
                addinterface= False
                if 'interfaces' in vendors.columns:
                    #readInterfacesSpreadsheet(idSearch,path,sheetName,customerName):
                    if row['Interface1_URI']!="":
                        interface_Id.append(Readinterfaces(vendors.loc[i],1,path=path_results,client=customerName))
                if 'Interface2_URI' in vendors.columns:
                    #readInterfacesSpreadsheet(idSearch,path,sheetName,customerName):
                    if row['Interface2_URI']!="":                               
                        interface_Id.append(Readinterfaces(vendors.loc[i],2,path=path_results,client=customerName))
                if 'Interface3_URI' in vendors.columns:
                    #readInterfacesSpreadsheet(idSearch,path,sheetName,customerName):
                    if row['Interface3_URI']!="":                               
                        interface_Id.append(Readinterfaces(vendors.loc[i],3,path=path_results,client=customerName))
                
                org['interfaces']=interface_Id
                contact_Id=[]
                #print(vendors.columns)
                if 'Contact1_FirstName' in vendors.columns:
                    if row['Contact1_FirstName']!="":
                        contact_Id.append(readContacts(vendors.loc[i],1,path=path_refdata,path1=path_results,client=customerName))
                if 'Contact2_FirstName' in vendors.columns:
                    if row['Contact2_FirstName']!="":
                        contact_Id.append(readContacts(vendors.loc[i],2,path=path_refdata,path1=path_results,client=customerName))
                org['contacts']=contact_Id
                #ORG UUID
                cont=cont+1
                if cont==84:
                    a=1
                #def __init__(self,idorg,name,orgcode,vendorisactive,orglanguage):
                #org=faf.Organizations(uuidOrg,org_name,org_code,activeVendor,"eng",accounts)
                #org.printorganizations(orgdescription,orgaliases,orgaddresses,orgphonNumbers,orgemails,orgurls,orgvendorCurrencies,contact_Id,interface_Id,org_erpCode,path)'''
                cont=0
                mf.printObject(org,path_results,count,"organization_byLine.json",False)
                print(f"INFO Record: {count} created ")
                orga.append(org)
                org={}
                addnoteapp=False
                interface_Id=[]
                #old_org=org_code
                contact_Id=[]
                org_erpCode=""
                toc = time.perf_counter()
            orgFull['organizations']=orga
            
            mf.printObject(orgFull,path_results,count,"organization.json",True)   
        else:
            print(f"ERROR Record: file not uploaded")


    def readInterfacesSpreadsheet(self, idSearch,path,sheetName,customerName):
        rowi=""
        col_types={"CTACT CODE":str}
        interfacesId=[]
        conID=""
        FN=""
        LN=""
        contcategories=""
        interface = pd.read_excel(path,sheet_name=sheetName, dtype=col_types)
        #print(interface)
        interface_filter = interface[interface['CTACT CODE']== idSearch]
        interface_filter = interface_filter.apply(lambda x: x.fillna(""))
        print("Interface founds: ",len(interface_filter))
        for inter, rowi in interface_filter.iterrows():
            intName=""
            inttype=""
            interNote=""
            interStanote=""
            intUri=""
            creuser=""
            crepass=""
            if rowi[1]:
                interId=str(uuid.uuid4())
                if rowi[1]:
                    intName =str(rowi[1])
                if rowi[2]: inttype =faf.interfacetype(rowi[2])
                if rowi[3]: intUri =str(rowi[3])
                if rowi[4]: interNote =str(rowi[2])
                if rowi[5]: creuser =str(rowi[5])
                if rowi[6]: crepass =str(rowi[6])
                if rowi[7]: interStanote=str(rowi[6])
                org=faf.interfaces(interId,intName,intUri,inttype)
                org.printinterfaces(customerName, interNote,interStanote)
                if (crepass!="") or (creuser!=""):
                    org.printcredentials(interId,creuser,crepass, customerName)

                interfacesId.append(interId)
        return interfacesId

#Interfaces(line=vendors.loc[i],path=path_results,client=customerName))
    def Readinterfaces(self, dfRow,m,**kwargs):
        try:
            intName=""
            inttype=""
            interNote=""
            interStanote=""
            intUri=""
            creuser=""
            crepass=""
            n=str(m)
            interId=""
            inttypetemp=""
            inttype=""
            if dfRow['Interface'+n+'_URI']!="":
                interId=str(uuid.uuid4())
                if dfRow['Interface'+n+'_Name']!="":
                    intName =str(dfRow['Interface'+n+'_Name'])
                if dfRow['Interface'+n+'_Type']!="": 
                    inttypetemp =dfRow['Interface'+n+'_Type']
                    inttype=faf.interfacetype(inttypetemp)
                else: 
                    inttype=="Other"
                interStanote=""
                interNote=""
                deliveryMethod="Online" 
                #"Online","FTP","Email", "Other"
                statisticsFormat=""
                if dfRow['Interface'+n+'_URI']!="": intUri =str(dfRow['Interface'+n+'_URI'])
                if dfRow['Interface'+n+'_Notes']!="": interNote =str(dfRow['Interface'+n+'_Notes'])
                if dfRow['Interface'+n+'_Username']!="": creuser =str(dfRow['Interface'+n+'_Username'])
                if dfRow['Interface'+n+'_Password']!="": crepass =str(dfRow['Interface'+n+'_Password'])
                if dfRow['Interface'+n+'_StatisticsAvailable']: interStanote=f"Statistics Available:"+str(dfRow['Interface'+n+'_StatisticsAvailable'])
                if dfRow['Interface'+n+'_DeliveryMethod']:
                    if dfRow['Interface'+n+'_DeliveryMethod']!="": 
                        deliveryMethod="Other"#dfRow['Interface'+n+'_DeliveryMethod']
                if dfRow['Interface'+n+'_Statisticsformat']!="":
                    statisticsFormat=dfRow['Interface'+n+'_Statisticsformat']
                org=faf.interfaces(interId,intName,intUri,deliveryMethod,inttype)
                org.printinterfaces(kwargs['path'],kwargs['client'], interNote,interStanote,statisticsFormat)
                if (crepass!="") or (creuser!=""):
                    org.printcredentials(kwargs['path'],interId,creuser,crepass, kwargs['client'])

                #interfacesId.append(interId)
            return interId
        except Exception as ee:
                print(f"ERROR: Interfaces schema:{ee}")



    def readContactsSpreadsheet(idSearch,path,sheetName,customerName):
        #Organizations
        rowc=""

        col_types={"CTACT CODE":str}
        contactsId=[]
        conID=""
        FN=""
        LN=""
        contcategories=""
        contacts = pd.read_excel(path,sheet_name=sheetName, dtype=col_types)
        #print(contacts)
        contact_filter = contacts[contacts['CTACT CODE']== idSearch]
        contact_filter = contact_filter.apply(lambda x: x.fillna(""))
        print("Contacts founds: ",len(contact_filter))
        for c, rowc in contact_filter.iterrows():
            if rowc[1]:
                contactName_temp=rowc[1]
                ContactName=faf.SplitString(contactName_temp)
                FN=str(ContactName[0])
                LN=str(ContactName[1])
            else:
                FN="NaN"
                LN="NaN"
                #Title go to notes and categorical
                #contactTitle="NULL"
                #if namesheet.cell_value(c,4)!="NULL":
                #    contactTitle=str(namesheet.cell_value(c,4))
                #address
            contactLang="en-us"
            contactnotes=""
            addcontnote=True
            if rowc['contact_notes']:
                contactnotes=rowc['contact_notes']
            #Contacts phone
            contactphoneN=[]
            addpho=True
            if addpho:
                contactphoneN.append(faf.org_phoneNumbers(contact_filter.loc[c],3,4,5))
                if faf.is_empty(contactphoneN[0]):
                    contactphoneN=[]
            #Contact emails
            contactemail=[]
            addmails=True
            if addmails: 
                contactemail.append(faf.org_emails(contact_filter.loc[c],6,7))
                if faf.is_empty(contactemail[0]):
                    contactemail=[]
            #Contact Address
            contactaddresses=[]
            addadd=False
            if addadd: 
                contactaddresses.append(faf.org_addresses(contact_filter.loc[c],6,7))
                if faf.is_empty(contactaddresses[0]):
                    contactaddresses=[]
            #INACTIVE / ACTIVE
            contactinactive= False
            #Contact URL
            contacturls=[]
            addurl=False
            if addurl:  
                contacturls.append(faf.org_urls(contact_filter.loc[c],0))
                if faf.is_empty(contacturls[0]):
                    contacturls=[]
            contcategories=faf.org_categorie("nn")
            conID=str(uuid.uuid4())
            contactsId.append(conID)
            #(self,contactID,contactfirstName, contactlastName, contactcategories):
            ctc=faf.contactsClass(conID,FN,LN,contcategories,contactLang)
            #def printcontacts(self,cont_phone,cont_email, cont_address,cont_urls,cont_categories,contactnotes,fileName):
            ctc.printcontactsClass(contactphoneN, contactemail, contactaddresses, contacturls,contcategories,contactnotes,customerName)  
            return contactsId

        
def readContacts(dfRow,m,**kwargs):
    #Organizations
    rowc=""
    contactsId=[]
    conID=""
    FN=""
    LN=""
    desc=""
    #print(dfRow)
    n=str(m)
    try:
        FN=""
        LN=""
        FN=dfRow['Contact'+n+'_FirstName']
        if dfRow['Contact'+n+'_LastName']!="":  
            LN=dfRow['Contact'+n+'_LastName']
        if dfRow['Contact'+n+'_Status']=="":inactive=False
        else: inactive=True
        contactLang="en-us"
        contactnotes=""
        if dfRow['Contact'+n+'_Note']!="":
            contactnotes=str(dfRow['Contact'+n+'_Note']).strip()
        #PHONE CONTACT
        contactPhone={}
        contactphoneN=[]
        if dfRow['Contact'+n+'_Phonenumber']!="":
            phoneContact=str(dfRow['Contact'+n+'_Phonenumber'])
            phonetypeContact=""
            if ['Contact'+n+'_PrimaryPhoneNumberType']!="":
                phonetypeContacttemp=dfRow['Contact'+n+'_PrimaryPhoneNumberType']
                if phonetypeContacttemp=="Office": phonetypeContact="Office"
                elif phonetypeContacttemp=="Mobile": phonetypeContact="Mobile"
                elif phonetypeContacttemp=="Fax": phonetypeContact="Fax"
                elif phonetypeContacttemp=="Other": phonetypeContact="Other"
                else:
                    phonetypeContact="Other"
            catephone=[]
            catcontacttosearch=""
            if dfRow['Contact'+n+'_PrimaryPhoneNumberCategories']:
                if dfRow['Contact'+n+'_PrimaryPhoneNumberCategories']!="":
                    catcontacttosearch=str(dfRow['Contact'+n+'_PrimaryPhoneNumberCategories']).strip()
                    catephone.append(faf.readJsonfile(kwargs['path'],"cairn_categories.json","categories",catcontacttosearch,"value"))
                    if catephone is None:
                        catephone=[]
            contactPhone=faf.dic(phoneNumber=phoneContact,language="en-us",type=phonetypeContact, isPrimary=True, categorie=catephone)            
            contactphoneN.append(contactPhone)
        #URL CONTACTS
        contacturls=[]
        catepurl=[]
        if dfRow['Contact'+n+'_URL']!="":
            em=dfRow['Contact'+n+'_URL']
            if dfRow['Contact'+n+'_URLCategories']!="":
                catcontacttosearch=dfRow['Contact'+n+'__URLCategories']
                catepurl.append(faf.readJsonfile(kwargs['path'],"cairn_categories.json","categories",catcontacttosearch,"value"))
            urlcontact=faf.dic(value=em,description="",language="en-us", categories=catepurl,notes="")
            contacturls.append(urlcontact)

        #EMAILS CONTACT
        valueMail=""
        contactemail=[]
        catemail=[]
        if dfRow['Contact'+n+'_Email']!="":
            if dfRow['Contact'+n+'_Email']!="":
                valueMail=dfRow['Contact'+n+'_Email']
            if dfRow['Contact'+n+'_EmailDescription']:
                desc=dfRow['Contact'+n+'_EmailDescription']
            catemail=[]
            if dfRow['Contact'+n+'_EmailCategories']:
                catmailtosearch=dfRow['Contact'+n+'_EmailCategories']
                if catmailtosearch=="Account Rep": catmailtosearch="Account Representative"
                catemail.append(faf.readJsonfile(kwargs['path'],"cairn_categories.json","categories",catmailtosearch,"value"))
                desc=""
                if catemail is not None:
                    catemail=[]
            emailcontact=faf.dic(value=valueMail, description=desc,categories=catemail)
            contactemail.append(emailcontact)
        #ADDRESS CONTACTS
        contactaddresses=[]
     
        if dfRow['Contact'+n+'_PrimaryAddress1']!="":
            cat=[]
            if dfRow['Contact'+n+'_Address1']: addressLine1=dfRow['Contact'+n+'_Address1']
            if dfRow['Contact'+n+'_Address1_2']: addressLine2=dfRow['Contact'+n+'_Address1_2']
            if dfRow['Contact+n+_Address1City']: city=dfRow['Contact1_Address1City']
            if dfRow['Contact'+n+'_Address1State']: stateRegion=dfRow['Contact'+n+'_Address1State']
            if dfRow['Contact'+n+'_Address1Zip']: zipCode=dfRow['Contact'+n+'_Address1Zip']
            if dfRow['Contact'+n+'_Address1Country']: country=dfRow['Contact'+n+'_Address1Country']
            cateaddress=[]
            if dfRow['Contact'+n+'_Address1Categories']!="":
                categorieaddresstosearch=dfRow['Contact'+n+'_Address1Categories']
                cateaddress.append(faf.readJsonfile(kwargs['path'],"cairn_categories.json","categories",categorieaddresstosearch,"value"))
            addressconta=faf.dic(address=faf.dic(addressLine1=addressLine1,addressLine2=addressLine2,city=city,country=country,stateRegion=stateRegion,zipCode=zipCode),categories=cateaddress,language="en")            
            contactaddresses.append(addressconta)
        #CATEGORIE BY CONTACT
        catecontact=""
        contcategories=[]
        if dfRow['Contact'+n+'_Categories']:
            if dfRow['Contact'+n+'_Categories']!="":
                categoriecotacttosearch=dfRow['Contact'+n+'_Categories']
                catecontact=faf.readJsonfile(kwargs['path'],"cairn_categories.json","categories",categoriecotacttosearch,"value")
                if catecontact is None:
                    contcategories=[]
                else:
                    contcategories.append(catecontact)
            
        conID=str(uuid.uuid4())
        contactsId=conID
        #contactsId.append(conID)
        #(self,contactID,contactfirstName, contactlastName, contactcategories):
        ctc=faf.contactsClass(conID,FN,LN,contcategories,contactLang)
        #def printcontacts(self,cont_phone,cont_email, cont_address,cont_urls,cont_categories,contactnotes,fileName):
        contactprefix=""
        ctc.printcontactsClass(kwargs['path1'],contactprefix,contactphoneN, contactemail, contactaddresses, contacturls,contcategories,contactnotes,kwargs['client'])  
        return contactsId
    except Exception as ee:
        print(f"ERROR: Contacts schema: {ee}")

    def org_languages(self,value):
        try:
            value=value.upper()
            if value=="ENGLISH":
                valueR="eng"
            elif value=="SPANISH":
                valueR="spa"
            elif value=="NULL":
                valueR="eng"
            elif value is None:
                valueR="eng"
            elif value=="":
                valueR=""
            else:
                valueR="eng"
            return valueR
    
        except ValueError:
            print("org_addresses Error: "+str(ValueError))


    def mapping(self,dftoSearch,toSearch):
        try:                    
            dataToreturn=""
            temp = dftoSearch[dftoSearch['LEGACY SYSTEM']== toSearch]
            #print("poLines founds records: ",len(temp))
            if len(temp)>0:
                for x, cptemp in temp.iterrows():
                    dataToreturn=cptemp['FOLIO']
            else:
                mf.write_file(ruta=self.path_logs+"\\organizationMapping.txt",contenido=f"{toSearch}")
                dataToreturn=None
            return dataToreturn