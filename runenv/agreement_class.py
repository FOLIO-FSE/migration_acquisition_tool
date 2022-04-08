import datetime
import warnings
from datetime import datetime
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
import pandas as pd
import validator
import ast
#import tkinter as tk
#from tkinter import filedialog, messagebox, ttk
import backup_restore as br
import functions_AcqErm as mf
import time
from datetime import datetime
import yaml
import shutil
import functions_AcqErm as mf

################################
##AGREEMENT FUNCTION
################################
class Agreements():
    def __init__(self,client):
        self.customerName=client
    
    def readagreements(self,dataframe):
        self.agreements= dataframe
        print(self.agreements)
        agree={}
        count=1
        for i, row in self.agreements.iterrows():
            print(f"Record No. {count}")
            try:
                #agree["id"]= "bc4f9842-e3e4-4088-aa64-7505ac8ce1f2"
                if row['isPerpetual']:
                    if row['isPerpetual']=="NO": agree["isPerpetual"]= {"value": "no","label": "No"}
                    else: agree["isPerpetual"]= {"value": "Yes","label": "Yes"}
                
                if row['AgreementName']:
                    agree["name"]=str(row['AgreementName']).strip()
                orgs=[]
                OrganizationUUID=""
                vendorToSearch1=""
                #OrganizationUUID=readJsonfile(f"{path_refdata}",f"{self.customerName}_organizations.json","organizations","undefined","code")
                #if OrganizationUUID is not None:
                if row['organizationCode_1']:
                    vendorToSearch=str(row['organizationCode_1']).strip()
                    OrganizationUUID=readJsonfile({self.path_refdata},f"{self.customerName}_organizations.json","organizations",vendorToSearch,"code")
                    if OrganizationUUID is None:
                        write_file(path=f"{path_logs}\\vendorsNotFounds.log",contenido=f"{vendorToSearch}")
                        countvendorerror+=1
                        printpoline=False
                        OrganizationUUID=readJsonfile(f"{path_refdata}",f"{self.customerName}_organizations.json","organizations","undefined","code")
                    rolOrganization_1_value=""
                    rolOrganization_1_label=""
                    vendorToSearch1=vendorToSearch
                    if  row['rolOrganization_1']:
                        if str(row['rolOrganization_1'])=="Subscription agent":
                            rolOrganization_1_value="subscription_agent"
                            rolOrganization_1_label="Subscription agent"
                        elif str(row['rolOrganization_1'])=="Vendor":  
                            rolOrganization_1_value="vendor"
                            rolOrganization_1_label="Vendor"
                        elif str(row['rolOrganization_1'])=="Content provider":  
                            rolOrganization_1_value="content_provider"
                            rolOrganization_1_label="Content Provider"
                        else:
                            rolOrganization_1_value="vendor"
                            rolOrganization_1_label="Vendor"
                    org=dic(org=dic(orgsUuid=OrganizationUUID[0], name=OrganizationUUID[1]), role=dic(value=rolOrganization_1_value,label=rolOrganization_1_label))
                    orgs.append(org)
                OrganizationUUID=""
            
            #OrganizationUUID=""            
            #OrganizationUUID=readJsonfile(f"{path_refdata}",f"{self.customerName}_organizations.json","organizations","undefined","code")
                if row['organizationCode_2']:
                    vendorToSearch=str(row['organizationCode_2']).strip()
                    if vendorToSearch1!=vendorToSearch:
                        OrganizationUUID=readJsonfile(path_refdata,f"{self.customerName}_organizations.json","organizations",vendorToSearch,"code")
                        if OrganizationUUID is None:
                            write_file(path=f"{path_logs}\\vendorsNotFounds.log",contenido=f"{vendorToSearch}")
                            countvendorerror+=1
                            printpoline=False
                            OrganizationUUID=readJsonfile(f"{path_refdata}",f"{self.customerName}_organizations.json","organizations","undefined","code")
                        rolOrganization_1_value=""
                        rolOrganization_1_label=""
                        if  row['rolOrganization_2']:
                            if str(row['rolOrganization_2'])=="Subscription agent":
                                rolOrganization_1_value="subscription_agent"
                                rolOrganization_1_label="Subscription agent"
                            elif str(row['rolOrganization_2'])=="Vendor":  
                                rolOrganization_1_value="vendor"
                                rolOrganization_1_label="Vendor"
                            elif str(row['rolOrganization_2'])=="Content provider":  
                                rolOrganization_1_value="content_provider"
                                rolOrganization_1_label="Content Provider"
                            else:
                                rolOrganization_1_value="vendor"
                                rolOrganization_1_label="Vendor"
                        org=dic(org=dic(orgsUuid=OrganizationUUID[0], name=OrganizationUUID[1]), role=dic(value=rolOrganization_1_value,label=rolOrganization_1_label))
                        orgs.append(org)
        
                agree["orgs"]= orgs
                agree["externalLicenseDocs"]= []
                agree["outwardRelationships"]= []
                agree["customProperties"]= {}
                contacts=[]
                if row['ContactIDRol']:
                    idcontact="ed078eb4-1a55-4425-93b1-144bf5429414"
                    rolevalue="authorized_signatory"
                    rollabel="Authorized signatory"
                    contacts.append(dic(user=idcontact,role=dic(value=rolevalue,label=rollabel)))
                agree["contacts"]= contacts
                agree["tags"]= []
                agree["inwardRelationships"]= []
                renewalpriority=""
                if row['renewalPriority']:
                    if row['renewalPriority']=="Definitely renew":
                        renewalpriority=dic(value="definitely_renew",label="Definitely renew")
                    elif row['renewalPriority']=="For review":
                        renewalpriority=dic(value="for_review",label="For review")
                    elif row['renewalPriority']=="Definitely cancel":
                        renewalpriority=dic(value="definitely_cancel",label="Definitely cancel")
                    agree["renewalPriority"]=renewalpriority
                
                linkedLicenses=[]
                if row['LicenseID']:
                    statuslic="controlling"
                    statuslabel="Controlling"
                    linkedLicenses.append(dic(remoteId=row['LicenseID'],status=dic(value=statuslic,label=statuslabel)))
                agree["linkedLicenses"]= linkedLicenses
                agree["docs"]= []
            
                agree["usageDataProviders"]= []
                agree["agreementStatus"]= dic(value="active",label="Active")
                agree["supplementaryDocs"]= []
                timesStartdate="2021-01-01"
                timeEnddate=""
                if row['startDate']:
                    date=""
                    date=row['startDate']
                    timesStartdate = date.strftime("%Y-%m-%d")
                    agree["startDate"]= timesStartdate
            
                if row['endDate']:
                    date=""
                    date=row['endDate']
                    timeEnddate = date.strftime("%Y-%m-%d")
                    agree["endDate"]= timeEnddate
                
                agree["periods"]= [{"startDate": timesStartdate,"endDate":timeEnddate,"periodStatus":"current"}]
                agree["cancellationDeadline"]= ""
                if row['alternateNames']:
                    agree["alternateNames"]= [row['alternateNames']]
            
                printObject(agree,path_results,count,f"{self.customerName}_agreements",False)
                agree={}
                count+=1
            except Exception as ee:
                print(f"ERROR: {ee}")

        print(f"============REPORT======================")
        print(f"RESULTS Record processed {count}")
        print(f"RESULTS Agreements {countpol}")
        print(f"RESULTS vendor with errors: {countvendorerror}")
        print(f"RESULTS end")