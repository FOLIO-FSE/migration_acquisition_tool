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
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import backup_restore as br
import main_functions as mf
import time
from datetime import datetime
import yaml
import shutil
import main_functions as AcqErm

################################
##USERS FUNCTION
################################
class users():
    def __init__(self,client,path_dir):
        try:    
            self.customerName=client
            self.path_results=f"{path_dir}\\results"
            self.path_data=f"{path_dir}\\data"
            self.path_logs=f"{path_dir}\\logs"
            self.path_refdata=f"{path_dir}\\refdata"
            #self.userbyline=open(f"{self.path_logs}\\{self.customerName}_usersbyline.json", 'w') 
        except Exception as ee:
            print(f"ERROR: {ee}")
            
    def readMappingfile(self):
        self.customerName=pd.dataframe()
        filetoload=self.path_refdata+f"\\userMapping.xlsx"
        print("INFO Reading mapping file")
        self.groups=self.customerName.importDataFrame(filetoload,sheetName="groups")
        self.departments=self.customerName.importDataFrame(filetoload,sheetName="departments")
        self.userStatus=self.customerName.importDataFrame(filetoload,sheetName="userStatus")
        self.addressType=self.customerName.importDataFrame(filetoload,sheetName="addressType")
        with open(self.path_refdata+"\\users_mapping.json") as json_mappingfile:
            self.mappingdata = json.load(json_mappingfile)

        

                
    def readusers(self,dataframe):
        self.readMappingfile()
        self.users= dataframe
        #print(self.users)
        enduser={}
        allusers=[]
        count=1
        usuarios={}
        for i, row in self.users.iterrows():
            start_time = time.perf_counter()
            firstName=""
            userName=""
            lastName="Error No LastName"
            email=""
            preferredContactTypeId="002"
            patronGroup=""
            departmentUsers=[]
            department=""
            try:
                if 'personal.lastName' in self.users.columns:
                    if row['personal.lastName']:
                        lastName=str(row['personal.lastName']).strip()
                        print(f"INFO Processing user record # {count} User-Name: {userName}")
                    if 'personal.firstName' in self.users.columns:
                        if row['personal.firstName']:
                            firstName=str(row['personal.firstName']).strip()
                if 'personal.email' in self.users.columns:
                    if row['personal.email']:
                        email=str(row['personal.email']).strip()

                    if 'username' in self.users.columns:
                        if row['username']:
                            userName=str(row['username']).strip()
                            enduser['username']=userName
                        
                        else:
                            userName="@cairn.edu"
                            enduser['username']=userName
                    
                    enduser['externalSystemId']=userName
                if 'active' in self.users.columns:
                    if row['active']:
                        activeUser= True
                        enduser['active']=activeUser
                if 'patronGroup' in self.users.columns:
                    if row['patronGroup']:
                        patronGroup=str(row['patronGroup'])
                        if patronGroup=="1":
                            patronGroup="Student_Undergraduate"
                        elif patronGroup=="2":
                            patronGroup="Student_Graduate"
                        elif patronGroup=="3":    
                                patronGroup="Student_One-Year Programs"
                        elif patronGroup=="4":    
                                patronGroup="Student_Degree Completion"
                        elif patronGroup=="5": 
                            patronGroup="Student_Cairn Online"
                        elif patronGroup=="10": 
                            patronGroup="Faculty"
                        elif patronGroup=="11": 
                            patronGroup="Faculty_Adjunct"
                        elif patronGroup=="12": 
                            patronGroup="Faculty_Affiliate Artist"
                        elif patronGroup=="14": 
                            patronGroup="Staff"
                        elif patronGroup=="15": 
                            patronGroup="Staff_Library"
                        elif patronGroup=="16": 
                            patronGroup="InterLibrary Loan"
                        elif patronGroup=="17": 
                            patronGroup="Test Account"
                        elif patronGroup=="20": 
                            patronGroup="Alumni"
                        elif patronGroup=="21": 
                            patronGroup="Consortia"
                        elif patronGroup=="22": 
                            patronGroup="Dual Enrollment_Homeschool"
                        elif patronGroup=="23": 
                            patronGroup="Dual Enrollment_Partner"
                        elif patronGroup=="24": 
                            patronGroup="John Jay Institute"
                        elif patronGroup=="200": 
                            patronGroup="NON-CURRENT"
                        elif patronGroup=="999": 
                            patronGroup="Continuing Education"
                        else:
                            patronGroup="NON-CURRENT"
                        patronGroupId=mf.readJsonfileRetor(self.path_refdata,"cairn_usergroups.json","usergroups",patronGroup,"group")
                        if patronGroupId is None:
                            patronGroupId="ec5b6d29-004d-4944-8998-edaad57a37f0"
                enduser['patronGroup']=patronGroupId
                department=""
                phone=""
                if 'departments[0]' in self.users.columns:
                    if row['departments[0]']:
                        department=str(row['departments[0]'])
                        if department=="0":	department="Undefined"
                        elif department=="1": department="Liberal Arts & Sciences"
                        elif department=="2": department="Liberal Arts"
                        elif department=="3": department="English"
                        elif department=="4": department="History"
                        elif department=="6": department="Mathematics"
                        elif department=="7": department="Criminal Justice"
                        elif department=="8": department="Mathematics"
                        elif department=="9": department="Studio Art"
                        elif department=="10": department="Politics"
                        elif department=="12": department="Biblical Studies"
                        elif department=="13": department="Camping"
                        elif department=="14": department="School of Divinity"
                        elif department=="16": department="Pastoral Ministries"
                        elif department=="17": department="Youth and Family Ministries"
                        elif department=="18": department="Degree Completion"
                        elif department=="19": department="Intercultural Studies"
                        elif department=="20": department="Accounting (BS)"
                        elif department=="21": department="Business Administration"
                        elif department=="22": department="Computer Science"
                        elif department=="23": department="Information Systems"
                        elif department=="24": department="School of Education"
                        elif department=="26": department="Early Childhood Ed PK-4"
                        elif department=="29": department="Health and Physical Education"
                        elif department=="30": department="Music Education"
                        elif department=="31": department="Secondary Social Studies Education"
                        elif department=="35": department="Music (BMus)"
                        elif department=="36": department="Music (BA)" 
                        elif department=="40": department="Social Work"
                        elif department=="41": department="Psychology"
                        elif department=="42": department="Pre-Art Therapy"
                        elif department=="44": department="Community Arts"
                        elif department=="45": department="School of Divinity"
                        elif department=="46": department="Student Ministries"
                        elif department=="49": department="TESOL Certification"
                        elif department=="50": department="Vox Bivium"
                        elif department=="51": department="Part-time Only"
                        elif department=="52": department="Undeclared"
                        elif department=="53": department="AIMS"
                        elif department=="54": department="JJI"
                        elif department=="55": department="DEH"
                        elif department=="56": department="DEN"
                        elif department=="57": department="CALVACAD"
                        elif department=="58": department="CALVCHR"
                        elif department=="59": department="DAYSPRING"
                        elif department=="60": department="DCCS"
                        elif department=="61": department="Home School"
                        elif department=="62": department="ICHS"
                        elif department=="63": department="LVCHS"
                        elif department=="64": department="PLUMSTEAD"
                        elif department=="65": department="SANTIAGO"
                        elif department=="66": department="TALLOAKS"
                        elif department=="67": department="TCA"
                        elif department=="68": department="TCCS"
                        elif department=="69": department="TKCA"
                        elif department=="70": department="VERITACAD"
                        elif department=="71": department="VERITAS"
                        elif department=="72": department="VERITAS PR"
                        elif department=="73": department="WESTMONT"
                        elif department=="74": department="WESTSHORE"
                        elif department=="75": department="CLARITAS"
                        elif department=="76": department="GREYSTONE"
                        elif department=="77": department="SHALOM"
                        elif department=="78": department="AGD"
                        elif department=="79": department="RLCA"
                        elif department=="89": department="Health Care Management"
                        elif department=="90": department="Marketing"
                        elif department=="91": department="Sports Management"
                        elif department=="92": department="Biology (BA)"
                        elif department=="93": department="Biology (BS)"
                        elif department=="94": department="Pre-Med Biology (BS)"
                        elif department=="95": department="Pre-Physical Therapy Biology (BS)"
                        elif department=="96": department="Pre-Physical Therapy (BA)"
                        elif department=="250": department="PALCI"
                        elif department=="100": department="Biblical Studies (AA)"
                        elif department=="101": department="Master of Divinity"
                        elif department=="103": department="MAR	Religion"
                        elif department=="104": department="Theology"
                        elif department=="105": department="Education"
                        elif department=="106": department="Educational Leadership & Administration"
                        elif department=="107": department="Applied Behavior Analysis (MA)"
                        elif department=="108": department="Instruction"
                        elif department=="109": department="Entrepreneurial Management"
                        elif department=="110": department="Organizational Development"
                        elif department=="113": department="Counseling (M.S.)"
                        elif department=="121": department="Graduate Non-Matriculating"
                        elif department=="150": department="DL Accounting (BS) Business Admin"
                        elif department=="151": department="DL Business Administration MBA"
                        elif department=="152": department="DL Computer Science MBA"
                        elif department=="153": department="Dual-Level Christian Studies - Divinity"
                        elif department=="160": department="Dual-Level Degree Completion - Counseling MS"
                        elif department=="161": department="Dual-Level Degree Completion - Religion"
                        elif department=="171": department="Dual-Level Education - English"
                        elif department=="172": department="Dual Level Education - History"
                        elif department=="173": department="Dual-Level Education - Mathematics"
                        elif department=="174": department="Dual-Level Health/PE - Education"
                        elif department=="175": department="Dual-Level Elementary/EC Education PK-4"
                        elif department=="185": department="DL Instruction - Elem/EC Education PK-4"
                        elif department=="186": department="DL Instruction - Secondary Soc Studies"
                        elif department=="187": department="DL Instruction - Health and Physical Ed"
                        elif department=="200": department="Library"
                        elif department=="201": department="General Staff"
                        elif department=="202": department="School of Liberal Arts & Sciences"
                        elif department=="203": department="School of Divinity"
                        elif department=="204": department="School of Busines"
                        elif department=="205": department="School of Music"
                        elif department=="206": department="School of Education"
                        elif department=="207": department="School of Liberal Arts & Sciences"
                        elif department=="208": department="School of Social Work"
                        elif department=="209": department="Office of the Provost"
                        elif department=="210": department="Sr. VP"
                        elif department=="211": department="Faculty Emeritus"
                        elif department=="240": department="Part-Time Audit Only"
                        elif department=="241": department="Part-Time Grad Audit Only"
                        elif department=="251": department="SEPTLA"
                        elif department=="252": department="TCLC"
                        departmentId=""
                        departmentId=mf.readJsonfileRetor(self.path_refdata,"cairn_departments.json","departments",department,"name")
                        if departmentId is not None:
                            departmentUsers.append(departmentId)
                            enduser['departments']=departmentUsers
                barcode=""
                if 'barcode' in self.users.columns:
                    if row['barcode']:
                        barcode=str(row['barcode']).strip()
                        enduser['barcode']=barcode
                if 'personal.phone' in self.users.columns:
                    if row['personal.phone']:
                        phone=str(row['personal.phone']).strip()
                
                addressesarray=[]
                addressTypeId=""
                addressLine1=""
                addressLine2=""
                addresses1=""

                if 'personal.addresses[0].addressTypeId' in self.users.columns:
                    if row['personal.addresses[0].addressTypeId']:
                        addressTypeId=str(row['personal.addresses[0].addressTypeId']).strip()
                if 'personal.addresses[0].addressLine1' in self.users.columns:
                    if row['personal.addresses[0].addressLine1']:
                        addressLine1=str(row['personal.addresses[0].addressLine1']).strip()
                if 'personal.addresses[0].addressLine2' in self.users.columns:
                    if row['personal.addresses[0].addressLine2']:
                       
                        addressLine2=str(row['personal.addresses[0].addressLine2']).strip()
                addresses1=mf.dic(addressLine1=addressLine1,addressTypeId="e08fedfb-ca20-4e0d-864c-808ccda8c726",primaryAddress=True)
                addressesarray.append(addresses1)        
                addressTypeId=""
                addressLine1=""
                addressLine2=""
                city=""
                region=""
                country=""
                
                if 'personal.addresses[1].addressTypeId' in self.users.columns:
                    if row['personal.addresses[1].addressTypeId']:
                        addressTypeId=str(row['personal.addresses[1].addressTypeId']).strip()
                if 'personal.addresses[1].addressLine1' in self.users.columns:
                    if row['personal.addresses[1].addressLine1']:
                        addressLine1=str(row['personal.addresses[1].addressLine1']).strip()
                if 'personal.addresses[1].addressLine2' in self.users.columns:
                    if row['personal.addresses[1].addressLine2']:
                        addressLine2=str(row['personal.addresses[1].addressLine2']).strip()
                if 'personal.addresses[1].city' in self.users.columns:
                    if row['personal.addresses[1].city']:
                        city=str(row['personal.addresses[1].city']).strip()
                
                addresses2=""
                postalCode=""
                country="US"
                region=""
                
                if 'personal.addresses[1].countryId' in self.users.columns:
                    if row['personal.addresses[1].countryId']:
                        country="US"
                
                if 'personal.addresses[1].postalCode' in self.users.columns:
                    if row['personal.addresses[1].postalCode']:
                        postalCode=str(row['personal.addresses[1].postalCode']).strip()
                if 'personal.addresses[1].region' in self.users.columns:
                    if row['personal.addresses[1].region']:
                        region=str(row['personal.addresses[1].region']).strip()
                if row['personal.addresses[1].addressLine1']!="":
                    addresses2=mf.dic(countryId=country,addressLine1=addressLine1,addressLine2=addressLine2,city=city,region=region,postalCode=postalCode,addressTypeId="e4c39958-73ec-46d7-afcf-0b95d44cacba")
                    addressesarray.append(addresses2)
                enduser['personal']=mf.dic(lastName=lastName,firstName=firstName,email=email,phone=phone,addresses=addressesarray,preferredContactTypeId="002")
                
                expirationDate=""
                fecha_dt=""
                if 'expirationDate' in self.users.columns:
                    if row['expirationDate']:
                        expirationDate=row['expirationDate']
                        fecha_dt=expirationDate.strftime("%Y-%m-%dT00:00:00.000+00:00")
                                                       #2021-12-31T05:00:00.000+00:00
                        print(fecha_dt)
                
                enduser['expirationDate']=fecha_dt
                
                sierraRenewalsInformation=""
                if 'sierraRenewalsInformation' in self.users.columns:
                    if row['sierraRenewalsInformation']:                        
                        sierraRenewalsInformation=str(row['sierraRenewalsInformation'])
                        
                sierraCheckoutInformation=""
                if 'sierraCheckoutInformation' in self.users.columns:
                    if row['sierraCheckoutInformation']:                        
                        sierraCheckoutInformation=str(row['sierraCheckoutInformation']).strip()
                        
                if row['sierraRenewalsInformation']!=0 and row['sierraCheckoutInformation']!=0:                        
                    enduser['customFields']=mf.dic(sierraCheckoutInformation=sierraCheckoutInformation,sierraRenewalsInformation=sierraRenewalsInformation)
                elif row['sierraRenewalsInformation']!=0:
                    enduser['customFields']=mf.dic(sierraRenewalsInformation=sierraRenewalsInformation)
                elif row['sierraCheckoutInformation']!=0:
                    enduser['customFields']=mf.dic(sierraCheckoutInformation=sierraCheckoutInformation)
                enduser['type']= "object"    
                preferredContactTypeId="002"                
                        
                #enduser['personal']=faf.dic(lastName=lastName,firstName=firstName,email=email,preferredContactTypeId="002")
                mf.printObject(enduser,self.path_logs,count,"cairn_usersbyline",False)
                allusers.append(enduser)
                enduser={}
                count+=1
            except Exception as ee:
                print(f"ERROR: {ee}")
        usuarios['users']=allusers
        mf.printObject(usuarios,self.path_results,count,"cairn_users",True)



        print(f"============REPORT======================")
        print(f"RESULTS Record processed {count}")

        print(f"RESULTS end")