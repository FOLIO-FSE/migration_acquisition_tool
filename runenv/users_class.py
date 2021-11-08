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

        

                
    def readusers(self,client, dataframe):
        self.readMappingfile()
        self.users= dataframe
        #print(self.users)
        enduser={}
        allusers=[]
        count=1
        usuarios={}
        for i, row in self.users.iterrows():
            try:
                start_time = time.perf_counter()
                firstName=""
                userName=""
                lastName="Error No LastName"
                email=""
                preferredContactTypeId="002"
                patronGroup=""
                departmentUsers=[]
                department=""
                printusers=True
                per={}
                patronGroupId="000000-000000-000000-00000-0000000"
                if 'patronGroup' in self.users.columns:
                    if row['patronGroup']:
                        result=self.groups(self.groups,str(row['patronGroup']).strip())
                        if result is not None:
                            patronuserId=mf.readJsonfile(self.path_refdata,client+"_usergroups.json","usergroups",result,"name")
                                if patronuserId is None:
                                    mf.write_file(ruta=self.path_logs+"\\patronusersNotFounds.log",contenido=f"{result}")
                                    printusers=False
                                else:
                                    result=str(row['patronGroup']).strip()
                                    if result is not None:
                                        patronuserId=mf.readJsonfile(self.path_refdata,client+"_usergroups.json","usergroups",result,"name")
                                        if patronuserId is None:
                                            mf.write_file(ruta=self.path_logs+"\\patronusersNotFounds.log",contenido=f"{result}")
                                            printusers=False
                    enduser['patronGroup']=patronGroupId
                    userName=""    
                    if 'username' in self.users.columns:
                        if row['username']:
                            userName=str(row['username']).strip()
                            enduser['username']=userName
                        else:
                            userName="@institution"
                    enduser['username']=userName
                    externalSystemId=""
                    if 'externalSystemId' in self.users.columns:
                        if row['externalSystemId']:
                            externalSystemId=str(row['externalSystemId']).strip()
                    enduser['externalSystemId']=externalSystemId
                    activeUser="Active"
                    if 'active' in self.users.columns:
                        if row['active']:
                            activeUser=str(row['active']).strip()
                    enduser['active']=activeUser
                    department=""
                    if 'departments' in self.users.columns:
                        if row['departments']:
                            department=str(row['departments'])
                            departmentId=mf.readJsonfileRetor(self.path_refdata,client+"_departments.json","departments",department,"name")
                            if departmentId is not None:
                                departmentUsers.append(departmentId)
                    enduser['departments']=departmentUsers
                    barcode=""
                    if 'barcode' in self.users.columns:
                        if row['barcode']:
                            barcode=str(row['barcode']).strip()
                    enduser['barcode']=barcode
                    
                    if 'personal.lastName' in self.users.columns:
                        lastName=""
                        if row['personal.lastName']:
                            lastName=str(row['personal.lastName']).strip()
                            print(f"INFO Processing user record # {count} User-Name: {lastName}")
                            per['lastName']=lastName
                            firstName=""
                            if 'personal.firstName' in self.users.columns:
                                if row['personal.firstName']:
                                    firstName=str(row['personal.firstName']).strip()
                            per['firstName']=firstName
                            email=""
                            if 'personal.email' in self.users.columns:
                                if row['personal.email']:
                                    email=str(row['personal.email']).strip()
                                else:
                                    email="@institution"
                                per['email']=email
                            if 'personal.phone' in self.users.columns:
                                if row['personal.phone']:
                                    phone=str(row['personal.phone']).strip()
                                    per['phone']=phone
                            personalpreferredFirstName=""
                            if 'personal.preferredFirstName' in self.users.columns:
                                if row['personal.preferredFirstName']:
                                    personalpreferredFirstName=row['personal.preferredFirstName']
                                    per['personal.preferredFirstName']=personalpreferredFirstName
                                    
                            addressesarray=[]
                            addressTypeId=""
                            addressLine1=""
                            addressLine2=""
                            addresses1=""
                            iter=0
                            sw=True
                            while sw:
                                addr={}
                                primaryAddress=False
                                field=f"personal.addresses[{iter}].addressTypeId"
                                if field in self.users.columns:
                                    if row[field]:
                                        addressTypeId=str(row[field]).strip()
                                        addr['addressTypeId']=addressTypeId
                                        field=f"personal.addresses[{iter}].addressLine1"
                                        if field in self.users.columns:
                                            if row[field]:
                                                addressLine1=str(row[field]).strip()
                                                addr['addressLine1']=addressLine1
                                            if iter==0:
                                                primaryAddress=True
                                        field=f"personal.addresses[{iter}].addressLine2"
                                        if field in self.users.columns:
                                            if row[field]:
                                                addressLine2=str(row[field]).strip()
                                                addr['addressLine2']=addressLine2
                                        field=f"personal.addresses[{iter}].city"
                                        if field in self.users.columns:
                                            if row[field]:
                                                city=str(row[field]).strip()                                    
                                                addr['city']=city
                                        field=f"personal.addresses[{iter}].countryId"
                                        if field in self.users.columns:
                                            if row[field]:
                                                country=str(row[field]).strip()                                    
                                                addr['country']=country
                                        field=f"personal.addresses[{iter}].postalCode"        
                                        if field in self.users.columns:
                                            if row[field]:
                                                postalCode=str(row[field]).strip()                                    
                                                addr['postalcode']=postalCode
                                        field=f"personal.addresses[{iter}].region"        
                                        if field in self.users.columns:
                                            if row[field]:
                                                region=str(row[field]).strip()                                    
                                                addr['region']=region
                                        addressesarray.append(addr)                                    
                                else:
                                    sw=False
                                iter+=1           
                            dateOfBirth=""
                            field=f"personal.dateOfBirth"        
                            if field in self.users.columns:
                                if row[field]:
                                    dateOfBirth=str(row[field]).strip()                                    
                                    per['dateOfBirth']=dateOfBirth
                            personalemail=""        
                            ##customFields
                            customFields=[]
                            customFields=self.customFields()
                            if len(customFields)>0:
                                cf={}
                                for field in customFields:
                                    if customFields in self.users.columns:
                                        cf['customFields']=row[customFields]
                                enduser['customFields']=cf
                                
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
                
                     
                        enduser['type']= "object"    
                        preferredContactTypeId="002"                
                        
                        #enduser['personal']=faf.dic(lastName=lastName,firstName=firstName,email=email,preferredContactTypeId="002")
                        mf.printObject(enduser,self.path_logs,count,client+"_usersbyline",False)
                        allusers.append(enduser)
                        enduser={}
                        count+=1
                except Exception as ee:
                    print(f"ERROR: {ee}")
                usuarios['users']=allusers
                mf.printObject(usuarios,self.path_results,count,client+"_users",True)
                print(f"============REPORT======================")
                print(f"RESULTS Record processed {count}")
                print(f"RESULTS end")
        
        
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
    
    def customFields(self):
        customlist=[]
        for i in self.mappingdata['data']:
            try:
                if i['value'] == "customFields":
                    customlist.append(i['legacy_field'])
            except Exception as ee:
                print(f"ERROR: {ee}")
        return customlist