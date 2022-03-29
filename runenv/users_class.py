import dataframe_class as pd
import functions_AcqErm as mf
import datetime
import warnings
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
#import tkinter as tk
#from tkinter import filedialog, messagebox, ttk
import yaml
import shutil

################################
##USERS FUNCTION
################################
class users():
    def __init__(self,client,path_dir):
        try:    
            self.customerName=client
            self.path_results=f"{path_dir}/results"
            self.path_data=f"{path_dir}/data"
            self.path_logs=f"{path_dir}/logs"
            self.path_refdata=f"{path_dir}/refdata"
            #self.userbyline=open(f"{self.path_logs}/{self.customerName}_usersbyline.json", 'w') 
        except Exception as ee:
            print(f"ERROR: {ee}")
            
    def readMappingfile(self):
        self.customerName=pd.dataframe()
        filetoload=self.path_refdata+f"/userMapping.xlsx"
        print("INFO Reading mapping file")
        self.groups=self.customerName.importDataFrame(filetoload,sheetName="groups")
        print(self.groups)
        self.departments=self.customerName.importDataFrame(filetoload,sheetName="departments")
        self.userStatus=self.customerName.importDataFrame(filetoload,sheetName="userStatus")
        self.addressType=self.customerName.importDataFrame(filetoload,sheetName="addressType")
        with open(self.path_refdata+"/users_mapping.json") as json_mappingfile:
            self.mappingdata = json.load(json_mappingfile)
        

        

                
    def readusers(self,client, **kwargs):
        self.readMappingfile()
        users=  kwargs['dfusers']
        #print(self.users)
        barcodelist=[]
        allusers=[]
        count=1
        usuarios={}
        goodusers=0
        baduser=0
        for i, row in users.iterrows():
            try:
                start_time = time.perf_counter()
                enduser={}
                worseusers={}
                firstName=""
                userName=""
                lastName="Error No LastName"
                email=""
                preferredContactTypeId="002"
                patronGroup=""
                departmentUsers=[]
                department=""
                printusers=True
                patronGroupId="000000-000000-000000-00000-0000000"
                if 'patronGroup' in users.columns:
                    if row['patronGroup']:
                        result=str(row['patronGroup']).strip()
                        patronuserId=mf.readJsonfile(self.path_refdata,client+"_usergroups.json","usergroups",result,"group")
                        if patronuserId is None:
                            mf.write_file(ruta=self.path_logs+"/patronusersNotFounds.log",contenido=f"{result}")
                            printusers=False
                        else:
                            patronGroupId=str(patronuserId[0])
                        
                    enduser['type']="Patron"
                    if patronGroupId=="000000-000000-000000-00000-0000000":
                        printusers=False
                        patronGroupId=result
                    enduser['patronGroup']=patronGroupId
                    enduser['proxyFor']=[]
                    userBarcode=""
                    if 'barcode' in users.columns:
                        if row['barcode']:
                            checkbarcode=str(row['barcode']).strip()
                            countlist = barcodelist.count(str(checkbarcode))
                            if countlist>0:
                                printusers=False
                            else:
                                userBarcode=checkbarcode                        
                                barcodelist.append(userBarcode)
                                enduser['barcode']=userBarcode

                    userName=""    
                    if 'username' in users.columns:
                        if row['username']:
                            userName=str(row['username']).strip()
                        else:
                            userName=userBarcode
                            
                    enduser['username']=userName
                    externalSystemId=""
                    if 'externalSystemId' in users.columns:
                        if row['externalSystemId']:
                            externalSystemId=str(row['externalSystemId']).strip()
                    enduser['externalSystemId']=externalSystemId
                    activeUser=True
                    if 'active' in users.columns:
                        if row['active']:
                            result=self.mapping(self.userStatus,str(row['active']).strip())
                            if result is not None:
                                activeUser=result
                                
                    enduser['active']=activeUser
                    departmentUsers=[]
                    if 'departments' in users.columns:
                        if row['departments']:
                            department=str(row['departments']).strip()
                            departmentId=mf.readJsonfileRetor(self.path_refdata,client+"_departments.json","departments",department,"name")
                            #C:\Users\asoto\Documents\EBSCO\Migrations\folio\client_data\uai\refdata
                            if departmentId is not None:
                                departmentUsers.append(departmentId)
                    enduser['departments']=departmentUsers
                    per={}
                    if 'personal.lastName' in users.columns:
                        lastName=""
                        if row['personal.lastName']:
                            print(row['personal.lastName'])
                            lastName=str(row['personal.lastName']).strip()
                            print(f"INFO Processing user record # {count} User-LastName: {lastName}")
                            per['lastName']=lastName
                            firstName=""
                            if 'personal.firstName' in users.columns:
                                if row['personal.firstName']:
                                    firstName=str(row['personal.firstName']).strip()
                            per['firstName']=firstName
                            email=""
                            if 'personal.email' in users.columns:
                                if row['personal.email']:
                                    email=str(row['personal.email']).strip()
                                else:
                                    email="biblioteca@uai.cl"
                                per['email']=email
                            phone=""
                            if 'personal.phone' in users.columns:
                                if row['personal.phone']:
                                    phone=str(row['personal.phone']).strip()
                                    per['phone']=phone
                            personalpreferredFirstName=""
                            if 'personal.preferredFirstName' in users.columns:
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
                                ptype=""
                                result="HOME"
                                field=f"personal.addresses[{iter}].addressTypeId"
                                if field in users.columns:
                                    if row[field]:
                                        result=str(row[field]).strip()
                                        ptype=mf.readJsonfile(self.path_refdata,client+"_usergroups.json","usergroups",result,"group")
                                        if ptype is None:
                                            mf.write_file(ruta=self.path_logs+"/patronusersNotFounds.log",contenido=f"{result}")
                                            printusers=False
                                        else:
                                            patronGroupId=ptype
                                    else:        
                                        
                                        addr['addressTypeId']=addressTypeId
                                        field=f"personal.addresses[{iter}].addressLine1"
                                        if field in users.columns:
                                            if row[field]:
                                                addressLine1=str(row[field]).strip()
                                                addr['addressLine1']=addressLine1
                                            if iter==0:
                                                primaryAddress=True
                                        field=f"personal.addresses[{iter}].addressLine2"
                                        if field in users.columns:
                                            if row[field]:
                                                addressLine2=str(row[field]).strip()
                                                addr['addressLine2']=addressLine2
                                        field=f"personal.addresses[{iter}].city"
                                        if field in users.columns:
                                            if row[field]:
                                                city=str(row[field]).strip()                                    
                                                addr['city']=city
                                        field=f"personal.addresses[{iter}].countryId"
                                        if field in users.columns:
                                            if row[field]:
                                                country=str(row[field]).strip()                                    
                                                addr['country']=country
                                        field=f"personal.addresses[{iter}].postalCode"        
                                        if field in users.columns:
                                            if row[field]:
                                                postalCode=str(row[field]).strip()                                    
                                                addr['postalcode']=postalCode
                                        field=f"personal.addresses[{iter}].region"        
                                        if field in users.columns:
                                            if row[field]:
                                                region=str(row[field]).strip()                                    
                                                addr['region']=region
                                        addressesarray.append(addr)                                    
                                else:
                                    sw=False
                                iter+=1          
                                per['addresses']= addressesarray
                            dateOfBirth=""
                            fecha_dt=""
                            field=f"personal.dateOfBirth"   
                                 
                            if field in users.columns:
                                if row[field]:
                                    fecha_dt=str(row[field])
                                    #fecha_dt=fecha_dt.replace("/","-")
                                    year=fecha_dt[-4:]
                                    month=fecha_dt[3:5]
                                    day=fecha_dt[:2]
                                    dateOfBirth=f"{year}-{month}-{day}T00:00:00.000+00:00"                           
                                    per['dateOfBirth']=dateOfBirth
                            personalemail=""        
                            ##customFields
                            per['preferredContactTypeId']="002"
                            enduser['personal']=per
                            enrollmentDate=""
                            if 'enrollmentDate' in users.columns:
                                if row['enrollmentDate']:
                                    dateenrollment=row['enrollmentDate']
                                    enrollmentDate=dateenrollment.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
                                    enduser['enrollmentDate']=enrollmentDate
                            expirationDate=""
                            customFields=[]
                            customFieldslist=self.customFields()
                            if len(customFieldslist)>0:
                                cf={}
                                for field in customFieldslist:
                                    if field in users.columns:
                                        if row[field]:
                                            infocustom=str(row[field]).strip()
                                            infocustom=infocustom.replace(".0","")
                                            cf[field]=infocustom
                                enduser['customFields']=cf
                            
                            fecha_dt=""
                            if 'expirationDate' in users.columns:
                                if row['expirationDate']:
                                    expirationDate=row['expirationDate']
                                    fecha_dt=expirationDate.strftime("%Y-%m-%dT00:00:00.000+00:00")
                                                       #2021-12-31T05:00:00.000+00:00
                                    print(fecha_dt)
                
                                enduser['expirationDate']=fecha_dt
                        enduser['type']= "object"
                        worseusers=enduser
                        if printusers:
                            mf.printObject(enduser,self.path_results,count,client+"_usersbyline",False)
                            allusers.append(enduser)
                            goodusers+=1
                        else:
                            mf.printObject(worseusers,self.path_results,count,client+"worse_usersbyline",False)
                            baduser+=1
                        printusers=True                        
                        enduser={}
                        count+=1
            except Exception as ee:
                print(f"ERROR: {ee}")
        usuarios['users']=allusers
        mf.printObject(usuarios,self.path_results,count,client+"_users",True)
        print(f"============REPORT======================")
        print(f"RESULTS Record processed {count}")
        print(f"RESULTS Record processed {goodusers}")
        print(f"RESULTS bad records processed {baduser}")
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
                mf.write_file(ruta=self.path_logs+"/workflowNotfound.log",contenido=f"{toSearch}")
                dataToreturn=None
            return dataToreturn
        
        except Exception as ee:
            print(f"ERROR: {ee}")
    
    def customFields(self):
        customlist=[]
        for i in self.mappingdata['data']:
            try:
                if i['value'] == "customFields":
                    customlist.append(i['folio_field'])
            except Exception as ee:
                print(f"ERROR: {ee}")
        return customlist