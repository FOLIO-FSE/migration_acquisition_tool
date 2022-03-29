import backup_restore as br
import migration_report as mr
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
##import tkinter as tk
##from tkinter import filedialog, messagebox, ttk
import time
import yaml
import shutil
import datetime
        
################################
##ORDERS CLASS
################################

class licenses():
    def __init__(self,client,path_dir):
        try:
            self.migrationreport_a=mr.MigrationReport()
            self.migrationreport_a.add_general_statistics("License Report")
            self.customerName=client
            self.customerName=pd.dataframe()    
            self.getidfile=False
            dt = datetime.datetime.now()
            self.dt=dt.strftime('%Y%m%d-%H-%M')
            self.path_dir=path_dir
            #os.mkdir(f"{path_dir}/results")
            self.path_results=f"{path_dir}/results"
            #os.mkdir(f"{path_dir}/data")
            self.path_data=f"{path_dir}/data"
            #os.mkdir(f"{path_dir}/logs")
            self.path_logs=f"{path_dir}/logs"
            #os.mkdir(f"{path_dir}/refdata")
            self.path_refdata=f"{path_dir}/refdata"
            self.path_mapping_files=f"{path_dir}/mapping_files"
            logging.basicConfig(filename=f"{self.path_logs}/licenses-{self.dt}.log", encoding='utf-8', level=logging.INFO,format='%(message)s')
            mappingfile=self.path_mapping_files+"/license_mapping.json"
            if os.path.exists(mappingfile):  
                with open(mappingfile) as json_mappingfile:
                    self.mappingdata = json.load(json_mappingfile)
        except Exception as ee:
            print(f"ERROR: license Class {ee}")                    
                    
    def readMappingfile(self):
        try:
            self.flag=True
            filetoload=self.path_mapping_files+f"/acquisitionMapping.xlsx"            
            if os.path.exists(filetoload):
                myobj = datetime.datetime.now()
                self.dobj=myobj.strftime('%T')
                logging.info(f"{self.dobj} INFO Acquisition Mapping spreadsheet OK")
                print(f"{self.dobj} INFO Acquisition Mapping spreadsheet found")
                print(f"{self.dobj} INFO Reading Mapping {filetoload}")
                logging.info(f"{self.dobj} INFO Reading Mapping {filetoload}")
                #self.customerName.importDataFrame(filetoload,sheetName="acquisitionMethod", dfname="Acquisition Method")
                self.dforg=self.customerName.importupla(tupla=mf.jsontotupla(json_file=self.path_refdata+f"/{self.client}_organizations.json",schema="organizations"),dfname="Organizations",columns=["id", "code","name","value","json"])
                #self.custprops=self.customerName.importupla(tupla=mf.jsontotupla(json_file=self.path_refdata+f"/{self.client}_licenses_custprops.json"),dfname="License custprops",columns=["id", "code","name","value","json"])
                #self.refdata=self.customerName.importupla(tupla=mf.jsontotupla(json_file=self.path_refdata+f"/{self.client}_licenses_refdata.json"),dfname="License refdata",columns=["id", "code","name","value","json"])
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
        except Exception as ee:
            print(f"ERROR: license Class {ee}")
            
        

    def readlicenses(self, client, **kwargs):
        self.client=client
        self.readMappingfile()
        self.count=0
        countpol=0
        countlicerror=0
        license={}
        self.flag=self.readMappingfile()
        totaltime=0
        dic={}
        org=[]
        if self.flag:
            if 'dflicenses' in kwargs:      
                self.licenses=kwargs['dflicenses']
                if 'dfnotes' in kwargs:
                    self.dfnotes=kwargs['dfnotes']
                    #print(dfnotes)
                    self.customerName=notes.notes(client,self.path_dir,dataframe=self.dfnotes)
                    self.swnotes=True
                else:
                    self.swnotes=False        
                self.totalrows=len(self.licenses)
                noprint=True
                dt = datetime.datetime.now()
                nametodisplay=""
                self.dt=dt.strftime('%Y%m%d-%H-%M')
                for i, row in self.licenses.iterrows():
                    try:
                        self.count+=1
                        tini = time.perf_counter()
                        if 'id' in self.licenses.columns: licId=row[field]
                        else:licId=str(uuid.uuid4())
                        license['id']=licId
                        
                        for namecol in self.licenses.columns:
                            field=namecol
                            #print(row[field])
                            if field in self.licenses.columns: 
                                if row[field]:
                                    licId=str(row[field]).strip()#str(uuid.uuid4())
                                    if field=="name":
                                        nametodisplay=row[field]
                                    if field=="orgs[0]":
                                        licId=self.license_organizations(row[field],customer=f"{self.customerName}")
                                        field="orgs"
                            tend = time.perf_counter()
                            totaltime=round((tend - tini))
                            if field!="LicenseOrg.Role[0]":
                                license[field]=licId
                        '''if self.swnotes:      #dataframe,toSearch,linkId):
                            self.customerName.readnotes(client,dataframe=self.dfnotes,toSearch=code,linkId=linkId)'''
                        
                        mf.printObject(license,self.path_results,self.count,f"{client}_licenses_{self.dt}",False)
                        print(f"{self.dobj} RECORD # {self.count}/{self.totalrows} created | License Name:{nametodisplay} | (Time:{totaltime} sec.)") 
                    except Exception as ee:
                        print(f"ERROR: license Class {ee}")

    def license_organizations(self,dfRow,customer):
        try:
            dic={}
            licenseOrg={}
            orgs=[]
           #print("argumentos de *argv:", row[arg])
            if len(dfRow)>0:
                toSearch=str(dfRow)
                toSearchnewvalue=toSearch.replace("&"," ")
                provider=self.searchdata_dataframe(dftosearchtemp=self.dforg,fieldtosearch="name",fieldtoreturn1="name",fieldtoreturn="id",toSearch=toSearchnewvalue)
                if provider:
                    licenseOrg["orgsUuid"]=provider[0]
                    licenseOrg["name"]=provider[1]
                    #"orgs": [{"id": "", "org": {"id": "", "orgsUuid": "19fdabf3-b73a-4da1-bb5b-25e4aa737ab7", "name": "Proquest Info Learning Co"}, "role": {"id": "", "value": "licensor", "label": "Licensor"}}
                else:
                    #Define undefined provider
                    licenseOrg["orgsUuid"]="1e94d6db-e89d-43fe-afe5-09a2f5a24866"
                    licenseOrg["name"]="undefined"
                dic["id"]=""
                dic["org"]=licenseOrg
                dic["note"]=""
                dic["role"]= {"value": "licensee", "label": "Licensee"}
                orgs.append(dic)
            return orgs
        except Exception as ee:
            print(f"ERROR: license Class {ee}")


    def searchdata_dataframe(self,**kwargs): #dftosearchtemp,fieldtosearch,fieldtoreturn,toSearch):
        try:
            dataToreturn=[]
            #print(dftosearchtemp)
            fieldtosearch=kwargs['fieldtosearch']
            fieldtoreturn=kwargs['fieldtoreturn']
            fieldtoreturn1=kwargs['fieldtoreturn1']
            toSearch=kwargs['toSearch']
            dftosearchtemp=kwargs['dftosearchtemp']
            tempo = dftosearchtemp[dftosearchtemp[fieldtosearch]== toSearch]
            #fieldtoreturn=fieldtoreturn
            #print("Mapping found: ",len(temp))
            if len(tempo)>0:
                for x, cptemp in tempo.iterrows():
                    if fieldtoreturn1 is not None:
                        dataToreturn.append(cptemp[fieldtoreturn])
                        dataToreturn.append(cptemp[fieldtoreturn1])
                    else:
                        dataToreturn.append(cptemp[fieldtoreturn])
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
