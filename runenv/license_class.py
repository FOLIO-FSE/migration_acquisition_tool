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
##ORDERS CLASS
################################

class licenses():
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
            with open(self.path_refdata+"\\license_mapping.json") as json_mappingfile:
                self.mappingdata = json.load(json_mappingfile)
        except Exception as ee:
            print(f"ERROR: license Class {ee}")

    def readMappingfile(self):
        self.customerName=pd.dataframe()
        filetoload=self.path_refdata+f"\\acquisitionMapping.xlsx"
        print("INFO Reading Mapping file")
        self.acquisitionMethod=self.customerName.importDataFrame(filetoload,sheetName="acquisitionMethod", dfname="Acquisition Method")

    def readlicenses(self, client, **kwargs):
        self.client=client
        self.readMappingfile()
        self.count=0
        countpol=0
        countlicerror=0
        if 'dflicenses' in kwargs:      
            self.licenses=kwargs['dflicenses']
            if 'dfnotes' in kwargs:
                self.dfnotes=kwargs['dfnotes']
                #print(dfnotes)
                self.customerName=notes.notes(client,self.path_dir,dataframe=self.dfnotes)
                self.swnotes=True
            else:
                self.swnotes=False        
            #poLines=dfPolines        
            noprint=True
            dt = datetime.now()
            self.dt=dt.strftime('%Y%m%d-%H-%M')
            for i, row in self.licenses.iterrows():
                try:
                    pass
                
                    if self.swnotes:      #dataframe,toSearch,linkId):
                        self.customerName.readnotes(client,dataframe=self.dfnotes,toSearch=code,linkId=linkId)
                        
                except Exception as ee:
                    print(f"ERROR: license Class {ee}")