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
################################################
### NOTES
################################################
# 
class notes():
    def __init__(self,client,path_dir):
        
        self.customerName=client
        #os.mkdir(f"{path_dir}\\results")
        self.path_results=f"{path_dir}\\results"
        #os.mkdir(f"{path_dir}\\data")
        self.path_data=f"{path_dir}\\data"
        #os.mkdir(f"{path_dir}\\logs")
        self.path_logs=f"{path_dir}\\logs"
        #os.mkdir(f"{path_dir}\\refdata")
        self.path_refdata=f"{path_dir}\\refdata"
    #(uuidOrg,typeId,customerName,15,16,17)
    #client,self.path_dir
    def readnotes(self,client,dataframe):
         self.notes= dataframe
         
         count=1
         for i, row in self.notes.iterrows():
            notes={}
            try:
                print(f"Record No. {count}")
                agreetosearch=""
                noprint=False
                idtolink=""
                agreetosearch=str(row['name']).strip()
                nameid=mf.readJsonfile(self.path_refdata,client+"_agreements.json","agreements",agreetosearch,"name")
                cont="<p>"
                l=[]
                if nameid is not None:
                    noprint=True
                    idtolink=str(nameid[0]).strip()
                    if 'content[0]' in self.notes.columns:
                        if row['content[0]']:
                            cont=cont+"Type: "+str(row['content[0]']).strip()+"</p>"
                    if 'content[1]' in self.notes.columns:
                        if row['content[1]']:
                            if cont!="":
                                cont=cont+"<p>Format: "+str(row['content[1]']).strip()+"</p>"
                            else:
                                cont=cont+"Format: "+str(row['content[1]']).strip()+"</p>"
                else:
                    cont=agreetosearch
                notes["id"]=str(uuid.uuid4())
                notes["typeId"]=str("d2f44fe0-3709-4616-911e-8d8773ace0c0")
                notes["type"]="Agreements note"
                notes["domain"]="agreements"
                notes["title"]="Coral Notes"
                notes["content"]=cont
                l.append(mf.dic(id=idtolink,type="agreement"))
                notes["links"]=l
                # self.print_notes(self,typeId="d2f44fe0-3709-4616-911e-8d8773ace0c0",type="Agreements note",domain="agreements",title="Notes",content=cont,linkId=[{"id": id,"type": "agreement"}])        
                count+=1
                if noprint:
                    mf.printObject(notes,self.path_results,count,self.customerName+"_notes",False)
                else:
                    mf.printObject(notes,self.path_results,count,self.customerName+"_notes_worse",False)
            except Exception as ee:
                print(f"ERROR: {ee}")
            
                            
    def print_notes(self,*kwargs):
        count=1
        try:
            notes={}
            notes["typeId"]= kwargs['typeId']
            notes["type"]= kwargs['type']
            notes["domain"]= kwargs['domain']
            notes["title"]= kwargs['title']
            notes["content"]= kwargs['cont']
            notes["links"]= kwargs['linkId']# [{"id": linkId,"type": typelinkId}]
            x=0
            #print(notes)
            mf.printObject(notes,self.path_results,count,self.customerName+"_notes",False)
            
        except Exception as ee:
            print(f"ERROR: {ee}")
            
       
######END NOTES