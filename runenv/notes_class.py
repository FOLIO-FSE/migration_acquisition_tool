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
        self.valuetitle=""
        self.valuetypeId=""
        self.valuedomainId=""
        v=""
        typev=""
        typed=""
        with open(self.path_refdata+"\\notes_mapping.json") as json_mappingfile:
            self.mappingdata = json.load(json_mappingfile)
            for i in self.mappingdata['data']:
                if i['folio_field']=='title':
                    v=str(i['value']).strip()
                    if v is not None: 
                        self.valuetitle=v
                if i['folio_field']=='typeId':
                    typev=str(i['value']).strip()
                    if typev is not None: 
                        self.valuetypeId=typev
                if i['folio_field']=='domain':
                    typed=str(i['value']).strip()
                    if typed is not None: 
                        self.valuedomainId=typed
        return            
                        
            #print(self.mappingdata)
    #(uuidOrg,typeId,customerName,15,16,17)
    #client,self.path_dir
    def readnotes(self,client,dataframe,toSearch,linkId):
        self.notes= dataframe
        countnote=1
        noprint=False
        dfnote = self.notes[self.notes['code']== toSearch]
        print("notes founds records: ",len(dfnote))
        for i, nrow in dfnote.iterrows():
            notes={}
            l=[]
            try:
                countnote+=1

                if self.valuetypeId: 
                    notes["typeId"]=self.valuetypeId
                    cate=mf.readJsonfile(self.path_refdata,client+"_noteTypes.json","noteTypes",self.valuetypeId,"id")
                    if cate is None:
                        mf.write_file(ruta=self.path_logs+"\\notetypesNotFounds.log",contenido=f"{self.valuetypeId}")
                        noteType=""
                    else:
                        noteType=cate[1]
                        noprint=True
                        notes["id"]=str(uuid.uuid4())
                        notes['type']=noteType
                        if self.valuetitle: notes["title"]=self.valuetitle
                        else: notes["title"]="Notes"
                    
                        if self.valuedomainId: notes["domain"]=self.valuedomainId
                        else: notes["domain"]=""
                        iter=0
                        sw=True
                        cont=""
                        while sw:
                            field=f"content[{iter}]"
                            if field in dfnote.columns:
                                if nrow[field]:
                                    if nrow[field]!="":
                                        cont=cont+str(nrow[field])
                            else:
                                sw=False
                            iter+=1
                        if cont is not None:
                            notes["content"]=cont
                        l.append(mf.dic(id=linkId,type=self.valuedomainId[:-1]))
                        notes["links"]=l

                if noprint:
                    mf.printObject(notes,self.path_results,countnote,client+"_notes",False)
                else:
                    mf.printObject(notes,self.path_results,countnote,client+"worse_notes",False)
            except Exception as ee:
                print(f"ERROR: {ee}")                 
             
    def agreementReadnotes(self,client,dataframe):
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