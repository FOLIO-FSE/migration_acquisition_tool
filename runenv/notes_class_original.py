import datetime
import warnings
import datetime
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
import functions_AcqErm as mf

################################################
### NOTES
################################################
# 
class notes():
    def __init__(self,client,path_dir, **kwargs):
        if 'dataframe' in kwargs:
            self.notes= kwargs['dataframe']
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
            mappingfile=self.path_refdata+"\\notes_mapping.json"
            if os.path.exists(mappingfile):  
                with open(mappingfile) as json_mappingfile:
                    self.mappingdata = json.load(json_mappingfile)
            else:
                print("INFO Notes Script: include: {self.path_refdata}\\notes_mapping.json file")
                flag=False
            
            return          
                        
            #print(self.mappingdata)
    #(uuidOrg,typeId,customerName,15,16,17)
    #client,self.path_dir
    def readnotes(self,client,**kwargs): #dataframe,toSearch,linkId):
        self.totalnotes=0
        if 'toSearch' in kwargs:
            toSearch=str(kwargs['toSearch']).strip()
        if 'linkId' in kwargs:
            linkId=str(kwargs['linkId']).strip()
        if 'filenamenotes' in kwargs:
            filenamenotes=str(kwargs['filenamenotes']).strip()
        else:
            filenamenotes="_notes"
        countnote=1
        noprint=False
        #print(self.notes['code'])
        dfnote = self.notes[self.notes['code']== str(toSearch)]
        self.totalnotes=len(dfnote)
        #print(f"INFO Notes for :{toSearch} =>{totalnotes} records")
        field="typeId"
        self.valuetypeId=""
        valuenote=self.readmappingjson(folio_field=field)
        if valuenote:
            self.valuetypeId=valuenote.get("value")
        
        field="domain"
        valuenote=self.readmappingjson(folio_field=field)
        if valuenote:
            self.valuedomainId=valuenote.get("value")
        returnNote=[]
        for i, nrow in dfnote.iterrows():
            notes={}
            l=[]
            linkType=""
            cont=""
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
                        noprint=False
                        notes["id"]=str(uuid.uuid4())
                        notes['type']=noteType
                        if self.valuetitle: notes["title"]=self.valuetitle
                        else: notes["title"]="Notes"
                        if self.valuedomainId: 
                            notes["domain"]=self.valuedomainId
                            if self.valuedomainId.upper()=="ORGANIZATIONS":
                                linkType="organization"
                            elif self.valuedomainId.upper()=="ORDERS":
                                linkType="poLine"
                            elif self.valuedomainId.upper()=="LICENSES":
                                linkType="license"
                            elif self.valuedomainId.upper()=="AGREEMENTS":
                                linkType="agreement"
                            else:
                                linkType="error"
                                return 
                        else: 
                            notes["domain"]=""
                        iter=0
                        sw=True
                        cont=""
                        while sw:
                            field=f"content[{iter}]"
                            info=""
                            if field in dfnote.columns:
                                if nrow[field]:
                                    if nrow[field]!="":
                                        da=str(nrow[field]).strip()
                                        if da!="-  -":
                                            notecaption=""
                                            info=da
                                            if type(info) is datetime.date:
                                                info=info.strftime("%Y-%m-%d")
                                            elif type(info) is str:
                                                info=info.replace(".0","")
                                                info=str(info).strip()
                                            elif type(info) is int:
                                                info=str(info).strip()
                                                info=info.replace(".0","")
                                            valuenote=self.readmappingjson(folio_field=field)
                                            if valuenote:
                                                notecaption=valuenote.get("legacy_field")
                                            info=str(info).strip()
                                            cont=cont+f"{notecaption}: {info} "
                            else:
                                sw=False
                            iter+=1
                        if cont is not None:
                            if cont!="":
                                notes["content"]=cont
                                noprint=True
                        l.append(mf.dic(id=linkId,type=linkType))
                        notes["links"]=l
                if noprint:
                    mf.printObject(notes,self.path_results,countnote,client+f"_{filenamenotes}",False)
                else:
                    mf.printObject(notes,self.path_results,countnote,client+f"worse_notes_without_content",False)
            except Exception as ee:
                print(f"ERROR: Notes Class {ee}")      
        return self.totalnotes
               
                
    def readmappingjson(self, **kwargs):
        try:
            valuesfield={}
            mapfile={}
            fieldToserch=str(kwargs['folio_field'])
            for i in self.mappingdata['data']:
                    if i['folio_field']==fieldToserch:
                            valuesfield['value']=str(i['value']).strip()
                            valuesfield['description']=str(i['description']).strip()
                            valuesfield['legacy_field']=str(i['legacy_field']).strip()
            return valuesfield
        except Exception as ee:
            print(f"ERROR: Orders Class {ee}")

    def agreementReadnotes(self,client,dataframe):
         self.notes= dataframe
         
         count=1
         for i, row in self.notes.iterrows():
            notes={}
            try:
                print(f"INFO Record No. {count}")
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
            
    def readmapping(self,toSearch):
        with open(self.path_refdata+"\\notes_mapping.json") as json_mappingfile:
            self.mappingdata = json.load(json_mappingfile)
            for i in self.mappingdata['data']:
                if i['folio_field']==toSearch:
                    contentNote=str(i['description']).strip()+" "
        return contentNote             
                    
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