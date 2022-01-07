import backup_restore as br
import functions_AcqErm as mf
import notes_class as notes
import datetime
import warnings
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
#from tabulate import tabulate
#import tkinter as tk
#from tkinter import filedialog, messagebox, ttk
import yaml
import shutil
        
################################
##ORDERS CLASS
################################
class compare_createdvstenant():
    def __init__(self,path_dir,client,**kwargs):
        try:
            dt = datetime.datetime.now()
            self.dt=dt.strftime('%Y%m%d-%H-%M')    
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
            #self.path_mapping_files=f"{path_dir}\\mapping_files"
            logging.basicConfig(filename=f"{self.path_logs}\\compare-{self.dt}.log", encoding='utf-8', level=logging.INFO,format='%(message)s')
            logging.basicConfig(filename=f"{self.path_logs}\\compare-DEBUG-{self.dt}.log", encoding='utf-8', level=logging.DEBUG,format='%(message)s')
            print("INFO reading json")
            if 'json_file' in kwargs:
                json_file=kwargs['json_file']
                with open(json_file, "r", encoding="utf") as file_j:
                    for linea in file_j:
                        recitem=linea
                        recitem=recitem.replace(",\n", "")
                        data = json.loads(recitem)
                        barcode=str(data['barcode'])
                        newid=str(uuid.uuid4())
                        data['id']=newid
        except Exception as ee:
            print(f"ERROR: GET TITLE {ee}")
            logging.info(f"ERROR: GET TITLE {ee}")            

    def get_title(self,client,**kwargs):
        try:
            pathPattern1=mf.okapiPath(kwargs['element'])
            element=kwargs['element']
            pathPattern=pathPattern1[0]
            searchValue=kwargs['searchValue']
            client=mf.SearchClient(client)
            okapi_url=str(client.get('x_okapi_url'))
            okapi_tenant=str(client.get('x_okapi_tenant'))
            okapi_token=str(client.get('x_okapi_token'))
            dic={}
            #pathPattern="/instance-storage/instances" #?limit=9999&query=code="
            #https://okapi-ua.folio.ebsco.com/instance-storage/instances?query=hrid=="264227"
            pathPattern="/instance-storage/instances" #?limit=9999&query=code="
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            length="1"
            start="1"
            #element="instances"
            #https://okapi-trinitycollegelibrarycambridge.folio.ebsco.com/instance-storage/instances?query=(identifiers any ".b10290242")
            query=f"?query=(identifiers="
            #query=f"query=hrid=="
            #/finance/funds?query=name==UMPROQ
            search='"'+searchValue+'")'
            #.b10290242
            #paging_q = f"?{query}"+search
            paging_q = f"{query} "+search
            path = pathPattern+paging_q
            #data=json.dumps(payload)
            url = okapi_url + path
            req = requests.get(url, headers=okapi_headers)
            idhrid=[]
            #print(req.text)
            if req.status_code != 201:
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                if (total_recs!=0):
                    rec=json_str[element]
                    #print(rec)
                    l=rec[0]
                    if 'id' in l:
                        idhrid.append(l['id'])
                        idhrid.append(l['title'])
                
            if len(idhrid)==0:
                return None
            elif len(idhrid)>0:
                return idhrid            
            else:
                return None
        except Exception as ee:
            print(f"ERROR: GET TITLE {ee}")
            logging.info(f"ERROR: GET TITLE {ee}")    
            
            
    def createinstance(self, client,titleOrPackage,titleUUID,linkId,poNumbertitle):
        try:
            idinstance=""
            self.createinstanid=True
            print(f"INFO Creating instance for title {titleOrPackage} {titleUUID}")
            idinstance=str(uuid.uuid4())
            instance= {
                                        "id": idinstance,
                                        "_version": 1,
                                        "source": "FOLIO",
                                        "title": titleOrPackage,
                                        "alternativeTitles": [],
                                        "editions": [],
                                        "series": [],
                                        "identifiers": [
                                            {"value": str(titleUUID),
                                            "identifierTypeId": "5e1c71c5-c4ce-4585-a057-88eb3675f353"}
                                            ],
                                        "contributors": [],
                                        "subjects": [],
                                        "classifications": [],
                                        "publication": [],
                                        "publicationFrequency": [],
                                        "publicationRange": [],
                                        "electronicAccess": [],
                                        "instanceTypeId": "30fffe0e-e985-4144-b2e2-1e8179bdb41f",
                                        "instanceFormatIds": [],
                                        "instanceFormats": [],
                                        "physicalDescriptions": [],
                                        "languages": [],
                                        "notes": [],
                                        "discoverySuppress": False,
                                        "statisticalCodeIds": [],
                                        "statusId": "26f5208e-110a-4394-be29-1569a8c84a65",
                                        "tags": {"tagList": []},
                                        "holdingsRecords2": [],
                                        "natureOfContentTermIds": []
                                        }
            
            logging.info(f"INFO: Title created {titleOrPackage} | {idinstance}") 
            mf.printObject(instance,self.path_results,0,f"{client}_instances_{self.dt}",False)
            self.createordertitle(client,idinstance,titleOrPackage,linkId,poNumbertitle)
            return idinstance
        except Exception as ee:
            print(f"ERROR: GET TITLE {ee}")
            

if __name__ == "__main__":
    path="C:\Users\asoto\code\migration_acquisition_tool\client_data"
    file="michstate_prod_purchaseOrderbyline_with_new_instance_20220104-10-02_modified.json"
    client="michstate_prod"
    client.compare_createdvstenant(path, client,json_file=file)