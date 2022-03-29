import dataframe_class as pd
import agreement_class as agree
import users_class as users
import notes_class as appnotes
import compositePurchaseorders_class as orders
import organizations_class as org
import license_class as lic
import windows_class as windows
import time
import datetime
import warnings
from datetime import datetime
from datetime import date
import json
import uuid
import os
import os.path
import requests
import io
import math
import csv
import random
import logging
import validator
import ast
import yaml
import shutil
import codecs
from pathlib import Path, PureWindowsPath

    
def GetprintObject(objectToPrint,path,x,file_name,prettyJson):
        try:
            outfilename=""
            #toPrint=json_validator(objectToPrint)
            if prettyJson:
                path_file=path_file=f"{path}/{file_name}.json"
                #outfilename = json.load(objectToPrint)
                with codecs.open(path_file,"w+", encoding="utf-8") as outfile:
                    json.dump(objectToPrint,outfile,indent=2,ensure_ascii=False)
            else:
                path_file=path_file=f"{path}/{file_name}.json"
                outfilename = json.dumps(objectToPrint,ensure_ascii=False)
                with codecs.open(path_file,"a+", encoding="utf-8") as outfile:
                    outfile.write(outfilename+"\n")
            return None
        except Exception as ee:
            print(f"ERROR: {ee}")

#function to print json files            
def printObject(objectToPrint,path,x,file_name,prettyJson):
        try:
            outfilename=""
            #toPrint=json_validator(objectToPrint)
            if prettyJson:
                path_file=path_file=f"{path}/{file_name}.json"
                #outfilename = json.load(objectToPrint)
                with codecs.open(path_file,"w+", encoding="utf-8") as outfile:
                    json.dump(objectToPrint,outfile,indent=2,ensure_ascii=False)
            else:
                path_file=path_file=f"{path}/{file_name}.json"
                outfilename = json.dumps(objectToPrint,ensure_ascii=False)
                with codecs.open(path_file,"a+", encoding="utf-8") as outfile:
                    outfile.write(outfilename+"\n")
            return None
        except Exception as ee:
            print(f"ERROR: (printObject function) {ee}")
                
def SearchClient(code_search):
        # Opening JSON file
        dic= {}
        pathfile=os.path.dirname(os.path.realpath(__file__))
        f = open(os.path.join(pathfile, "okapi_customers.json"),"r", encoding="utf-8")
        data = json.load(f)
        #print("INFO reading OKAPI DATA from okapi_customer.json file")
        for i in data['okapi']:
            try:
                a_line=str(i)
                if i['name'] == code_search:
                #if (a_line.find(code_search) !=-1):
                    dic=i
                    del dic['name']
                    #del dic['user']
                    #del dic['password']
                    del dic['x_okapi_version']
                    del dic['x_okapi_status']
                    del dic['x_okapi_release']
                    break
                f.close()
            except ValueError as error:
                print(f"Error Search Okapi: {error}")
        return dic
    
def get_one_schema(code_search):
    valor=[]
    try:
        pathfile=os.path.dirname(os.path.realpath(__file__))
        f = open(os.path.join(pathfile, "setting_data.json"),"r", encoding="utf-8")
        data = json.load(f)
        for i in data['settings']:
            a_line=str(i)
            if i['name'] == code_search:
            #if (a_line.find(code_search) !=-1):
                valor.append(i['pathPattern'])
                valor.append(i['name'])
                break
        f.close()
        return valor
    except ValueError:
        print("schema does not found")
        return 0
    
def make_get(Pattern,okapi_url, okapi_tenant, okapi_token,queryString,json_file,refdatapath):
    try:
        dt = datetime.now()
        dt=dt.strftime('%d_%m_%Y_%H_%M')
        pathPattern=Pattern
        okapi_url=okapi_url
        json_file=json_file
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        #username="folio"
        #password="Madison"
        #payload = {'username': username, 'password': password}
        length="99999"
        #typein="General note Orders"
        ##fc="&metadata.createdByUserId='2bd750b9-1362-4807-bd73-2be9d8d63436'"
        start="0"
        if queryString!="":
            #paging_q = f"?limit={length}#&offset={start}"
            paging_q = f"?limit={length}&query={queryString}" # f"/notes?query=type=="General note Orders""
           #paging_q = f"?limit={length}&query=type=={typein}"
           #paging_q = f"?limit={length}&domain=orders"
        else:
            paging_q = f"?limit={length}"
            path = pathPattern+paging_q
            #data=json.dumps(payload)
            url = okapi_url + path
            req = requests.get(url, headers=okapi_headers,timeout=40)
    except requests.ConnectionError as e:
           print("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
           print(str(e))            
    except requests.Timeout as e:
           print("OOPS!! Timeout Error")
           print(str(e))
    except requests.RequestException as e:
           print("OOPS!! General Error")
           print(str(e))
    except KeyboardInterrupt:
              print("Someone closed the program")
    else:
        if req.status_code != 201:
            #print(req.encoding)
            #print(req.text)
            #print(req.headers)
            if req.status_code==200:
                #print(f"INFO Downloading schema: {Pattern} for {url} = {req}")
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                printObject(json_str,refdatapath,0,f"{json_file}",True)
                #archivo=open(json_file, 'w',encoding='utf8')
                #
                   #total_recs = int(json_str["totalRecords"])
                   #archivo.write(json.dumps(json_str, indent=2))
                   #archivo.write(json.dumps(json_str)+"\n")
                   #print('Datos en formato JSON',json.dumps(json_str, indent=2))
                   #archivo.close()
                return total_recs
            elif req.status_code==500:
                print(req.text)
            elif req.status_code==502:
                print(req.text)
            elif req.status_code==504:
                print(req.text)
            elif req.status_code==403:
                print(req.text)

def make_get_erm(pathPattern,okapi_url, okapi_tenant, okapi_token,queryString,json_file,refdatapath):
        try:
            pathPattern=pathPattern
            okapi_url=okapi_url
            archivo=open(json_file+".json", 'w')
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            #username="folio"
            #password="Madison"
            #payload = {'username': username, 'password': password}
            perpage = 100
            page = 1
            paging_q = "?perPage={}&page={}"
            #length="9999"
            #start="1"
            #paging_q = f"?limit={length}&offset={start}"
            pathlic = pathPattern+paging_q.format(perpage,page)
            #data=json.dumps(payload)
            url = okapi_url + pathlic
            req = requests.get(url, headers=okapi_headers)
            #print(req.text)
            if req.status_code != 201:
                #print(req)        
                #print(req.encoding)
                #print(req.text)
                #print(req.headers)
                response = json.loads(req.text)
                #total_recs = int(json_str["totalRecords"])
                #archivo.write(json.dumps(response,indent=3))
                printObject(response,refdatapath,0,f"{json_file}",True)
                #print('Datos en formato JSON',json.dumps(response, indent=2))
                #print('Datos en formato JSON',json.dumps(response))
                with open(json_file+".json", 'a+') as archivo:
                    while len(response) == perpage:
                        page += 1
                        paging_q = "?perPage={}&page={}"
                        path = pathPattern+paging_q.format(perpage, page)
                        url = okapi_url + path
                        req = requests.get(url, headers=okapi_headers)
                        response = json.loads(req.text)
                        #archivo.write(json.dumps(response, indent=2))
                        printObject(response,refdatapath,0,f"{json_file}",True)
                        #archivo.write(json.dumps(response,indent=3))
                        #archivo.write(json.dumps(response))
                        #print('Datos en formato JSON',json.dumps(response, indent=2))
                        print('Datos en formato JSON',json.dumps(response))
                    #archivo.write(json.dumps(response, indent=3))
                    printObject(response,refdatapath,0,f"{json_file}",True)
        except requests.RequestException as e:
           print("OOPS!! General Error")
           print(str(e))
                      
def floatHourToTime(fh):
    h, r = divmod(fh, 1)
    m, r = divmod(r*60, 1)
    return (
        int(h),
        int(m),
        int(r*60),
    )
        
def timeStamp(dateTimeObj):
    try:
        date_object = datetime.timestamp(dateTimeObj)
        timestampStr = date_object.strptime("%Y-%m-%dT%H:%M:%S.000+00:00")
        return timestampStr
    except ValueError as error:
            print(f"Error: {error}")
        
def timeStampString(dateTimeObj):
        try:
        #dateTimeObj = dateTimeObj.strptime(dateTimeObj, "%Y-%m-%d").strftime("%d-%m-%Y")
            dateTimeObj=dateTimeObj.replace("/","-")
            if dateTimeObj.find("-")!=-1: 
                a=dateTimeObj.split("-")
                day=a[0]
                if len(day)==1:
                    day=f"0{day}"
                month=a[1]
                if len(month)==1:
                    month=f"0{month}"
                year=a[2]
                my_string=f"{year}-{month}-{day}"
                date1 = datetime.strptime(my_string, "%Y-%m-%d")
            return date1
        except ValueError as ee:
            print(f"Error: {ee}")

            
def write_file(**kwargs):
        try:
            if 'ruta' in kwargs:
                ext=kwargs['ruta']
                extension=ext[-3:]
                if extension=="csv":
                    if 'contenido' in kwargs:
                        row=kwargs['contenido']
                    else:
                        row=""
                    with open(kwargs['ruta'],"a+", encoding="utf8") as outfile:
                        writer = csv.writer(outfile)
                        writer.writerow(row)
                else:
                    if 'contenido' in kwargs:
                        data=kwargs['contenido']
                    else: 
                        data=""
                with open(kwargs['ruta'],"a+", encoding="utf8") as outfile:
                    outfile.write(data+"\n")
        except Exception as ee:
            print(f"ERROR: {ee}")
            
def okapiPath(code_search):
        valor=[]
        try:
            #valor="0"
            pathfile=os.path.dirname(os.path.realpath(__file__))
            
            f = open(os.path.join(pathfile, "setting_data.json"))
            data = json.load(f)
            for i in data['settings']:
                a_line=str(i)
                if i['name'] == code_search:
                #if (a_line.find(code_search) !=-1):
                    valor.append(i['pathPattern'])
                    valor.append(i['name'])
                    break
            f.close()
            return valor
        except ValueError as error:
            print(f"Error: {error}")
            return 0

def checkURL(code_search):
    try:
        a=code_search.find("http://")
        if a!=-1:
            urlok=True    
            return urlok
        else:
            urlok=False
            return urlok
    except ValueError as error:
        print(f"Error: {error}")
        
#############################
#ACQ_ERM_MIGRATION TOOLS
#(customerName=customerName,getrefdata=getrefdata,scriptTorun=scriptTorun,graphicinterfaces=graphicinterfaces)  
#############################
class AcqErm():    
    def __init__(self,customertosearch):
        self.customerName=customertosearch
        
    def okapi_customer(self):
        try:
            data={}
            data=SearchClient(self.customerName)
            if len(data)!=0:
                self.x_okapi_url= data["x_okapi_url"]
                self.x_okapi_tenant= data["x_okapi_tenant"]
                self.x_okapi_token= data["x_okapi_token"]
                self.content_type= "application/json"
                self.user=data["user"]
                self.password=data["password"]
                return True
            else:
                return False
        except ValueError as error:
            print(f"Error Critical in okapi customer function : {error}")

    def createdFolderStructure(self):
        self.exist=True
        now = datetime.now()
        client={}
        path=os.path.dirname(os.path.realpath(__file__))      
        self.path_original=path
        x=path.find("runenv")
        self.path=path[:-7]
        self.path_dir=f"{self.path}/client_data/migration_{self.customerName}"
        self.createDirectory(self.path_dir)
        self.path_data=f"{self.path_dir}/data"
        self.path_refdata=f"{self.path_dir}/refdata"
        self.path_logs=f"{self.path_dir}/logs"
        self.path_results=f"{self.path_dir}/results"
        self.path_mapping_files=f"{self.path_dir}/mapping_files"
        self.path_reports=f"{self.path_dir}/reports"
        print("\n"+f"Reference Data")
        print(f"INFO: Reference Data:{self.getrefdata}")
        if self.getrefdata:
            #schemas=["categories","acquisitionsUnits","organizations","mtypes","locations","funds","expenseClasses","noteTypes","servicepoints","overdueFinePolicies","lostItemFeePolicies","usergroups","departments","tenant.addresses"]
            if self.sctr=="l":
                schemas=["licenses_refdata","licenses_custprops","organizations","noteTypes"]
            elif self.sctr=="a":
                schemas=["categories","acquisitionsUnits","organizations","noteTypes","tenant.addresses"]
            elif self.sctr=="o":
                schemas=["categories","acquisitionsUnits","organizations","noteTypes"]
            elif self.sctr=="p":
                schemas=["categories","acquisitionsUnits","organizations","mtypes","locations","funds","expenseClasses","noteTypes","servicepoints","tenant.addresses"]
            else:
                schemas=["categories","acquisitionsUnits","organizations","mtypes","locations","funds","expenseClasses","noteTypes","servicepoints","overdueFinePolicies","lostItemFeePolicies","usergroups","departments","tenant.addresses"]
            #print(f"INFO Getting Okapi customer from okapi_customer files")
            #client=br.SearchClient(self.customerName)
            if self.customerName is not None:
                self.refdata_path=f"{self.path_dir}/refdata"
                queryString=""
                for arv in schemas:
                    try:
                    #(Pattern,okapi_url, okapi_tenant, okapi_token,queryString,json_file,path):
                        #print(arv)
                        pattern=get_one_schema(str(arv))
                        if arv=="licenses_refdata" or arv=="licenses_custprops":
                            totalrecs=make_get_erm(pattern[0],self.x_okapi_url, self.x_okapi_tenant, self.x_okapi_token,queryString,f"{self.customerName}_{arv}",self.refdata_path)
                        else:
                            totalrecs=make_get(pattern[0],self.x_okapi_url, self.x_okapi_tenant, self.x_okapi_token,queryString,f"{self.customerName}_{arv}",self.refdata_path)
                        print(f"INFO schema: {arv} total records: {totalrecs}")
                    except Exception as ee:
                        print(f"ERROR: schema: {arv} {ee}")
            else:
                print(f"ERROR: Client Name was't found in /runenv/okapi_customers.json file, it needs to be included: {self.upd}")        

        folder=["logs","results","data","refdata","mapping_files","reports"]
        print("\n"+"Folders CLIENT_DATA")
        date_time = now.strftime("%m_%d_%y_(%H_%M)")
        for arg in folder:
            try: 
                os.mkdir(f"{self.path_dir}/{arg}")
                print(f"INFO creating folder {arg} for {self.customerName}")
                self.exist=False
                if arg=="mapping_files":
                    shutil.copy(f"{self.path_original}/loadSetting_template.json", os.path.join(self.path_dir, arg, "loadSetting.json"))
                    dic= []
                    loadset={}
                    f = open(f"{self.path_dir}/{arg}/loadSetting.json",)
                    data = json.load(f)
                    for i in data['loadSetting']:
                        #a_line=str(i)
                        i['customer']=self.customerName
                        i['path_root']=f"{self.path_dir}"
                        i['path_results']=f"{self.path_results}"
                        i['path_logs']=f"{self.path_logs}"
                        i['path_refdata']=f"{self.path_refdata}"
                        i['path_data']=f"{self.path_data}"
                        i['mapping_files']=f"{self.path_mapping_files}"
                        i['path_reports']=f"{self.path_reports}"
                        
                    dic.append(i)
                    loadset['loadSetting']=dic
                    f.close
                    with open(f"{self.path_dir}/{arg}/loadSetting.json","w+", encoding="utf-8") as outfile:
                        json.dump(loadset,outfile,indent=2)
                    shutil.copy(f"{self.path_original}/acquisitionMapping_template.xlsx", os.path.join(self.path_dir, arg, "acquisitionMapping.xlsx"))
                    refnumt=[]
                    refNumberType={}
                    refnumt.append({"Vendor continuation reference number":"","Vendor order reference number":"","Vendor subscription reference number":"","Vendor internal number":"","Vendor title number":""})
                    refNumberType['refNumberType']=refnumt
                    printObject(refNumberType,f"{self.path_dir}/{arg}",0,"refNumberType",True)
                    shutil.copy(f"{self.path_original}/composite_purchase_order_mapping_template.json", f"{self.path_dir}/{arg}/composite_purchase_order_mapping.json")
                    shutil.copy(f"{self.path_original}/organization_mapping_template.json", f"{self.path_dir}/{arg}/organization_mapping.json")
                    shutil.copy(f"{self.path_original}/agreement_mapping_template.json", f"{self.path_dir}/{arg}/agreement_mapping.json")
                    shutil.copy(f"{self.path_original}/license_mapping_template.json", f"{self.path_dir}/{arg}/license_mapping.json")
                    shutil.copy(f"{self.path_original}/users_mapping_template.json", f"{self.path_dir}/{arg}/users_mapping.json")
                    shutil.copy(f"{self.path_original}/notes_mapping_template.json", f"{self.path_dir}/{arg}/notes_mapping.json") 
                    self.path_usersMapping=f"{self.path_dir}/{arg}/users_mapping.json"
                    self.path_licenseMapping=f"{self.path_dir}/{arg}/license_mapping.json"
                    self.path_agreementMapping=f"{self.path_dir}/{arg}/agreement_mapping.json"
                    self.path_notesMapping=f"{self.path_dir}/{arg}/notes_mapping.json"
                    self.path_purchaseMapping=f"{self.path_dir}/{arg}/composite_purchase_order_mapping.json"
                    self.path_organizationsMapping=f"{self.path_dir}/{arg}/organization_mapping.json"
                    if self.sctr=="l":
                        if os.path.exists(f"{self.path_dir}/{arg}/{self.customerName}_licenses_custprops.json"):
                            pass
                        else:
                            shutil.copy(f"{self.path_refdata}/{self.customerName}_licenses_custprops.json", f"{self.path_dir}/{arg}/{self.customerName}_licenses_custprops.json")
                        if os.path.exists(f"{self.path_dir}/{arg}/{self.customerName}_licenses_refdata.json"):
                            pass
                        else:
                            shutil.copy(f"{self.path_refdata}/{self.customerName}_licenses_refdata.json", f"{self.path_dir}/{arg}/{self.customerName}_licenses_refdata.json")
               
            except OSError as error: 
                print(f"INFO client folder /{arg} for {self.customerName} found")
                if arg=="mapping_files":
                    try:
                        if os.path.exists(f"{self.path_dir}/{arg}/loadSetting.json"):
                            pass
                        else:
                            shutil.copy(f"{self.path_original}/loadSetting_template.json", os.path.join(self.path_dir, arg, "loadSetting.json"))
                            dic= []
                            loadset={}
                            f = open(f"{self.path_dir}/{arg}/loadSetting.json",)
                            data = json.load(f)
                            for i in data['loadSetting']:
                                #a_line=str(i)
                                i['customer']=self.customerName
                                i['path_root']=f"{self.path_dir}"
                                i['path_results']=f"{self.path_dir}/results"
                                i['path_logs']=f"{self.path_dir}/logs"
                                i['path_refdata']=f"{self.path_dir}/refdata"
                                i['path_data']=f"{self.path_dir}/data"
                                i['path_mapping_files']=f"{self.path_dir}/mapping_files"
                                i['path_reports']=f"{self.path_dir}/reports"
                            dic.append(i)
                            loadset['loadSetting']=dic
                            f.close
                            with open(f"{self.path_dir}/{arg}/loadSetting.json","w+", encoding="utf-8") as outfile:
                                json.dump(loadset,outfile,indent=2)
                        if os.path.exists(f"{self.path_dir}/{arg}/composite_purchase_order_mapping.json"):
                            self.path_purchaseMapping=f"{self.path_dir}/{arg}/composite_purchase_order_mapping.json"
                        else:
                            shutil.copy(f"{self.path_original}/composite_purchase_order_mapping_template.json", f"{self.path_dir}/{arg}/composite_purchase_order_mapping.json")
                        if os.path.exists(f"{self.path_dir}/{arg}/organization_mapping.json"):
                            pass
                        else:
                            shutil.copy(f"{self.path_original}/organization_mapping_template.json", f"{self.path_dir}/{arg}/organization_mapping.json")
                        if os.path.exists(f"{self.path_dir}/{arg}/agreement_mapping.json"):
                            pass 
                        else:
                            shutil.copy(f"{self.path_original}/agreement_mapping_template.json", f"{self.path_dir}/{arg}/agreement_mapping.json")                    
                        if os.path.exists(f"{self.path_dir}/{arg}/license_mapping.json"):
                            pass 
                        else:
                            shutil.copy(f"{self.path_original}/license_mapping_template.json", f"{self.path_dir}/{arg}/license_mapping.json")
                        if self.sctr=="l":
                            if os.path.exists(f"{self.path_dir}/{arg}/{self.customerName}_licenses_custprops.json"):
                                pass
                            else:
                                shutil.copy(f"{self.path_refdata}/{self.customerName}_licenses_custprops.json", f"{self.path_dir}/{arg}/{self.customerName}_licenses_custprops.json")
                            
                            if os.path.exists(f"{self.path_dir}/{arg}/{self.customerName}_licenses_refdata.json"):
                                pass
                            else:
                                shutil.copy(f"{self.path_refdata}/{self.customerName}_licenses_refdata.json", f"{self.path_dir}/{arg}/{self.customerName}_licenses_refdata.json")
                                
                        if os.path.exists(f"{self.path_dir}/{arg}/users_mapping.json"):
                            pass 
                        else:
                            shutil.copy(f"{self.path_original}/users_mapping_template.json", f"{self.path_dir}/{arg}/users_mapping.json")
                        if os.path.exists(f"{self.path_dir}/{arg}/notes_mapping.json"):
                            pass 
                        else:
                            shutil.copy(f"{self.path_original}/notes_mapping_template.json", f"{self.path_dir}/{arg}/notes_mapping.json")
                        self.path_usersMapping=f"{self.path_dir}/{arg}/users_mapping.json"                
                        self.path_licenseMapping=f"{self.path_dir}/{arg}/license_mapping.json"
                        self.path_agreementMapping=f"{self.path_dir}/{arg}/agreement_mapping.json"
                        self.path_notesMapping=f"{self.path_dir}/{arg}/notes_mapping.json"
                        self.path_purchaseMapping=f"{self.path_dir}/{arg}/composite_purchase_order_mapping.json"
                        self.path_organizationsMapping=f"{self.path_dir}/{arg}/organization_mapping.json"
                    except OSError as error:
                        print(f"INFO client mapping files /{arg} for {self.customerName} found")
        return self.path_dir
                
    def createdFolderStructureenv(self):
        client={}
        path=os.path.dirname(os.path.realpath(__file__)) 
        folder=["logs","results"]
        print("\n"+"Folders RUNENV")
        for arg in folder:
            try: 
                os.mkdir(f"{path}/{arg}/{self.customerName}")
                print(f"INFO creating folder {arg} for {self.customerName}")  
            except OSError as error: 
                print(f"INFO folder {arg} for {self.customerName} found")
        return None
    
    def createDirectory(self,dirname):
        try:
            os.mkdir(f"{dirname}")
        except OSError as error: 
            pass
        
    def menu(self,**kwargs):
        try: 
            self.sctr=kwargs['scriptTorun']
            self.getrefdata=kwargs['getrefdata']
            self.graphicinterfaces=kwargs['graphicinterfaces']
            print(f"INFO CUSTOMER: {self.customerName} | SCRIPT  {self.sctr} | DOWNLOAD ACQ/ERM REFDATA {self.getrefdata} | GRAPHIC {self.graphicinterfaces}")
            client=self.customerName
            self.createdFolderStructureenv()
            self.createdFolderStructure()
            #if self.graphicinterfaces:
            if self.exist==False:
                #root = Tk()
                #e = windows.window(root,"Purchase Orders","1000x500", self.customerName)
                #root.mainloop()
                print(f"INFO the FOLDERS for {self.customerName}  were created in..{self.path_data}")
                print(f"Warning:")
                print(f"INFO 1. Check the ..{self.path_data}/loadSetting.json file be sure to include the file name to read")
                print(f"INFO 2. Need to include the mapping file too in ..{self.path_mapping_files}")
                print(f"INFO 3. Run again the script...")                
                return
            else:
                print("\n"+f"INFO LOADING DATAFRAMES")
                if self.sctr=="a":   
                    self.value="agreement"
                    self.value_a="agree"
                    self.df=self.value
                    ls=self.load_settings()
                    self.customerName=pd.dataframe()
                    #print(ls[self.value_a])
                    filetoload=os.path.join(self.path_mapping_files, str(ls[self.value_a]['fileName']))
                    self.df=self.customerName.importDataFrame(filetoload,
                                            orderby=ls[self.value_a]['orderby'],
                                            distinct=ls[self.value_a]['distinct'],                                            
                                            sheetName=ls[self.value_a]['sheetName'],
                                            mapping_file=self.path_agreementMapping,
                                            dfname=self.value)
                    if self.df is not None:
                        self.customerName=agree.Agreements(client,self.path_dir)
                        self.customerName.readagreements(client,self.df)
                    else:
                        print(f"INFO file Name must be included in the ..{self.path_mapping_files}/loadSetting.json")   
                elif self.sctr=="l": 
                    self.value="licenses"
                    self.value_a="lic"
                    self.df=self.value
                    ls=self.load_settings() 
                    existname=str(ls[self.value_a]['fileName'])
                    if existname!="":
                        filetoload=os.path.join(self.path_mapping_files, +str(ls[self.value_a]['fileName']))
                        self.customerName=pd.dataframe()
                        self.dflicenses=self.customerName.importDataFrame(filetoload,
                                            orderby=ls[self.value_a]['orderby'],
                                            distinct=ls[self.value_a]['distinct'],                                            
                                            sheetName=ls[self.value_a]['sheetName'],
                                            mapping_file=self.path_licenseMapping,
                                            dfname=self.value)
                        #Notes
                        self.value="notes"
                        self.value_a=f"note[0]"
                        print(f"INFO NOTE NO. {self.value_a}===================")
                        self.df=self.value
                        ls=self.load_settings()
                        existname=str(ls[self.value_a]['fileName'])
                        self.notes=None
                        if existname!="":
                            swno=False
                            #print(ls[self.value_a])
                            filetoload=f"{self.path_mapping_files}/"+str(ls[self.value_a]['fileName'])
                            if filetoload!="":
                                self.customerName=pd.dataframe()
                                filenametoprint=str(ls[self.value_a]['fileName'])
                                readmapping=f"{self.path_mapping_files}/"+str(ls[self.value_a]['mappingfile'])
                                linkidfilewithid=f"{self.path_results}/"+str(ls[self.value_a]['linkidfile'])                    
                                if readmapping=="":
                                    self.notes=self.customerName.importDataFrame(filetoload,
                                    orderby=ls[self.value_a]['orderby'],
                                    distinct=ls[self.value_a]['distinct'],                                            
                                    sheetName=ls[self.value_a]['sheetName'],
                                    mapping_file=self.path_notesMapping,
                                    dfname=self.value)
                                else:
                                    self.notes=self.customerName.importDataFrame(filetoload,
                                    orderby=ls[self.value_a]['orderby'],
                                    distinct=ls[self.value_a]['distinct'],                                            
                                    sheetName=ls[self.value_a]['sheetName'],
                                    mapping_file=readmapping,
                                    dfname=self.value)              
                    
                        if self.dflicenses is not None:                        
                            self.customerName=lic.licenses(client,self.path_dir)
                            if self.notes is not None:
                                self.customerName.readlicenses(client,dflicenses=self.dflicenses, dfnotes=self.notes)
                            else:
                                self.customerName.readlicenses(client,dflicenses=self.dflicenses)
                    else:
                        print(f"INFO file Name must be included in the ..{self.path_mapping_files}/loadSetting.json") 
                        
                elif self.sctr=="o":
                    swnotes=False
                    self.value="organizations"
                    self.value_a="org"
                    self.df=self.value
                    ls=self.load_settings()
                    existname=str(ls[self.value_a]['fileName'])
                    if existname!="":
                        filetoload=f"{self.path_data}/"+str(ls[self.value_a]['fileName'])
                        self.customerName=pd.dataframe()
                        #print(ls[self.value_a])
                        filetoload=f"{self.path_data}/"+str(ls[self.value_a]['fileName'])
                        self.dforganizations=self.customerName.importDataFrame(filetoload,
                                            orderby=ls[self.value_a]['orderby'],
                                            distinct=ls[self.value_a]['distinct'],                                            
                                            sheetName=ls[self.value_a]['sheetName'],
                                            mapping_file=self.path_organizationsMapping,
                                            dfname=self.value)
                    #Contacts
                    self.value_a="contacts"
                    ls=self.load_settings()
                    existname=str(ls[self.value_a]['fileName'])
                    if existname!="":
                        lsc=self.load_settings()
                        filetoload=f"{self.path_data}/"+str(lsc[self.value_a]['fileName'])
                        self.dfcontacts=self.customerName.importDataFrame(filetoload,
                                            orderby=lsc[self.value_a]['orderby'],
                                            distinct=lsc[self.value_a]['distinct'],                                            
                                            sheetName=lsc[self.value_a]['sheetName'],
                                            mapping_file=self.path_organizationsMapping,
                                            dfname=self.value_a)
                    
                    #Interfaces
                    self.value_a="interfaces"
                    lsi=self.load_settings()
                    existname=str(lsi[self.value_a]['fileName'])
                    if existname!="":
                        filetoload=f"{self.path_data}/"+str(lsi[self.value_a]['fileName'])
                        self.dfinterfaces=self.customerName.importDataFrame(filetoload,
                                            orderby=lsi[self.value_a]['orderby'],
                                            distinct=lsi[self.value_a]['distinct'],                                            
                                            sheetName=lsi[self.value_a]['sheetName'],
                                            mapping_file=self.path_organizationsMapping,
                                            dfname=self.value_a)
                        #Notes
                        self.value="notes"
                        self.value_a=f"note[0]"
                        print(f"INFO NOTE NO. {self.value_a}===================")
                        self.df=self.value
                        lsi=self.load_settings()
                        existname=str(ls[self.value_a]['fileName'])
                        if existname!="":
                            ls=self.load_settings()
                            existname=str(ls[self.value_a]['fileName'])
                            self.notes=None
                            if existname!="":
                                swno=False
                                #print(ls[self.value_a])
                                filetoload=f"{self.path_data}/"+str(ls[self.value_a]['fileName'])
                                if filetoload!="":
                                    self.customerName=pd.dataframe()
                                    filenametoprint=str(ls[self.value_a]['fileName'])
                                    readmapping=f"{self.path_mapping_files}/"+str(ls[self.value_a]['mappingfile'])
                                    linkidfilewithid=f"{self.path_results}/"+str(ls[self.value_a]['linkidfile'])                    
                                    if readmapping=="":
                                        self.notes=self.customerName.importDataFrame(filetoload,
                                            orderby=ls[self.value_a]['orderby'],
                                            distinct=ls[self.value_a]['distinct'],                                            
                                            sheetName=ls[self.value_a]['sheetName'],
                                            mapping_file=self.path_notesMapping,
                                            dfname=self.value)
                                    else:
                                        self.notes=self.customerName.importDataFrame(filetoload,
                                            orderby=ls[self.value_a]['orderby'],
                                            distinct=ls[self.value_a]['distinct'],                                            
                                            sheetName=ls[self.value_a]['sheetName'],
                                            mapping_file=readmapping,
                                            dfname=self.value)
                    #print(self.notes)'''
                        if self.dforganizations is not None:                        
                            self.customerName=org.organizations(client,self.path_dir)
                            if self.notes is not None:
                                self.customerName.readOrganizations(client,dforganizations=self.dforganizations, dfcontacts=self.dfcontacts, dfinterfaces=self.dfinterfaces, dfnotes=self.notes)
                            else:
                                self.customerName.readOrganizations(client,dforganizations=self.dforganizations, dfcontacts=self.dfcontacts, dfinterfaces=self.dfinterfaces)
                        else:
                            print(f"INFO file Name must be included in the ..{self.path_mapping_files}/loadSetting.json") 
                    
                elif self.sctr=="p":
                    try:
                        self.value="purchaseOrders"
                        self.value_a="po"                    
                        self.df=self.value
                        ls=self.load_settings()
                        existname=str(ls[self.value_a]['fileName'])
                        if existname!="":
                            filetoload=f"{self.path_data}/"+str(ls[self.value_a]['fileName'])
                            self.customerName=pd.dataframe()
                            #print(ls[self.value_a])
                            self.dforders=self.customerName.importDataFrame(filetoload,
                                            orderby=ls[self.value_a]['orderby'],
                                            distinct=ls[self.value_a]['distinct'],                                            
                                            sheetName=ls[self.value_a]['sheetName'],
                                            mapping_file=self.path_purchaseMapping,
                                            dfname=self.value)

                            self.value_a="poLines"
                            lsa=self.load_settings()
                            existname=""
                            existname=str(lsa[self.value_a]['fileName'])
                            if existname!="":
                                filetoload=f"{self.path_data}/"+str(lsa[self.value_a]['fileName'])
                                self.dfpoLines=self.customerName.importDataFrame(filetoload,
                                            orderby=lsa[self.value_a]['orderby'],
                                            distinct=lsa[self.value_a]['distinct'],
                                            sheetName=lsa[self.value_a]['sheetName'],
                                            mapping_file=self.path_purchaseMapping,
                                            dfname=self.value_a)
                            else:
                                self.dfpoLine=self.dforders
                            #Notes
                            self.value="notes"
                            self.value_a=f"note[0]"
                            print(f"INFO NOTE NO. {self.value_a}===================")
                            self.df=self.value
                            ls=self.load_settings()
                            existname=str(ls[self.value_a]['fileName'])
                            self.notes=None
                            if existname!="":
                                swno=False
                                #print(ls[self.value_a])
                                filetoload=f"{self.path_data}/"+str(ls[self.value_a]['fileName'])
                                if filetoload!="":
                                    self.customerName=pd.dataframe()
                                    filenametoprint=str(ls[self.value_a]['fileName'])
                                    readmapping=f"{self.path_mapping_files}/"+str(ls[self.value_a]['mappingfile'])
                                    linkidfilewithid=f"{self.path_results}/"+str(ls[self.value_a]['linkidfile'])                    
                                    if readmapping=="":
                                        self.notes=self.customerName.importDataFrame(filetoload,
                                            orderby=ls[self.value_a]['orderby'],
                                            distinct=ls[self.value_a]['distinct'],                                            
                                            sheetName=ls[self.value_a]['sheetName'],
                                            mapping_file=self.path_notesMapping,
                                            dfname=self.value)
                                    else:
                                        self.notes=self.customerName.importDataFrame(filetoload,
                                            orderby=ls[self.value_a]['orderby'],
                                            distinct=ls[self.value_a]['distinct'],                                            
                                            sheetName=ls[self.value_a]['sheetName'],
                                            mapping_file=readmapping,
                                            dfname=self.value)
                            if self.dforders is not None: 
                                self.customerName=orders.compositePurchaseorders(client,self.path_dir)
                            if self.notes is not None:
                                self.customerName.readorders(client, dfOrders=self.dforders, dfPolines=self.dfpoLines, dfnotes=self.notes,notes_mapping_file=readmapping)
                            else:
                                self.customerName.readorders(client, dfOrders=self.dforders, dfPolines=self.dfpoLines)
                        else:
                            print(f"INFO Purchase Orders file Name must be included in the ..{self.path_mapping_files}/loadSetting.json")                     
                    except ValueError as error:
                        print(f"Error: {error}")
                        
                elif self.sctr=="u": 
                    self.value="users"
                    self.value_a="user"
                    self.df=self.value
                    ls=self.load_settings()
                    self.customerName=pd.dataframe()
                    #print(ls[self.value_a])
                    filetoload=f"{self.path_data}/"+str(ls[self.value_a]['fileName'])
                    self.dfusers=self.customerName.importDataFrame(filetoload,
                                            orderby=ls[self.value_a]['orderby'],
                                            distinct=ls[self.value_a]['distinct'],                                            
                                            sheetName=ls[self.value_a]['sheetName'],
                                            mapping_file=self.path_usersMapping,
                                            dfname=self.value)
                    #print(self.dfusers)
                    self.customerName=users.users(client,self.path_dir)
                    self.customerName.readusers(client,dfusers=self.dfusers)
                elif self.sctr=="n":   
                    iter=0
                    swno=True
                    
                    while swno:
                        self.value="notes"
                        self.value_a=f"note[{iter}]"
                        print(f"INFO NOTE NO. {self.value_a}===================")
                        self.df=self.value
                        try:
                            ls=self.load_settings()
                        except ValueError as error:
                            print(f"INFO No Notes")
                            swno=False
                        if ls is not None:
                            #print(ls[self.value_a])
                            filetoload=f"{self.path_data}/"+str(ls[self.value_a]['fileName'])
                            if filetoload!="":
                                self.customerName=pd.dataframe()
                                filenametoprint=str(ls[self.value_a]['fileName'])
                                readmapping=f"{self.path_mapping_files}/"+str(ls[self.value_a]['mappingfile'])
                                if str(ls[self.value_a]['linkidfile'])!="":
                                    linkidfilewithid=f"{self.path_results}/"+str(ls[self.value_a]['linkidfile'])                    
                                else:
                                    linkidfilewithid=""
                                if readmapping=="":
                                    self.df=self.customerName.importDataFrame(filetoload,
                                            orderby=ls[self.value_a]['orderby'],
                                            distinct=ls[self.value_a]['distinct'],                                            
                                            sheetName=ls[self.value_a]['sheetName'],
                                            mapping_file=self.path_notesMapping,
                                            dfname=self.value)
                                else:
                                    self.df=self.customerName.importDataFrame(filetoload,
                                            orderby=ls[self.value_a]['orderby'],
                                            distinct=ls[self.value_a]['distinct'],                                            
                                            sheetName=ls[self.value_a]['sheetName'],
                                            mapping_file=readmapping,
                                            dfname=self.value)        
                                if self.df is not None:
                                    #def __init__(self,client,path_dir, **kwargs):
                                    if linkidfilewithid!="":
                                        self.customerName=appnotes.notes(client,self.path_dir,dataframe=self.df, notes_mapping_file=readmapping,linkidfile=linkidfilewithid)
                                    else:
                                        self.customerName=appnotes.notes(client,self.path_dir,dataframe=self.df, notes_mapping_file=readmapping)
                                    self.customerName.readfile(readmapping,dfwithids=self.df,filenamenotes=self.value_a)
                                    #self.customerName=notes.notes(client,self.path_dir,dataframe=self.df)
                        
                        else:
                            swno=False
                        iter+=1
                else:
                    print(f"ERROR: you have selected the script wrong")


        except ValueError as error:
            print(f"Error: {error}")

#    class path_importer():
    def load_settings(self):
        try:
            #def readagreements(**kwargs):
            #CUSTOMER CONFIGURATION FILE (PATHS, PURCHASE ORDER FILE NAME AND FILTERS)
            #path_root=f"{kwargs['rootpath']}"
            #customerName=kwargs['customerName']
            f = open(f"{self.path_mapping_files}/loadSetting.json",)
            settingdata = json.load(f)
            countpol=0
            countpolerror=0
            countvendorerror=0
            #READING THE LOADSETTING JSON FILE FROM "/CUSTOMER/REFDATA" FOLDER
            print("\n"+f"Loading settings from loadSetting.json file....")
            istherenotesApp=[]
            load_file=""
            lf={}
            for i in settingdata['loadSetting']:
                try:
                    for lf in i[self.value]:
                        if self.value_a in lf:
                            load_file=str(lf[self.value_a]['fileName'])
                            f.close()
                            return lf
                except ValueError as error:
                    print(f"Error: {error}")
            if load_file!="":
                print("\n"+f"INFO file to import found: {load_file}")
                return lf
            else:
                print(f"Error: Opps the {self.value} file Name is not include in loadSetting.json  file, please include the {self.value} file name to continue")
                return None
        except ValueError as error:
                print(f"Error: {error}")
                return None
                
                
                
    def json_validator(self,data):
        try:
            json_data = ast.literal_eval(json.dumps(str(data)))
            #print(data)
            #json.loads(str(data))
            return True
        except ValueError as error:
            print("invalid json: %s" % error)
            return False
        
    def csv(self,**kwargs):
        try:
            ext=kwargs['path']
            extension=ext[-3:]
            if extension=="csv":
                row=kwargs['contenido']
                with open(kwargs['path'],"a", encoding="utf8") as outfile:
                    writer = csv.writer(outfile)
                    writer.writerow(row)
        except Exception as ee:
            print(f"ERROR: {ee}")  
            
    def SearchMappingDetails(self,**kwargs):
        #field,schema,file_name
        # Opening JSON file
        dic =dic= {}
        f = open(kwargs['file_name'],)
        data = json.load(f)
        for i in data['organizationMapping']:
            a_line=str(i)
            if i[field] == kwargs['file_name']:
                 dic=i
                 break
        f.close()
        return dic['legacyName']
    

    

    #CHECK PO NUMBER
    #THE PO NUMBER CANNOT INCLUDE SPECIAL CHARACTER    
    def check_poNumber(self, value, path):
        try:
            #define input string
            input_string = value
            final_string = "" #define string for ouput
            for character in input_string:
                if(character.isalnum()):
                # if character is alphanumeric concat to final_string
                    final_string = final_string + character
                    value=final_string
            valuefix=""
            Newmpol=""
            keepit=False
            #sp_chars = [';', ':', '!', "*","<","/","_","-","(",")","|"," ","@","¿","?","=","#","!"] 
            #valuefix = filter(lambda i: i not in sp_chars, value)
            if value.find(" ")!=-1: value=value.replace(" ","")
            if value.find("#")!=-1: value=value.replace("#","")
            if value.find(">")!=-1: value=value.replace(">","")
            if value.find("<")!=-1: value=value.replace("<","")
            if value.find("/")!=-1: value=value.replace("/","")
            if value.find(":")!=-1: value=value.replace(":","")
            if value.find("-")!=-1: value=value.replace("-","")
            if value.find("_")!=-1: value=value.replace("_","")
            if value.find("(")!=-1: value=value.replace("(","")
            if value.find(")")!=-1: value=value.replace(")","")
            if value.find("&")!=-1: value=value.replace("&","")
            if value.find(".")!=-1: value=value.replace(".","")
            if value.find("'")!=-1: value=value.replace("'","")
            if value.find(",")!=-1: value=value.replace(",","")
            if value.find("|")!=-1: value=value.replace("|","")
            if value.find("!")!=-1: value=value.replace("!","")
            if value.find("=")!=-1: value=value.replace("=","")
            if value.find("@")!=-1: value=value.replace("@","")
            if value.find("?")!=-1: value=value.replace("?","")
            if value.find("¿")!=-1: value=value.replace("¿","")
            if value.find("*")!=-1: value=value.replace("*","")
            if len(value)>18: 
                value=""
                keepit=True
                for i in range(2):
                    Newmpol=str(random.randint(100, 1000))
                    with open(path+"\oldNew_ordersID.txt", "a") as clean:
                        clean.write(str(value)+"/"+str(Newmpol)+"\n")
                    value=Newmpol            
            return value
        except Exception as ee:
            print(f"INFO check_poNumber function failed {ee}")        
        
    def searchKeysByVal(self,dict, byVal):
        try:
            keysList = ""
            keyslist=dict.get(byVal)
            return keyslist
        except Exception as ee:
            print(f"INFO csearchKeysByVal function failed {ee}")    

    def tsv_To_dic(self,path):
        try:
            with open(path, mode='r') as infile:
                reader = csv.reader(infile)
                mydict = {rows[0]:rows[1] for rows in reader}
            return mydict            
        except Exception as ee:
            print(f"INFO tsv_To_dic function failed {ee}")   


        
    def getId(self,namesearchValue,path,elementtosearch,okapi_url,okapi_token,okapi_tenant):
        try:
            if namesearchValue is None: searchValue="undefined"
            if namesearchValue=="NULL": searchValue="undefined"
            if namesearchValue=="": searchValue="undefined"
            dic={}
            path1=""        
            pathPattern=path
            #pathPattern1="/organizations/organizations" #?limit=9999&query=code="
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            length="1"
            start="1"
            element=elementtosearch
            query=f"query=name=="
            #/organizations-storage/organizations?query=code==UMPROQ
            paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
            #paging_q = f"?{query}"+search_string
            path1 = pathPattern1+paging_q
            #data=json.dumps(payload)
            url1 = okapi_url + path1
            req = requests.get(url1, headers=okapi_headers)
            idorg=""
            #Search by name
            if req.status_code != 201:
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                if (total_recs!=0):
                    rec=json_str[element]
                    #print(rec)
                    l=rec[0]
                    if 'id' in l:
                        idorg=l['id']
                        return idorg
        except requests.exceptions.HTTPError as err:
            print("error Organization GET")    

  
    def get_OrgId(self,searchValue,customerName):
        try:
            searchValue=searchValue.replace("&","")
            searchValue=searchValue.replace("+","")
            client=SearchClient(customerName)
            okapi_url=str(client.get('x_okapi_url'))
            okapi_tenant=str(client.get('x_okapi_tenant'))
            okapi_token=str(client.get('x_okapi_token'))
            dic={}
            path1=""        
            #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
            pathPattern1="/organizations/organizations" #?limit=9999&query=code="
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            length="1"
            start="1"
            element="organizations"
            query=f"query=code=="
            #/organizations-storage/organizations?query=code==UMPROQ
            paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
            #paging_q = f"?{query}"+search_string
            path1 = pathPattern1+paging_q
            #data=json.dumps(payload)
            url1 = okapi_url + path1
            req = requests.get(url1, headers=okapi_headers)
            idorg=""
            #Search by name
            if req.status_code != 201:
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                if (total_recs!=0):
                    rec=json_str[element]
                    #print(rec)
                    l=rec[0]
                    if 'id' in l:
                        idorg=l['id']
                    return idorg
            #Search by code
                elif (total_recs==0):
                    query=f"query=name=="
                    paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
                    #paging_q = f"?{query}"+orgname
                    path1 = pathPattern1+paging_q
                    #data=json.dumps(payload)
                    url1 = okapi_url + path1
                    req = requests.get(url1, headers=okapi_headers)
                    json_str = json.loads(req.text)
                    total_recs = int(json_str["totalRecords"])
                    if (total_recs!=0):
                        rec=json_str[element]
                        #print(rec)
                        l=rec[0]
                        if 'id' in l:
                            idorg=l['id']
                        return idorg
        except requests.exceptions.HTTPError as err:
            print(f"error Organization GET {err}")
        
    def get_OrgId_license(self,searchValue,customerName):
        try:
            client=SearchClient(customerName)
            okapi_url=str(client.get('x_okapi_url'))
            okapi_tenant=str(client.get('x_okapi_tenant'))
            okapi_token=str(client.get('x_okapi_token'))
            dic={}
            path1=""        
            #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
            pathPattern1="/organizations/organizations" #?limit=9999&query=code="
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            length="1"
            start="1"
            element="organizations"
            query=f"query=name=="
            #/organizations-storage/organizations?query=code==UMPROQ
            paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
            #paging_q = f"?{query}"+search_string
            path1 = pathPattern1+paging_q
            #data=json.dumps(payload)
            url1 = okapi_url + path1
            req = requests.get(url1, headers=okapi_headers)
            idorg=[]
            #Search by name
            if req.status_code != 201:
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                if (total_recs!=0):
                    rec=json_str[element]
                    #print(rec)
                    l=rec[0]
                    if 'id' in l:
                        idorg.append(l['id'])
                        idorg.append(l['name'])
                    return idorg
            #Search by code
                elif (total_recs==0):
                    searchValue=searchValue.replace(" ","")
                    searchValue=searchValue.replace(",","")
                    searchValue=searchValue.replace(".","")
                    searchValue=searchValue.replace("&","")
                    searchValue=searchValue.replace("'","")
                    query=f"query=code=="
                    paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
                    #paging_q = f"?{query}"+orgname
                    path1 = pathPattern1+paging_q
                    #data=json.dumps(payload)
                    url1 = okapi_url + path1
                    req = requests.get(url1, headers=okapi_headers)
                    json_str = json.loads(req.text)
                    total_recs = int(json_str["totalRecords"])
                    if (total_recs!=0):
                        rec=json_str[element]
                        #print(rec)
                        l=rec[0]
                        if 'id' in l:
                            idorg.append(l['id'])
                            idorg.append(l['name'])
                            return idorg
        except requests.exceptions.HTTPError as err:
            print("error Organization GET")
               
    def to_string(self,value):
        try:
            valueR=""
            if int(value):
            #print("numero")
                valueR=str(round(value))
            elif float(value):
            #print("decimal")
                valueR=str(round(value))
            else:
                valueR=value
            return valueR
        except requests.exceptions.HTTPError as err:
            print(f"error Organization GET {err}")
    
    def get_funId(self,searchValue,orderFormat,client):
        try:
            client=SearchClient(client)
            okapi_url=str(client.get('x_okapi_url'))
            okapi_tenant=str(client.get('x_okapi_tenant'))
            okapi_token=str(client.get('x_okapi_token'))
            dic={}
            path1=""        
            #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
            pathPattern1="/finance/funds" #?limit=9999&query=code="
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            length="1"
            start="1"
            element="funds"
            query=f"query=code=="
            #/organizations-storage/organizations?query=code==UMPROQ
            paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
            #paging_q = f"?{query}"+search_string
            path1 = pathPattern1+paging_q
            #data=json.dumps(payload)
            url1 = okapi_url + path1
            req = requests.get(url1, headers=okapi_headers)
            idorg=[]
            #Search by name
            if req.status_code != 201:
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                if (total_recs!=0):
                    rec=json_str[element]
                    #print(rec)
                    l=rec[0]
                    if 'id' in l:
                        idorg.append(l['id'])
                        idorg.append(l['code'])
                        return idorg
            #Search by code
                elif (total_recs==0):
                    query=f"query=name=="
                    paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
                    #paging_q = f"?{query}"+orgname
                    path1 = pathPattern1+paging_q
                    #data=json.dumps(payload)
                    url1 = okapi_url + path1
                    req = requests.get(url1, headers=okapi_headers)
                    json_str = json.loads(req.text)
                    total_recs = int(json_str["totalRecords"])
                    if (total_recs!=0):
                        rec=json_str[element]
                        #print(rec)
                        l=rec[0]
                        if 'id' in l:
                            idorg.append(l['id'])
                            idorg.append(l['code'])
                            return idorg
        except requests.exceptions.HTTPError as err:
            print(f"error Organization GET {err}")
    
    def get_matId(self,searchValue,client):
        try:
            client=SearchClient(client)
            okapi_url=str(client.get('x_okapi_url'))
            okapi_tenant=str(client.get('x_okapi_tenant'))
            okapi_token=str(client.get('x_okapi_token'))
            dic={}
            path1=""        
            #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
            pathPattern1="/material-types" #?limit=9999&query=code="
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            length="1"
            start="1"
            element="mtypes"
            query=f"query=name=="
            #/organizations-storage/organizations?query=code==UMPROQ
            paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
            #paging_q = f"?{query}"+search_string
            path1 = pathPattern1+paging_q
            #data=json.dumps(payload)
            url1 = okapi_url + path1
            req = requests.get(url1, headers=okapi_headers)
            idorg=""
            #Search by name
            if req.status_code != 201:
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                if (total_recs!=0):
                    rec=json_str[element]
                    #print(rec)
                    l=rec[0]
                    if 'id' in l:
                        idorg=l['id']
                        return idorg
            #Search by code
                elif (total_recs==0):
                    query=f"query=code=="
                    paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
                    #paging_q = f"?{query}"+orgname
                    path1 = pathPattern1+paging_q
                    #data=json.dumps(payload)
                    url1 = okapi_url + path1
                    req = requests.get(url1, headers=okapi_headers)
                    json_str = json.loads(req.text)
                    total_recs = int(json_str["totalRecords"])
                    if (total_recs!=0):
                        rec=json_str[element]
                        #print(rec)
                        l=rec[0]
                        if 'id' in l:
                            idorg=l['id']
                            return idorg
        except requests.exceptions.HTTPError as err:
            print(f"error Organization GET {err}")

    def get_funId_no_name(self, searchValue,client):
        try:
            client=SearchClient(client)
            okapi_url=str(client.get('x_okapi_url'))
            okapi_tenant=str(client.get('x_okapi_tenant'))
            okapi_token=str(client.get('x_okapi_token'))
            dic={}
            path1=""        
            #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
            pathPattern1="/finance/funds" #?limit=9999&query=code="
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            length="1"
            start="1"
            element="funds"
            query=f"query=code=="
            #/organizations-storage/organizations?query=code==UMPROQ
            paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
            #paging_q = f"?{query}"+search_string
            path1 = pathPattern1+paging_q
            #data=json.dumps(payload)
            url1 = okapi_url + path1
            req = requests.get(url1, headers=okapi_headers)
            idorg=""
            #Search by name
            if req.status_code != 201:
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                if (total_recs!=0):
                    rec=json_str[element]
                    #print(rec)
                    l=rec[0]
                    if 'id' in l:
                        idorg=l['id']
                        return idorg
            #Search by code
                elif (total_recs==0):
                    query=f"query=name=="
                    paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
                    #paging_q = f"?{query}"+orgname
                    path1 = pathPattern1+paging_q
                    #data=json.dumps(payload)
                    url1 = okapi_url + path1
                    req = requests.get(url1, headers=okapi_headers)
                    json_str = json.loads(req.text)
                    total_recs = int(json_str["totalRecords"])
                    if (total_recs!=0):
                        rec=json_str[element]
                        #print(rec)
                        l=rec[0]
                        if 'id' in l:
                            idorg=l['id']
                            return idorg
        except requests.exceptions.HTTPError as err:
            print(f"error Organization GET {err}")
    
    def get_locId(self, searchValue,client):
        try:
            client=SearchClient(client)
            okapi_url=str(client.get('x_okapi_url'))
            okapi_tenant=str(client.get('x_okapi_tenant'))
            okapi_token=str(client.get('x_okapi_token'))
            dic={}
            path1=""        
            #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
            pathPattern1="/locations" #?limit=9999&query=code="
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            length="1"
            start="1"
            element="locations"
            query=f"query=code=="
            #/organizations-storage/organizations?query=code==UMPROQ
            paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
            #paging_q = f"?{query}"+search_string
            path1 = pathPattern1+paging_q
            #data=json.dumps(payload)
            url1 = okapi_url + path1
            req = requests.get(url1, headers=okapi_headers)
            idorg=""
            #Search by name
            if req.status_code != 201:
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                if (total_recs!=0):
                    rec=json_str[element]
                    #print(rec)
                    l=rec[0]
                    if 'id' in l:
                        idorg=l['id']
                        return idorg
            #Search by code
                elif (total_recs==0):
                    query=f"query=code=="
                    paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
                    #paging_q = f"?{query}"+orgname
                    path1 = pathPattern1+paging_q
                    #data=json.dumps(payload)
                    url1 = okapi_url + path1
                    req = requests.get(url1, headers=okapi_headers)
                    json_str = json.loads(req.text)
                    total_recs = int(json_str["totalRecords"])
                    if (total_recs!=0):
                        rec=json_str[element]
                        #print(rec)
                        l=rec[0]
                        if 'id' in l:
                            idorg=l['id']
                            return idorg
        except requests.exceptions.HTTPError as err:
            print(f"error Organization GET {err}")
        

        
       
    def order_closeReason(self,reasonvalue, reasonnote):
        try:
            reason={}    
            reason["reason"]=""
            reason["note"]= ""
            return reason
        except ValueError:
            print("Concat Error")
        
    



 

    def timeStampStringSimple(self,dateTimeObj):
        try:
            fecha_dt = dateTimeObj.strptime(dateTimeObj, "%Y-%m-%d").strftime("%d-%m-%Y")
        #fecha_dt = datetime.strptime(dateTimeObj, '%Y-%m-%d')
        #dateTimeObj = fecha_dt.strftime(format)
            timestampStr = fecha_dt.strftime("%Y-%m-%d")
            return timestampStr
        except ValueError:
            print("Module folioAcqfunctions organizations time Error: "+str(ValueError)) 
    #PRINT FILES
           

    # write a row to the csv file

        
################################################
### NOTES
################################################

class notes():
    def __init__(self):
        self.idNotes= str(uuid.uuid4())
    #(uuidOrg,typeId,customerName,15,16,17)

def print_notes(linkId,typelinkId,**kwargs):
    try:
        notes={}
        notes["typeId"]= kwargs['typeId']
        notes["type"]= kwargs['type']
        notes["domain"]= kwargs['domain']
        notes["title"]= kwargs['title']
        notes["content"]= kwargs['cont']
        notes["links"]= [{"id": linkId,"type": typelinkId}]
        x=0
        #print(notes)
        return notes
        
    except Exception as ee:
        print(f"ERROR: {ee}")
            
       
######END NOTES
    

################################################
### CONTACTS
################################################
class contactsClass():
    
    def __init__(self,contactID,contactfirstName, contactlastName, contactcategories,contactlanguage):
        self.contactid=contactID
        self.contactfirstName= contactfirstName
        self.contactlastName= contactlastName
        self.language= contactlanguage
        self.contactinactive= False
        self.categories=contactcategories
#(kwargs['path1'],contactphoneN, contactemail, contactaddresses, contacturls,contcategories,contactnotes,kwargs['client'])  
    def printcontactsClass(self,path,contactprefix,cont_phone,cont_email, cont_address,cont_urls,cont_categories,contactnotes,fileName):
        #contactFile=open(fileName+"_contacts.json", 'a')
        contacto={
                "prefix": contactprefix,
                "id": self.contactid,
                "firstName": self.contactfirstName,
                "lastName": self.contactlastName,
                "language": self.language,
                "notes": contactnotes,
                "phoneNumbers": cont_phone,
                "emails": cont_email,
                "addresses": cont_address,
                "urls": cont_urls,
                "categories": cont_categories,
                "inactive": self.contactinactive,
           }
        #json_contact = json.dumps(contacto)
        #json_str = json.loads(contacto)
        #print('Datos en formato JSON', json_contact)
        printObject(contacto,path,1,"contacts",False)
        #contactFile.write(json_contact+"\n")
        
 
    
#end
################################################
### INTERFACES
################################################
class interfaces():

    def __init__(self,interUuid, intername, interuri, delivMeth, type):
        self.interid=interUuid
        self.intername = intername
        self.interuri = interuri
        self.deliveryMethod= delivMeth
        self.interavailable=True
        self.intertype=type
        
    def printinterfaces(self, path, fileName,notes,statisticsNotes,statisticsFormat):
        #intFile=open(fileName+"_interfaces.json", 'a')
        dato={
            "id": self.interid,
            "name": self.intername,
            "uri": self.interuri,
            "notes":notes,
            "available":self.interavailable,
            "deliveryMethod": self.deliveryMethod,
            "statisticsFormat": statisticsFormat,
            "locallyStored": "",
            "onlineLocation": "",
            "statisticsNotes": statisticsNotes,
            "type": self.intertype
           }
        printObject(dato,path,1,"interfases",False)
        #json_interfaces = json.dumps(dato)
        #print('Datos en formato JSON', json_str)
        #intFile.write(json_interfaces+"\n")

### CREDENTIALS
    def printcredentials(self, path,idInter, login, passW, fileName):
        #creFile=open(fileName+"_credentials.json", 'a')
        cred ={
            #"id": str(uuid.uuid4()),
            "username": login, 
            "password": passW,
            "interfaceId": idInter
             }
        printObject(cred,path,1,"credentials",False)
        #json_cred = json.dumps(cred)
        #print('Credentials: ', json_cred)
        #creFile.write(json_cred+"\n")
        
    def urltype(self,value):
        urlname=[]
        if value=="1":
            urlname.append("Admin")
            urlname.append("Admin")
        elif value=="2":
            urlname.append("FTP")
            urlname.append("Admin")
        elif value=="3":
            urlname.append("Other")
            urlname.append("Other")
        elif value=="4":
            urlname.append("Statistics")
            urlname.append("End user")
        elif value=="Support":
            urlname.append("Support")
            urlname.append("End user")
        else:
            urlname.append("Other")
            urlname.append("Other")
        return urlname


################################################
### ORGANIZATIONS
################################################
class Organizations():
    def __init__(self,idorg,name,orgcode,vendorisactive,orglanguage,account):
        self.id=idorg
        self.name=name
        self.code=orgcode
        self.language=orglanguage
        self.exportToAccounting= True
        self.status="Active"
        self.isVendor= True
        self.accounts=account
            
    def printorganizations(self,org_desc,org_aliases,org_addresses,org_phoneNum,org_emails,org_urls,org_vendorCurrencies,org_contacts, org_interfaces,org_erpCode,file_name):
        #orgFile=open(fileName+"_organizations.json", 'a')
        organization1 = {
            "id": self.id,
            "name": self.name,
            "code": self.code,
            "erpCode": org_erpCode,
            "description": org_desc,
            "exportToAccounting" : self.exportToAccounting,
            "status": self.status,
            "language": self.language,
            "aliases": org_aliases,
            "addresses": org_addresses,
            "phoneNumbers": org_phoneNum,
            "emails": org_emails,
            "urls": org_urls,
            "contacts": org_contacts,
            #"agreements": org_agreements,
            "vendorCurrencies": org_vendorCurrencies,
            "claimingInterval": 30,
            "discountPercent": 0,
            "expectedInvoiceInterval": 0,
            "renewalActivationInterval": 0,
            "interfaces": org_interfaces,
            "accounts": self.accounts,
            "isVendor": self.isVendor,
            "paymentMethod": "EFT",
            "accessProvider": True,
            "governmental": False,
            "licensor": False,
            "liableForVat": False,
            "materialSupplier": True,
            "expectedActivationInterval": 0,
            "subscriptionInterval": 0,
            "changelogs": []
            }
        x=0
        printObject(organization1,file_name,x,"organization",False)
        #json_organization = json.dumps(organization)
        #print('Datos en formato JSON', json_organization)
        #orgFile.write(json_organization+"\n")    

#end
###############################################
####PURCHASE ORDERS FUNCTIONS
#########################################

def orderDetails(**kwargs):
    try:
        details={}
        for key, value in kwargs.items():
            details[key]=value#receivingNote
        return details
    except ValueError:
        print("Error")

#READ MAPPING FILE

    
    
################################################
### ORGANIZATIONS FUNCTIONS
################################################
def org_aliases(**kwargs):
    alia={}
    aliaR=[]
    x=kwargs['data'].split(",")
    for value in x:
        try:
            alia['value']=value
            alia['description']=""
            aliaR.append(alia)
            alia={}
        except Exception as ee:
            print(f"ERROR: {ee}")
    return aliaR

def org_languages(**kwargs):
    try:
        value=kwargs['value'].upper()
        type=kwargs['type']
        if value=="ENGLISH":
            if type==1:
                valueR="eng"
            elif type==2:
                valueR="eng-us"
        elif value=="SPANISH":
            if type==1:
                valueR="spa"
            elif type==2:
                valueR="es-es"
        elif value=="NULL":
            valueR="eng"
        elif value is None:
            valueR="eng"
        elif value=="":
            valueR=""
        else:
            if type==1:
                valueR="eng"
            elif type==2:
                valueR="eng-us"
        return valueR
    
    except ValueError:
        print("org_addresses Error: "+str(ValueError))
        
def concatfields(dfRow,*argv):
    try:
        concatfield=""
        for arg in argv:
            if dfRow[arg]:
                concatfield=concatfield+"$"+dfRow[arg]
        if len(concatfield)>0:
            return concatfield
        else:
            return None
    except ValueError:
        print("Concat Error")
################################################################
def org_addresses_utm(dfRow, *argv):
    try:
        addr={}
        addrR=[]
        count=1
        for arg in argv:
            addr['addressLine1']=dfRow[arg]
            if dfRow[arg+1]: addr['addressLine2']=dfRow[arg+1]
            if dfRow[arg+2]: addr['city']=dfRow[arg+2]
            if dfRow[arg+3]: addr['stateRegion']=dfRow[arg+3]
            if dfRow[arg+4]: addr['zipCode']=dfRow[arg+4]
            if dfRow[arg+5]: addr['country']=dfRow[arg+5]
            if dfRow[arg+6]: addr['categories']=org_categorie(dfRow[arg+6])
            if dfRow[arg+7]: addr['language']="eng"
            if (count==1): addr["isPrimary"]=True
            count=count+1
            addrR.append(addr)
            addr={}
            return addrR
    except ValueError:
        print("org_addresses Error: "+str(ValueError))
        
def org_addresses_mls(dfRow, *argv, **kwargs):
    try:
        addr={}
        addrR=[]
        count=1
        cat=[]        
        for i in argv:
            if dfRow[i]=="":
                addr['addressLine1']=dfRow[i]
                if dfRow[i+1]: addr['addressLine2']=dfRow[i+1]
                if dfRow[i+2]: addr['city']=dfRow[i+2]
                if dfRow[i+3]: addr['stateRegion']=dfRow[i+3]
                if dfRow[i+4]: addr['zipCode']=dfRow[i+4]
                if dfRow[i+5]: addr['country']=dfRow[i+5]
                if dfRow[i+6] and dfRow[i+6]=="No":
                    addr["isPrimary"]=False
                if dfRow[i+6] and dfRow[i+6]=="Yes":
                    addr["isPrimary"]=True

                if dfRow[i+7]:
                    cadena=str(dfRow[i+7]).strip()
                    c=cadena.find(";")
                    if c > 0:
                        chunked=cadena.split(";")
                        cat.append(get_Id(kwargs['customer'],searchValue=chunked[0].strip(),element="categories"))
                        cat.append(get_Id(kwargs['customer'],searchValue=chunked[1][1:].strip(),element="categories"))
                    else:
                        cat.append(get_Id(kwargs['customer'],search=cadena))
                    addr['categories']=cat 
                addr['language']="eng"
                addrR.append(addr)
                cat=[]
                addr={}
        return addrR
    except ValueError:
        print("org_addresses Error: "+str(ValueError))
        
################################################################        
def org_addresses(dfRow,concat, *argv):
    try:
        if concat:
            addr={}
            addrR=[]
            dir2=""
            dir=""
            cadena=""
            for arg in argv:
                cadena=dfRow[arg]
                if cadena!="$ $ $ $":
                    print(cadena)
                    if len(cadena)>0:
                        x=cadena.count("$")                
                        if (x>0):
                            chunked=cadena.split("$")
                            if (x==1):
                                addr['addressLine1']=chunked[0]
                                addr['addressLine2']=""
                                cadena=chunked[1]
                                if (cadena.find(",")!=-1):
                                    y=cadena.find(",")
                                    addr['city']=cadena[:y]
                                    addr['country']=""
                                    cadena=cadena[y+2:]
                                    if (cadena.find(" ")!=-1):
                                        y=cadena.find(" ")
                                        addr['stateRegion']=cadena[:y]
                                        addr['zipCode']=cadena[y+1:]
                                        addr['categories']=org_categorie("nn")
                                        addr['language']=""
                                        addr["isPrimary"]=True    
                            elif (x==2):
                                addr['addressLine1']=chunked[0]
                                addr['addressLine2']=chunked[1]
                                cadena=chunked[2]
                                if (cadena.find(",")!=-1):
                                    y=cadena.find(",")
                                    addr['city']=cadena[:y]
                                    addr['country']=""
                                    cadena=cadena[y+2:]
                                    if (cadena.find(" ")!=-1):
                                        y=cadena.find(" ")
                                        addr['stateRegion']=cadena[:y]
                                        addr['zipCode']=cadena[y+1:]                     
                                        addr['categories']=org_categorie("nn")
                                        addr['language']=""
                                        addr["isPrimary"]=True    
                            elif (x==3):
                                pass
                            elif (x==4):
                                addr['addressLine1']=chunked[0]
                                addr['addressLine2']=chunked[1]
                                addr['city']=chunked[2]
                                addr['country']=chunked[3]
                                addr['zipCode']=chunked[4]
                                addr['categories']=org_categorie("nn")
                                addr['language']="eng-uk"
                                addr["isPrimary"]=True    
                                
                                #addr['addressLine1']=dfRow[10]
                                #addr['addressLine2']=dfRow[11]
                                #addr['city']=dfRow[12]
                                #addr['stateRegion']=dfRow[13]
                                #addr['zipCode']=dfRow[14]
                                #addr['country']=""
                                #addr['categories']=org_categorie("nn")
                                #addr['language']=""
                                #addr["isPrimary"]=True
                                #addrR.append(addr)

                            elif (x==5):
                                pass
                    else:
                        addr['addressLine1']=dfRow[arg]
                        addr['addressLine2']=""
                        addr['city']=""
                        addr['stateRegion']=""
                        addr['zipCode']=""
                        addr['country']=""
                        addr['categories']=org_categorie("nn")
                        addr['language']="eng"
                        addr["isPrimary"]=True
                    addrR.append(addr)
                    cadena=""
                    addr={}
                return addrR
        
            return addrR
    except ValueError:
            print("org_addresses Error: "+str(ValueError))
        
def org_addresses_trinity(dfRow,concat, *argv):
    try:
        addr={}
        addrR=[]
        dir2=""
        dir=""
        cadena=""
        for arg in argv:
            cadena=dfRow[arg]
            if cadena!="$ $ $ $":
                print(cadena)
                if len(cadena)>0:
                    x=cadena.count("$")                
                    if (x>0):
                        chunked=cadena.split("$")
                        if x==1:
                            addr['addressLine1']=chunked[0]
                            addr['addressLine2']=""
                            cadena=chunked[1]
                            if (cadena.find(",")!=-1):
                                y=cadena.find(",")
                                addr['city']=cadena[:y]
                                addr['country']=""
                                cadena=cadena[y+2:]
                                if (cadena.find(" ")!=-1):
                                    y=cadena.find(" ")
                                    addr['stateRegion']=cadena[:y]
                                    addr['zipCode']=cadena[y+1:]
                                    addr['categories']=org_categorie("nn")
                                    addr['language']=""
                                    addr["isPrimary"]=True    
                        elif x==2:
                            addr['addressLine1']=chunked[0]
                            addr['addressLine2']=chunked[1]
                            cadena=chunked[2]
                            if (cadena.find(",")!=-1):
                                y=cadena.find(",")
                                addr['city']=cadena[:y]
                                addr['country']=""
                                addr['stateRegion']=""
                                addr['zipCode']=cadena[y+1:]                     
                                addr['categories']=org_categorie("nn")
                                addr['language']=""
                                addr["isPrimary"]=True    
                        elif x==3:
                            addr['addressLine1']=chunked[0]
                            addr['addressLine2']=chunked[1]
                            addr['city']=chunked[2]
                            addr['country']=""
                            addr['stateRegion']=""
                            addr['zipCode']=chunked[3]           
                            addr['categories']=[]
                            addr['language']=""
                            addr["isPrimary"]=True  
                        elif x==4:
                            addr['addressLine1']=chunked[0]
                            addr['addressLine2']=chunked[1]
                            addr['city']=chunked[2]
                            addr['country']=chunked[3]
                            addr['zipCode']=chunked[4]
                            addr['categories']=org_categorie("nn")
                            addr['language']="eng-uk"
                            addr["isPrimary"]=True    
                        elif x==5:
                            addr['addressLine1']=chunked[0]
                            addr['addressLine2']=chunked[1]
                            addr['city']=chunked[2]
                            addr['country']=chunked[3]
                            addr['zipCode']=chunked[4]
                            addr['categories']=org_categorie("nn")
                            addr['language']="eng-uk"
                            addr["isPrimary"]=True 
                else:
                    addr['addressLine1']=dfRow[arg]
                    addr['addressLine2']=""
                    addr['city']=""
                    addr['stateRegion']=""
                    addr['zipCode']=""
                    addr['country']=""
                    addr['categories']=org_categorie("nn")
                    addr['language']="eng"
                    addr["isPrimary"]=True
                addrR.append(addr)
                cadena=""
                addr={}
        return addrR                            
    except ValueError:
            print("org_addresses Error: "+str(ValueError))
            
def org_phoneNumbers(dfRow,*argv):
    pho={}
    phoR=[]
    count=1
    for arg in argv:
        #print("argumentos de *argv:", row[arg])
        if len(dfRow[arg])>0:
            if dfRow[arg]: pho["phoneNumber"]=dfRow[arg]
            if dfRow[arg+1]: pho["type"]="Office"
            if dfRow[arg+2]: pho["language"]="eng" 
            if dfRow[arg+3]: pho["categories"]=[]
            if (count==1): pho["isPrimary"]= True
            count=count+1
            phoR.append(pho)
            pho={}
    return phoR


def org_emails(dfRow,*argv):
    emai={}
    emaiR=[]
    count=1
    for arg in argv:
        #print("argumentos de *argv:", row[arg])
        if len(dfRow[arg])>0:
            if dfRow[arg]:   emai['value']=dfRow[arg] 
            if dfRow[arg+1]: emai['description']=dfRow[arg+1]
            if dfRow[arg+2]: emai['language']="eng"
            if dfRow[arg+3]: emai['categories']=org_categorie(dfRow[arg+3])
            if (count==1): emai['isPrimary']=True
            count=count+1
            emaiR.append(emai)
            emai={}
    return emaiR

def dic(**kwargs):
    try:
        details={}
        for key, value in kwargs.items():
            details[key]=value
        return details
    except ValueError:
        print("Error")

def org_urls(dfRow,*argv):
    urls={}
    urlsR=[]
    for arg in argv:
        #print("argumentos de *argv:", row[arg])
        if len(dfRow[arg])>0:
            if (dfRow[arg].find("http://")!=-1 or dfRow[arg].find("https://")!=-1): 
                urls['value']=dfRow[arg]
            else:
                urls['value']="http://"+dfRow[arg]
                urls['notes']=""#dfRow[arg]
            if dfRow[arg+1]: urls['description']=dfRow[arg+1]
            if dfRow[arg+2]: urls['language']="eng"
            if dfRow[arg+3]: urls['categories']=org_categorie(dfRow[arg+3])
            if dfRow[arg+4]: urls['notes']=""
            urlsR.append(urls)
            urls={}
    return urlsR

def org_contacts(dfRow, *argv):
    contactsId=[]
    person={}
    for arg in argv:
        #print("argumentos de *argv:", row[arg])
        if len(dfRow[arg])>0:
            if dfRow[arg]:
                contactprefix= dfRow[arg]
                contactName_temp=str(dfRow[arg])+" "+str(dfRow[arg])
                ContactName=SplitString(contactName_temp)
                FN=str(ContactName[0])
                LN=str(ContactName[1])
            else:
                FN="NaN"
                LN="NaN"
                #Title go to notes and categorical
                #contactTitle="NULL"
                #if namesheet.cell_value(c,4)!="NULL":
                #    contactTitle=str(namesheet.cell_value(c,4))
                #address
            contactLang="en-us"
            contactnotes=""
            addcontnote=True
            if addcontnote:
                if dfRow[arg]:
                    contactnotes=contact_notes(dfRow[arg])

            addcono=True
            if addcono:
                if dfRow[arg]:
                    contactnotes= dfRow[arg]
            #Contacts phone
            contactphoneN=[]
            addpho=True
            if addpho:
                contactphoneN=""
                contactphoneN=org_phoneNumbers(dfRow[arg],23,31,35,39,)
            #Contact emails
            contactemail=[]
            addmails=True
            if addmails:
                contactemail=""
                contactemail=org_emails(dfRow[arg],15,23)

            #Contact Address
            contactaddresses=[]
            addadd=False
            if addadd:
                contactaddresses=""
                contactaddresses=org_addresses(dfRow[arg],47)

            #INACTIVE / ACTIVE
            contactinactive= False
            #Contact URL
            contacturls=[]
            addurl=False
            if addurl:
                contacturls="" 
                contacturls=org_urls(dfRow[arg],43)
                
            contcategories=[]
            if dfRow[6]:
                contcategories=org_categorie(dfRow[arg])
            conID=str(uuid.uuid4())
            contactsId.append(conID)
            #(self,contactID,contactfirstName, contactlastName, contactcategories):
            ctc=contactsClass(conID,FN,LN,contcategories,contactLang)
            #def printcontacts(self,cont_phone,cont_email, cont_address,cont_urls,cont_categories,contactnotes,fileName):
            ctc.printcontactsClass(contactprefix,contactphoneN, contactemail, contactaddresses, contacturls,contcategories,contactnotes,customerName)  
    return contactsId


def org_account(dfRow,*argv):
    accou={}
    accouR=[]
    for arg in argv:
        print("argumentos de *argv:", dfRow[arg])
        accouR.append(accou)
        accou={}
    return accouR

def org_acqunit(dfRow,*argv):
    acqunit={}
    acqunitR=[]
    for arg in argv:
        print("argumentos de *argv:", dfRow[arg])
        acqunit.append(acqunit)
        acquint={}
    return acqunit

def org_agreements(dfRow,*argv):
    agre={}
    for arg in argv:            
        agre["name"]= "History Follower Incentive"
        agre["discount"]= 10
        agre["referenceUrl"]= "http://my_sample_agreement.com"
        agre["notes"]= "note"
    return agre


def contact_notes(dfRow,*argv):
    nt=""
    for arg in argv:
        if (dfRow.find(' - ') !=-1):
            result=dfRow.find(' - ')
            nt=dfRow[result+3:]
        elif (dfRow.find(' -- ') !=-1):
            result=dfRow.find(' -- ')
            nt=dfRow[result+3:]
        elif (dfRow.find('; ') !=-1):
            result=dfRow.find('; ')
            nt=dfRow[result+2:]
        elif (dfRow.find(' | ') !=-1):
            result=dfRow.find(' | ')
            nt=dfRow[result+3:]
        elif (dfRow.find(' / ') !=-1):
            result=dfRow.find(' / ')
            nt=dfRow[result+3:]
        elif (dfRow.find(', ') !=-1):
            result=dfRow.find(', ')
            nt=dfRow[result+2:]
        else:
            nt=dfRow[arg]
    return nt

def org_categorie(valueA):
    catego=[]
    
    if valueA=="company URL":
        catego.append("d963c6fa-7aa8-4b65-8f64-5f119ef17cd1")
    elif valueA=="Office":
        catego.append("6a60106e-6ffb-4e02-a872-f6941f76245e")
    elif valueA=="Fax":
        catego.append("d78d4e2e-11f9-4397-971e-300cb3dd8522")
    elif valueA=="nn":
        catego=[] #GENERAL
    else:
        value=cat(valueA)
        if len(value)>0:
            catego.append(value)
    return catego
#end

def get_licId1(orgname):
        dic={}
        #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
        #https://okapi-macewan.folio.ebsco.com/licenses/licenses?stats=true&term=Teatro Español del Siglo de Oro&match=name
        pathPattern="/licenses/licenses" #?limit=9999&query=code="
        okapi_url="https://okapi-macewan.folio.ebsco.com"
        okapi_token="eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsInVzZXJfaWQiOiI4MjEzODdhZS1hNzkxLTQ5NTgtYTg3ZS1jYTFmMDE2NzA2YmUiLCJpYXQiOjE2MTA5MzAwMjEsInRlbmFudCI6ImZzMDAwMDEwMzcifQ.ygLWuFDNUT8No5TF6FD9NNRpNk4Z_iSRVmPmxaH_UsE"
        okapi_tenant="fs00001037"
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        length="1"
        start="1"
        element="organizations"
        query=f"?stats=true&term="
        #/organizations-storage/organizations?query=code==UMPROQ
        paging_q = f"{query}"+orgname+"&match=name"
        path = pathPattern+paging_q
        #data=json.dumps(payload)
        url = okapi_url + path
        req = requests.get(url, headers=okapi_headers)
        idorg=[]
        if req.status_code != 201:
            json_str = json.loads(req.text)
            total_recs = int(json_str["totalRecords"])
            if (total_recs!=0):
                #print('Datos en formato JSON',json.dumps(json_str))
                rec=json_str["results"]
                #print(json_str)
                l=rec[0]
                if 'id' in l:
                    idorg.append(l['id'])
                    #idorg.append(l['name'])
        if len(idorg)==0:
            return "00000-000000-000000-00000"
        else:
            return idorg
        
def urlValidator(value):
    try:
        valid=False
        #valid=validator.url(str(value))
        if (value.find("http")!= -1):
            #print("Url is valid only for folio ")
            valid=True
        return valid
    except ValueError:
        print("error")
        
def is_empty(data_structure):
    if data_structure:
            #print("No está vacía")
            return False
    else:
            #print("Está vacía")
            return True

def interfacetype(categ):
    try:
        catego=[]
        if (categ.find('Admin') != -1): catego.append("Admin")
        if (categ.find('Statistics') != -1): catego.append("Admin")
        if (categ.find('End user') != -1): catego.append("End user")
        if (categ.find('Other') != -1): catego.append("Other")
        if (categ.find('Report') != -1): catego.append("Reports")
        if (categ.find('Orders') != -1): catego.append("Orders")
        if (categ.find('Invoices') != -1): catego.append("Invoices")
        if categ=="": catego.append("Other")
        return catego
    except Exception as ee:
        print(f"ERROR: {ee}")      


def floatHourToTime(fh):
    h, r = divmod(fh, 1)
    m, r = divmod(r*60, 1)
    return (
        int(h),
        int(m),
        int(r*60),
    )
    
def exitfile(arch):    
    if os.path.isfile(arch):
        print ("File exist")
        os.remove(arch)
    else:
        print ("File not exist")


def search(fileB,code_search):
    idlicense=""
    foundc=False
    with open(fileB,'r',encoding = 'utf-8') as h:
        for lineh in h:
            if (lineh.find(code_search) != -1):
                #print(lineh)
                foundc=True
                if (foundc):                    
                    idlicense=lineh[8:44]
                    break
    if (foundc):
        return idlicense
    else:
        idlicense="No Vendor"
        return idlicense

def SearchJsonFile_UTM(code_search,schema):
        # Opening JSON file
        dic =""
        f = open("UTM/categories.json",)
        data = json.load(f)
        for i in data[schema]:
            a_line=str(i)
            if i['value'] == code_search:
            #if (a_line.find(code_search) !=-1):
                 dic=i['id']

                 break
        f.close()
        return dic
    
def SearchJsonFile(code_search,code_return,**kwargs ):
        # Opening JSON file
        dic =""
        f = open(kwargs['filetosearch'],)
        data = json.load(f)
        for i in data[kwargs['schema']]:
            if i[kwargs['field']] == code_search:
            #if (a_line.find(code_search) !=-1):
                 dic=i[code_return]

                 break
        f.close()
        return dic
    
def SplitString(string_to_split):
    string_fn=""
    string_ln=""
    if (string_to_split.find('@') !=-1):
            result=string_to_split.find('@')
            largo=len(string_to_split)
            #print("largo:", largo)
            #print("position @:",result)
            string_fn=" "
            string_ln=string_to_split[0:result]
    elif (string_to_split.find(' ') !=-1):
            largo=len(string_to_split)
            result=string_to_split.find(' ')
            #print("Large:", largo)
            #print("position blank:",result)
            string_fn=string_to_split[0:result]
            string_ln=string_to_split[result+1:largo]
            if (string_ln.find(' - ') !=-1):
                result=string_ln.find(' - ')
                #print("Large:", largo)
                #print("position blank:",result)
                string_ln=string_ln[:result]
            elif (string_ln.find(', ') !=-1):
                result=string_ln.find(', ')
                #print("Large:", largo)
                #print("position blank:",result)
                string_ln=string_ln[:result]


    else:
            #print("is not last name, first name")    
            string_fn=" "
            string_ln=string_to_split
    return string_fn, string_ln

###########################
### LICENCES FUNCTIONS
##########################

def get_licId(licToSearch,okapi_url,okapi_token,okapi_tenant):
    try:
        dic={}
        #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
        #https://okapi-macewan.folio.ebsco.com/licenses/licenses?stats=true&term=Teatro Español del Siglo de Oro&match=name
        pathPattern="/licenses/licenses" #?limit=9999&query=code="
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        length="1"
        start="1"
        element="organizations"
        query=f"?stats=true&term="
        #/organizations-storage/organizations?query=code==UMPROQ
        paging_q = f"{query}"+licToSearch+"&match=name"
        path = pathPattern+paging_q
        #data=json.dumps(payload)
        url = okapi_url + path
        req = requests.get(url, headers=okapi_headers)
        idorg=""
        if req.status_code != 201:
            json_str = json.loads(req.text)
            total_recs = int(json_str["totalRecords"])
            if (total_recs!=0):
                #print('Datos en formato JSON',json.dumps(json_str))
                rec=json_str["results"]
                #print(json_str)
                l=rec[0]
                if 'id' in l:
                    idorg=l['id']
                    #idorg.append(l['name'])
                    return idorg
    except ValueError as error:
            print("Error: %s" % error)

######END

###################
###NOTES
###########################
### FUNDS
#############

def readfunds(path,sheetName,customerName):
        try:
            be={"AnnisWaterResearchCenter":"AWRI",
                             "BrooksCollegeofInterdisciplinaryStudies":"BCOIS",
                             "ClinicalLaboratorySciences":"CLS",
                             "CollegeofCommunityandPublicServices":"CCPS",
                             "CollegeofEducation":"COE",
                             "CollegeofEducationFunding":"COE$",
                             "CollegeofHealthProfessions":"CHS",
                             "CollegeofLiberalArtsandSciences":"CLAS",
                             "GeneralFunds":"GEN",
                             "InterlibraryLoanBookPurchases":"ILL",
                             "JuvenileMaterials":"JUV",
                             "KirkhofCollegeofNursing":"KCON",
                             "PadnosCollegeofEngineeringandComputing-Computer Science":"PCECC",
                             "PadnosCollegeofEngineeringandComputing-Engineering":"PCECE",
                             "SeidmanCollegeofBusiness":"SCB"
                             }
            funds= readFileToDataFrame(path,orderby="",distinct=[])            
            count=1
            for c, row in funds.iterrows():
                cp={}
                if row[0]:
                    searchvalue=row[0]
                    budgetId=get_Id(customerName,searchValue=searchvalue,element="budgets")
                    searchvalue=row[1]
                    searchvalue=searchKeysByVal(be, searchvalue)      
                    expId=get_Id(customerName,searchValue=searchvalue,element="expenseClasses")
                    cp["id"]=str(row[2])
                    cp["budgetId"]=budgetId
                    cp["expenseClassId"]=expId
                    cp["status"]="Active"
                    count+=1
                    
                    printObject(cp,path,count)
        except ValueError as error:
            print("Error: %s" % error)                 


def get_Id(customerName, **kwargs):
    try:
        #print(kwargs)
        pathPattern1=okapiPath(kwargs['element'])
        element=kwargs['element']
        pathPattern=pathPattern1[0]
        searchValue=kwargs['searchValue']
        client=SearchClient(customerName)
        okapi_url=str(client.get('x_okapi_url'))
        okapi_tenant=str(client.get('x_okapi_tenant'))
        okapi_token=str(client.get('x_okapi_token'))
        dic={}
        path1=""        
        #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
        #pathPattern1="/organizations/organizations" #?limit=9999&query=code="
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        length="1"
        start="1"
        #element="organizations"
        #query=f"query=id=="
        #query=f"/"
        #/organizations-storage/organizations?query=code==UMPROQ
        #paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
        #paging_q = f"?{query}"+search_string
        paging_q = f"/{searchValue}"
        path1 = pathPattern+paging_q
        #data=json.dumps(payload)
        url1 = okapi_url + path1
        req = requests.get(url1, headers=okapi_headers)
        idorg=""
        #Search by name
        if req.status_code == 200:
            json_str = json.loads(req.text)
            #total_recs = int(json_str["totalRecords"])
            #if (total_recs!=0):
            #    rec=json_str[element]
                #print(rec)
            #    l=rec[0]
            #    if 'id' in l:
            #        idorg=l['id']

            return False
        else:
            return True
            #Search by code
            #elif (total_recs==0):
            #    query=kwargs['query']  #f"query=name=="
            #    paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
            #    #paging_q = f"?{query}"+orgname
            #    path1 = pathPattern+paging_q
                #data=json.dumps(payload)
            #    url1 = okapi_url + path1
            #    req = requests.get(url1, headers=okapi_headers)
            #    json_str = json.loads(req.text)
            #    total_recs = int(json_str["totalRecords"])
            #    if (total_recs!=0):
            #        rec=json_str[element]
                    #print(rec)
            #        l=rec[0]
            #        if 'id' in l:
            #            idorg=l['id']

            #            return idorg
    except requests.exceptions.HTTPError as err:
        return True
        print("error Organization GET{searchValue}")
        

        
def get_Id_with_values(customerName, **kwargs):
    try:
        #print(kwargs)
        pathPattern1=okapiPath(kwargs['element'])
        element=kwargs['element']
        searchValue=kwargs['searchValue']
        pathPattern=pathPattern1[0]
        client=SearchClient(customerName)
        okapi_url=str(client.get('x_okapi_url'))
        okapi_tenant=str(client.get('x_okapi_tenant'))
        okapi_token=str(client.get('x_okapi_token'))
        dic={}
        path1=""        
        #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
        #pathPattern1="/organizations/organizations" #?limit=9999&query=code="
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        length="1"
        start="1"
        #element="organizations"
        query="query="+kwargs['query']+"="
        #/organizations-storage/organizations?query=code==UMPROQ
        paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
        #paging_q = f"?{query}"+search_string
        path1 = pathPattern+paging_q
        #data=json.dumps(payload)
        url1 = okapi_url + path1
        req = requests.get(url1, headers=okapi_headers)
        idorg=""
        #Search by name
        if req.status_code != 201:
            json_str = json.loads(req.text)
            total_recs = int(json_str["totalRecords"])
            if (total_recs!=0):
                rec=json_str[element]
                #print(rec)
                l=rec[0]
                if 'id' in l:
                    idorg=l['id']

                    return idorg
    except requests.exceptions.HTTPError as err:
        print("error Organization GET")
        
def get_Id_value(customerName, **kwargs):
    try:
        #print(kwargs)
        pathPattern1=okapiPath(kwargs['element'])
        element=kwargs['element']
        pathPattern=pathPattern1[0]
        searchValue=kwargs['searchValue']
        client=SearchClient(customerName)
        okapi_url=str(client.get('x_okapi_url'))
        okapi_tenant=str(client.get('x_okapi_tenant'))
        okapi_token=str(client.get('x_okapi_token'))
        dic={}
        path1=""        
        #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
        #pathPattern1="/organizations/organizations" #?limit=9999&query=code="
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        length="1"
        start="1"
        #element="organizations"
        query=f"query=value=="
        #/organizations-storage/organizations?query=code==UMPROQ
        paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
        #paging_q = f"?{query}"+search_string
        path1 = pathPattern+paging_q
        #data=json.dumps(payload)
        url1 = okapi_url + path1
        req = requests.get(url1, headers=okapi_headers)
        idorg=""
        #Search by name
        if req.status_code != 201:
            json_str = json.loads(req.text)
            total_recs = int(json_str["totalRecords"])
            if (total_recs!=0):
                rec=json_str[element]
                #print(rec)
                l=rec[0]
                if 'id' in l:
                    idorg=l['id']

                    return idorg
            #Search by code
            elif (total_recs==0):
                query=f"query=name=="
                paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
                #paging_q = f"?{query}"+orgname
                path1 = pathPattern+paging_q
                #data=json.dumps(payload)
                url1 = okapi_url + path1
                req = requests.get(url1, headers=okapi_headers)
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                if (total_recs!=0):
                    rec=json_str[element]
                    #print(rec)
                    l=rec[0]
                    if 'id' in l:
                        idorg=l['id']

                        return idorg
    except requests.exceptions.HTTPError as err:
        print("error Organization GET")
                        
def get_Id_value(customerName, **kwargs):
    try:
        #print(kwargs)
        pathPattern1=okapiPath(kwargs['element'])
        element=kwargs['element']
        pathPattern=pathPattern1[0]
        searchValue=kwargs['searchValue']
        client=SearchClient(customerName)
        okapi_url=str(client.get('x_okapi_url'))
        okapi_tenant=str(client.get('x_okapi_tenant'))
        okapi_token=str(client.get('x_okapi_token'))
        dic={}
        path1=""        
        #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
        #pathPattern1="/organizations/organizations" #?limit=9999&query=code="
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        length="1"
        start="1"
        #element="organizations"
        query=f"query=value=="
        #/organizations-storage/organizations?query=code==UMPROQ
        paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
        #paging_q = f"?{query}"+search_string
        path1 = pathPattern+paging_q
        #data=json.dumps(payload)
        url1 = okapi_url + path1
        req = requests.get(url1, headers=okapi_headers)
        idorg=""
        #Search by name
        if req.status_code != 201:
            json_str = json.loads(req.text)
            total_recs = int(json_str["totalRecords"])
            if (total_recs!=0):
                rec=json_str[element]
                #print(rec)
                l=rec[0]
                if 'id' in l:
                    idorg=l['id']

                    return idorg
            #Search by code
            elif (total_recs==0):
                query=f"query=name=="
                paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
                #paging_q = f"?{query}"+orgname
                path1 = pathPattern+paging_q
                #data=json.dumps(payload)
                url1 = okapi_url + path1
                req = requests.get(url1, headers=okapi_headers)
                json_str = json.loads(req.text)
                total_recs = int(json_str["totalRecords"])
                if (total_recs!=0):
                    rec=json_str[element]
                    #print(rec)
                    l=rec[0]
                    if 'id' in l:
                        idorg=l['id']

                        return idorg
    except requests.exceptions.HTTPError as err:
        print("error Organization GET")

def itemstorageitems(customerName, **kwargs):
    try:
        #print(kwargs)
        pathPattern1=okapiPath(kwargs['element'])
        element=kwargs['element']
        pathPattern=pathPattern1[0]
        searchValue=kwargs['searchValue']
        client=SearchClient(customerName)
        okapi_url=str(client.get('x_okapi_url'))
        okapi_tenant=str(client.get('x_okapi_tenant'))
        okapi_token=str(client.get('x_okapi_token'))
        dic={}
        path1=""        
        #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
        #pathPattern1="/organizations/organizations" #?limit=9999&query=code="
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        length="1"
        start="1"
        limit=1
        #element="organizations"
        #query=f"query=barcode=="
        #/organizations-storage/organizations?query=code==UMPROQ
        paging_q = kwargs['query']
        #paging_q = f"?{query}"+search_string
        path1 = f"{pathPattern}?limit={limit}&{paging_q}{searchValue}"
        #data=json.dumps(payload)
        url1 = f"{okapi_url}{path1}"
        req = requests.get(url1, headers=okapi_headers)
        item={}
        itemrec=[]
        #Search by name
        if req.status_code != 201:
            json_str = json.loads(req.text)
            total_recs = int(json_str["totalRecords"])
            if (total_recs!=0):
                rec=json_str[element]
                #print(rec)
                item=rec[0]
                if 'id' in item:
                    #print(item['id'])
                    itemrec.append(item['id'])
                if 'effectiveLocationId' in item:
                    itemrec.append(item['effectiveLocationId'])
        return itemrec
    except Exception as err:
        print("error ", str(err))        

def usersStorage(customerName, **kwargs):
    try:
        #print(kwargs)
        pathPattern1=okapiPath(kwargs['element'])
        element=kwargs['element']
        pathPattern=pathPattern1[0]
        searchValue=kwargs['searchValue']
        searchValue2=kwargs['searchValue2']
        client=SearchClient(customerName)
        okapi_url=str(client.get('x_okapi_url'))
        okapi_tenant=str(client.get('x_okapi_tenant'))
        okapi_token=str(client.get('x_okapi_token'))
        dic={}
        path1=""        
        #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
        #pathPattern1="/organizations/organizations" #?limit=9999&query=code="
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        length="1"
        start="1"
        limit=9999
        #element="organizations"
        #query=f"query=barcode=="
        #/organizations-storage/organizations?query=code==UMPROQ
        paging_q = kwargs['query']
        if paging_q.find("personal.lastName")==-1:
        #paging_q = f"?{query}"+search_string
            path1 = f"{pathPattern}?limit={limit}&{paging_q}={searchValue}"
        else:
            path1 = f"{pathPattern}?limit={limit}&{paging_q}"
        #data=json.dumps(payload)
        url1 = f"{okapi_url}{path1}"
        req = requests.get(url1, headers=okapi_headers)
        item={}
        itemrec=[]
        #Search by name
        if req.status_code == 400:
            itemrec=[]
        elif req.status_code != 201:
            json_str = json.loads(req.text)
            #print(json_str)
            total_recs = int(json_str["totalRecords"])
            if (total_recs==1):
                rec=json_str[element]
                #print(rec)
                item=rec[0]
                itemrec.append(item['id'])
                itemrec.append(item['username'])
                if 'barcode' in item: 
                    itemrec.append(item['barcode'])
                else:
                    itemrec.append(item['username'])
                itemrec.append(total_recs)
            elif (total_recs>1):
                a=0
                itemrec=[]
                data = json.loads(req.text)
                for i in data['users']:
                    #a_line=str(i)
                    username=i['username']
                    #print(i)
                    if username.find(searchValue2[:7])!=-1:  
                        #print(req.text)
                        itemrec.append(i['id'])
                        itemrec.append(i['username'])
                        if 'barcode' in i: 
                            itemrec.append(i['barcode'])
                        else:
                            itemrec.append(i['username'])
                        itemrec.append(1)
                        break
            else:
               itemrec=[]               
        if itemrec is None:
            itemrec=[]
        return itemrec
    except Exception as err:
        print("error ", str(err))

     
def get_Id1(customerName, **kwargs):
    try:
        #print(kwargs)
        pathPattern1=okapiPath(kwargs['element'])
        element=kwargs['element']
        pathPattern=pathPattern1[0]
        searchValue=kwargs['searchValue']
        client=SearchClient(customerName)
        okapi_url=str(client.get('x_okapi_url'))
        okapi_tenant=str(client.get('x_okapi_tenant'))
        okapi_token=str(client.get('x_okapi_token'))
        dic={}
        path1=""        
        #pathPattern="/organizations-storage/organizations" #?limit=9999&query=code="
        #pathPattern1="/organizations/organizations" #?limit=9999&query=code="
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        length="1"
        start="1"
        #element="organizations"
        query=f"query=username=="
        #/organizations-storage/organizations?query=code==UMPROQ
        paging_q = f"?{query}"+'"'+f"{searchValue}"+'"'
        #paging_q = f"?{query}"+search_string
        path1 = pathPattern+paging_q
        #data=json.dumps(payload)
        url1 = okapi_url + path1
        req = requests.get(url1, headers=okapi_headers)
        idorg=""
        #Search by name
        if req.status_code != 201:
            json_str = json.loads(req.text)
            total_recs = int(json_str["totalRecords"])
            if (total_recs!=0):
                rec=json_str[element]
                #print(rec)
                l=rec[0]
                if kwargs['id'] in l:
                    idorg=l['barcode']
            #Search by code
        return idorg   
    except Exception as err:
        print("error ", str(err))
        idorg="nobarcode"
        return idorg
    
def readJsonfile_1(path,json_file,schema):
    try:
        f = open(json_file)
        data = json.load(f)
        count=0
        con={}
        lic={}
        for i in data[schema]:
            count+=1
            print("record: "+str(count))
            j_content=i
            id=j_content['id']
            if j_content['customProperties']['InterlibraryLoan'] is not None:
                interlibraryLoanId=""
                interlibraryLoaninternal=""
                interlibraryLoanvalue=""
                interlibraryLoanId=j_content['customProperties']['InterlibraryLoan'][0]['id']
                interlibraryLoaninternal=j_content['customProperties']['InterlibraryLoan'][0]['internal']
                interlibraryLoanvalue=j_content['customProperties']['InterlibraryLoan'][0]['value']
                lic['id']=str(id)
                lic['customProperties']={"InterlibraryLoan":[{"id": interlibraryLoanId ,"internal":False}]}
                printObject(lic,path,str(count),"Inter_lic_to_change",False)
            if j_content['customProperties']['ConcurrentUsers'] is not None:
                interlibraryLoanId=""
                interlibraryLoaninternal=""
                interlibraryLoanvalue=""
                interlibraryLoanId=j_content['customProperties']['ConcurrentUsers'][0]['id']
                interlibraryLoaninternal=j_content['customProperties']['ConcurrentUsers'][0]['internal']
                interlibraryLoanvalue=j_content['customProperties']['ConcurrentUsers'][0]['value']
                con['id']=str(id)
                con['customProperties']={"ConcurrentUsers":[{"id": interlibraryLoanId ,"internal":False}]}
                printObject(con,path,str(count),"concurrent_lic_to_change",False)
    except Exception as err:
        print("error ", str(err))

def readJsonfile_Cornell(path,json_file,schema):
    try:
        f = open(json_file)
        data = json.load(f)
        count=0
        con={}
        lic={}
        for i in data[schema]:
            count+=1
            print("record: "+str(count))
            j_content=i
            #print(j_content)
            #print(j_content['compositePoLines'][0]['acquisitionMethod'])
            c=0
            if j_content['compositePoLines'][0]['acquisitionMethod']=="Approval":
               printObject(j_content,path,str(count),"cornell_purchaseOrders_approvalPlan",False)               
    except Exception as err:
        print("error ", str(err))

def readJsonfile_mls(path,json_file,schema):
    try:
        f = open(path+"/"+json_file)
        data = json.load(f)
        count=0
        con={}
        lic={}
        for i in data[schema]:
            count+=1
            print("record: "+str(count))
            j_content=i
            id=j_content['code']
            name=j_content['name']
            printObject(id+","+name,path,str(count),"michigan_location_codes",False)
    except Exception as err:
        print("error ", str(err))

def jsontotupla(**kwargs):
    tupla=[]
    if 'schema' in kwargs:
        json_file=kwargs['json_file']
        with open(json_file, "r", encoding="utf") as file_j:
            data = json.load(file_j)
        schema=kwargs['schema']
        for i in data[schema]:
            try:
                if 'id' in i: 
                    id=i['id']
                else: 
                    id=""
                if 'name' in i:
                    name=i['name']
                else: 
                    name=""
                if 'code' in i: 
                    code=i['code']
                else: 
                    if name:
                        code=name
                    else:
                        code=""
                    
                if 'value' in i: 
                    value=i['value']
                else: 
                    if code:
                        value=code
                    else:
                        value=""
                tupla.append([id,code,name,value,data])
                #if schema=="organizations":
                #    print(tupla)
            except Exception as err:
                print("error ", str(err))#print(tupla)

    return tupla


    
def readJsonfile(path,json_file,schema,toSearch,fielTosearch):
    try:
        filetoload=f"{path}/{json_file}"
        f = open(filetoload,encoding='utf-8')
        data = json.load(f)
        count=0
        con={}
        lic={}
        id=[]
        for i in data[schema]:
            count+=1
            j_content=i
            if j_content[fielTosearch].upper()==toSearch.upper():
                id.append(j_content['id'])
                if "name" in j_content:
                    id.append(j_content['name'])#return j_content
                    
                elif "code" in j_content:
                    id.append(j_content['code'])

                elif "type" in j_content:
                    id.append(j_content['type'])
                elif "id" in j_content:
                    id.append(j_content['id'])
        if len(id)>0:
            return id
        else:
            return None
    except Exception as err:
        print(f"INFO error {err}")
        return None

def readJsonfileRetor(path,json_file,schema,toSearch,fielTosearch):
    try:
        filetoload=f"{path}/{json_file}"
        f = open(filetoload, encoding="utf-8")
        data = json.load(f)
        count=0
        con={}
        lic={}
        for i in data[schema]:
            count+=1
            j_content=i
            if j_content[fielTosearch]==toSearch:
                id=j_content['id']
                return id
    except Exception as err:
        print(f"INFO error {err}")
        return None
        
def readJsonfile_identifier(path,json_file,schema,toSearch,tovalue):
    try:
        f = open(path+"/"+json_file)
        data = json.load(f)
        count=0
        con={}
        lic={}
        sw=False
        for i in data[schema]:
            count+=1
            j_content=i
            if j_content['title']==toSearch:
                if len(j_content['identifiers'])>0:
                    for x in j_content['identifiers']:
                        if x['value']==tovalue:
                            sw= True
        if sw:
           return sw
        else:
            return None
    except Exception as err:
        return None
        print("error ", str(err))
    
def readJsonfile_identifier(path,json_file,schema):
    try:
        f = open(path+"/"+json_file)
        data = json.load(f)
        count=0

        lic=""
        sw=False
        for i in data[schema]:
            count+=1
            j_content=i
            lic=j_content['id']
            print(f"record:{count}")
            with open(path+"/Trinity_idtodelete.txt","a+") as outfile:
                outfile.write(lic+"\n")
        print(f"end")
    except Exception as err:
        return None
        print("error ", str(err))

def readJsonfile_fund(path,json_file,schema,toSearch,fielTosearch):
    try:
        f = open(path+"/"+json_file)
        data = json.load(f)
        count=0
        con={}
        lic={}
        id=[]
        for i in data[schema]:
            count+=1
            j_content=i
            if j_content[fielTosearch]==toSearch:
                id.append(j_content['id'])
                id.append(j_content['code'])
                return id
    except Exception as err:
        return None
        print("error ", str(err))





def importUsers(path,file_name,sheetName,out_file):
    try:
        df= importDataFrame(file_name,orderby="",distinct=[],delimiter="",sheetName="")
        users=[]
        patrons={}
        x=0
        for i, row in df.iterrows():
            rec={}
            if row['RUT']:
                varrut=str(row['RUT']).strip()
                varrut = varrut.replace(' ',"")
                rec["username"]= varrut
            if str(row['STATUS(VIGENTE)']).strip() == "Vigente":
                rec["active"]= True
            else : rec["active"]= False
            a=[]
            rec["externalSystemId"]= str(row['EMAIL'])
            rec["barcode"]= str(row['ID_CREDENCIAL'])
            rec["departments"]=[]
            rec["proxyFor"]=[]
            rec['type'] = "Patron"
            #print (rec['type'])
            if row["tipo"]== "P":
                          rec["patronGroup"]="9464bf39-f3af-4772-b8de-f61bba801863"
            elif row["tipo"] == "F":  
                          rec["patronGroup"]="4e261356-72b3-4ef7-a78c-746ebbe66c1c"     
            else :  rec["patronGroup"]="8a43c885-7559-4e7d-812a-95ea7a498522"
            rec["personal"]= { 
                                "lastName":str(row['APELLIDOS']),
                                "firstName": str(row['NOMBRES']),
                                "phone":str(row['TELEFONO']),
                                "mobilePhone":"",
                                "email":str(row['EMAIL']),
                                
                                "addresses": [
                                    {
                                        "countryId": "CL",
                                        "addressLine1": row['DIRECCION'],
                                        "addressLine2": "",
                                        "city": "",
                                        "region": "",
                                        "postalCode": "",
                                        "addressTypeId": "93d3d88d-499b-45d0-9bc7-ac73c3a19880",
                                        "primaryAddress": True
                                    }
                                ],
                                "preferredContactTypeId": "002"
                                }
            rec["customFields"]= {
                "rut": str(row['RUT']),
                "nombrecarrera": str(row['NOMBRE_CARRERA']),
                "genero": str(row['GENERO']),
                "campus": str(row['CAMPUS']),
                "codcarrera": str(row['COD_CARRERA']),
                "nacionalidad": "NACIONAL"
            }
            
            printObject(rec,path,out_file+"_by_line",False)
            x+=1
            print(f"registro{x}")
            users.append(rec)
            
        patrons['users']=users    
        printObject(patrons,path,out_file,True)
    except Exception as ee:
        print(ee)

def reports(**kwargs):
    df=kwargs['df']
    path_data=kwargs['pdata']
    path_logs=kwargs['plog']
    json_file=kwargs['file_report']
    schema=kwargs['schema']
    dfFieldtoCompare=kwargs['dfFieldtoCompare']  
    recout=0
    recin=0
    for i, row in df.iterrows():
        try:
            toSearch=row[dfFieldtoCompare]
            exit=readJsonfile(path_data,json_file,schema,toSearch,"poNumber")
            if exit is None:
                recout+=1
            else:
                recin+=1
                write_file(path=f"{path_logs}/recordNotcreated.log",contenido=toSearch)
            
        except Exception as ee:
            print(ee)
    print(f"REPORT: Records created {recout} record not created {recin} check file for recordNotcreated.log")
    return recout, recin

################################
##ORDERS FUNCTION
################################
#def readorders(path,file_name,sheetName,customerName,spread):
#        try:
def readorders(**kwargs):
    #CUSTOMER CONFIGURATION FILE (PATHS, PURCHASE ORDER FILE NAME AND FILTERS)
    path_root=f"{kwargs['rootpath']}"
    customerName=kwargs['customerName']
    f = open(f"{path_root}/refdata/loadSetting.json",)
    settingdata = json.load(f)
    countpol=0
    countpolerror=0
    countvendorerror=0
    #READING THE LOADSETTING JSON FILE FROM "/CUSTOMER/REFDATA" FOLDER
    print("\n"+f"Dataframe")
    istherenotesApp=[]
    for i in settingdata['loadSetting']: 
        try:
            path_results=i['path_results']
            path_logs=i['path_logs']
            path_refdata=i['path_refdata'] 
            path_data=i['path_data']
            poLineNumberfield=""
            purchaseOrderFileData=""
            notesapp2Pofield=""
            notesapp1Pofield=""
            purchaseOrderFileData=str(i['purchaseOrders_file']['name'])
            filetoload=""
            if purchaseOrderFileData!="":
                filetoload=f"{path_data}/{purchaseOrderFileData}"
                orders= importDataFrame(filetoload,
                                            orderby=i['purchaseOrders_file']['orderby'],
                                            distinct=i['purchaseOrders_file']['distinct'],
                                            delimiter=i['purchaseOrders_file']['sep'],
                                            sheetName=i['purchaseOrders_file']['sheetName'])
            else:
                print(f"Error: Name file to upload is missing  {path_refdata}/loadSetting.json")  
                return None    
            purchaseOrderFileData=""
            filetoload=""
            purchaseOrderFileData=str(i['poLines_file']['name'])    
            if purchaseOrderFileData!="":
                filetoload=f"{path_data}/{purchaseOrderFileData}"
                poLines= importDataFrame(filetoload,
                                            orderby=i['poLines_file']['orderby'],
                                            distinct=i['poLines_file']['distinct'],
                                            delimiter=i['poLines_file']['sep'],
                                            sheetName=i['poLines_file']['sheetName'])
                if i['poLines_file']['poNumberfield']!="":
                    poLineNumberfield=str(i['poLines_file']['poNumberfield']).strip()
                else:
                    print(f"Error: seccion poLines_file foreing key is required  {path_refdata}/loadSetting.json")      
                    return None
            else:
                print(f"Error: Name poLines file to load is missing  {path_refdata}/loadSetting.json")  
                return None
            notesapp1=""
            purchaseOrderFileData=""
            purchaseOrderFileData=str(i['notes_file1']['name'])
            filetoload=""
            #notesapp1=pd.DataFrame()
            if purchaseOrderFileData!="":
                notes_file1=str(i['notes_file1']['name'])
                filetoload=f"{path_data}/{purchaseOrderFileData}"
                notesapp1= importDataFrame(filetoload,
                                            orderby=i['notes_file1']['orderby'],
                                            distinct=i['notes_file1']['distinct'],
                                            delimiter=i['notes_file1']['sep'],
                                            sheetName=i['notes_file1']['sheetName'])
                istherenotesApp.append(notesapp1)
                if i['notes_file1']['poNumberfield']!="":
                    notesapp1Pofield=i['notes_file1']['poNumberfield']
                else:
                    print(f"Error: seccion Notes: notes1_file foreing key is required  {path_refdata}/loadSetting.json")      
                    return None
               
            notesapp2=""
            purchaseOrderFileData=""
            purchaseOrderFileData=str(i['notes_file2']['name'])
            filetoload=""
            #notesapp2=pd.DataFrame()
            if purchaseOrderFileData!="":
                notes_file2=str(i['notes_file2']['name'])
                filetoload=f"{path_data}/{purchaseOrderFileData}"
                notesapp2= importDataFrame(filetoload,
                                            orderby=i['notes_file2']['orderby'],
                                            distinct=i['notes_file2']['distinct'],
                                            delimiter=i['notes_file2']['sep'],
                                            sheetName=i['notes_file2']['sheetName'])
                istherenotesApp.append(notesapp2)
                if i['notes_file2']['poNumberfield']!="":
                    notesapp2Pofield=i['notes_file2']['poNumberfield']
                else:
                    print(f"Error: seccion Notes: notes1_file foreing key is required  {path_refdata}/loadSetting.json")      
                    return None
                
            print(f"INFO MAPPING FILES")
            
            filetoload=f"{path_refdata}/acquisitionMapping_{customerName}.xlsx"
            workflowStatus_Map=importDataFrame(filetoload,orderby="",distinct=[],delimiter="",sheetName="workflowStatus")
            orderFormat_Map=importDataFrame(filetoload,orderby="",distinct=[],delimiter="",sheetName="orderFormat")
            acquisitionMethod_Map= importDataFrame(filetoload,orderby="",distinct=[],delimiter="",sheetName="acquisitionMethod")
            paymentStatus_Map= importDataFrame(filetoload,orderby="",distinct=[],delimiter="",sheetName="paymentStatus")
            receiptStatus_Map= importDataFrame(filetoload,orderby="",distinct=[],delimiter="",sheetName="receiptStatus")
            fundsExpenseClass_Map= importDataFrame(filetoload,orderby="",distinct=[],delimiter="",sheetName="fundsExpenseClass")
            funds_Map= importDataFrame(filetoload,orderby="",distinct=[],delimiter="",sheetName="funds")            
            organizationCodeToChange_Map=importDataFrame(filetoload,orderby="",distinct=[],delimiter="",sheetName="organizationCodeToChange_Map")
            
        except ValueError as error:
            print(f"Error: {error}")    
        
    orderList=[]      
    orderDictionary={}      
    list={}
    #customerName=kwargs['client']
    changeVendor={}
    sivendor=0
    novendor=0
    cont=0
    sw=0
    count=0
    countlist=0
    store={}
    purchase=[]
    purchaseOrders={}
    notesprint={}
    notesapp=[]
    orderList=[]      
    orderDictionary={}      
    list={}
    for i, row in orders.iterrows():
        try:
            printpoline=True
            Order={}
            tic = time.perf_counter()
            count+=1
            #Order Number
            poNumberSuffix=""
            poNumberPrefix=""
            poNumber=""
            #if row['Prefix'] in orders.columns():
            if row['Prefix']: 
                    poNumberPrefix=str(row['Prefix'])
                    Order["poNumberPrefix"]=poNumberPrefix.strip()
            if row['Suffix'] in orders.columns:
                if row['Suffix']:
                   poNumberSuffix=str(row['Suffix'])
                   Order["poNumberSuffix"]=poNumberSuffix.strip()
            #if row['PO number'] in orders.columns:
            if row['PO number']:
                    masterPo=str(row['PO number']).strip()
                    po=check_poNumber(masterPo,path_results)
                    #print(po)
                    #poNumber=po[1:]
                    poNumber=po
                    #print(po)
                    Order["poNumber"]= poNumber
            else:
                randompoNumber=str(round(random.randint(100, 1000)))
                poNumber=str(randompoNumber)
                write_file(path=f"{path_logs}/oldNew_ordersID.log",contenido=f"{vendorToSearch}")
                
                po=poNumber
            #CHECKING DUPLICATED PO number    
            countlist = orderList.count(str(po))
            if countlist>0:
                poNumber=str(po)+str(countlist)
                
            orderList.append(str(po))
            #print(orderList)                
            print("INFO RECORD: "+str(count)+"    poNumber:  "+poNumber)
            #idOrder
            
            if 'UUID' in orders.columns: Order["id"]=str(row['UUID'])#str(uuid.uuid4())
            else: Order["id"]=str(uuid.uuid4())
            #Order["approvedById"]=""
            #Order["approvalDate"]= ""
            #Order["closeReason"]=dic(reason="",note="")
            Order["manualPo"]= False
            #PURCHASE ORDER NOTES
            notea=[]
            if row['ORDNOTE']!="":
                notea.append(str(row['ORDNOTE']).strip())
            Order["notes"]=notea
                    
            #IS SUSCRIPTION FALSE/TRUE
            Order_type=""
            Order_type="One-Time"
            isongoing=dic(isSubscription=False)
            isSubscription= False
            isSuscriptiontem=""
            if row['isSuscription']:
                isSuscriptiontem=str(row['isSuscription']).strip()
                isSuscriptiontem=isSuscriptiontem.upper()
                if isSuscriptiontem=="YES":
                    isSubscription= True
                    Order_type="Ongoing"
                    reviewPeriod=""
                    #print(orders.columns)
                    if row['REVIEWPERIOD']:
                        reviewPeriod=int(row['REVIEWPERIOD'])
                    renewalDate=""
                    ongoingNote=""
                    interval=365
                    if row['RENEWINTERVAL']: interval=int(row['RENEWINTERVAL'])
                    if row['RENEWDATE']: renewalDate=timeStamp(row['RENEWDATE'])#f"2022-06-30T00:00:00.00+00:00"
                    isongoing=dic(interval=interval, isSubscription=True, manualRenewal=True, 
                                               reviewPeriod=reviewPeriod, renewalDate=renewalDate)
                    
            Order["orderType"]=Order_type
            Order["ongoing"]=isongoing
            ######################
            shipTo=""
            Order["billTo"]="5b1f5f52-7ca8-4690-ac78-2d1bf9e410c0"
            Order["shipTo"]="5b1f5f52-7ca8-4690-ac78-2d1bf9e410c0"
            #shipto=str(row['SHIPTO']).strip()
            #shipto=shipto.upper()
            #if shipto=="MSU BOOKS RECEIVING":                
            #    Order["shipTo"]="77f43ed0-eee7-4b45-a35c-7e17f235e0bb"
            #elif shipto=="MSU DOCS":                
            #    Order["shipTo"]="246ce69e-fab7-4990-8b5a-4c3bd135226d"
            #elif shipto=="MSU SERIALS ACQUISITIONS":                
            #    Order["shipTo"]="369633a7-d2b9-423f-a035-310bc901062f"
            #elif shipto=="John F. Schaefer Law Library":
            #    Order["shipTo"]="9bb185d8-34a6-4eb8-a514-70048413271c"


                
            
            OrganizationUUID=""            
            OrganizationUUID=readJsonfile(f"{path_refdata}",f"{customerName}_organizations.json","organizations","undefined","code")
            if OrganizationUUID is not None:
                if row['VENDOR']:
                    vendorToSearch=str(row['VENDOR']).strip()
                    OrganizationUUID=readJsonfile(path_refdata,f"{customerName}_organizations.json","organizations",vendorToSearch,"code")
                    if OrganizationUUID is None:
                        write_file(path=f"{path_logs}/vendorsNotFounds.log",contenido=f"{vendorToSearch}")
                        countvendorerror+=1
                        printpoline=False
            else:
                print(f"ERROR {customerName}_organizations.json not exit in {customerName}/refdata folder")
                return None
            
            Order["vendor"]=OrganizationUUID
     
            #WorkFlow Status
            if masterPo=="o1015692":
                a=1
                
            workflowStatus="Pending"
            approvedStatus= False
            if row['ORDWORKFLOW']:
                workflow=str(row['ORDWORKFLOW']).strip()
                workflow=workflow.upper()
                if workflow== "OPEN":
                    approvedStatus= True
                    workflowStatus="Open"
                
            Order["approved"]= approvedStatus
            Order["workflowStatus"]= workflowStatus

            #Reencumber
            reEncumber=False
            if row['reEncumber']:
                reencumbertem=str(row['reEncumber']).strip()
                reencumbertem=reencumbertem.upper()
                if reencumbertem=="YES":
                    reEncumber=True
            Order["reEncumber"]= reEncumber
            #PURCHASE ORDERS LINES
            compositePo=[]
            linkid=""
            compositePo=""
            #COMPOSITE_PO_LINES
            if printpoline:
                compositePo=compositePoLines(poLines,poLineNumberfield, notesapp1,notesapp1Pofield, notesapp2, notesapp2Pofield, istherenotesApp, OrganizationUUID,masterPo,poNumber,customerName,path_results,path_refdata,path_logs)
                if compositePo is not None: 
                    Order["compositePoLines"]=compositePo
                    countpol+=1
                else: 
                    Order["compositePoLines"]=[]
                    countpolerror+=1
            
                OrganizationUUID=[]
                if row['ORDACQUNIT']:
                    Acquisitionstemp=str(row['ORDACQUNIT']).strip()
                    OrganizationUUID.append(readJsonfile(path_refdata,f"{customerName}_AcquisitionsUnits.json","AcquisitionsUnits",Acquisitionstemp,"name"))
                    if len(OrganizationUUID)==0:
                        write_file(path=f"{path_logs}/AdqNotFound.log",contenido=f"{Acquisitionstemp}")
                Order["acqUnitIds"]=OrganizationUUID
            
                printObject(Order,path_results,count,f"{customerName}_purchaseOrderbyline.json",False)
                purchase.append(Order)
        except Exception as ee:
            print(f"ERROR: {ee}")
    purchaseOrders['purchaseOrders']=purchase    
    printObject(purchaseOrders,path_results,count,f"{customerName}_purchaseOrders",True)
    print(f"============REPORT======================")
    report=[]
    print(poLineNumberfield)
    report=reports(df=orders,plog=path_logs,pdata=path_results,file_report=f"{customerName}_purchaseOrders.json",schema="purchaseOrders",dfFieldtoCompare=poLineNumberfield)
    print(f"RESULTS Record processed {count}")
    print(f"RESULTS poLines {countpol}")
    print(f"RESULTS poLines with errors: {countpolerror}")
    print(f"RESULTS vendor with errors: {countvendorerror}")
    print(f"RESULTS end")
    
#########################################
#POLINES FUNCTION             
#########################################
def compositePoLines(poLines,poLineNumberfield,
                     notesapp1,notesapp1Pofield,
                     notesapp2,notesapp2Pofield,istherenotesApp,
                     vendors,masterPo,poLineNumber,customerName,path_results,path_refdata,path_logs):                    
    try:
       #locationMapping={"af":"mnaf","aq":"mnaq","br":"mnbr","ca":"mnca","cv":"mncv","dv":"mndv","ir":"mnir","ns":"mnns","ov":"mnov","ss":"mnss"}
        cpList=[]
        count=1 
        poLines = poLines[poLines[poLineNumberfield]== masterPo]
        print("poLines founds records: ",len(poLines))
        for c, cprow in poLines.iterrows():
            cp={}
            if 'UUIDPOLINES' in poLines: linkid=cprow['UUIDPOLINES']#str(uuid.uuid4())
            else: linkid=str(uuid.uuid4())
            cp["id"]=linkid
            cp["poLineNumber"]=str(poLineNumber)+"-"+str(count)
            if cprow['Publisher']:
                cp["publisher"]=cprow['Publisher']
            #cp["purchaseOrderId"]=""
            #cp["id"]=""
            #cp["edition"]=""
            checkinItems= False
            if cprow['checkinItems']:
                checkinItemstem=str(cprow['checkinItems']).strip()
                checkinItemstem=checkinItemstem.upper()
                if checkinItemstem=="YES":
                    checkinItems= True
            cp["checkinItems"]=checkinItems
            #cp["instanceId"]=""
            #cp["agreementId"]= ""
            acquisitionMethod="Purchase"
            if cprow['Acquisition Method']:
                acquisitionMethod=str(cprow['Acquisition Method']).strip()
                if acquisitionMethod=="Approval plan":    acquisitionMethod="Approval Plan"
                elif acquisitionMethod=="DDA":            acquisitionMethod="Demand Driven Acquisitions (DDA)"
                elif acquisitionMethod=="EBA":            acquisitionMethod="Evidence Based Acquisitions (EBA)"
                elif acquisitionMethod=="Exchange":       acquisitionMethod="Exchange"
                elif acquisitionMethod=="Membership":     acquisitionMethod="Technical"
                elif acquisitionMethod=="Gift":           acquisitionMethod="Gift"
                elif acquisitionMethod=="Purchase at vendor system":  acquisitionMethod="Purchase At Vendor System"
                elif acquisitionMethod=="Purchase":       acquisitionMethod="Purchase"
                elif acquisitionMethod=="Depository":     acquisitionMethod="Depository"
                else: acquisitionMethod="Purchase At Vendor System"
            cp["acquisitionMethod"]= acquisitionMethod   
            #cp["alerts"]=dic(alert="Receipt overdue",id="9a665b22-9fe5-4c95-b4ee-837a5433c95d")
            cp["cancellationRestriction"]: False
            #cp["cancellationRestrictionNote"]=""
            #cp["claims"]=dic()Fa""
            collection=False
            if cprow["collection"]:
                collection=str(cprow["collection"]).strip()
                collection=collection.upper()
                if collection=="YES":
                    collection=True
            cp["collection"]=collection
            
            rush=False
            if cprow["rush"]:
                rush=str(cprow["rush"]).strip()
                rush=rush.upper()
                if rush=="YES":
                    rush=True
            cp["rush"]=rush

            #cp["contributors"]=[dic()]
            
            quantityPhysical=1
            if cprow['Quantity Physical']:
                quantityPhysical=int(cprow['Quantity Physical'])
                
            quantityElectronic=1
            if cprow['Quantity electronic']:#if cprow['QUANTITY'] NOT MIGRATED:
                quantityElectronic=int(cprow['Quantity electronic'])    
            ###
            #LOCATIONS(ORDER)
            ################################
            locationId=""
            locsw=True    
            if cprow['LOCATION']:
                locationtoSearch=str(cprow['LOCATION']).strip()
                if locationtoSearch.find(",")==-1:
                    locsw=True
                    locationtoSearch=locationtoSearch.replace(" ","")
                    locationId=readJsonfile(path_refdata,f"{customerName}_locations.json","locations",locationtoSearch,"code")
                    #locationId=get_locId(locationtoSearch, customerName)
                    #if locationId is None:
                    #    locationtoSearch=str(cprow['LOCATION']).strip()
                    #    vendorrealToSearch=searchKeysByVal(locationMapping,locationtoSearch)
                    #    locationId=readJsonfile(path_refdata,f"{customerName}_locations.json","locations",vendorrealToSearch,"code")
                    if locationId is None:
                        locationId="None"
                        write_file(path=f"{path_logs}/locationsNotFounds.log",contenido=locationtoSearch)                            
                else:
                    loca=[]
                    x = locationtoSearch.split(",")
                    locsw=False
                    lc=0
                    locationIdA=""
                    for i in x:
                        locationtoSearch=i
                        locationId=readJsonfile(path_refdata,"{customerName}_locations.json","locations",locationtoSearch,"code")
                            #locationId=get_locId(locationtoSearch, customerName)
                        locationIdA=locationId
                        #if locationIdA is None:
                        #    vendorrealToSearch=searchKeysByVal(locationMapping,locationtoSearch)
                        #    locationId=readJsonfile(path_refdata,"{customerName}_locations.json","locations",vendorrealToSearch,"code")
                                #locationIdA=get_locId(vendorrealToSearch, customerName)
                        if locationIdA is None:
                            locationIdA="None"
                            write_file(path=f"{path_logs}/locationsNotFounds.txt",contenido=f" {poLineNumber} {locationtoSearch} undefined locations")
                                
                        lc+=1
                        if cprow['Order format']:
                            orderFormat=str(cprow['Order format']).strip()
                            #Locations for print/mixed resources
                            if orderFormat=="Physical":
                                loca.append({"locationId":locationIdA,"quantity":(quantityPhysical-1), "quantityPhysical":(quantityPhysical-1)}) 
                            elif orderFormat=="Electronic Resource":
                                loca.append({"locationId":locationIdA,"quantity":(quantityElectronic-1), "quantityElectronic":(quantityElectronic-1)})
                            else:
                                loca.append({"locationId":locationIdA,"quantity":(quantityPhysical-1), "quantityPhysical":(quantityPhysical-1)}) 
                        locationIdA=""
            ##TITLE
            ispackage=False
            if cprow['ispackage']:
                ispackagetem=str(cprow['ispackage']).strip()
                ispackagetem=ispackagetem.upper()
                if ispackagetem=="YES":
                    ispackage=True
            cp["isPackage"]=ispackage
                
            titleOrPackage="No Title"
            enum=["Instance, Holding, Item","Instance, Holding","Instance","None"]
            instance_holdings_items=enum[3]
            if cprow['Create inventory']:
                instholitem=str(cprow['Create inventory']).strip()
                if instholitem=="Instance, Holding, Item":  
                    instance_holdings_items=enum[0]
                elif instholitem=="Instance, Holding":
                    instance_holdings_items=enum[1]
                elif instholitem=="Instance":
                    instance_holdings_items=enum[2]
                else: 
                    instance_holdings_items=enum[3]
                    
            if cprow['TITLE']:
                if ispackage:
                    titleOrPackage=str(cprow['TITLE']).strip()
                    cp["titleOrPackage"]=titleOrPackage 
                else: 
                    #ispackage==False:
                    titleUUID=str(cprow['TITLE']).strip()
                    ordertitleUUID=get_title(customerName,element="instances",searchValue=titleUUID)
                    #ordertitleUUID=readJsonfile_identifier(path_refdata,f"{customerName}_instances.json","instances",titleUUID)
                    if len(ordertitleUUID)!=0:# is None:
                        #instance_holdings_items="None"
                        print(ordertitleUUID)
                        cp["instanceId"]=str(ordertitleUUID[0])
                        cp["titleOrPackage"]=str(ordertitleUUID[1])
                        cp["isPackage"]=False
                    else: 
                        titleOrPackage=cprow['TITLE']
                        cp["titleOrPackage"]=titleOrPackage
                        #instance_holdings_items="None"
                        #instance_holdings_items="Instance"
                        write_file(path=f"{path_logs}/titlesNotFounds.log",contenido=f"{poLineNumber}  {titleUUID} {titleOrPackage}")
                        instance= {
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
                        printObject(instance,path_results,0,f"{customerName}_instances",False)
            else:
                cp["titleOrPackage"]=titleOrPackage
            ################################
            ### ORDER FORMAT
            ################################            
            orderFormat=""
            materialType=""
            accessProvider=vendors
            accessproviderUUID=""
            if cprow['Access provider']:
                accessProvidertosearch=str(cprow['Access provider']).strip()
                accessproviderUUID=readJsonfile(path_refdata,f"{customerName}_organizations.json","organizations",accessProvidertosearch,"code")
                if accessproviderUUID is None:
                    write_file(path=f"{path_logs}/providerNotFounds.log",contenido=f"{accessProvidertosearch}")
                else:
                    accessProvider=accessproviderUUID
            
            materialSupplier=vendors        
            if cprow['(Physical Resource) Material supplier']:
                materialaccessProvidertosearch=str(cprow['(Physical Resource) Material supplier']).strip()
                accessproviderUUID=readJsonfile(path_refdata,f"{customerName}_organizations.json","organizations",materialaccessProvidertosearch,"code")
                if accessproviderUUID is None:
                    write_file(path=f"{path_logs}/materialProviderNotFounds.log",contenido=f"{materialaccessProvidertosearch}")
                else:
                    materialSupplier=accessproviderUUID            
            
            
            listUnitPrice=0.00
            if cprow['UNIT PRICE']:
                listUnitPrice=float(cprow['UNIT PRICE'])
                
            if cprow['Order format']:
                orderFormat=str(cprow['Order format']).strip()
                orderFormat=orderFormat.upper()
                #Locations for print/mixed resources
                if orderFormat=="PHYSICIAL" or orderFormat=="PHYSICAL" or orderFormat=="PHYSICAL RESOURCE" or orderFormat=="MIXED P/E":
                    #Material Type physical
                    materialType=""
                    if cprow['(Physical Resource)Material type']:
                        mtypestosearch=str(cprow['(Physical Resource)Material type']).strip()
                        materialType=readJsonfile(path_refdata,f"{customerName}_mtypes.json","mtypes",mtypestosearch,"name")
                        #materialType=get_matId(mtypestosearch,customerName)
                        if materialType is None:
                            materialType=get_matId(mtypestosearch,customerName)
                            write_file(path=f"{path_logs}/materialTypeNotFounds.log",contenido=f"{poLineNumber} {mtypestosearch}")
                            if materialType is None:
                                write_file(path=f"{path_logs}/materialTypeNotFounds.log",contenido=f"{poLineNumber} {mtypestosearch}")
                                materialType="materialtypeUndefined"


  
                    cp["orderFormat"]="Physical Resource"
                    cp["cost"]=dic(currency="USD",listUnitPrice=listUnitPrice, quantityPhysical=quantityPhysical, poLineEstimatedPrice=listUnitPrice, discountType="percentage")
                    if materialType: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier, materialType=materialType)
                    else: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier)
                    if accessProvider: cp["eresource"]=dic(activated=False,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                    else: cp["eresource"]=dic(activated=False,createInventory=instance_holdings_items,trial=False)
                    if locsw: 
                        cp["locations"]=[dic(locationId=locationId,quantity=quantityPhysical, quantityPhysical=quantityPhysical)]
                    else:
                        cp["locations"]=loca
                        
                elif orderFormat=="ELECTRONIC" or orderFormat=="ELECTRONIC RESOURCE":
                    cp["orderFormat"]="Electronic Resource"
                    materialType=""
                    if cprow['Material type']:
                        mtypestosearch=""
                        mtypestosearch=str(cprow['Material type']).strip()
                        materialType=readJsonfile(path_refdata,f"{path_logs}/_mtypes.json","mtypes",mtypestosearch,"name")
                        #materialType=get_matId(mtypestosearch,customerName)
                        if materialType is None:
                            materialType=get_matId(mtypestosearch,customerName)
                            write_file(path=f"{path_logs}/materialTypeNotFounds.txt",contenido=f"{poLineNumber} {mtypestosearch}")
                            if materialType is None:
                                write_file(path=f"{path_logs}/materialTypeNotFounds.txt",contenido=f"{poLineNumber} {mtypestosearch}")
                                materialType="materialtypeUndefined"

                    cp["cost"]=dic(currency="USD",listUnitPrice=listUnitPrice,listUnitPriceElectronic=listUnitPrice, quantityElectronic=quantityElectronic, poLineEstimatedPrice=listUnitPrice,discountType="percentage")
                    if materialType: 
                        cp["eresource"]=dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider,materialType=materialType)
                    else: 
                        cp["eresource"]=dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                        
                    if locationId: 
                        if locsw:
                            cp["locations"]=[dic(locationId=locationId,quantity=1, quantityElectronic=quantityElectronic)]
                        else:
                            cp["locations"]=loca 
                            
                elif orderFormat=="Mixed P/E":
                    cp["orderFormat"]="P/E Mix"
                    cp["cost"]=dic(currency="USD",listUnitPrice=listUnitPrice,listUnitPriceElectronic=listUnitPrice, quantityPhysical=quantityPhysical, quantityElectronic=1, poLineEstimatedPrice=listUnitPrice,discountType="percentage")
                    if accessProvider: cp["eresource"]=dic(activated=False,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                    else: cp["eresource"]=dic(activated=False,createInventory=instance_holdings_items,trial=False)
                    cp["locations"]=[dic(locationId=locationId,quantity=2, quantityElectronic=1,quantityPhysical=quantityPhysical)]
                    if materialType: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier,
                                   expectedReceiptDate="",receiptDue="",materialType=materialType)
                    else: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier,
                                   expectedReceiptDate="",receiptDue="")
                else:   
                    cp["orderFormat"]="Other"
                    cp["cost"]=dic(currency="USD",listUnitPrice=listUnitPrice, quantityPhysical=1, poLineEstimatedPrice=listUnitPrice, discountType="percentage")
                    if materialType: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier, materialType=materialType)
                    else: cp["physical"]=dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier)
                    if accessProvider: cp["eresource"]=dic(activated=False,createInventory="None",trial=False,accessProvider=accessProvider)
                    else: cp["eresource"]=dic(activated=False,createInventory="None",trial=False)
                    if locationId: 
                        if locsw:
                            cp["locations"]=[dic(locationId=locationId,quantity=1, quantityElectronic=quantityElectronic)]
                        else:
                            cp["locations"]=loca 

            #FUNDS DISTRIBUTIONS
            cp["fundDistribution"]=[]
            #EXPENSES CLASES
            #FUND DISTRIBUTION BY RESOURCE
            codeTosearch=str(cprow['FUND']).strip()
            codeTosearch=codeTosearch.replace(" ","")
            if codeTosearch=="none":
                codeTosearch=""
            if codeTosearch:
                occurencias=int(codeTosearch.count("%"))
                if occurencias==0:
                    searchExpensesValue=""
                    expenseClassId=""
                    if cprow['Expense Class']:
                        searchtoValue=str(cprow['Expense Class']).strip()
                        expenseClassId=readJsonfile(path_refdata,f"{customerName}_expenseClasses.json","expenseClasses",searchtoValue,"code")
                        #expenseClassId=get_Id(customerName,element="expenseClasses",searchValue=searchtoValue)
                    #get_funId(searchValue,orderFormat,client):
                    fundId=readJsonfile_fund(path_refdata,f"{customerName}_funds.json","funds",codeTosearch,"code")
                    #fundId=get_funId(codeTosearch,orderFormat,customerName)
                    if fundId is not None:
                        code=fundId[1]
                        fundId=fundId[0]
                        valuefund=100.0
                        if expenseClassId:
                            cp["fundDistribution"]=[dic(code=code,fundId=fundId,expenseClassId=expenseClassId,distributionType="percentage",value=valuefund)]
                        else:
                            cp["fundDistribution"]=[dic(code=code,fundId=fundId,distributionType="percentage",value=valuefund)]
                    else:
                        write_file(path=f"{path_logs}/fundsNotfounds.log",contenido=f"{poLineNumber} {codeTosearch}")
                        cp["fundDistribution"]=[]
                else:
                    fundlist=[]
                    fundDistribution=[]
                    fundlist = codeTosearch.split(",")
                    i=0
                    for fundin in fundlist:
                        cadena=fundin
                        codeTosearch=cadena[:5]
                        valuefund=cadena[6:10]
                        searchExpensesValue=""
                        expenseClassId=""
                        
                        if cprow['Expense Class']:
                            searchtoValue=str(cprow['Expense Class']).strip()
                            expenseClassId=readJsonfile(path_refdata,f"{customerName}_expenseClasses.json","expenseClasses",searchtoValue,"code")
                            if expenseClassId is None:
                                write_file(path=f"{path_logs}/expensesNotfounds.log",contenido=f"{poLineNumber} {searchtoValue}")
                            #expenseClassId=get_Id(customerName,element="expenseClasses",searchValue=searchtoValue)
                        #get_funId(searchValue,orderFormat,client):
                        fundId=readJsonfile_fund(path_refdata,f"{customerName}_funds.json","funds",codeTosearch,"code")
                        #fundId=get_funId(codeTosearch,orderFormat,customerName)
                        if fundId is not None:
                            code=fundId[1]
                            fundId=fundId[0]
                            if expenseClassId:
                                fundDistribution.append(dic(code=code,fundId=fundId,expenseClassId=expenseClassId,distributionType="percentage",value=valuefund))
                        
                    cp["fundDistribution"]=fundDistribution
                    
            #Ongoing
            receivingNote=""
            if cprow['receivingNote']: receivingNote=str(cprow['receivingNote'])
            subscriptionFrom=""
            subscriptionTo=""
            subscriptionInterval=""
            if cprow['Subscription from']: subscriptionFrom=timeStamp(cprow['Subscription from'])
            if cprow['Subscription to']: subscriptionTo=timeStamp(cprow['Subscription to'])
            if cprow['Subscription interval']: subscriptionInterval=int(cprow['Subscription interval'])
            productIds=[]
            #if cprow['CODE1']:
            #    if cprow['CODE1']!="-":
            #        if cprow['CODE1']!="NONE":
            #            productIds.append(dic(productId=str(cprow['CODE1']).strip(), productIdType="8e3dd25e-db82-4b06-8311-90d41998c109"))
            #if cprow['CODE3']:
            #    if cprow['CODE3']!="-":
            #        productIds.append(dic(productId=str(cprow['CODE3']).strip(), productIdType="8e3dd25e-db82-4b06-8311-90d41998c109"))
            #if cprow['RECORD #(BIBLIO)']:
            #    reportNumberdata="."+str(cprow['RECORD #(BIBLIO)']).strip()
            #    productIds.append(dic(productId=reportNumberdata, productIdType="37b65e79-0392-450d-adc6-e2a1f47de452"))
            
            cp["details"]=dic(receivingNote=receivingNote,productIds=productIds,subscriptionFrom=subscriptionFrom,
                                                       subscriptionInterval=subscriptionInterval, subscriptionTo=subscriptionTo)
            
            #cp["donor"]=""
            paymentStatus="Ongoing"
            if cprow["paymentStatus"]:
                cp["paymentStatus"]=cprow["paymentStatus"]
            description=""
            
            if cprow['Internal Note']:
                description=str(cprow['Internal Note']).strip()
            cp["description"]=description
            
            #cp["publicationDate"]=""
            receiptStatus="Awaiting Receipt"
            cp["receiptDate"]=""
            if cprow['receiptDate']:
                receitdate=str(cprow['receiptDate'])
                M=receitdate[0:2]
                D=receitdate[3:5] 
                Y=receitdate[6:10]
                if Y=="96" or Y=="97" or Y=="98" or Y=="99":
                    Y=f"19{Y}"
                cp["receiptDate"]=f"{Y}-{M}-{D}T00:00:00.00+00:00"
            cp["receiptStatus"]=receiptStatus
            #cp["reportingCodes"]=dic(code="",id="",description="")
            Requester=""
            if cprow['Requester']:
                Requester=str(cprow['Requester'])
            cp["requester"]=Requester
            if cprow['Selector']:
                cp['selector']=str(cprow['Selector'])
                

                
            cp["source"]="User"
            vendorAccount=""
            referenceNumbers=[]
            #referenceNumbers.append(dic(refNumber=refNumber, refNumberType="Vendor title number"))
            #if cprow['RECORD #(ORDER)']:
            #    refNumber=str(cprow['RECORD #(ORDER)']).strip()
            #    referenceNumbers.append(dic(refNumber=refNumber, refNumberType="Vendor order reference number"))
                
            #instrVendor=""   
            #lw=False
            #notesapp2 = notesapp2[notesapp2['RECORD #(ORDER)']== idSearch]
            #print("Instruction vendors were founds: ",len(notesapp2))
            #for a, nrow in notesapp2.iterrows():
            #    if nrow['VEN. NOTE']:
            #        instrVendor=nrow['VEN. NOTE']
            #        lw=True
            
            #cp["vendorDetail"]=dic(instructions=instrVendor, referenceNumbers=referenceNumbers, vendorAccount="")
            #NOTES GRAL
            for nt in istherenotesApp:
                notes_single_line(linkid,masterPo, nt,notesapp1Pofield,"PAID","Orders note", path_results,path_refdata,count)

                        
            cpList.append(cp)
            #cpList.append(linkid)
            count=count+1
        return cpList    
    except Exception as ee:
        print(ee)
        write_file(path=f"{path_logs}/poLinesErrors.log",contenido=f"Order:{masterPo} {ee}")       

def orderTransactionSummaries():
    path= "C:/Users/asoto/Documents/EBSCO/Migrations/folio/runenv/results/utm"
    json_file="utm_purchaseOrders.check.json"
    customerName="utm"
    searchValue="id"
    f = open(path+"/"+json_file)
    data = json.load(f)
    count=0
    con={}
    lic={}
    sw=False
    c=0
    r=1
    for i in data['purchaseOrders']:
        try:
            print(f"Record {r}")
            r+=1
            count+=1
            j_content=i
            idPO= j_content['id']
            existe=get_Id(customerName, element="ordersTransactionSummaries", searchValue=idPO)
            if existe:
                transactions={}
                transactions={
                                "id":idPO,
                                "numTransactions": 1
                             }
                c+=1
                print("creating record")
                printObject(transactions,f"C:/Users/asoto/Documents/EBSCO/Migrations/folio/runenv/results/utm",0,"utm_New_transactions",False)
        except Exception as err:
            print("error ", str(err))
    print(f"{c}")
    
    #NOTES:
    def notes_single_line(linkId,idToSearch, notesapp,notesappPofield,title,noteTypestoSearch, path_result,path_refdata,count):
        #NOTES
        resultNote={}
        noteTypesUUID=readJsonfile(f"{path_refdata}",f"{customerName}_noteTypes.json","noteTypes",noteTypestoSearch,"name")
        if noteTypesUUID is None:
            typeId="None"
        content=""
        notesfield=notesapp.columns
        notesapp_id = notesapp[notesapp[notesappPofield]== idToSearch]
        print("Notes were founds: ",len(notesapp_id))
        for a, nrow in notesapp_id.iterrows():
            content=""
            for note in notesfield:
                if nrow[note]!="":
                    content=f"{content} {note}: {nrow[note]}"
                if note=="Paid Date":
                    year=str(nrow['Paid Date']).strip()
                    length = len(year)
                    year=year[-4:]
                    title=f"Paid {year}"
            resultNote=print_notes(linkId,"poLine",typeId=noteTypesUUID,type=noteTypestoSearch,domain="orders",title=title,cont=content)
            printObject(resultNote,f"{path_result}",count,f"{customerName}_notes",False)
        return resultNote

################################
##AGREEMENT FUNCTION
################################

def readagreements(**kwargs):
    #CUSTOMER CONFIGURATION FILE (PATHS, PURCHASE ORDER FILE NAME AND FILTERS)
    path_root=f"{kwargs['rootpath']}"
    customerName=kwargs['customerName']
    f = open(f"{path_root}/refdata/loadSetting.json",)
    settingdata = json.load(f)
    countpol=0
    countpolerror=0
    countvendorerror=0
    #READING THE LOADSETTING JSON FILE FROM "/CUSTOMER/REFDATA" FOLDER
    print("\n"+f"Dataframe")
    istherenotesApp=[]
    for i in settingdata['loadSetting']: 
        try:
            path_results=i['path_results']
            path_logs=i['path_logs']
            path_refdata=i['path_refdata'] 
            path_data=i['path_data']
            poLineNumberfield=""
            purchaseOrderFileData=""
            notesapp2Pofield=""
            notesapp1Pofield=""
            agreement_file=str(i['agreement_file']['name'])
            filetoload=""
            if agreement_file!="":
                filetoload=f"{path_data}/{agreement_file}"
                agreements= importDataFrame(filetoload,
                                            orderby=i['agreement_file']['orderby'],
                                            distinct=i['agreement_file']['distinct'],
                                            delimiter=i['agreement_file']['sep'],
                                            sheetName=i['agreement_file']['sheetName'])
            else:
                print(f"Error: Name file to upload is missing  {path_refdata}/loadSetting.json")  
                return None    
            notesapp1=""
            agreement_file=""
            purchaseOrderFileData=str(i['notes_file1']['name'])
            filetoload=""
            #notesapp1=pd.DataFrame()
            if purchaseOrderFileData!="":
                notes_file1=str(i['notes_file1']['name'])
                filetoload=f"{path_data}/{purchaseOrderFileData}"
                notesapp1= importDataFrame(filetoload,
                                            orderby=i['notes_file1']['orderby'],
                                            distinct=i['notes_file1']['distinct'],
                                            delimiter=i['notes_file1']['sep'],
                                            sheetName=i['notes_file1']['sheetName'])
                istherenotesApp.append(notesapp1)
                if i['notes_file1']['poNumberfield']!="":
                    notesapp1Pofield=i['notes_file1']['poNumberfield']
                else:
                    print(f"Error: seccion Notes: notes1_file foreing key is required  {path_refdata}/loadSetting.json")      
                    return None
               
            notesapp2=""
            purchaseOrderFileData=""
            purchaseOrderFileData=str(i['notes_file2']['name'])
            filetoload=""
            #notesapp2=pd.DataFrame()
            if purchaseOrderFileData!="":
                notes_file2=str(i['notes_file2']['name'])
                filetoload=f"{path_data}/{purchaseOrderFileData}"
                notesapp2= importDataFrame(filetoload,
                                            orderby=i['notes_file2']['orderby'],
                                            distinct=i['notes_file2']['distinct'],
                                            delimiter=i['notes_file2']['sep'],
                                            sheetName=i['notes_file2']['sheetName'])
                istherenotesApp.append(notesapp2)
                if i['notes_file2']['poNumberfield']!="":
                    notesapp2Pofield=i['notes_file2']['poNumberfield']
                else:
                    print(f"Error: seccion Notes: notes1_file foreing key is required  {path_refdata}/loadSetting.json")      
                    return None
                
            
            
        except ValueError as error:
            print(f"Error: {error}")    
        
    agree={}
    count=1
    for i, row in agreements.iterrows():
        if count==32:
            a=1
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
            #OrganizationUUID=readJsonfile(f"{path_refdata}",f"{customerName}_organizations.json","organizations","undefined","code")
            #if OrganizationUUID is not None:
            if row['organizationCode_1']:
                vendorToSearch=str(row['organizationCode_1']).strip()
                OrganizationUUID=readJsonfile(path_refdata,f"{customerName}_organizations.json","organizations",vendorToSearch,"code")
                if OrganizationUUID is None:
                    write_file(path=f"{path_logs}/vendorsNotFounds.log",contenido=f"{vendorToSearch}")
                    countvendorerror+=1
                    printpoline=False
                    OrganizationUUID=readJsonfile(f"{path_refdata}",f"{customerName}_organizations.json","organizations","undefined","code")
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
            #OrganizationUUID=readJsonfile(f"{path_refdata}",f"{customerName}_organizations.json","organizations","undefined","code")
            if row['organizationCode_2']:
                vendorToSearch=str(row['organizationCode_2']).strip()
                if vendorToSearch1!=vendorToSearch:
                    OrganizationUUID=readJsonfile(path_refdata,f"{customerName}_organizations.json","organizations",vendorToSearch,"code")
                    if OrganizationUUID is None:
                        write_file(path=f"{path_logs}/vendorsNotFounds.log",contenido=f"{vendorToSearch}")
                        countvendorerror+=1
                        printpoline=False
                        OrganizationUUID=readJsonfile(f"{path_refdata}",f"{customerName}_organizations.json","organizations","undefined","code")
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
            
            printObject(agree,path_results,count,f"{customerName}_agreements",False)
            agree={}
            count+=1
        except Exception as ee:
            print(f"ERROR: {ee}")

    print(f"============REPORT======================")
    print(f"RESULTS Record processed {count}")
    print(f"RESULTS Agreements {countpol}")
    print(f"RESULTS vendor with errors: {countvendorerror}")
    print(f"RESULTS end")  
    
