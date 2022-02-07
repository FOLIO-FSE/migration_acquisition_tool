import argparse
import pathlib
import json
import requests
import sys
from requests.exceptions import HTTPError
import time
from datetime import datetime
#import functions_AcqErm as faf
import os
import os.path
from pathlib import Path, PureWindowsPath


#from folioclient.FolioClient import FolioClient

class backup:
    def __ini__(path,x_okapi_url, x_okapi_tenant, x_okapi_token):
        x_okapi_url = x_okapi_url
        x_okapi_tenant = x_okapi_tenant
        x_okapi_token = x_okapi_token
        content_type = "application/json"
        print('initializing Backup')
        #self.user = user
        #self.password = password
        #self.x_okapi_version = x-okapi-version
        #self.x_okapi_release = x-okapi-release
        #self.x_okapi_status = x-okapi-status

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
                    
    def make_get_credentials(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file):
        try:
            pathPattern=pathPattern
            okapi_url=okapi_url
            json_file=json_file
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            #username="folio"
            #password="Madison"
            #payload = {'username': username, 'password': password}
            length="999"
            #typein="General note Orders"
            ##fc="&metadata.createdByUserId='2bd750b9-1362-4807-bd73-2be9d8d63436'"
            start="0"
            #paging_q = f"?limit={length}#&offset={start}"
            #paging_q = f"/notes?query=type=="General note Orders""
            #paging_q = f"?limit={length}&query=type=={typein}"
            #paging_q = f"?limit={length}&domain=orders"
            d = open(json_file)
            data = json.load(d)
            tini=0
            for i in data['interfaces']:
                id=i['id']
                paging_q = f"?limit={length}"
                path = pathPattern+paging_q
                path=path.replace("{id}",id)
                #data=json.dumps(payload)
                url = okapi_url + path
                print(url)
                req = requests.get(url, headers=okapi_headers,timeout=40)
                print(req)
                if req.status_code != 201:
                    print(req)
                    print(req.encoding)
                    #print(req.text)
                    print(req.headers)
                    if req.status_code==200:
                        archivo=open(json_file, 'a',encoding='utf8')
                        json_str = json.loads(req.text)
                        #total_recs = int(json_str["totalRecords"])
                        archivo.write(json.dumps(json_str, indent=2))
                        #archivo.write(json.dumps(json_str)+"\n")
                        #print('Datos en formato JSON',json.dumps(json_str, indent=2))
                        archivo.close()
                        print('Success!')
                    elif req.status_code==500:
                        print(req.text)
                    elif req.status_code==502:
                        print(req.text)
                    elif req.status_code==504:
                        print(req.text)
                    elif req.status_code==403:
                        print(req.text)
        except KeyboardInterrupt:
            print("Someone closed the program")

                      
    def make_get_id(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file,path_result,schema,ale):
        try:
            pathPattern=pathPattern
            okapi_url=okapi_url
            po={}
            json_file=json_file
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            length="999"
            start="0"
            d = open(json_file)
            data = json.load(d)
            record=[]
            errorrecord=[]
            count=1
            tini=0
            for i in data[schema]:
                id=i['id']
                paging_q = f"/{id}"
                path = pathPattern+paging_q
                path=path.replace("{id}",id)
                url = okapi_url + path
                #print(url)
                req = requests.get(url, headers=okapi_headers,timeout=40)
                code=str(req.status_code)
                code=code[0]
                if code=="4" or code=="5":
                    errorrecord.append(i)
                    outfile = json.dumps(req.text)
                    printErrorMessages(str(id),req.status_code,str(req.text),ale+"_"+schema+"_"+"Errors_update.log")
                    print(f"Not exist")
                    #print(f"Record: {id} Not imported {req.status_code} - {req.text}")
                else:
                    print(f"INFO  Getting record #: {count}")
                    count+=1
                    #return req.text
                    json_str = json.loads(req.text)
                    record.append(json_str)
                    printObject(json_str,f"{path_result}",count,f"{schema}_records_gettingbyline",False)
                    workflowStatus=json_str['workflowStatus']
                    poNumber=json_str['poNumber']
                    title=json_str['compositePoLines'][0]['titleOrPackage']
                    contenido=[f"{workflowStatus}", f"{poNumber}", f"{title}"]
                    faf.write_file(path=f"{path_result}/report.csv",contenido=contenido)
                    #print(req.text)
            po['purchaseOrders']=record        
            printObject(po,f"{path_result}",count,f"{schema}_records_getting",True)
        except KeyboardInterrupt:
            print("Someone closed the program")
                                     
    def make_get_by_uuid(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file,path_result,schema,ale):
        try:
            pathPattern=pathPattern
            okapi_url=okapi_url
            json_file=json_file
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            length="9999"
            start="0"
            count=0
            with open(json_file, "r", encoding="utf") as file_j:
                for linea in file_j:
                    count+=1
                    print(f"INFO Reading record # {count}")
                    recitem=linea
                    recitem=recitem.replace(",\n", "")
                    data = json.loads(recitem)
                    if 'id' in data:
                        id=str(data['id'])
                    if 'code' in data:
                        id=str(data['code'])
                    paging_q = id #f"?limit={length}"
                    path = f"{pathPattern}/{paging_q}"
                    path=path.replace("{id}",id)
                    #data=json.dumps(payload)
                    url = okapi_url + path
                    #print(url)
                    req = requests.get(url, headers=okapi_headers,timeout=40)
                    code=str(req.status_code)
                    code=code[0]
                    if code=="4" or code=="5":
                        #not exist
                        print(f"INFO  Getting record #: {count} Not exist")                        
                        printObject(data,Path(f"{path_result}",count,f"{schema}_records_missingOntenant"),False)

        except KeyboardInterrupt:
            print("close program")
            
    def make_del_post(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file, schema,client):
        try:
            countrecord=0
            countdel=0
            countnodel=0
            deletedRecords=open("logs/"+client+"_"+schema+"_recordDeleted.txt", 'w')
            Recordsnodeleted=open("logs/"+client+"_"+schema+"_record_to_delete_not_found.txt", 'w')
            pathPattern=pathPattern
            okapi_url=okapi_url
            json_file=json_file
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            id=""
            paging_q="/"
            path = pathPattern+paging_q
            #data=json.dumps(payload)
            f = open(json_file)
            data = json.load(f)
            tini=0
            count=1
            errorrecord=[]
            dele=1
            upde=1
            for i in data[schema]:
                try:
                #if i['domain']=="orders":
                    #print(i)
                    countrecord=countrecord+1
                    id=i['id']
                    #po=i['poNumber']
                    print(f"Registro: {count}")
                    #print("Record no: "+str(countrecord)+" searching POnumber:"+str(po)+"            id:"+id)
                    paging_q="/"
                    path = pathPattern+paging_q
                    url = okapi_url + path+id
                    tini = time.perf_counter()
                    req = requests.delete(url, headers=okapi_headers,timeout=40)
                    tend = time.perf_counter()
                    code=str(req.status_code)
                    code=code[0]
                    if code=="4" or code=="5":
                        errorrecord.append(i)
                        outfile = json.dumps(req.text)
                        printErrorMessages(str(id),req.status_code,str(req.text),client+"_"+schema+"_"+"Errors_update.log")
                        print(f"Record: {id} Not deleted")
                        #print(f"Record: {id} Not imported {req.status_code} - {req.text}")
                        if "PO Number already exists" in req.text:
                            pass
                            #printworserecords(j_content,client,schema,"poNumberExist")
                        else: 
                            printworserecords(i,client,schema,"worse_records_deleted")
                    else:
                        print(f"Record: {id} deleted {req.status_code} - {req.text}")
                        dele+=1
                    j_content=i
                    path = pathPattern
                    url = okapi_url + path
                    req = requests.post(url, json=j_content, headers=okapi_headers,timeout=40)
                    #print(req.status_code)
                    #print(req.text)
                    code=str(req.status_code)
                    code=code[0]
                    if code=="4":
                        errorrecord.append(i)
                        outfile = json.dumps(req.text)
                        printErrorMessages(str(id),req.status_code,str(req.text),client+"_"+schema+"_"+"Errors_update.log")
                        print(f"Record: {id} Not imported")
                        #print(f"Record: {id} Not imported {req.status_code} - {req.text}")
                        if "PO Number already exists" in req.text:
                            printworserecords(j_content,client,schema,"poNumberExist")
                        elif "Budget expense class not found" in req.text:
                            printworserecords(j_content,client,schema,"BudgetexpenseclassNotfound")
                        elif "id value already exists in table" in req.text: 
                            printworserecords(j_content,client,schema,"idvaluealreadyexistsintable")
                        elif "Budget not found for transaction" in req.text:
                            printworserecords(j_content,client,schema,"Budgetnotfoundfortransaction")
                        elif "Order cannot be open as the associated vendor not found" in req.text:
                            printworserecords(j_content,client,schema,"Ordercannotbeopenastheassociatedvendornotfound")
                        elif "ISBN value is invalid" in req.text:
                            printworserecords(j_content,client,schema,"isbnValueInvalid")
                        elif "PO Line physical quantity and Locations physical quantity do not match" in req.text:
                            printworserecords(j_content,client,schema,"quantityandlocationnotmatch_update")
                        elif "Fund cannot be paid due to restrictions" in req.text:
                            printworserecords(j_content,client,schema,"fundrestrictions_update")
                        elif "Physical cost quantity must be specified" in req.text:
                            printworserecords(j_content,client,schema,"physicalCostmustSpecified_update")
                        else: 
                            printworserecords(j_content,client,schema,"worse_records_update")
                    elif code=="5":
                        print(str(countrecord)+" Record: "+str(id)+" error http 500 not imported")
                        printworserecords(j_content,client,schema,"http500_Error")
                    else:
                        tend = time.perf_counter()
                        print(str(countrecord)+" Record: "+str(id)+" imported")
                        upde+=1
                    count+=1
                except Exception as ee:
                    print(ee)
            deletedRecords.close()
            print(f"Records uploaded:  {dele}")
            print(f"Records deleted: {dele}")
        except ValueError:
            print("General Error on DEL")
                
    def make_del(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file, schema,client):
        try:
            countrecord=0
            countdel=0
            countnodel=0
            deletedRecords=open(client+"_"+schema+"_recordDeleted.txt", 'w')
            Recordsnodeleted=open(client+"_"+schema+"_record_to_delete_not_found.txt", 'w')
            pathPattern=pathPattern
            okapi_url=okapi_url
            json_file=json_file
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            id=""
            paging_q="/"
            path = pathPattern+paging_q
            #data=json.dumps(payload)
            url = okapi_url + path
            d = open(json_file)
            data = json.load(d)
            tini=0
            for i in data[schema]:
                try:
                #if i['domain']=="orders":
                    #print(i)
                    countrecord=countrecord+1
                    id=i['id']
                    #po=i['poNumber']
                    #print("==================================")
                    #print("Record no: "+str(countrecord)+" searching POnumber:"+str(po)+"            id:"+id)
                    url = okapi_url + path+id
                    tini = time.perf_counter()
                    req = requests.delete(url, headers=okapi_headers,timeout=40)
                    tend = time.perf_counter()
                    #print(req.status_code)
                    #print(req.headers)
                    #print(req.text)
                    if req.status_code==404:
                        print(str(countrecord)+" Record: "+str(id)+f" not found Deleting ({tini - tend:0.4f}) seconds")
                        Recordsnodeleted.write(str(id)+f"({tini - tend:0.4f}) seconds \n")
                        countnodel=+1
                        print("==================================")
                        printworserecords(req.text,client,schema,client+"_"+schema+"_nodeleted")
                    elif req.status_code==204:
                        print(str(countrecord)+" Record: "+str(id)+f" has been deleted (time in {tini - tend: 0.4f}) seconds")
                        deletedRecords.write(str(id)+f" ({tini - tend: 0.4f}) seconds\n")
                        countdel=+1
                        print("==================================")
                        printworserecords(req.text,client,schema,client+"_"+schema+"deleted")
                                         
                    elif req.status_code==503:
                        print(req.text)
                        Recordsnodeleted.write(str(id)+f"({tini - tend:0.4f}) seconds \n")
                        time.sleep(60) # Sleep for 3 seconds
                        printworserecords(req.text,client,schema,client+"_"+schema+"nodeleted_Error503")
                    elif req.status_code==504:
                        print(req.text)
                        Recordsnodeleted.write(str(id)+f"({tini - tend:0.4f}) seconds \n")
                        printworserecords(req.text,client,schema,client+"_"+schema+"nodeleted_Error504")
                        time.sleep(60) # Sleep for 3 seconds
                
                except Exception as ee:
                    print(ee)
            deletedRecords.close()
        except Exception as ee:
            print(f"ERROR: schema: {ee}")
            
    def make_del_byLine(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file, schema,client):
        try:
            countrecord=0
            count=1
            countdel=0
            countnodel=0
            #deletedRecords=open(client+"_"+schema+"_recordDeleted.txt", 'w')
            #Recordsnodeleted=open(client+"_"+schema+"_record_to_delete_not_found.txt", 'w')
            pathPattern=pathPattern
            okapi_url=okapi_url
            #filenamea=json.loads(json_file)
            totallines=0
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            now=datetime.now()
            dobj=now
            date_time = now.strftime("%m_%d_%y_(%H_%M)")
            with open(json_file,'r', encoding='utf8') as fp:                
                data = fp.readlines()
                for line1 in data: 
                    totallines+=1
                print(f"Total records to be deleted: {totallines}")
                for line in data:
                    count+=1
                    
                    recitem=line
                    recitem=recitem.replace(",\n", "")
                    j_content = json.loads(recitem)
                    if 'id' in j_content:
                        id=str(j_content['id'])
                    if 'code' in j_content:
                        id=str(j_content['code'])
                    paging_q = id #f"?limit={length}"
                    path = f"{pathPattern}/{paging_q}"
                    url = okapi_url + path
                    tini = time.perf_counter()
                    req = requests.delete(url, headers=okapi_headers,timeout=40)
                    tend = time.perf_counter()
                    code=str(req.status_code)
                    code=code[0]
                    if code=="4":
                        totaltime=round((tend - tini))
                        print(f"Record Not deleted ({totaltime} seconds)")
                        outfile = json.dumps(req.text)
                        #json_strErr=json.loads(req.text)
                        #cod=json_strErr['errors'][0]['code']
                        #printErrorMessages(str(count),j_content, outfile,client, schema,date_time)
                        countnodel+=1
                    elif  code=="5":
                        totaltime=round((tend - tini))
                        print(f"{dobj} Record: {count} not delete  ({totaltime} seconds)")
                        printworserecords(j_content,client,schema,date_time+"http_error_500")
                        countnodel+=1
                    else:
                        totaltime=round((tend - tini))
                        print(f"Record {count} deleted ({totaltime} seconds)")
                        
                        #print(f"{dobj} Record: {count} deleted  ({totaltime} seconds)")
                        countdel+=1

            print(f"INFO Deleted records:{schema}")        
            print(f"INFO Deleted records:{countdel}/{totallines}")
            print(f"INFO Not deleted records:{countnodel}/{totallines}")
            print(f"INFO END")
        except Exception as ee:
            print(f"ERROR: schema: {ee}")
                #(pathPattern,okapi_url,okapi_tenant,okapi_token,idRecord,recordtoUpdate)
                
def make_post_byline_credentials(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file, schema,client):
    try:
        now = datetime.now()
        error=[]
        count=0
        #rec={}
        countrecord=0
        pathPattern=pathPattern
        okapi_url=okapi_url
        #json_file=json_file
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        id=1
        path = pathPattern
        url = okapi_url + path
        errorrecord=[]
        count=0
        recNum=0
        witherr=0
        httperr=0
        totallines=0
        interfaceuuid=""
        dateTime = now.strftime("%m_%d_%y_(%H_%M)")
        with open(json_file) as fp:
            data = fp.readlines()
            for line1 in fp: 
                totallines+=1
                print(f"Total lines to be uploaded: {totallines} for file: {json_file}")
            for line in data:
                try:
                    recNum+=1
                    j_content=json.loads(line)
                    tini = time.perf_counter()
                    if j_content['interfaceId']:
                        interfaceuuid=j_content['interfaceId']
                        url = url.replace("{id}",interfaceuuid)
                        req = requests.post(url, json=j_content, headers=okapi_headers,timeout=40)
                        url = okapi_url + pathPattern
                    #print(req.status_code)
                    #print(req.text)
                    code=str(req.status_code)
                    code=code[0]
                    if code=="4":
                        #json_strErr=json.loads(req.text)
                        #cod=json_strErr['errors'][0]['code']
                        #(recNum,j_content, reqtext, client, schema,date_time):
                        outfile = json.dumps(req.text)
                        printErrorMessages(recNum,j_content, outfile, client, schema,dateTime)
                        witherr+=1
                    elif  code=="5":
                        printworserecords(j_content,client,schema,dateTime+"http_error_500")
                        print(f"{recNum} Record: not imported Error http 500")
                        httperr+=1
                    else:
                        tend = time.perf_counter()
                        print(f"{recNum} Record: imported")                        
                        count+=1
                except Exception as ee:
                    print(f"ERROR: {ee}")
        print(f"Imported records: {count} / {totallines}")
        print(f"Not Imported records {witherr} / {totallines}")
        print(f"Not Imported records (Error: http 500): {httperr} / {totallines}")
    except requests.exceptions.ConnectionError as e:
        r = "No response"
    except ValueError:
        print("Error importing General")
        
def make_post_byline(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file, schema,client):
    try:
        now = datetime.now()
        error=[]
        count=0
        #rec={}
        countrecord=0
        pathPattern=pathPattern
        okapi_url=okapi_url
        #json_file=json_file
        okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
        id=1
        path = pathPattern
        url = okapi_url + path
        errorrecord=[]
        count=0
        recNum=0
        witherr=0
        httperr=0
        totallines=0
        dateTime = now.strftime("%m_%d_%y_(%H_%M)")
        with open(json_file, encoding="utf8") as fp:
            try:
                data = fp.readlines()
                for line1 in fp: 
                    totallines+=1
                    print(f"Total lines to be uploaded: {totallines} for file: {json_file}")
                for line in data:
                    try:
                        recNum+=1
                        j_content=json.loads(line)
                        tini = time.perf_counter()
                        req = requests.post(url, json=j_content, headers=okapi_headers,timeout=40)
                        #print(req.status_code)
                        #print(req.text)
                        code=str(req.status_code)
                        code=code[0]
                        if code=="4":
                            #json_strErr=json.loads(req.text)
                            #cod=json_strErr['errors'][0]['code']
                            #(recNum,j_content, reqtext, client, schema,date_time):
                            outfile = json.dumps(req.text)
                            printErrorMessages(recNum,j_content, outfile, client, schema,dateTime)
                            witherr+=1
                        elif  code=="5":
                            printworserecords(j_content,client,schema,dateTime+"http_error_500")
                            print(f"{recNum} Record: not imported Error http 500")
                            httperr+=1
                        else:
                            tend = time.perf_counter()
                            print(f"{recNum} Record: imported")                        
                            count+=1
                    except Exception as ee:
                        print(f"ERROR: {ee}")
            except Exception as ee:
                print(f"ERROR: {ee}")
        print(f"Imported records: {count} / {totallines}")
        print(f"Not Imported records {witherr} / {totallines}")
        print(f"Not Imported records (Error: http 500): {httperr} / {totallines}")
    except requests.exceptions.ConnectionError as e:
        r = "No response"
    except ValueError:
        print("Error importing General")


def make_post(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file, schema,client):
        try:
            #myobj = datetime.datetime.now()
            #dobj=myobj.strftime('%T')
            tend = time.perf_counter()
            
            now = datetime.now()
            dobj=now
            error=[]
            count=0
            #rec={}
            countrecord=0
            pathPattern=pathPattern
            okapi_url=okapi_url
            #json_file=json_file
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            id=1
            path = pathPattern
            url = okapi_url + path
            f = open(json_file, encoding='utf-8')
            data = json.load(f)
            errorrecord=[]
            date_time = now.strftime("%m_%d_%y_(%H_%M)")
            for i in data[schema]:
                try:
                    tini = time.perf_counter()
                    j_content=i
                    #print(j_content)
                    #j_content =json.loads(line)
                    countrecord+=1
                    #print("Record no: "+str(countrecord))
                    url = okapi_url + path
                    req = requests.post(url, json=j_content, headers=okapi_headers,timeout=40)
                    tend = time.perf_counter()
                    #print(req.status_code)
                    #print(req.text)
                    code=str(req.status_code)
                    code=code[0]
                    if code=="4":
                        errorrecord.append(i)
                        outfile = json.dumps(req.text)
                        #json_strErr=json.loads(req.text)
                        #cod=json_strErr['errors'][0]['code']
                        printErrorMessages(str(countrecord),j_content, outfile,client, schema,date_time)
                    elif  code=="5":
                        print(f"{dobj} Record: {countrecord} not imported  ({totaltime} seconds)")
                        printworserecords(j_content,client,schema,date_time+"http_error_500")
                    else:
                        totaltime=round((tend - tini))
                        print(f"{dobj} Record: {countrecord} imported  ({totaltime} seconds)")
                        count+=1
                    id+=1
                except Exception as ee:
                    print(ee)
            printbadrecords(j_content,client,schema)
            print("Imported records:",str(count))
        except requests.exceptions.ConnectionError as e:
                r = "No response"
        except Exception as ee:
            print(f"ERROR: schema: {ee}")

def printbadrecords(data,custom,schemas):
    records={}
    records['errors']=data
    outfilename = json.dumps(records,indent=2)
    with open("runenv/logs/{custom}/"+custom+"_"+schemas+"_records_No_uploaded.json","w") as outfile:
        json.dump(records,outfile)
    outfile.close()
    
def printworserecords(data,custom,schemas,file_name):
    path=Path(os.path.dirname(os.path.realpath(__file__)))
    with open(Path(f"{path}/logs/{custom}/{file_name}.json"),"a+") as outfile:
        outfile.write(json.dumps(data)+ "\n")
    outfile.close()
    
def make_put(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file, schema,client):
        try:
            rec={}
            countrecord=0
            pathPattern=pathPattern
            okapi_url=okapi_url
            #json_file=json_file
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            id=""
            paging_q="/{id}"
            path = pathPattern
            url = okapi_url + path
            f = open(json_file)
            data = json.load(f)
            errorrecord=[]
            count=0
            now=datetime.now()
            date_time = now.strftime("%m_%d_%y_(%H_%M)")
            for i in data[schema]:
                try:
                    j_content=i
                    id=i['id']
                    path = pathPattern+paging_q
                    path=path.replace("{id}",id)
                    #print("old:"+j_content)
                    #if j_content['workflowStatus']=="Pending":
                    #    j_content['workflowStatus']="Open"
                    #print(j_content)
                    #j_content =json.loads(line)
                    countrecord=countrecord+1
                    #print("Record no: "+str(countrecord))
                    url = okapi_url + path
                    tini = time.perf_counter()
                    req = requests.put(url, json=j_content, headers=okapi_headers,timeout=10)
                    #print(req.status_code)
                    #print(req.text)
                    code=str(req.status_code)
                    code=code[0]
                    if code=="4" or code=="5":
                        errorrecord.append(i)
                        outfile = json.dumps(req.text)
                        printErrorMessages(str(countrecord),j_content,req.text,client,schema,date_time)
                    else:
                        tend = time.perf_counter()
                        print(f"Record: {countrecord} Record: updated")
                        count+=1
                    tend = time.perf_counter()
                    printErrorMessages(str(countrecord),j_content,req.text,client,schema,date_time)
                except Exception as ee:
                    print(ee)
            printbadrecords(j_content,client,schema)
            print("Imported records:",str(count))    
        except ValueError:
            print("General Error on POST:"+req.text+"\nError Number:  "+str(req.status_code))
            
def make_get_put(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file, schema,client):
        try:
            now = datetime.now()
            rec={}
            countrecord=0
            pathPattern=pathPattern
            okapi_url=okapi_url
            #json_file=json_file
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            id=""
            paging_q="/{id}"
            path = pathPattern
            url = okapi_url + path
            f = open(json_file,encoding="utf-8")
            data = json.load(f)
            errorrecord=[]
            count=0
            l={}
            date_time = now.strftime("%m_%d_%y_(%H_%M)")
            for i in data[schema]:
                try:
                    j_content=i
                    if i['workflowStatus']=="Pending" and i['reEncumber']:
                        id=i['id']
                        path = pathPattern+paging_q
                        path=path.replace("{id}",id)
                        #print("old:"+json.dumps(j_content))
                        url = okapi_url + path
                        req = requests.get(url, headers=okapi_headers,timeout=40)
                        json_str = json.loads(req.text)
                        l=json_str
                        code=str(req.status_code)
                        code=code[0]
                        if code=="4" or code=="5":
                            print("record not found")
                        else:
                            if l['workflowStatus']=="Pending":
                                l['workflowStatus']="Open"
                                j_content = l
                            #print("New Jcontent: "+json.dumps(j_content))
                            #j_content =json.loads(line)
                            countrecord=countrecord+1
                            #print("Record no: "+str(countrecord))
                            url = okapi_url + path
                            tini = time.perf_counter()
                            req = requests.put(url, json=j_content, headers=okapi_headers,timeout=10)
                            #print(req.status_code)
                            #print(req.text)
                            code=str(req.status_code)
                            code=code[0]
                            if code=="4" or code=="5":
                                errorrecord.append(i)
                                outfile = json.dumps(req.text)
                                printErrorMessages(str(id),outfile,req.text,client,schema,date_time)
                                print(f"Record: {count} Not imported {req.status_code} - {req.text}")
                            else:
                                tend = time.perf_counter()
                                print(f"Record: {count} Record: updated")
                                count+=1
                            #id+=1
                            #tend = time.perf_counter()
                            #printErrorMessages(str(id),outfile,req.text,client,schema,date_time)
                except Exception as ee:
                    print(ee)
            printbadrecords(j_content,client,schema)
            print("Imported records:",str(count))    
        except ValueError:
            print("General Error on POST:"+req.text+"\nError Number:  "+str(req.status_code))
                        
def make_put_byline(pathPattern,okapi_url, okapi_tenant, okapi_token,json_file, schema,client):
        try:
            rec={}
            countrecord=0
            pathPattern=pathPattern
            okapi_url=okapi_url
            json_file=json_file
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            paging_q="/{id}"
            path = pathPattern+paging_q
            url = okapi_url + path
            with open(json_file) as data:
                for line in data:
                    line= data.readline()
                    line=line.replace("\n","")
                    idreplace=line[:44]
                    #print(line)
                    path=path.replace("{id}",idreplace)
                    j_content =json.loads(line)
                    countrecord=countrecord+1
                    #print("Record no: "+str(countrecord))
                    url = okapi_url + path
                    tini = time.perf_counter()
                    req = requests.put(url, json=j_content, headers=okapi_headers,timeout=10)
                    print(req.status_code)
                    print(req.text)
                    tend = time.perf_counter()
                    paging_q="/{id}"
                    path = pathPattern+paging_q
                    url = okapi_url + path
                    #printErrorMessages(str(countrecord),req.status_code,req.text,tini,tend,client+"PUT_logFileName.txt")
        except ValueError:
            print("General Error on POST:"+req.text+"\nError Number:  "+str(req.status_code))          
            
def make_post_login(pathPattern,okapi_url, okapi_tenant, okapi_user,okapy_password, schema,client):
        try:
            u=okapi_user
            p=okapy_password
            rec={}
            countrecord=0
            pathPattern=pathPattern
            okapi_url=okapi_url
            userpass={
                "username": okapi_user,
                "password": okapy_password
            }
            #userpass={"username":okapi_user,"password":okapy_password}
            okapi_headers = {"x-okapi-tenant": okapi_tenant,"content-type": "application/json", "accept": "application/json"}
            path = pathPattern
            url = okapi_url + path
            tini = time.perf_counter()
            req = requests.post(url, json=userpass, headers=okapi_headers,timeout=10)
            print(req)
            print(req.encoding)
            print(req.text)          
            print(req.headers)
            token=req.headers['x-okapi-token']
            tend = time.perf_counter()
            #errorMessages(str(countrecord),req.status_code,req.text,tini,tend,client+"POST_logFileName.txt")
        except ValueError:
            print("General Error on POST:"+req.text+"\nError Number:  "+req.status_code)
            
def filebyline(filetoformat,schema,client):
        try:
            f = open(filetoformat)
            archivo=open(client+"_"+str(filetoformat)+"byline.json", 'w', encoding='utf8')
            # returns JSON object as
            # a dictionary
            data = json.load(f)
            # Iterating through the json
            # list
            for i in data[schema]:
                print(i)
                a_line=str(i)
                archivo.write(a_line+"\n")
            print("file by line, ready")
            # Closing file
            f.close()
        except: 
            print("OOPS!! Error creating the File by line")
            
#printErrorMessages(str(id),req.status_code,str(j_content),client+"_"+schema+"_"+"Errors.txt")
def printErrorMessages(recNum,j_content, reqtext, client, schema,date_time):
    path=os.path.dirname(os.path.realpath(__file__))
    if "PO Number already exists" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_poNumberNotUnique")
        print(f"Record: {recNum} Not imported PO number already exist")                       
    elif "id value already exists in table" in reqtext: 
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_uuiIdDuplicated")
        print(f"Record: {recNum} Not imported id value already exit")
    elif "Order cannot be open as the associated access provider not found" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_accessProviderNotFound")
        print(f"Record: {recNum} Not imported order cannot be open")
    elif "Budget expense class not found" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_budgetexpenseclassNotfound")
        print(f"Record: {recNum} imported with errors")
    elif "Budget not found for transaction" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_budgetNotFoundForTransaction")
        print(f"Record: {recNum} Not imported")
    elif "Order cannot be open as the associated vendor not found" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_vendorNotFound")
        print(f"Record: {recNum} Not imported")
    elif "ISBN value is invalid" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_isbnValueInvalid")
        print(f"Record: {recNum} Not imported")
    elif "PO Line physical quantity and Locations physical quantity do not match" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_physicalLocCostQtyMismatch")
        print(f"Record: {recNum} Not imported")
    elif "Fund cannot be paid due to restrictions" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_fundcannotbepaidduetorestrictions")
        print(f"Record: {recNum} Not imported")
    elif "Physical cost quantity must be specified" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_physicalCostmustSpecified")
        print(f"Record: {recNum} Not imported")
    elif "compositePoLines[0].locations[0].locationId" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_locationIdMissing")
    elif "Ongoing field must be absent for One-time order" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_ongoingNotAllowed")
        print(f"Record: {recNum} Not imported")
    elif "All expected transactions already processed" in reqtext:
        print(f"Record:{recNum} imported with errors")
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_all_expectedtransactionsalreadyprocessed")
    elif "incorrectFundDistributionTotal" in reqtext:
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_incorrectFundDistributionTotal")
    elif "inactiveExpenseClass" in reqtext:
        print(f"Record:{recNum} imported with errors")
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_inactiveExpenseClass")
    elif "Order cannot be open as the associated vendor is inactive" in reqtext:
        print(f"Record:{recNum} Not imported")
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_ordercannotbeopenastheasssociatedvendorisinactive")
    elif "PO Line electronic quantity and Locations electronic quantity do not match" in reqtext:
        print(f"Record:{recNum} Not imported")
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_poLineelectronicquantityandLocationselectronicquantitydonotmatch")
    elif "compositePoLines[0].physical.materialType" in reqtext:
        print(f"Record:{recNum} Not imported")
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_physical.materialTypeBlank")
    elif "" in reqtext:
        print(f"Record:{recNum} Not imported blank record")
        printworserecords(j_content,client,schema,client+"_"+schema+"_"+date_time+"_worse_records")
    with open(f"{path}/logs/{client}/{client}_{date_time}_all_errors.log", "a", encoding="utf-8") as out_file1:
        content=f"Record: {recNum} {reqtext}"
        out_file1.write(content+"\n")
    out_file1.close()
    
        
                            
def Clients():
    try:
        # Opening JSON file
        dic=[]
        pathfile=os.path.dirname(os.path.realpath(__file__))
        f = open(f"{pathfile}/okapi_customers.json",)
        data = json.load(f)
        for i in data['okapi']:
            a_line=str(i)
            dic.append(i['name'])#+"- Version:"+['x-okapi-version']+"-Release: "+['x-okapi-release'])
        f.close()
        return dic
    except: 
        print("OOPS!! General error occurred in Clients")
        
def schemas():
        # Opening JSON file
        dic=[]
        pathfile=os.path.dirname(os.path.realpath(__file__))
        f = open(f"{pathfile}/setting_data.json",)
        data = json.load(f)
        for i in data['settings']:
            a_line=str(i)
            dic.append(i['name'])#+"- Version:"+['x-okapi-version']+"-Release: "+['x-okapi-release'])
        f.close()
        return dic

def get_one_schema(code_search):
    valor=[]
    try:
        #valor="0"
        pathfile=os.path.dirname(os.path.realpath(__file__))
        f = open(f"{pathfile}/setting_data.json",)
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

def get_all_schemas(self,code_search):
        f = open("setting_data.json",)
        data = json.load(f)
        for i in data['settings']:
            valor=i['path']
            break
        f.close()
        return valor

def SearchClient(code_search):
        # Opening JSON file
        dic =dic= {}
        pathfile=os.path.dirname(os.path.realpath(__file__))
        f = open(f"{pathfile}/okapi_customers.json",)
        data = json.load(f)
        for i in data['okapi']:
            a_line=str(i)
            if i['name'] == code_search:
            #if (a_line.find(code_search) !=-1):
                 dic=i
                 del dic['name']
                 del dic['user']
                 del dic['password']
                 del dic['x_okapi_version']
                 del dic['x_okapi_status']
                 del dic['x_okapi_release']
                 break
        f.close()
        return dic
    
def main():
    try:
        okapi=""
        tenant=""
        token=""
        filename=""
        client = {}
        print(Clients())
        print("Enter Customer name:")
        cuts_name = str(input())
        client=SearchClient(cuts_name)
        if len(client)>0:
            ale=str(cuts_name)
            okapi=str(client.get('x_okapi_url'))
            tenant=str(client.get('x_okapi_tenant'))
            token=str(client.get('x_okapi_token'))
            opt=1
            path_data=""
#            client=faf.AcqErm(client)
            #client.createdFolderStructureenv(client)
            while opt!=0:
                print("0. Exit"+"\n"+"1. GET"+"\n"+"11 GET CREDENTIAL"+"\n"+" 13 GET BY UUID"+"\n"+"2. POST"+"\n"+"  21 POST by line"+"  22 POST credential by line"+"\n"+"3.PUT"+"\n"+"31. PUT by line"+"\n"+"32. GET and PUT"+"\n"+"4.DEL"+"\n"+"   41 DEL by Line"+"\n"+"   42 DEL + POST"+"\n"+"5.GET TOKEN")
                opt = int(input())
                print(schemas())
                print("Enter schema name:")
                sn = input()
                #print("searching the path in setting file...")
                schema_name=str(sn)
                paths=get_one_schema(schema_name)
                load_data=os.path.dirname(os.path.realpath(__file__))
                path_data=Path(f"{load_data}/data")
                path_refdata=Path(f"{load_data}/results/{cuts_name}")
                if len(paths)>0:
                    print("the path has been found "+schema_name)
                    pathschema=paths[0]
                    nameschema=paths[1]
                    if opt==1:
                        tic = time.perf_counter()
                        a=backup()
                        filename = str(cuts_name+"_"+schema_name)
                        print("query? - all hit enter")
                        APIquery=input()
                        backup.make_get(pathschema,okapi,tenant,token,APIquery,filename,path_refdata)
                        toc = time.perf_counter()
                        print(f"Getting time in {toc - tic:0.4f} seconds")
                        #filebyline(filename,nameschema,ale)
                    elif opt==11:
                        tic = time.perf_counter()
                        print("Interfaces file name like customerCode_interfaces.json")
                        filename=input()
                        a=backup()
                        backup.make_get_credentials(pathschema,okapi,tenant,token,cuts_name+"_"+filename)
                        toc = time.perf_counter()
                        print(f"Getting time in {toc - tic:0.4f} seconds")
                    elif opt==12:
                        tic = time.perf_counter()
                        print("file to read: ")
                        filename=input()
                        a=backup()
                        backup.make_get_credentials(pathschema,okapi,tenant,token,filename)
                        toc = time.perf_counter()
                        print(f"Getting time in {toc - tic:0.4f} seconds")
                    elif opt==13:
                        tic = time.perf_counter()
                        print("file to read: ")
                        filename=input()
                        a=backup()
                        backup.make_get_by_uuid(pathschema,okapi,tenant,token,Path(f"{path_data}/{filename}"),f"{path_refdata}",nameschema,ale)
                        #backup.make_get_id(pathschema,okapi,tenant,token,f"{path_data}/{filename}",f"{path_refdata}",nameschema,ale)
                        toc = time.perf_counter()
                        print(f"Getting time in {toc - tic:0.4f} seconds")          
                    elif opt==2:
                        tic = time.perf_counter()
                        #michstate_test_
                        #filename = str("michstate_test_"+schema_name+".json")
                        filename = str(f"{cuts_name}_{schema_name}.json")
                        print("Name file to upload: "+filename)
                        fileN = input()
                        if fileN!="":
                            filename=fileN
                        make_post(pathschema,okapi,tenant,token,Path(f"{path_data}/{filename}"),nameschema,ale)
                        toc = time.perf_counter()
                        print(f"Getting time in {toc - tic:0.4f} seconds hours")
                    elif opt==21:
                        tic = time.perf_counter()
                        #michstate_test_
                        filename = str(f"{cuts_name}_{schema_name}.json")
                        print(f"Does the Name file to upload: filename?")
                        filename = input()
                        #bbb = input()
                        #a=backup()
                        print(f"{path_data}/{filename}")
                        
                        make_post_byline(pathschema,okapi,tenant,token,Path(f"{path_data}/{filename}"),nameschema,ale)
                        toc = time.perf_counter()
                        print(f"Getting time in {toc - tic:0.4f} seconds hours")
                    elif opt==22:
                        tic = time.perf_counter()
                        #michstate_test_
                        filename = str(f"{cuts_name}_{schema_name}.json")
                        print(f"Does the Name file to upload: filename?")
                        filename = input()
                        #bbb = input()
                        #a=backup()
                        make_post_byline_credentials(pathschema,okapi,tenant,token,Path(f"{path_data}/{filename}"),nameschema,ale)
                        toc = time.perf_counter()
                        print(f"Getting time in {toc - tic:0.4f} seconds hours")
                    elif opt==3:
                        tic = time.perf_counter()
                        #michstate_test_
                        #filename = str("michstate_test_"+schema_name+".json")
                        filename = str(cuts_name+"_"+schema_name+".json")
                        print("Name file to upload: "+filename)
                        option= input()
                        if option!='':
                            filename=option
                        #a=backup()
                        make_put(pathschema,okapi,tenant,token,Path(f"{path_data}/{filename}"),nameschema,cuts_name)
                        toc = time.perf_counter()
                        print(f"Getting time in {toc - tic:0.4f} seconds hours")
                    elif opt==31:
                        tic = time.perf_counter()
                        #michstate_test_
                        #filename = str("michstate_test_"+schema_name+".json")
                        filename = str(cuts_name+"_"+schema_name+".json")
                        print("Name file to upload: "+filename)
                        filename = input()
                        #a=backup()
                        make_put_byline(pathschema,okapi,tenant,token,Path(f"{path_data}/{filename}"),nameschema,ale)
                        toc = time.perf_counter()
                    elif opt==32:
                        tic = time.perf_counter()
                        #michstate_test_
                        #filename = str("michstate_test_"+schema_name+".json")
                        filename = str(cuts_name+"_"+schema_name+".json")
                        print("Name file to upload: "+filename)
                        filename= input()
                        #a=backup()
                        make_get_put(pathschema,okapi,tenant,token,Path(f"{path_data}/{filename}"),nameschema,ale)
                        toc = time.perf_counter()
                        print(f"Getting time in {toc - tic:0.4f} seconds hours")
                    elif opt==4:
                        tic = time.perf_counter()
                        filename=ale+"_"+str(sn)+".json"
                        #filename="licenses.json"
                        print("is it the JSON file name:"+filename+" ?")
                        fileN=input()
                        if fileN!="":
                            filename=fileN
                        backup.make_del(pathschema,okapi,tenant,token,Path(f"{path_data}/{filename}"),nameschema,ale)
                        toc = time.perf_counter()
                        toctic=(((toc - tic)/60)/60)
                        print(f"Deleting time in {toc - tic:0.4f} seconds {toctic} hours")
                    elif opt==41:
                        tic = time.perf_counter()
                        filename=ale+"_"+str(sn)+".json"
                        #filename="licenses.json"
                        print("is it the JSON file name:"+filename+" ?")
                        fileN=input()
                        if fileN!="":
                            filename=fileN
                        backup.make_del_byLine(pathschema,okapi,tenant,token,Path(f"{path_data}/{filename}"),nameschema,ale)
                    elif opt==42:
                        tic = time.perf_counter()
                        #filename=ale+"_"+str(sn)+".json"
                        #filename="licenses.json"
                        print("Enter JSON file name:")
                        filename=input()
                        backup.make_del_post(pathschema,okapi,tenant,token,filename,nameschema,ale)
                        toc = time.perf_counter()
                        toctic=(((toc - tic)/60)/60)
                        print(f"Deleting time in {toc - tic:0.4f} seconds {toctic} hours")                    
                    elif opt==5:
                        #print("User:")
                        user_validation = "admin"
                        #print("Password:")
                        password_validation = "Concep#rd@1060"
                        #a=backup()
                        #make_post_login(pathPattern,okapi_url, okapi_tenant, okapi_user,okapy_password, schema,client):
                        make_post_login(pathschema,okapi,tenant,user_validation,password_validation,nameschema,ale)
                else:
                    print("the path has not been found "+schema_name)
                    #sys.exit()
        else:
            print("Customer does not exist in the okapi file, try again the okapi customer should be include in okapi file")
    except Exception as ee:
        print(f"ERROR: schema: {ee}")

def printObject(objectToPrint,path,x,file_name,prettyJson):
    try:
        outfilename=""
        #toPrint=json_validator(objectToPrint)
        if prettyJson:
            try:
                path_file=f"{path}/{file_name}.json"
                #outfilename = json.load(objectToPrint)
                with open(path_file,"w+", encoding="utf-8") as outfile:
                    json.dump(objectToPrint,outfile,indent=2)
            except Exception as ee:
                print("ERROR: "+ee)
        else:
            path_file=path_file=f"{path}/{file_name}.json"
            outfilename = json.dumps(objectToPrint)
            with open(path_file,"a+") as outfile:
                outfile.write(outfilename+"\n")
        return None
    except Exception as ee:
        print("ERROR: "+ee)
    
    
#===================MAIN==================================
if __name__ == "__main__":
    """This is the Starting point for the script"""
    print("MENU")
    main()
    #https:/okapi-michstate-lm.folio.ebsco.com/finance-storage/transactions?limit=9999&query=transactionType=Encumbrance
