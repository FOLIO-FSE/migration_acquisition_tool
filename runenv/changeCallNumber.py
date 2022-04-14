import csv
import pandas as pd
import json
import requests
import sys
from requests.exceptions import HTTPError
import time

def make_post_login(pathPattern,okapi_url, okapi_tenant, okapi_user,okapy_password):
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
            #print(req)
            #print(req.encoding)
            #print(req.text)          
            #print(req.headers)
            token=req.headers['x-okapi-token']
            tend = time.perf_counter()
            return token
            #errorMessages(str(countrecord),req.status_code,req.text,tini,tend,client+"POST_logFileName.txt")
        except ValueError:
            print("General Error on POST:"+req.text+"\nError Number:  "+req.status_code)
            return None
        


def make_get_put(pathPattern,okapi_url, okapi_tenant, okapi_token,prefix,callNumber,sufix):
        try:
            okapi_headers = {"x-okapi-token": okapi_token,"x-okapi-tenant": okapi_tenant,"content-type": "application/json"}
            path = pathPattern
            url = okapi_url + path
            req = requests.get(url, headers=okapi_headers,timeout=40)
            json_str = json.loads(req.text)
            #print(req.text)
            l=json_str
            code=str(req.status_code)
            code=code[0]
            if code=="4" or code=="5":
                print("record not found")
            else:
                if prefix:
                    l['callNumberPrefix']=prefix
                if callNumber:
                    l['callNumber']=callNumber
                if sufix:
                    l['callNumberSuffix']=sufix
                j_content = l
                url = okapi_url + path
                tini = time.perf_counter()
                req = requests.put(url, json=j_content, headers=okapi_headers,timeout=10)
                #print(req.status_code)
                #print(req.text)
                code=str(req.status_code)
                code=code[0]
                if code=="4" or code=="5":
                    print(f"Record: {pathPattern} No updated")
                    print(req)
                    print(req.encoding)
                    print(req.text)          
                    print(req.headers)
                else:
                    tend = time.perf_counter()
                    print(f"Record: {pathPattern} updated")
                    return True
                    
        except ValueError:
            print("General Error on POST:")
            return False


if __name__ == "__main__":
    okapi_user = "admin_uai"
    okapy_password = "Uai.2022"
    pathPattern="/authn/login"
    okapi_url="https://okapi-uai.folio.ebsco.com"
    okapi_tenant="fs00001091"
    okapi_token=make_post_login(pathPattern,okapi_url, okapi_tenant, okapi_user,okapy_password)
    count=0
    if okapi_token is not None:
        fileName= "C:\\Users\\asoto\\Documents\\EBSCO\\Migrations\\folio\\client_data\\uai\\Copy of Holdings Clasificación.xlsx"
        df = pd.read_excel(fileName, engine='openpyxl')
        df = df.apply(lambda x: x.fillna(""))
        totalrows=len(df)
        print(f"INFO ORDERS Total: {totalrows}") 
        for i, row in df.iterrows():
            prefix=""
            CallNumeber=""
            sufix=""
            holdingsRecordId=""
            holdingsRecordId=str(row['holdingId']).strip()
            prefix=str(row['Prefijo']).strip()
            callNumeber=str(row['Clasificación']).strip()
            sufix=str(row['Sufijo']).strip()
            flag =make_get_put(f"/holdings-storage/holdings/{holdingsRecordId}",okapi_url, okapi_tenant, okapi_token,prefix,callNumeber,sufix)
            if flag:
                print(f"Record {count} // {holdingsRecordId} changed")
                with open("holdingsIdChanged","a+", encoding="utf8") as outfile:
                    writer = csv.writer(outfile)
                    writer.writerow(holdingsRecordId)
                    count+=1
            else:
                print(f"Record {holdingsRecordId} no changed")