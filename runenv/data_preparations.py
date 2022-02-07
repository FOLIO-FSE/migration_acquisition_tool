import csv
import pandas as pd

import logging
import time
import datetime as datetime
import random
import json
import codecs
from xlsx2csv import Xlsx2csv
from io import StringIO



def chagebibdata():
    orderList=[]
    dt = datetime.datetime.now()
    dt=dt.strftime('%d%m%Y')
    count=0
    dataitemhol=[]
    in_file=open("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\data\\BIBLIOGRAPHIC_1574568070002776_1.mrk","r", encoding="utf-8")
    in_holdings="C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\data\\Holdings_20220105.tsv"
    #hol = pd.read_csv(in_holdings,dtype ='str')
    hol = pd.read_csv(in_holdings,sep="\t", dtype ='str')
    totalhol=len(hol)
    errorscount=0
    logging.info(f'Total Hol {totalhol}')
    hol['MMS ID'] = hol['MMS ID'].astype('string')
    hol['MMS ID'] = hol['MMS ID'].str.strip()
    hol['MMS ID NEW'] = hol['MMS ID NEW'].astype('str')
    hol['MMS ID NEW'] = hol['MMS ID NEW'].str.strip()
    items_holding=[]
    logging.basicConfig(filename="C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\results\\changebibdata"+str(dt)+".log", encoding='utf-8', level=logging.INFO)
    #print(hol['MMS ID NEW'])
    logging.info(f"START")
    for a_line in in_file:
        tini = time.perf_counter()
        tag=a_line[1:4]
        new_line=a_line.replace("\n","")
        printtag=True
        tag035MMSI=""
        if tag=="001":
            recordfound=0
            cadena=new_line
            id=str(cadena[6:]).strip()
            tag035MMSI=f"=035  \\$a{id}"
            mmsid = hol[hol['MMS ID']== id]
            #print(mmsid)
            recordfound=len(mmsid)
            for c, cprow in mmsid.iterrows():
                new_id=cprow['MMS ID NEW']
                new_line=f"=001  {new_id}"
                tag001=""
                tag004=""
                tag0041=""
                tag001=str(cprow['Holding Id']).strip()
                tag0041=new_id
                print(tag0041)
                print(tag001)
                swloc=False
                swcall=False
                location=""
                callNumber=""
                ubica=""
                if cprow['852 MARC']:
                    loca=str(cprow['852 MARC']).strip()
                    swloc=False
                    swcall=False
                    x=loca.find("$$b")
                    if x!=-1:
                        y=loca.find("$$c")
                        btk=str(loca[x+3:y-1]).strip()
                        swloc=True
                    else:
                        btk="undefined"
                    c=loca.find("$$c")
                    if c!=-1:
                        d=loca.find("$$h")
                        if d!=-1:
                            ubica=str(loca[c+3:d-1]).strip()
                        else:
                            ubica=str(loca[c+3:]).strip()
                            e=ubica.find("$$")
                            if e!=-1:
                                ubica=str(loca[c+3:d-1])    
                    else: 
                        ubica="undefined"
                        
                    
                    location=f"{btk} {ubica}"
                    location=location.replace("VINA", "VIÑA")
                    location=location.replace("undefined", "UNASSIGNED")
                    location=location.replace("POS", "POST")
                    location=location.replace("SAN", "PRE1")
                    
                    p=loca.find("$$h")
                    if p!=-1:
                        cla=loca[p:]
                        callNumber=str(cla).strip()
                        swcall=True
                    else:
                        callNumber="undefined"
                else:
                    location="unmapped"
                location=location.replace("$h$$h", "$$h")
                if swcall and swloc:
                    with open("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\results\\mfhf_"+str(dt)+".mrk","a",encoding="utf-8") as f:
                        f.write(f"=LDR  00232nx  a22000974n 4500"+"\n")
                        f.write(f"=001  {tag001}"+"\n")
                        f.write(f"=004  {tag0041}"+"\n")
                        f.write(f"=008  9810090p\\\\\\\\8\\\\\\4001aueng0000000"+"\n")
                        f.write(f"=852  0\$b{location}$h{callNumber}"+"\n")
                        f.write("\n")
                else:
                    with open("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\results\\mfhf_worse_"+str(dt)+".mrk","a",encoding="utf-8") as f:
                        f.write(f"=LDR  00232nx  a22000974n 4500"+"\n")
                        f.write(f"=001  {tag001}"+"\n")
                        f.write(f"=004  {tag0041}"+"\n")
                        f.write(f"=008  9810090p\\\\\\\\8\\\\\\4001aueng0000000"+"\n")
                        f.write(f"=852  0\$b{location}$h{callNumber}"+"\n")
                        f.write("\n")
                
                #print(items)
            if recordfound==0:
                errorscount+=1
                print(f"record {count} -- {new_id}")
                print(f"Record Holding {new_id} {count}/{totalhol}")
                #logging.info(f"NOT Record Holding | {new_id} | {count}/{totalhol}")
                errordesc=f"NOT Record Holding {count}  {new_id}   count:{errorscount}"            
                with open("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\results\\malos_"+str(dt)+".txt","a",encoding="utf-8") as errorline:
                    errorline.write(errordesc+"\n")
                id=str(cadena[6:]).strip()
                # new_id=id[:-4]
                new_line=f"=001  {new_id}"
            else:
                tend = time.perf_counter()
                totaltime=round((tend - tini))
                print(f"record {count} -- {new_id}")
                print(f"Record Holding {new_id} {count}/{totalhol} -- {tag0041} {swcall} - ({totaltime}) seconds")
                buenosa=f"Record {count}/{totalhol} |   {new_id}    {tag0041}   {swcall}    ({totaltime} seconds)"
                #logging.info(f"Record Holding {new_id} {count}/{totalhol} -- {tag0041} {swcall}")
                #dataitemhol.append([tag001,new_id,callNumber,location])
                with open("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\results\\buenos_"+str(dt)+".txt","a",encoding="utf-8") as buenos:
                    buenos.write(buenosa+"\n")
            count+=1
        elif tag=="STA" or tag=="OWN" or tag=="SID":
            printtag=False
        if printtag:
            with open("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\results\\marc_modified_"+str(dt)+".mrk","a",encoding="utf-8") as newmarc:
                if tag=="001":
                    countlist = orderList.count(str(new_line))
                    if countlist>0:
                        new_line=new_line+str(round(random.randint(10, 100)))
                        with open("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\results\\duplicatedID"+str(dt)+".txt","a",encoding="utf-8") as newmarc:
                            newmarc.write(f"id"+"\n")
                    orderList.append(str(new_line))
                    newmarc.write(new_line+"\n")
                    if tag035MMSI!="":
                        newmarc.write(tag035MMSI+"\n")
                else:
                    newmarc.write(new_line+"\n")
        new_line=""
        printtag=True
                
    #header=['holding','bibId','clasification', 'location']
    #with open("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\results\\holdings_12122021.csv","a",encoding="utf-8") as itemhol:
    #    writer = csv.writer(f)
    #    writer.writerow(itemhol)
    #    writer.writerow(dataitemhol)
def tocsv():
    dt = datetime.datetime.now()
    dt=dt.strftime('%Y%m%d')
    count=0 
    dataitemhol=[]
    in_holdings=open("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\data\\location.csv","r")
    hol = pd.read_csv(in_holdings,encoding="utf-8", dtype ='str')
    hol['location_code']=hol['legacy_code']
    hol['folio_code']=hol['legacy_code']
    totalhol=len(hol)       
    uai_csv_data = hol.to_csv("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\results\\locations_12112021.tsv", sep="\t", index=False, header = True, quoting=csv.QUOTE_NONE)
        
def spreadsheet_to_csv():
    in_items="C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\tadeo\\data\\items.xlsx"
    buffer = StringIO()
    start_time = time.perf_counter()
    Xlsx2csv(in_items, outputencoding="utf-8").convert(buffer)
    buffer.seek(0)
    items = pd.read_csv(buffer)
    print("total items original: "+str(len(items)))
    tt=len(items)
    end_time = time.perf_counter()
    total_time= round((end_time - start_time))/60
    print(f" records {tt} / total {total_time}:{60} seconds")
    uai_csv_data = items.to_csv("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\tadeo\\results\\tadeo_items.tsv", sep="\t", index=False, header = True, quoting=csv.QUOTE_NONE)
    print("end")
    
def holding_to_csv():
    dt = datetime.datetime.now()
    dt=dt.strftime('%Y%m%d')
    count=0
    dataitemhol=[]
    #in_file=open("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\data\\BIBLIOGRAPHIC_1539004070002776_1\\BIBLIOGRAPHIC_1539004070002776_1.mrk","r", encoding="utf-8")
    in_items="C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\data\\Holding M_Final.xlsx"
    start_time = time.perf_counter()
    items = pd.read_excel(in_items, engine='openpyxl', dtype ='str')
    print("total items original: "+str(len(items)))
    tt=len(items)
    end_time = time.perf_counter()
    total_time= round((end_time - start_time))/60
    print(f" records {tt} / total {total_time}:{60} seconds")
    uai_csv_data = items.to_csv("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\data\\Holdings_"+str(dt)+".tsv", sep="\t", index=False, header = True, quoting=csv.QUOTE_NONE)    

def fix_dup():
    import uuid
    in_holdings="C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\uai\\data\\Ejemplares_columnB - Copy.xlsx"
    #hol = pd.read_csv(in_holdings,dtype ='str')
    print("reading ejemplares")
    hol = pd.read_excel(in_holdings, engine='openpyxl', dtype ='str')
    totalhol=len(hol)
    print(f"total items {totalhol}")
    #errorscount=0
    tupla=[]
    print("reading json")
    json_file="C:\\Users\\asoto\\Documents\\EBSCO\\Migrations\\folio\\client_data\\uai\\results\\folio_item_20211218-022127 - Copy.json"
    with open(json_file, "r", encoding="utf") as file_j:
        for linea in file_j:
            recitem=linea
            recitem=recitem.replace(",\n", "")
            data = json.loads(recitem)
            barcode=str(data['barcode'])
            newid=str(uuid.uuid4())
            data['id']=newid
            tupla.append([barcode,data])
    #print (tupla)
    df = pd.DataFrame(tupla, columns=['barcode', 'json'])
    print(df)
    print(f'Total Hol {totalhol}')
    dfnote = hol[hol['Estado']== "No migró"]
    totdfnote=len(dfnote)
    path_file="C:\\Users\\asoto\\Documents\\EBSCO\\Migrations\\folio\\client_data\\uai\\results\\folio_itemII_20211218-022127-NoMigrated.json"
    print(f"Total no migrados: {totdfnote}")
    count=0
    for i, row in dfnote.iterrows():
        barcode=str(row['Barcode']).strip()
        count+=1
        print(f"record {count}")
        dfrec = df[df['barcode']== barcode]
        for j, nrow in dfrec.iterrows():
            item_record=nrow['json']
            outfilename = json.dumps(item_record,ensure_ascii=False)
            with codecs.open(path_file,"a+", encoding="utf-8") as outfile:
                outfile.write(outfilename+",\n")
                
def exceltodataframe(path: str, sheet_name: str):
    import dataframe_class as pd
    temp="uai"
    temp=pd.dataframe()
    df=temp.importDataFrame(path)
    print (df)
    return df
    
def fix_dupmich():
    import uuid
    in_holdings="C:\\Users\\asoto\\code\\migration_acquisition_tool\\client_data\\michstate_prod\\data\\MSU Historic orders 2 years order info title only(1).xlsx"
    #in_holdings="C:\\Users\\asoto\\code\\migration_acquisition_tool\\client_data\\michstate_prod\\data\\MSU Historic orders 2 years order info title only(1) - Copy.xlsx"
    
    #hol = pd.read_csv(in_holdings,dtype ='str')
    buffer = StringIO()
    Xlsx2csv(in_holdings, outputencoding="utf-8").convert(buffer)
    buffer.seek(0)
    hol = pd.read_csv(buffer)
    print("reading ejemplares")
    #hol = pd.read_excel(in_holdings, engine='openpyxl', dtype ='str')
    if 'RECORD #(ORDER)' in hol:
        hol['RECORD #(ORDER)']=hol['RECORD #(ORDER)'].astype('str')
        hol['RECORD #(ORDER)']=hol['RECORD #(ORDER)'].str.strip()    
    totalhol=len(hol)
    hol = hol.apply(lambda x: x.fillna(""))
    print(f"total items {totalhol}")
    #errorscount=0
    tupla=[]
    print("reading json")
    #json_file="C:\\Users\\asoto\\Documents\\EBSCO\\Migrations\\folio\\client_data\\uai\\results\\folio_item_20211218-022127 - Copy.json"
    count=0
    #json_file="C:\\Users\\asoto\\code\\migration_acquisition_tool\\client_data\\michstate_prod\\results\\michstate_prod_purchaseOrderbyline_with_new_instance_20220104-10-02.json"
    json_file="C:\\Users\\asoto\\code\\migration_acquisition_tool\\client_data\\michstate_prod\\results\\michstate_prod_purchaseOrderbyline_20220104-10-02.json"
    with open(json_file, "r", encoding="utf") as file_j:
        for linea in file_j:
            recitem=linea
            recitem=recitem.replace(",\n", "")
            data = json.loads(recitem)
            poNumber=str(data['poNumber'])
            poNumber="o"+poNumber
            dfnote = hol[hol['RECORD #(ORDER)']== poNumber]
            totdfnote=len(dfnote)
            print(f"record {count}/ 68611 -  {poNumber}")
            for i, row in dfnote.iterrows():
                if row['CLOSE REASON']:
                    closereason=str(row['CLOSE REASON']).strip()
                    closeReason={"reason": closereason,"note": ""}
                    data['closeReason']=closeReason
                #print(data)
                count+=1
                
                value=str(row['RLOC']).strip()
                if value=="MSU REPLACEMENTS":
                    shipto="aa4e5202-a945-4094-87da-e82ac7184221"
                elif value=="MSU SERIAL BACK ORDER":
                    shipto="cdd3895c-980e-4511-8b56-6cc039d653ac"
                elif value=="MSU DOCS":
                    shipto="246ce69e-fab7-4990-8b5a-4c3bd135226d"
                elif value=="MSU SERIALS ACQUISITIONS":
                    shipto="369633a7-d2b9-423f-a035-310bc901062f"
                elif value=="MSU BOOKS RECEIVING":
                    shipto="77f43ed0-eee7-4b45-a35c-7e17f235e0bb"	
                elif value=="MSU MONORUSH":
                    shipto="d989ee9b-4434-4641-9ed2-94b2f2df8018"
                elif value=="MSU GAST BUSINESS LIBRARY":
                    shipto="2bd81248-eb52-499c-bc49-bd380f2eb982"
                elif value=="MSU LESLIE MCROBERTS":
                    shipto="61ba8775-4a0f-4e29-9b8d-862f8f37287d"
                elif value=="MSU TAD BOEHMER":			
                    shipto="20035bef-25f5-4609-8e65-a3e1154aa2d1"
                data['shipTo']=shipto
                #del data['billTo']
                ven=data['vendor']
                for i in data['compositePoLines']:
                    poli=i
                    if 'physical' in i:
                        #poli['materialSupplier']=ven
                        data['compositePoLines'][0]['physical']['materialSupplier']=ven  
                    if 'eresource' in i:
                        #poli['accessProvider']=ven                 
                        data['compositePoLines'][0]['eresource']['accessProvider']=ven
                    if 'acquisitionMethod' in i:
                        if  poli['acquisitionMethod']=="DDA":
                            data['compositePoLines'][0]['acquisitionMethod']="Demand Driven Acquisitions (DDA)"
                    if 'acquisitionMethod' in i:
                        if  poli['acquisitionMethod']=="EBA":
                            data['compositePoLines'][0]['acquisitionMethod']="Evidence Based Acquisitions (EBA)"
                    if 'acquisitionMethod' in i:
                        if  poli['acquisitionMethod']=="Membership":
                            data['compositePoLines'][0]['acquisitionMethod']="Purchase At Vendor System"   
                    productos=[]
                    if row['CODE3']:
                        if row['CODE3']!="nan":
                            producti=str(row['CODE3']).strip()
                            pp=0
                            for l in data['compositePoLines'][0]['details']['productIds']:
                                aa=l
                                productos.append(aa)
                            productos.append({"productId": producti, "productIdType": "8e3dd25e-db82-4b06-8311-90d41998c109"})
                            data['compositePoLines'][0]['details']['productIds']=productos   
            #print(data)
            path_file=f"C:\\Users\\asoto\\code\\migration_acquisition_tool\\client_data\\michstate_prod\\results\\michstate_prod_purchaseOrders_20220104-10-02_modified.json"
            outfilename = json.dumps(data,ensure_ascii=False)
            with codecs.open(path_file,"a+", encoding="utf-8") as outfile:
                outfile.write(outfilename+"\n")
    print(f"END")
    
def extractdata():
    ids={}
    json_file="C:\\Users\\asoto\\code\\migration_acquisition_tool\\client_data\\michstate_prod\\results\\tocompare.json"
    out_fileid=f"C:\\Users\\asoto\\code\\migration_acquisition_tool\\client_data\\michstate_prod\\results\\idsmsu.json"
    count=1
    with open(json_file, "r", encoding="utf") as file_j:
        for linea in file_j:
            print(f"record {count}")
            recitem=linea
            recitem=recitem.replace(",\n", "")
            data = json.loads(recitem)
            poNumber=str(data['poNumber'])
            ids['legacy_id']=poNumber
            idpo=str(data['id'])
            ids['id']=idpo
            idpoline=str(data['compositePoLines'][0]["id"])
            ids['compositePoLines_id']=[idpoline]
            outfilename = json.dumps(ids,ensure_ascii=False)
            with codecs.open(out_fileid,"a+", encoding="utf-8") as outfile:
                outfile.write(outfilename+"\n")
            poNumber=""   
            idpoline=""
            idpo=""
            count+=1
    print("end")
            
def cairn_fix_holdings():
    temp=pd.DataFrame()
    in_holdings=open("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\cairn\\data\\20220202-125611_items_with_boundwiths_handled.tsv","r")
    hol = pd.read_csv(in_holdings,encoding="utf-8", sep='\t', dtype ='str')
    hol = hol.apply(lambda x: x.fillna(""))
    lendA = len(hol)
    temp = hol.drop(hol[hol.BARCODE==""].index)
    lendB = len(temp)
    print(f"{lendA} {lendB}")
    count=0
    delcount=0
    for i, row in temp.iterrows():
        count+=1
        print(f"count {count}")
        #print(row['CALL #(ITEM)'])
        if row['CALL #(ITEM)']!="":
            temp['CALL #(BIBLIO)'] = temp['CALL #(BIBLIO)'].replace(row['CALL #(BIBLIO)'],row['CALL #(ITEM)'])
            #row['CALL #(BIBLIO)']=row['CALL #(ITEM)']
        if row['IMESSAGE']=="c":
            temp['IMESSAGE']= temp['IMESSAGE'].replace(row['IMESSAGE'],"Check for CD")
        elif row['IMESSAGE']=="d":
            temp['IMESSAGE']= temp['IMESSAGE'].replace(row['IMESSAGE'],"Check for Disk")
            #row['IMESSAGE']="Check for Disk"
        elif row['IMESSAGE']=="e":
            #row['IMESSAGE']="Check for DVD"
            temp['IMESSAGE']= temp['IMESSAGE'].replace(row['IMESSAGE'],"Check for DVD")
        elif row['IMESSAGE']=="m":
            #row['IMESSAGE']="Check for Tape"
            temp['IMESSAGE']= temp['IMESSAGE'].replace(row['IMESSAGE'],"Check for Tape")
        elif row['IMESSAGE']=="p":
            #row['IMESSAGE']="Count Pieces"
            temp['IMESSAGE']= temp['IMESSAGE'].replace(row['IMESSAGE'],"Count Pieces")
        elif row['IMESSAGE']=="t":
            #row['IMESSAGE']="Send to TS"
            temp['IMESSAGE']= temp['IMESSAGE'].replace(row['IMESSAGE'],"Send to TS")
        else: 
             row['IMESSAGE)']=""

        
        
               
    uai_csv_data = temp.to_csv("C:\\Users\\asoto\\Documents\\EBSCO\Migrations\\folio\\client_data\\cairn\\results\\07022022_cairn_itemsmodified.tsv", sep="\t", index=False, header = True, quoting=csv.QUOTE_NONE)
    print(f"total: {count} borrados:{delcount}")

if __name__ == "__main__":
    #fix_dupmich()
    #holding_to_csv()
    """This is the Starting point for the script"""
    #chagebibdata()
    #https://raw.githubusercontent.com/folio-org/mod-inventory-storage/v22.0.3/ramls/instance.json
    #Insert the customer c-ode here or enter the customer running the code, by default blank
    #tocsv()
    #exceltodataframe("C:\\Users\\asoto\\Documents\\EBSCO\\Migrations\\folio\\client_data\\uai\\data\\Ejemplares M_Final.xlsx", "Sheet1")
    #fix_dup()
    #spreadsheet_to_csv()
    #spreadsheet_to_csv():
    cairn_fix_holdings()
    #extractdata()
    