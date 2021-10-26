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
import folioAcqfunctions as faf
from typing import List


###########################
### FUNDS
#############

def readfunds(path,file_name1,file_name2,sheetName,customerName):
        try:
            expenses=faf.readFileToDataFrame(file_name1,orderby="",distinct=[],sep="") 
            funds= faf.readFileToDataFrame(file_name2,orderby="",distinct=[],sep="")          
            count=1
            for c, row in expenses.iterrows():
                cp={}
                searchvalue=str(row['code']).strip()
                expId=faf.get_Id(customerName,searchValue=searchvalue,element="expenseClasses")
                for d, cprow in funds.iterrows():
                    searchvalue=str(cprow['code']).strip()
                    budgetId=faf.get_Id(customerName,searchValue=searchvalue+"-FYMSU2022",element="budgets")
                    cp["id"]=str(uuid.uuid4())
                    cp["budgetId"]=budgetId
                    cp["expenseClassId"]=expId
                    cp["status"]="Active"
                    count+=1
                    faf.printObject(cp,path,count,"ExpenseBudgets_productions2")
        except ValueError as error:
            print("Error: %s" % error)                 
                
def SearchClient(path):
        # Opening JSON file
        dic =dic= {}
        f = open("michstate_test_budgets.json",)
        data = json.load(f)
        count=0
        for i in data['budgets']:
            count=+1
            if i['allocated'] >0:
                i['allocated']=0.0
            if i['initialAllocation']>0:
                i['initialAllocation']=0.0
            if i['totalFunding']>0:
                i['totalFunding']=0.0
            if i['cashBalance']>0:
                i['cashBalance']=0.0
            if i['available']>0:
                i['available']=0.0
            if i['unavailable']>0:
                i['unavailable']=0.0
            if i['expenditures']>0:
                i['expenditures']=0.0
            if i['awaitingPayment']>0:
                i['awaitingPayment']=0.0
            if i['encumbered']>0:
                i['encumbered']=0.0
            del i['initialAllocation']
            del i ['allocationTo'] 
            del i['allocationFrom']
            del i['totalFunding']
            del i['cashBalance']
            del i['metadata']
            name=str(i['name'])
            namea=name.replace("FY2021","FYMSU2022")
            i['name']=namea
            fy=i['fiscalYearId']
            fya=fy.replace("71058bdc-7088-45e7-be69-5b89f7d50d2c","f28b758c-2bd8-480e-a5a3-eede648fdc34")
            i['fiscalYearId']=fya
            faf.printObject(i,path,count,"budgets")
        f.close()
def readInterfacesSpreadsheet(idSearch,path,sheetName,customerName):
    rowi=""
    col_types={"CTACT CODE":str}
    interfacesId=[]
    conID=""
    FN=""
    LN=""
    contcategories=""
    interface = pd.read_excel(path,sheet_name=sheetName, dtype=col_types)
    #print(interface)
    interface_filter = interface[interface['CTACT CODE']== idSearch]
    interface_filter = interface_filter.apply(lambda x: x.fillna(""))
    print("Interface founds: ",len(interface_filter))
    for inter, rowi in interface_filter.iterrows():
        intName=""
        inttype=""
        interNote=""
        interStanote=""
        intUri=""
        creuser=""
        crepass=""
        if rowi[1]:
            interId=str(uuid.uuid4())
            if rowi[1]:
                intName =str(rowi[1])
            if rowi[2]: inttype =faf.interfacetype(rowi[2])
            if rowi[3]: intUri =str(rowi[3])
            if rowi[4]: interNote =str(rowi[2])
            if rowi[5]: creuser =str(rowi[5])
            if rowi[6]: crepass =str(rowi[6])
            if rowi[7]: interStanote=str(rowi[6])
            org=faf.interfaces(interId,intName,intUri,inttype)
            org.printinterfaces(customerName, interNote,interStanote)
            if (crepass!="") or (creuser!=""):
                org.printcredentials(interId,creuser,crepass, customerName)
            
            interfacesId.append(interId)
    return interfacesId

def readContactsSpreadsheet(idSearch,path,sheetName,customerName):
    #Organizations
    rowc=""

    col_types={"CTACT CODE":str}
    contactsId=[]
    conID=""
    FN=""
    LN=""
    contcategories=""
    contacts = pd.read_excel(path,sheet_name=sheetName, dtype=col_types)
    #print(contacts)
    contact_filter = contacts[contacts['CTACT CODE']== idSearch]
    contact_filter = contact_filter.apply(lambda x: x.fillna(""))
    print("Contacts founds: ",len(contact_filter))
    for c, rowc in contact_filter.iterrows():
        if rowc[1]:
            contactName_temp=rowc[1]
            ContactName=faf.SplitString(contactName_temp)
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
        contactLang="en-uk"
        contactnotes=""
        addcontnote=True
        if addcontnote:
            if rowc[1]:
                contactnotes=faf.contact_notes(rowc[1],1)
            
        addcono=True
        if addcono:
            if rowc[8]:
                contactnotes= rowc[8]
        #Contacts phone
        contactphoneN=[]
        addpho=True
        if addpho:
            contactphoneN.append(faf.org_phoneNumbers(contact_filter.loc[c],3,4,5))
            if faf.is_empty(contactphoneN[0]):
                contactphoneN=[]
        #Contact emails
        contactemail=[]
        addmails=True
        if addmails: 
            contactemail.append(faf.org_emails(contact_filter.loc[c],6,7))
            if faf.is_empty(contactemail[0]):
                contactemail=[]
        #Contact Address
        contactaddresses=[]
        addadd=False
        if addadd: 
            contactaddresses.append(faf.org_addresses(contact_filter.loc[c],6,7))
            if faf.is_empty(contactaddresses[0]):
                contactaddresses=[]
        #INACTIVE / ACTIVE
        contactinactive= False
        #Contact URL
        contacturls=[]
        addurl=False
        if addurl:  
            contacturls.append(faf.org_urls(contact_filter.loc[c],0))
            if faf.is_empty(contacturls[0]):
                contacturls=[]
        contcategories=faf.org_categorie("nn")
        conID=str(uuid.uuid4())
        contactsId.append(conID)
        #(self,contactID,contactfirstName, contactlastName, contactcategories):
        ctc=faf.contactsClass(conID,FN,LN,contcategories,contactLang)
        #def printcontacts(self,cont_phone,cont_email, cont_address,cont_urls,cont_categories,contactnotes,fileName):
        ctc.printcontactsClass(contactphoneN, contactemail, contactaddresses, contacturls,contcategories,contactnotes,customerName)  
    return contactsId

###########################
#ORGANIZATIONS
###########################
        
def readOrganizations(path,file_name,sheetName,customerName):
        try:
            list={}
            org={}
            contact_Id=""
            interface_Id=""
            org_erpCode=""
            nextorg=""
            client=customerName
            customerName=path+"_"+customerName
            vendors=faf.importDataFrame(file_name,orderby="RECORD #(VENDOR)",distinct=[],delimiter="")
            cont=0
            vendors['ADDRESS 1 PRIMARY']= vendors['ADDRESS 1 PRIMARY'].map({'Yes':True ,'No':False})
            vendors['ADDRESS 2 PRIMARY'] = vendors['ADDRESS 2 PRIMARY'].map({'Yes':True ,'No':False})
            vendors['ADDRESS 3 PRIMARY'] = vendors['ADDRESS 3 PRIMARY'].map({'Yes':True ,'No':False})
            
            for i, row in vendors.iterrows():
                tic = time.perf_counter()
                uuidOrg=str(uuid.uuid4())
                org['id']=str(row['UUID'])
                org['status']="Active"
                org["isVendor"]= True
                #   print(row[1]) # Print the cell
                #print(vendors.iloc[i])
                #ORG CODE    
                if row['VENCODE']: org['code']=str(row['VENCODE']).strip()
                #ORG NAME
                if row['VENNAME']: org['name']=str(row['VENNAME']).strip()
                org_name=org['code']
                #print all column
                #print(vendors.iloc[[i],['ORGANIZATION NAME']])
                activeVendor= True
                #ORG DESCRIPTION
                orgdescription=""
                addorgdesc=False
                if addorgdesc and row['NOTE1']: 
                    org['description']=str(row['NOTE1'])
                #if row['ACCTNUM']:
                #    org_erpCode=str(row['ACCTNUM']).strip()
                #ORG ALIASES#################
                orgaliases=[]
                addAli = False
                if addAli:
                    orgaliases=""
                    #print(vendors.loc[i])
                    orgaliases=faf.org_aliases(vendors.loc[i],0)

                #Categories nn=blank
                orgCategoria="nn"
                #Addresses
                orgaddresses=[]
                addAdd= True

                
                #orgaddresses=str(row['ADDRESS1']).strip()
                if addAdd:
                    if row['ADDRESS 1 Line 1']!="":
                        cat=[]
                        cadena=str(row['ADDRESS 1 CATEGORY']).strip()
                        c=cadena.find(";")
                        searchA=""
                        searchB=""
                        if c > 0:
                            chunked=cadena.split(";")
                            searchA=str(chunked[0]).strip()
                            searchB=str(chunked[1]).strip()
                            cat.append(faf.get_Id_value(client,searchValue=searchA,element="categories"))
                            cat.append(faf.get_Id_value(client,searchValue=searchB,element="categories"))
                        else:
                            cat.append(faf.get_Id_value(client,searchValue=cadena,element="categories"))
                        
                        orgaddresses.append(faf.dic(addressLine1= row['ADDRESS 1 Line 1'],addressLine2=row['ADDRESS 1 Line 2'],
                                                    city=row['ADDRESS 1 CITY'],stateRegion= row['ADDRESS 1 STATE'],
                                                    zipCode=row['ADDRESS 1 ZIP'],country=row['ADDRESS 1 COUNTRY'],
                                                    isPrimary=row['ADDRESS 1 PRIMARY'],categories=cat,language="eng-us"))
                    
                    if row['ADDRESS 2 Line1']!="":
                        cat=[]
                        cadena=str(row['ADDRESS 2 CATEGORY']).strip()
                        c=cadena.find(";")
                        if c > 0:
                            chunked=cadena.split(";")
                            searchA=str(chunked[0]).strip()
                            searchB=str(chunked[1]).strip()
                            cat.append(faf.get_Id_value(client,searchValue=searchA,element="categories"))
                            cat.append(faf.get_Id_value(client,searchValue=searchB,element="categories"))

                        else:
                            cat.append(faf.get_Id_value(client,searchValue=cadena,element="categories"))
                        
                        orgaddresses.append(faf.dic(addressLine1= row['ADDRESS 2 Line1'],addressLine2=row['ADDRESS 2 Line 2'],
                                                    city=row['ADDRESS 2 CITY'],stateRegion= row['ADDRESS 2 STATE'],
                                                    zipCode=row['ADDRESS 2 ZIP'],country=row['ADDRESS 2 COUNTRY'],
                                                    isPrimary=row['ADDRESS 2 PRIMARY'],categories=cat,language="eng-us"))
                    if row['ADDRESS 3 Line 1']!="":
                        cat=[]
                        cadena=str(row['ADDRESS 3 CATEGORY']).strip()
                        c=cadena.find(";")
                        if c > 0:
                            chunked=cadena.split(";")
                            searchA=str(chunked[0]).strip()
                            searchB=str(chunked[1]).strip()
                            cat.append(faf.get_Id_value(client,searchValue=searchA,element="categories"))
                            cat.append(faf.get_Id_value(client,searchValue=searchB,element="categories"))
                        else:
                            cat.append(faf.get_Id_value(client,searchValue=cadena,element="categories"))
                        
                        orgaddresses.append(faf.dic(addressLine1= row['ADDRESS 3 Line 1'],addressLine2=row['ADDRESS 3 Line 2'],
                                                    city=row['ADDRESS 3 CITY'],stateRegion= row['ADDRESS 3 STATE'],
                                                    zipCode=row['ADDRESS 3 ZIP'],country=row['ADDRESS 3 COUNTRY'],
                                                    isPrimary=row['ADDRESS 3 PRIMARY'],categories=cat,language="eng-us"))
                    org["addresses"]=orgaddresses
                    print(org['addresses'])    
                #phoneNumbers
                addpho= True
                orgphonNumbers=[]
                if addpho:
                    #orgphonNumbers=str(row['PHONE NUM 1']).strip()
                    #if orgphonNumbers:
                    #    orgphonNumbers=faf.dic(phoneNumber= orgphonNumbers,type="Office", isPrimary= True, language="eng-uk",categories=[])
                     org['phoneNumbers']=faf.org_phoneNumbers(vendors.loc[i],29,30)

                #emails
                orgemails=[]
                addmails=True
                if addmails: 
                    orgemail=str(row['EMAIL']).strip()
                    if orgemail:
                        orgemails.append(faf.dic(value=orgemail,language="eng-us",categories=[]))
                        org['emails']=orgemails#orgemails=faf.org_emails(vendors.loc[i],6)

                #vendorCurrencies
                org['vendorCurrencies']=["USD"]
                
                #urlsOrg
                orgurls=[]
                addurl=False
                if addurl:
                    orgurl=str(row['ADDRESS2']).strip()
                    if orgurl:                    
                        orgurls.append(faf.dic(value=orgurl,language="eng-uk",categories=[]))
                        print(orgurls)
                        #orgurls=faf.org_urls(vendors.loc[i],7)
                    
                #accounts
                accounts=[]
                addacc=True
                nameaccount=""
                accountnum=""
                paymentMethod=""
                libraryCode=""
                libraryEdiCode=""
                
                if addacc:
                    if row['Account Name']:
                        nameaccount=row['Account Name']
                        if row['Account Number']: accountnum=row['Account Number']
                        if row['paymentMethod']: paymentMethod=row['paymentMethod']
                        if row['Library Code']: libraryCode= row['Library Code']
                        if row['Library EDI Code']: libraryEdiCode=row['Library EDI Code']
                        accounts.append(faf.dic(name=nameaccount,accountNo=accountnum,accountStatus="Active",paymentMethod=paymentMethod,libraryCode=libraryCode,libraryEdiCode=libraryEdiCode))
                        org['accounts']=accounts
                        accounts=[]
                    else:
                        org['accounts']=[]
                    #accounts=faf.org_account(vendors.loc[i],0)
                    
                #Acquisition Unit
                acqUnitIds= []
                addacc=True
                if addacc and row['Acquisitions Unit']:
                        org['acqUnitIds']=["e2a4d34c-66a1-435a-97f2-d14364b58c66"]
                        #acqUnitIds=faf.org_acqunit(vendors.loc[i],0)
                org['interfaces']=[]
                interface_Id=[]
                addinterface= False
                if addinterface:
                    #readInterfacesSpreadsheet(idSearch,path,sheetName,customerName):
                    interface_Id=(readInterfacesSpreadsheet(row[0],path,"INTERFACES",customerName))
                org['contacts']=[]
                contact_Id=[]
                addcontact= False
                isoncurrentsheet= False
                if addcontact:
                    if row[0]:
                        if isoncurrentsheet:
                            pass
                            #contact_Id.append(read_contacts(vendors.loc[i],0,fileN))
                        else:
                            contact_Id=(readContactsSpreadsheet(row[0],path,"CONTACTS",customerName))
                #ORG UUID

                cont=cont+1
                if cont==84:
                    a=1
                #def __init__(self,idorg,name,orgcode,vendorisactive,orglanguage):
                #org=faf.Organizations(uuidOrg,org_name,org_code,activeVendor,"eng",accounts)
                #org.printorganizations(orgdescription,orgaliases,orgaddresses,orgphonNumbers,orgemails,orgurls,orgvendorCurrencies,contact_Id,interface_Id,org_erpCode,path)
                faf.printObject(org,path,cont,"organization")
                org={}
                addnoteapp=True
                if addnoteapp:
                    
                    orgnote=faf.notes()
                    #print_notes(self,dfRow,typeId,linkId,fileName,*argv):
                    #faf.print_notes(linkid,"poLine",path, notetype="0ee34a03-b785-4079-b894-3ee3e3be4110",domain="orders",cont=content,title=title, )
                    if row['NOTE2']!="":
                        typeId="e14e0fc8-aa1b-4fd7-9ff5-f0776b2aee9c"
                        #print_notes(linkId,typelinkId,path,**kwargs):
                        faf.print_notes(uuidOrg,"organizations",path,typeId=typeId,type="Sierra Org Vendor Note 2",domain="organizations",title="Sierra Org Vendor Note 2",cont=row['NOTE2'],links="organization")
                    if row['NOTE3']!="":
                        typeId="de53e6d3-7f23-4723-b438-0c0f0672568b"
                        faf.print_notes(uuidOrg,"organizations",path,typeId=typeId,type="Sierra Org Vendor Note 3",domain="organizations", title="Sierra Org Vendor Note 3",cont=row['NOTE3'],links="organization")
                    
                interface_Id=[]
                #old_org=org_code
                contact_Id=[]
                org_erpCode=""
                toc = time.perf_counter()
                print(f"Record: {cont} "+str(org_name)+f"  procesing time in {toc - tic:0.4f} seconds")
                
        except ValueError:
            print("Organization Error: "+str(ValueError))

################################
################################
##ORDERS FUNCTION
################################
#def readorders(path,file_name,sheetName,customerName,spread):
#        try:
def readorders(spread,**kwargs):
    #def readorders(path,file_name,sheetName,customerName,spread):
    path_root=f"{kwargs['rootpath']}"
    path_results=f"{path_root}\\results"
    path_logs=f"{path_root}\\logs"
    path_refdata=f"{path_root}\\refdata"
    path_mapping=f"{path_root}\\mapping"
    path_data=f"{path_root}\\data"
    dfFile=f"{path_data}\\{fileToread}"
    orderList=[]      
    orderDictionary={}      
    list={}
    customerName=kwargs['client']
    changeVendor={}
    errorVendors=open(f"{path_logs}\\vendorsNotFounds.txt", 'w')
    errorLocations=open(f"{path_logs}\\locationsNotFounds.txt", 'w')
    errorProvider=open(f"{path_logs}\\providerNotFounds.txt", 'w')
    errorTitles=open(f"{path_logs}\\titlesNotFounds.txt", 'w')
    errorPuchaseordersmapping=open(f"{path_logs}\\purchaseordersNotFounds.txt", 'w')
    errorMaterialtype=open(f"{path_logs}\\materialTypeNotFounds.txt", 'w')
    poClean=open(f"{path_results}\\oldNew_ordersID.txt", "w")
    purchaseOrderbyline=open(f"{path_results}\\{customerName}_purchaseOrderbyline.json", 'w')
    notesbyline=open(f"{path_results}\\{customerName}_notesbyline.json", 'w')
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
    #Reading file seccion
    orders= faf.importDataFrame(dfFile,orderby=kwargs['orderby'],distinct=kwargs['distinct'],delimiter=kwargs["delimiter"],sheetName=kwargs['sheetName'])
    orders_cp = faf.importDataFrame(dfFile,orderby=kwargs['orderby'],distinct=kwargs['distinct'],delimiter=kwargs["delimiter"],sheetName=kwargs['sheetName'])#faf.importDataFrame(file_name,orderby="RECORD #(BIBLIO)",distinct=[],delimiter="",sheetName="base")
    orderList=[]      
    orderDictionary={}      
    list={}
    Notes_Multiple_Lines= faf.importDataFrame(dfFile,orderby=kwargs['orderby'],distinct=[],delimiter=kwargs["delimiter"],sheetName="Notes_Output")
    PML= faf.importDataFrame(dfFile,orderby=kwargs['orderby'],distinct=[],delimiter=kwargs["delimiter"],sheetName="Paid_Output")
    PSLine=PML#faf.importDataFrame(file_name,orderby="RECORD #(BIBLIO)",distinct=[],delimiter="",sheetName="paid")
    changeVendor={"proq ":"proq","alex":"proq","amate":"amat","brks":"ingr","cout":"proq","couni":"proq","proql":"proq","ama20":"amas","ama21":"amas","aux":"amas","auxs":"amas","ama":"amas","tou":"amas","bac32":"bach","bac92":"bach","brile":"brill","brils":"brill","brinv":"brill","cam":"ven25","cam13":"ven25","casa":"casas","uchig":"chidc","cic":"btaa","clabk":"clb02","cupe":"cup","cups":"cup","eastv":"easts","elper":"elss","esnys":"elss","evi05":"easts","evi06":"easts","evi07":"easts","evi17":"easts","evi19":"easts","evi36":"easts","front":"fro33","gale":"ceng","gals":"ceng","greyb":"greys","gvrl":"ceng","har29":"hars","har59":"hars","hare":"hars","hart":"hars","hog82":"hog","hoga":"hog","hogs":"hog","hwwil":"greys","ivp":"ivp12","japan":"japs","lcsea":"lccap","lcrio":"lccap","sacap":"lccap","mlcs":"mcls","oups":"oup","puv78":"puv","retta":"ret97","rit11":"ritts","rit85":"ritts","ritte":"ritts","ritt":"ritts","sags":"sages","salem":"greys","thor":"thors","turuk":"turps","vien":"vie08","viens":"vie08","worl":"wor95","ybp30":"gobi","ybp31":"gobi","ybp77":"gobi","ybp89":"gobi","ybp98":"gobi","ybp91":"gobi","ybpe":"gobi","ybpep":"gobi","ybpr":"gobi","ybps":"gobi","ybp80":"gobi", "none":"none",
                          "wiley":"wiles","docs":"docss","tous":"amas","aip":"aips","amats":"amat","har":"hars","lexs":"lexis","bline":"undefined","films":"undefined",
                          "omni":"undefined","moses":"undefined","hbrd":"undefined","mcbl":"undefined","ybpeb":"gobi"}            
    for i, row in orders.iterrows():
        try:
            Order={}
            tic = time.perf_counter()
            count=count + 1
            #Order Number
            poNumberSuffix=""
            poNumberPrefix=""
            poNumber=""
            #if row['Prefix']: 
            #    poNumberPrefix=str(row['Prefix'])
            #    Order["poNumberPrefix"]=poNumberPrefix.strip()
            #if row['Suffix']:
            #    poNumberSuffix=str(row['Suffix'])
            #    Order["poNumberSuffix"]=poNumberSuffix.strip()
            if row['RECORD #(ORDER)']:
                poo=str(row['RECORD #(ORDER)']).strip()
                po=faf.check_poNumber(poo,path_results)
                #print(po)
                poNumber=po[1:]
                #print(po)
                Order["poNumber"]= poNumber
            else:
                randompoNumber=str(round(random.randint(100, 1000)))
                poNumber=str(randompoNumber)
                po=poNumber
                
            countlist = orderList.count(str(po))
            if countlist>0:
                poNumber=str(po)+str(countlist)
                
            orderList.append(str(po))
            #print(orderList)                
            print("Record: "+str(count)+"    poNumber:  "+poNumber)
            #idOrder
            
            if 'UUID' in orders.columns: Order["id"]=str(row['UUID'])#str(uuid.uuid4())
            else: Order["id"]=str(uuid.uuid4())
            #Order["approvedById"]=""
            #Order["approvalDate"]= ""
            #Order["closeReason"]=faf.dic(reason="",note="")
            Order["manualPo"]= False
            notea=""
            Order["notes"]=[]
            #if row['ORD NOTE']!="-":
            #    notea=str(row['ORD NOTE']).strip()
            #    Order["notes"]=[notea]
            #if row['Add Note']:
            #    Order["notes"]=[str(row['Add Note'])]
            #Ongoing - One-Type
            isSubscription= False
            Order_type=str(row['ORD TYPE2']).strip()
            Order_type=Order_type.upper()
            Order["orderType"]=""
            if Order_type=="ON-GOING" or Order_type=="ONGOING": 
                Order["orderType"]="Ongoing"
                suscription=str(row['SUBSCRIPTION2']).strip()                                
                if suscription=="Yes" or suscription=="yes":
                    isSubscription= True
                    reviewPeriod=""
                    renewalDate=""
                    ongoingNote=""
                    interval=365
                    if row['RENEW STAT']: interval=int(row['RENEW STAT'])
                    if row['RENEW DATE']: renewalDate=faf.timeStamp(row['RENEW DATE'])#f"2022-06-30T00:00:00.00+00:00"
                    Order["ongoing"]=faf.dic(interval=interval, isSubscription=True, manualRenewal=True, 
                                               reviewPeriod=reviewPeriod, renewalDate=renewalDate)
                elif suscription=="no" or suscription=="No":
                    isSubscription= False
                    Order["orderType"]="Ongoing"
                    Order["ongoing"]=faf.dic(isSubscription=False)
            elif  Order_type=="One-time":
                Order["orderType"]="One-Time"
            else:
                Order["orderType"]="One-Time"
                
            ######################
             
         #Order["billTo"]=""
            shipto=str(row['RLOC']).strip()
            #shipto=shipto.upper()
            if shipto=="MSU BOOKS RECEIVING":                
                Order["shipTo"]="77f43ed0-eee7-4b45-a35c-7e17f235e0bb"
            elif shipto=="MSU DOCS":                
                Order["shipTo"]="246ce69e-fab7-4990-8b5a-4c3bd135226d"
            elif shipto=="MSU SERIALS ACQUISITIONS":                
                Order["shipTo"]="369633a7-d2b9-423f-a035-310bc901062f"
            elif shipto=="John F. Schaefer Law Library":
                Order["shipTo"]="9bb185d8-34a6-4eb8-a514-70048413271c"
            else:
                Order["shipTo"]="369633a7-d2b9-423f-a035-310bc901062f"
            #Order["template"]=""
            #Order["totalEstimatedPrice"]=""
            #Order["totalItems"]: 3
            #Order[""totalEncumbered"]=""
            #Order["totalExpended":]=""
            
            oV="180fea75-a46c-4d3c-bd95-88f46cf76c31"
            Order["vendor"]="180fea75-a46c-4d3c-bd95-88f46cf76c31"
            if row['VENDOR']:
                vendorToSearch=str(row['VENDOR']).strip()
                oV=faf.readJsonfile(path_refdata,f"{customerName}_organizations.json","organizations",vendorToSearch,"code")
                if oV is not None:
                    Order["vendor"]=str(oV)
                else:
                    vendorrealToSearch=faf.searchKeysByVal(changeVendor,vendorToSearch)
                    oV=faf.readJsonfile(path_refdata,f"{customerName}_organizations.json","organizations",vendorrealToSearch,"code")
                    Order["vendor"]=oV
                    if vendorrealToSearch is None:
                        tempvendor=faf.get_OrgId(vendorToSearch,customerName)
                        if tempvendor is None:
                            errorVendors.write(poNumber+" "+vendorToSearch+"\n")
                            Order["vendor"]="180fea75-a46c-4d3c-bd95-88f46cf76c31"
                            oV="180fea75-a46c-4d3c-bd95-88f46cf76c31"
                        else:
                            oV=tempvendor
            #WorkFlow Status
            if poo=="o1015692":
                a=1
                
            workflowStatus="Pending"
            if row['ORD STATUS']:
                workflowStatus=str(row['ORD STATUS']).strip()
                if workflowStatus== "Open":
                    Order["approved"]: True
                    Order["workflowStatus"]= "Open"
                elif workflowStatus== "Pending":
                    Order["approved"]: True
                    Order["workflowStatus"]= "Pending"
            #Reencumber
            Order["reEncumber"]= False
            
            if 'reEncumber' in orders.columns:
                if row['encumber']:
                    Order["reEncumber"]= True
            
            
            #else:
            #    Order["approved"]: False
            #    Order["workflowStatus"]= "Pending"
            compositePo=[]
            linkid=""
            compositePo=""
            compositePo=compositePoLines(orders_cp,PSLine,Notes_Multiple_Lines,oV,row['RECORD #(ORDER)'],poNumber,customerName,path_results,path_refdata,path_logs,spread)
            if compositePo is not None: Order["compositePoLines"]=compositePo
            else: Order["compositePoLines"]=[]    
                
            Order["acqUnitIds"]=["19a4c88d-5f8d-4d39-8462-f2f1e258916c"]
            faf.printObject(Order,path_results,count,f"{customerName}_purchaseOrders_byLine.json",False)
            purchase.append(Order)
        except Exception as ee:
            print(f"ERROR: {ee}")
    purchaseOrders['purchaseOrders']=purchase    
    faf.printObject(purchaseOrders,path_results,count,f"{customerName}_purchaseOrders.json",True)
            

    print(f"end")
             
def notes_single_line(linkId,idToSearch, noteapp,titleA,Namefield, path,count):
    #NOTES
    resultNote={}
    if Namefield=="NOTE(ORDER)":
        typeId="7caab130-7074-4788-88d6-5b0a0b2c9981"
        typeN="Sierra Acq Order Note"
    elif Namefield=="EXT. NOTE":
        typeId="c1b0c3f8-d2a0-4407-ab07-c868cf209ed8"
        typeN="Sierra Acq External Note"
    elif Namefield=="INT. NOTE":
        typeId="5c4475ff-1970-4982-b9d3-179d02bed9a7"
        typeN="Sierra Acq Internal Note"
    nrow={}
    content=""
    df_unique =noteapp.drop_duplicates(subset =Namefield, keep="first", inplace=False,ignore_index=True)
    df_unique = df_unique[df_unique['RECORD #(ORDER)']== idToSearch]
    print("Notes were founds: ",len(df_unique))
    for a, nrow in noteapp.iterrows():
        title=typeN
        if nrow[Namefield]:
            content=content+" "+ nrow[Namefield]
    if content!="":
        resultNote=faf.print_notes(linkId,"poLine",path,typeId=typeId,type=typeN,domain="orders",title=title,cont=content)
        faf.printObject(resultNote,path,count,"Sierra_int_ext_order_Notes.json",False)
    return resultNote

def Paid_Multiple_Lines(linkId,idToSearch, noteapp,titleA, path,count):
    #NOTES
    resultNote={}
    nrow={}    
    content=""
    noteapp = noteapp[noteapp['RECORD #(ORDER)']== idToSearch]
    print("Paids were founds: ",len(noteapp))
    title=titleA
    
    for a, nrow in noteapp.iterrows():
        if nrow['Paid Date']:
            content=content+" Paid Date: "+nrow['Paid Date']
        if nrow['Invoice Date']:
            content=content+" Invoice Date: "+nrow['Invoice Date']
        if nrow['Invoice Num']:
            content=content+" Invoice Number: "+str(nrow['Invoice Num'])
        if nrow['Amount Paid']:
            content=content+" Amount Paid: "+str(nrow['Amount Paid']).strip()
        if nrow['Voucher Num']:
            content=content+" Voucher Num: "+str(nrow['Voucher Num']).strip()
        if nrow['Copies']:
            content=content+" Copies: "+str(nrow['Copies']).strip()
        if nrow['Note']:
            content=content+" Note: "+str(nrow['Note']).strip()
        if nrow['Sub From']:
            date1=nrow['Sub From']
            content=content+" Sub From: "+str(date1)
        if nrow['Sub To']:
            date2=nrow['Sub To']
            content=content+" Sub To: "+str(date2)
            
    if content!="":
        resultNote=faf.print_notes(linkId,"poLine",path,typeId="3c61e420-be7d-4f29-ad50-58edc804ba35",type="Sierra Acq Paid Field",domain="orders",title=title,cont=content)
        faf.printObject(resultNote,path,count,"Sierra_Acq_Paid_Field_notes.json",False)
    return resultNote
        

                

                       
def compositePoLines(orders_copy,plm,nml,vendors,idSearch,poLineNumber,customerName,path_results,path_refdata,path_logs,spread):                    
    try:
        locationMapping={"af":"mnaf","aq":"mnaq","br":"mnbr","ca":"mnca","cv":"mncv","dv":"mndv","ir":"mnir","ns":"mnns","ov":"mnov","ss":"mnss"}

        cpList=[]
        count=1 
        orders_copy = orders_copy[orders_copy['RECORD #(ORDER)']== idSearch]
        print("poLines founds records: ",len(orders_copy))
        for c, cprow in orders_copy.iterrows():
            cp={}
            if 'UUIDPOLINES' in orders_copy: linkid=cprow['UUIDPOLINES']#str(uuid.uuid4())
            else: linkid=str(uuid.uuid4())
            cp["id"]=linkid
            cp["poLineNumber"]=str(poLineNumber)+"-"+str(count)
            #if cprow['Publisher']:
            #    cp["publisher"]=cprow['Publisher']
            #cp["purchaseOrderId"]=""
            #cp["id"]=""
            #cp["edition"]=""
            cp["checkinItems"]=False
            #cp["instanceId"]=""
            #cp["agreementId"]= ""
            acquisitionMethod="Purchase"
            if cprow['ACQ TYPE']:
                acquisitionMethod=cprow['ACQ TYPE']
                if acquisitionMethod=="Approval plan":    cp["acquisitionMethod"]="Approval Plan"
                elif acquisitionMethod=="DDA":            cp["acquisitionMethod"]="Demand Driven Acquisitions (DDA)"
                elif acquisitionMethod=="EBA":            cp["acquisitionMethod"]="Evidence Based Acquisitions (EBA)"
                elif acquisitionMethod=="Exchange":       cp["acquisitionMethod"]="Exchange"
                elif acquisitionMethod=="Membership":     cp["acquisitionMethod"]="Technical"
                elif acquisitionMethod=="Gift":           cp["acquisitionMethod"]="Gift"
                elif acquisitionMethod=="Purchase at vendor system":  cp["acquisitionMethod"]="Purchase At Vendor System"
                elif acquisitionMethod=="Purchase":       cp["acquisitionMethod"]="Purchase"
                elif acquisitionMethod=="Depository":     cp["acquisitionMethod"]="Depository"
                else: cp["acquisitionMethod"]="Purchase At Vendor System"
                
            #cp["alerts"]=faf.dic(alert="Receipt overdue",id="9a665b22-9fe5-4c95-b4ee-837a5433c95d")
            cp["cancellationRestriction"]: False
            #cp["cancellationRestrictionNote"]=""
            #cp["claims"]=faf.dic()Fa""
            cp["collection"]=False
            cp["rush"]=False
            collRush=cprow['ORD NOTE']
            
            if collRush=="Rush":
                cp["rush"]=True
            elif collRush=="Collection":
                cp["collection"]=True
            #cp["contributors"]=[faf.dic()]
            
            quantityPhysical=1
            if cprow['COPIES']:
                quantityPhysical=int(cprow['COPIES'])
                
            quantityElectronic=1
            if cprow['COPIES']:#if cprow['QUANTITY'] NOT MIGRATED:
                quantityElectronic=int(cprow['COPIES'])    
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
                    locationId=faf.readJsonfile(path_refdata,f"{customerName}_locations.json","locations",locationtoSearch,"code")
                    #locationId=faf.get_locId(locationtoSearch, customerName)
                    if locationId is None:
                        locationtoSearch=str(cprow['LOCATION']).strip()
                        vendorrealToSearch=faf.searchKeysByVal(locationMapping,locationtoSearch)
                        locationId=faf.readJsonfile(path_refdata,f"{customerName}_locations.json","locations",vendorrealToSearch,"code")
                        if locationId is None:
                            locationId="6930c8dd-a300-4c4a-9c27-0ac5974d5b25"
                            faf.write_file(path=f"{path_logs}\\locationsNotFounds.txt",contenido=locationtoSearch)

                            
                else:
                    loca=[]
                    x = locationtoSearch.split(",")
                    locsw=False
                    lc=0
                    locationIdA=""
                    for i in x:
                        locationtoSearch=i
                        locationId=faf.readJsonfile(path_refdata,"{customerName}_locations.json","locations",locationtoSearch,"code")
                            #locationId=faf.get_locId(locationtoSearch, customerName)
                        locationIdA=locationId
                        if locationIdA is None:
                            vendorrealToSearch=faf.searchKeysByVal(locationMapping,locationtoSearch)
                            locationId=faf.readJsonfile(path_refdata,"{customerName}_locations.json","locations",vendorrealToSearch,"code")
                                #locationIdA=faf.get_locId(vendorrealToSearch, customerName)
                            if locationIdA is None:
                                locationIdA="6930c8dd-a300-4c4a-9c27-0ac5974d5b25"
                                faf.write_file(path=f"{path_logs}\\locationsNotFounds.txt",contenido=f" {poLineNumber} {locationtoSearch} undefined locations")
                                
                        lc+=1
                        if cprow['PHYSIC/E']:
                            orderFormat=str(cprow['PHYSIC/E']).strip()
                            #Locations for print/mixed resources
                            if orderFormat=="Physical":
                                loca.append({"locationId":locationIdA,"quantity":(quantityPhysical-1), "quantityPhysical":(quantityPhysical-1)}) 
                            elif orderFormat=="Electronic":
                                loca.append({"locationId":locationIdA,"quantity":(quantityElectronic-1), "quantityElectronic":(quantityElectronic-1)})
                            else:
                                loca.append({"locationId":locationIdA,"quantity":(quantityPhysical-1), "quantityPhysical":(quantityPhysical-1)}) 
                        locationIdA=""
            ##TITLE
            ispackage=False
            #ispack=str(cprow["Package (checkbox)"]).strip()
            #if ispack=="Yes": 
            #    ispack=True
            #    cp["isPackage"]=True
                
            titleOrPackage="No Title"
            titleOrPackage=cprow['TITLE']
            instance_holdings_items="None"
            cp["titleOrPackage"]=titleOrPackage 
            if cprow['RECORD #(BIBLIO)']:
                if ispackage==False:
                    titleUUID="."+str(cprow['RECORD #(BIBLIO)']).strip()
                    ordertitleUUID=faf.get_title(customerName,element="instances",searchValue=titleUUID)
                    #ordertitleUUID=faf.readJsonfile_identifier(path_refdata,f"{customerName}_instances.json","instances",titleUUID)
                    if len(ordertitleUUID)==0:# is None:
                        #error=open(path_+"\\results\\TitlesNotFounds.txt", 'a')
                        
                        titleOrPackage=cprow['TITLE']
                        cp["titleOrPackage"]=titleOrPackage
                        instance_holdings_items="None"
                        #instance_holdings_items="Instance"
                        faf.write_file(path=f"{path_logs}\\TitlesNotFounds.txt",contenido=f"{poLineNumber}  {titleUUID} {titleOrPackage}")
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
                        faf.printObject(instance,path_results,0,f"{customerName}_instances.json",False)
                    else:
                        instance_holdings_items="None"
                        print(ordertitleUUID)
                        cp["instanceId"]=str(ordertitleUUID[0])
                        cp["titleOrPackage"]=str(ordertitleUUID[1])
                        cp["isPackage"]=False
                else:
                    titleOrPackage=cprow['TITLE']
                    cp["titleOrPackage"]=titleOrPackage 
            else:
                cp["titleOrPackage"]=cprow['TITLE']
            ################################
            ### ORDER FORMAT
            ################################            
            orderFormat=""
            materialType=""
            accessProvider=vendors
            materialSupplier=vendors
            listUnitPrice=0.00
            if cprow['UNIT PRICE']:
                listUnitPrice=float(cprow['UNIT PRICE'])
                
            if cprow['PHYSIC/E']:
                orderFormat=str(cprow['PHYSIC/E']).strip()
                orderFormat=orderFormat.upper()
                #Locations for print/mixed resources
                if orderFormat=="PHYSICIAL" or orderFormat=="PHYSICAL" or orderFormat=="PHYSICAL RESOURCE" or orderFormat=="MIXED P/E":
                    #Material Type physical
                    materialType=""
                    if cprow['FORM']:
                        mtypestosearch=str(cprow['FORM']).strip()
                        materialType=faf.readJsonfile(path_refdata,f"{customerName}_mtypes.json","mtypes",mtypestosearch,"name")
                        #materialType=faf.get_matId(mtypestosearch,customerName)
                        if materialType is None:
                            materialType=faf.get_matId(mtypestosearch,customerName)
                            faf.write_file(path=f"{path_logs}\\materialTypeNotFounds.txt",contenido=f"{poLineNumber} {mtypestosearch}")
                            if materialType is None:
                                faf.write_file(path=f"{path_logs}\\materialTypeNotFounds.txt",contenido=f"{poLineNumber} {mtypestosearch}")
                                materialType="materialtypeUndefined"


  
                    cp["orderFormat"]="Physical Resource"
                    cp["cost"]=faf.dic(currency="USD",listUnitPrice=listUnitPrice, quantityPhysical=quantityPhysical, poLineEstimatedPrice=listUnitPrice, discountType="percentage")
                    if materialType: cp["physical"]=faf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier, materialType=materialType)
                    else: cp["physical"]=faf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier)
                    if accessProvider: cp["eresource"]=faf.dic(activated=False,createInventory="None",trial=False,accessProvider=accessProvider)
                    else: cp["eresource"]=faf.dic(activated=False,createInventory="None",trial=False)
                    if locsw: 
                        cp["locations"]=[faf.dic(locationId=locationId,quantity=quantityPhysical, quantityPhysical=quantityPhysical)]
                    else:
                        cp["locations"]=loca
                        
                elif orderFormat=="Electronic":
                    
                    
                    cp["orderFormat"]="Electronic Resource"
                    if cprow['FORM']:
                        mtypestosearch=""
                        mtypestosearch=str(cprow['FORM']).strip()
                        materialType=faf.readJsonfile(path_refdata,f"{path_logs}\\_mtypes.json","mtypes",mtypestosearch,"name")
                        #materialType=faf.get_matId(mtypestosearch,customerName)
                        if materialType is None:
                            materialType=faf.get_matId(mtypestosearch,customerName)
                            faf.write_file(path=f"{path_logs}\\materialTypeNotFounds.txt",contenido=f"{poLineNumber} {mtypestosearch}")
                            if materialType is None:
                                faf.write_file(path=f"{path_logs}\\materialTypeNotFounds.txt",contenido=f"{poLineNumber} {mtypestosearch}")
                                materialType="materialtypeUndefined"

                    cp["cost"]=faf.dic(currency="USD",listUnitPrice=listUnitPrice,listUnitPriceElectronic=listUnitPrice, quantityElectronic=quantityElectronic, poLineEstimatedPrice=listUnitPrice,discountType="percentage")
                    if materialType: 
                        cp["eresource"]=faf.dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider,materialType=materialType)
                    else: 
                        cp["eresource"]=faf.dic(activated=True,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                        
                    if locationId: 
                        if locsw:
                            cp["locations"]=[faf.dic(locationId=locationId,quantity=1, quantityElectronic=quantityElectronic)]
                        else:
                            cp["locations"]=loca 
                            
                elif orderFormat=="Mixed P/E":
                    cp["orderFormat"]="P/E Mix"
                    cp["cost"]=faf.dic(currency="USD",listUnitPrice=listUnitPrice,listUnitPriceElectronic=listUnitPrice, quantityPhysical=quantityPhysical, quantityElectronic=1, poLineEstimatedPrice=listUnitPrice,discountType="percentage")
                    if accessProvider: cp["eresource"]=faf.dic(activated=False,createInventory=instance_holdings_items,trial=False,accessProvider=accessProvider)
                    else: cp["eresource"]=faf.dic(activated=False,createInventory=instance_holdings_items,trial=False)
                    cp["locations"]=[faf.dic(locationId=locationId,quantity=2, quantityElectronic=1,quantityPhysical=quantityPhysical)]
                    if materialType: cp["physical"]=faf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier,
                                   expectedReceiptDate="",receiptDue="",materialType=materialType)
                    else: cp["physical"]=faf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier,
                                   expectedReceiptDate="",receiptDue="")
                else:   
                    cp["orderFormat"]="Other"
                    cp["cost"]=faf.dic(currency="USD",listUnitPrice=listUnitPrice, quantityPhysical=1, poLineEstimatedPrice=listUnitPrice, discountType="percentage")
                    if materialType: cp["physical"]=faf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier, materialType=materialType)
                    else: cp["physical"]=faf.dic(createInventory=instance_holdings_items,volumes=[],materialSupplier=materialSupplier)
                    if accessProvider: cp["eresource"]=faf.dic(activated=False,createInventory="None",trial=False,accessProvider=accessProvider)
                    else: cp["eresource"]=faf.dic(activated=False,createInventory="None",trial=False)
                    if locationId: 
                        if locsw:
                            cp["locations"]=[faf.dic(locationId=locationId,quantity=1, quantityElectronic=quantityElectronic)]
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
                    if cprow['CODE2']:
                        searchtoValue=str(cprow['CODE2']).strip()
                        expenseClassId=faf.readJsonfile(path_refdata,f"{customerName}_expenseClasses.json","expenseClasses",searchtoValue,"code")
                        #expenseClassId=faf.get_Id(customerName,element="expenseClasses",searchValue=searchtoValue)
                    #get_funId(searchValue,orderFormat,client):
                    fundId=faf.readJsonfile_fund(path_refdata,f"{customerName}_funds.json","funds",codeTosearch,"code")
                    #fundId=faf.get_funId(codeTosearch,orderFormat,customerName)
                    if fundId is not None:
                        code=fundId[1]
                        fundId=fundId[0]
                        valuefund=100.0
                        if expenseClassId:
                            cp["fundDistribution"]=[faf.dic(code=code,fundId=fundId,expenseClassId=expenseClassId,distributionType="percentage",value=valuefund)]
                        else:
                            cp["fundDistribution"]=[faf.dic(code=code,fundId=fundId,distributionType="percentage",value=valuefund)]
                    else:
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
                        
                        if cprow['CODE2']:
                            searchtoValue=str(cprow['CODE2']).strip()
                            expenseClassId=faf.readJsonfile(path_refdata,f"{customerName}_expenseClasses.json","expenseClasses",searchtoValue,"code")
                            #expenseClassId=faf.get_Id(customerName,element="expenseClasses",searchValue=searchtoValue)
                        #get_funId(searchValue,orderFormat,client):
                        fundId=faf.readJsonfile_fund(path_refdata,f"{customerName}_funds.json","funds",codeTosearch,"code")
                        #fundId=faf.get_funId(codeTosearch,orderFormat,customerName)
                        if fundId is not None:
                            code=fundId[1]
                            fundId=fundId[0]
                            if expenseClassId:
                                fundDistribution.append(faf.dic(code=code,fundId=fundId,expenseClassId=expenseClassId,distributionType="percentage",value=valuefund))
                        
                    cp["fundDistribution"]=fundDistribution
                    
            #Ongoing
            receivingNote=""
            if cprow['IDENTITY']: receivingNote=str(cprow['IDENTITY'])
            subscriptionFrom=""
            subscriptionTo=""
            subscriptionInterval=""
            #if cprow['Subscription from']: subscriptionFrom=faf.timeStamp(cprow[18])
            #if cprow['Subscription to']: subscriptionTo=faf.timeStamp(cprow[19])
            if cprow['RENEW STAT']: subscriptionInterval=int(cprow['RENEW STAT'])
            productIds=[]
            if cprow['CODE1']:
                if cprow['CODE1']!="-":
                    if cprow['CODE1']!="NONE":
                        productIds.append(faf.dic(productId=str(cprow['CODE1']).strip(), productIdType="8e3dd25e-db82-4b06-8311-90d41998c109"))
            if cprow['CODE3']:
                if cprow['CODE3']!="-":
                    productIds.append(faf.dic(productId=str(cprow['CODE3']).strip(), productIdType="8e3dd25e-db82-4b06-8311-90d41998c109"))
            if cprow['RECORD #(BIBLIO)']:
                reportNumberdata="."+str(cprow['RECORD #(BIBLIO)']).strip()
                productIds.append(faf.dic(productId=reportNumberdata, productIdType="37b65e79-0392-450d-adc6-e2a1f47de452"))
            
            cp["details"]=faf.dic(receivingNote=receivingNote,productIds=productIds,subscriptionFrom=subscriptionFrom,
                                                       subscriptionInterval=subscriptionInterval, subscriptionTo=subscriptionTo)
            
            #cp["donor"]=""
            if cprow['STATUS(O)']:
                if cprow['STATUS(O)']=="Ongoing":
                    cp["paymentStatus"]="Ongoing"
                elif cprow['STATUS(O)']=="Pending":
                    cp["paymentStatus"]="Pending"
                else:
                    cp["paymentStatus"]="Awaiting Payment"
            else:
                cp["paymentStatus"]="Awaiting Payment"
            #if cprow['Internal Note']:
            #    cp["description"]=str(cprow['Internal Note'])
            
            #cp["publicationDate"]=""
            cp["receiptDate"]=""
            if cprow['RDATE']:
                receitdate=str(cprow['RDATE'])
                M=receitdate[0:2]
                D=receitdate[3:5] 
                Y=receitdate[6:10]
                if Y=="96" or Y=="97" or Y=="98" or Y=="99":
                    Y=f"19{Y}"
                cp["receiptDate"]=f"{Y}-{M}-{D}T00:00:00.00+00:00"
            cp["receiptStatus"]="Awaiting Receipt"
            #cp["reportingCodes"]=faf.dic(code="",id="",description="")
            cp["requester"]=""
            if cprow['REQUESTOR']:
                cp["requester"]=str(cprow['REQUESTOR'])
            #if cprow['Selector']:
            #    cp['selector']=str(cprow['Selector'])
                

                
            cp["source"]="User"
            

            vendorAccount=""
            referenceNumbers=[]
            a=1
            if a==1:
                plm = plm[plm['RECORD #(ORDER)']== idSearch]
                print("Paid were founds: ",len(plm))
                text=""
                sw= False
                fa=0
                fb=0
                refNumber=""
                oldRefnumber=""
                for a, nrow in plm.iterrows():
                    text=str(nrow['Note'])
                    fa=text.find("!")
                    if fa>0:
                        sw= True
                        refNumber=text[fa+1:]
                        #fb=refNumber.find("\\")
                        #if fb>0:
                    
                        if refNumber!=oldRefnumber:
                            referenceNumbers.append(faf.dic(refNumber=refNumber, refNumberType="Vendor title number"))
                            oldRefnumber=refNumber
                            
            if cprow['RECORD #(ORDER)']:
                refNumber=str(cprow['RECORD #(ORDER)']).strip()
                referenceNumbers.append(faf.dic(refNumber=refNumber, refNumberType="Vendor order reference number"))
                
            instrVendor=""   
            lw=False
            nml = nml[nml['RECORD #(ORDER)']== idSearch]
            print("Instruction vendors were founds: ",len(nml))
            for a, nrow in nml.iterrows():
                if nrow['VEN. NOTE']:
                    instrVendor=nrow['VEN. NOTE']
                    lw=True
            
            cp["vendorDetail"]=faf.dic(instructions=instrVendor, referenceNumbers=referenceNumbers, vendorAccount="")

            tagtext=""
            if cprow['CODE4']:
                tagtext=cprow['CODE4']
                if tagtext!="-":
                    cp["tags"]={"tagList": [tagtext]}
            ##NOTES####
            #NOTES Sierra Acq Order Date
            content=""
            date=""
            if cprow['ODATE']:
                if cprow['ODATE']!="  -  -  ":
                    datetime=cprow['ODATE']
                    #date=datetime.strftime('%Y-%m-%d')            
                    content="ODATE: "+str(datetime)+"\n"
            if cprow['CREATED(ORDER)']:
                datetime=cprow['CREATED(ORDER)']
                #date=datetime.strftime('%Y-%m-%d')
                content=content+"CREATED(ORDER): "+str(datetime)
            
            if content!="":    
                title="Sierra Acq Order Date"
                nn=faf.print_notes(linkid,"poLine",path_results,typeId="21d36bc7-2593-4b9e-ac34-f84a91e1babc",type="Sierra Acq Order Date",domain="orders",title=title,cont=content)
                #notesapp.append(nn)
                faf.printObject(nn,path_results,count,"Sierra_Acq_Order_Date_notes.json",False)
            
            #LOCATIONS (ORDER) Sierra Acq Multi Loc/Fund
            content=""
            if cprow['LOCATIONS(ORDER)']:
                if cprow['LOCATIONS(ORDER)']!="  -  -  ":
                    content="Locations Order: "+str(cprow['LOCATIONS(ORDER)']).strip()
                if cprow['FUNDS']:
                    content=content+"FUNDS: "+cprow['FUNDS']
            
            if content!="":    
                title="Sierra Acq Multi Loc/Fund"
                nn=faf.print_notes(linkid,"poLine",path_results,typeId="8e49bdd0-9b4e-4799-91ea-c554df4ae9b2",type="Sierra Acq Multi Loc/Fund",domain="orders",title=title,cont=content)
                #notesapp.append(nn)
                faf.printObject(nn,path_results,count,"Sierra_Acq_Multi_LocFund_notes.json",False)

            content=""
            if cprow['MISC']:
                content="MISC: "+cprow['MISC']
                if content!="-" or content!="":
                    title="Sierra Acq Misc"
                    nn=faf.print_notes(linkid,"poLine",path_results,typeId="002c288d-cd6d-4f61-a5d1-24db8e51f270",type="Sierra Acq Misc",domain="orders",title=title,cont=content)
                    #notesapp.append(nn)
                    faf.printObject(nn,path_results,count,"Sierra_Acq_Misc_notes.json",False)
                    
            #NOTES Sierra Acq Vendor address                 
            content=""
            venadd=""
            if cprow['VEN. ADDR.']:
                content="VEN. ADDR.: "+cprow['VEN. ADDR.']
                if content!="-" or content!="":
                    title="Sierra Acq Vendor address"
                    nn=faf.print_notes(linkid,"poLine",path_results,typeId="60381345-a0e1-43bf-957d-6947c5eb0ee7",type="Sierra Acq Vendor address",domain="orders",title=title,cont=content)
                    #notesapp.append(nn)
                    faf.printObject(nn,path_results,count,"Sierra_Acq_Vendor_address_notes.json",False)
            #NOTES GRAL
            idSearch=str(cprow['RECORD #(ORDER)']).strip()
            nn=""
            nn=notes_single_line(linkid,idSearch, nml,"NOTE(ORDER)","NOTE(ORDER)", path_results,count)
            nn=notes_single_line(linkid,idSearch, nml,"EXT. NOTE","EXT. NOTE", path_results,count)
            nn=notes_single_line(linkid,idSearch, nml,"INT. NOTE","INT. NOTE", path_results,count)
            
            if spread:
                nn1=Paid_Multiple_Lines(linkid,idSearch, plm,"Sierra Acq Paid.json",path_results,count)
                if len(nn1)>0:
                    faf.printObject(nn1,path_results,count,"Sierra_Acq_Paid.json",False)
            
            cpList.append(cp)
            #cpList.append(linkid)
            count=count+1
        return cpList    
    except ValueError as error:
        print("Error: %s" % error)            

def readorders_fix(path,file_name,sheetName,customerName):
        try:
            orderList=[]
            purchaseOrders={}
            orderDictionary={}      
            list={}
            count=0
            purchase=[]
            orders= faf.importDataFrame(file_name,orderby="RECORD #(ORDER)",distinct=['RECORD #(ORDER)'],delimiter="",sheetName="base")
            changeVendor={"proq ":"proq","alex":"proq","amate":"amat","brks":"ingr","cout":"proq","couni":"proq","proql":"proq","ama20":"amas","ama21":"amas","aux":"amas","auxs":"amas","ama":"amas","tou":"amas","bac32":"bach","bac92":"bach","brile":"brill","brils":"brill","brinv":"brill","cam":"ven25","cam13":"ven25","casa":"casas","uchig":"chidc","cic":"btaa","clabk":"clb02","cupe":"cup","cups":"cup","eastv":"easts","elper":"elss","esnys":"elss","evi05":"easts","evi06":"easts","evi07":"easts","evi17":"easts","evi19":"easts","evi36":"easts","front":"fro33","gale":"ceng","gals":"ceng","greyb":"greys","gvrl":"ceng","har29":"hars","har59":"hars","hare":"hars","hart":"hars","hog82":"hog","hoga":"hog","hogs":"hog","hwwil":"greys","ivp":"ivp12","japan":"japs","lcsea":"lccap","lcrio":"lccap","sacap":"lccap","mlcs":"mcls","oups":"oup","puv78":"puv","retta":"ret97","rit11":"ritts","rit85":"ritts","ritte":"ritts","ritt":"ritts","sags":"sages","salem":"greys","thor":"thors","turuk":"turps","vien":"vie08","viens":"vie08","worl":"wor95","ybp30":"gobi","ybp31":"gobi","ybp77":"gobi","ybp89":"gobi","ybp98":"gobi","ybp91":"gobi","ybpe":"gobi","ybpep":"gobi","ybpr":"gobi","ybps":"gobi","ybp80":"gobi", "none":"none",
                          "wiley":"wiles","docs":"docss","tous":"amas","aip":"aips","amats":"amat","har":"hars","lexs":"lexis","bline":"undefined","films":"undefined"}            
            error=open(path+"\\results\\CodesNotFounds.txt", 'w')
            with open("C:\\Users\\asoto\\Documents\\EBSCO\\Migrations\\folio\\mls\\golive\\michstate_prod_novendorsfound_update_A.json", "r", encoding="UTF-8") as lines:
            #with open("C:\\Users\\asoto\\Documents\\EBSCO\\Migrations\\folio\\mls\\golive\\michstate_prod_worse_records_update_A.json", "r", encoding="UTF-8") as lines:
                content=lines.readlines()
                for line in content:
                    json_str = json.loads(line)
                    count+=1
                    idSearch="o"+str(json_str['poNumber']).strip()
                    orders1 = orders[orders['RECORD #(ORDER)']== idSearch]
                    for i, row in orders1.iterrows():
                        print("Notes were founds: ",len(orders1))
                        vendorcode=row['VENDOR']
                        if vendorcode:
                            vendorToSearch=str(row['VENDOR']).strip()
                            oV=faf.readJsonfile(path,"michstate_prod_organizations.json","organizations",vendorToSearch,"code")
                            if oV is None:
                                vendorrealToSearch=faf.searchKeysByVal(changeVendor,vendorToSearch)
                                oV=faf.readJsonfile(path,"michstate_prod_organizations.json","organizations",vendorrealToSearch,"code")
                                if vendorrealToSearch is None:
                                    error.write(row['RECORD #(ORDER)']+" "+" "+vendorcode+"\n")
                                    oV="180fea75-a46c-4d3c-bd95-88f46cf76c31"
                        else:
                            Order["vendor"]="180fea75-a46c-4d3c-bd95-88f46cf76c31"
                            oV="180fea75-a46c-4d3c-bd95-88f46cf76c31"
                        json_str['vendor']=oV
                        #if json_str['compositePoLines'][0]['physical']['materialSupplier'] in json_str: 
                        #    json_str['compositePoLines'][0]['physical']['materialSupplier']=oV
                        #if ['compositePoLines'][0]['eresource']['accessProvider'] in json_str:
                        #    json_str['compositePoLines'][0]['eresource']['accessProvider']=oV
                        #json_str['accessProvider']=oV
                    purchase.append(json_str)
            purchaseOrders['purchaseOrders']=purchase
            faf.printObject(purchaseOrders,path,count,"michstate_prod_purchaseOrders_codevendor",True)
                
                      
        except ValueError as error:
            print("Error: %s" % error)

def readorders_fix():
    try:
        orderList=[]
        purchaseOrders={}
        orderDictionary={}
        f = open("mls\\golive\\michstate_prod_titlesOrder.json","r", encoding= 'utf-8')
        data = json.load(f)
        count=1
        instance=[]
        inventory={}
        titOrd=[]
        orderTitle={}
        for i in data['titles']:
            j_content=i
            title=""            
            title=j_content['title']
            identifiers=[]
            if len(j_content["productIds"])>1:
                count+=1
                print(f"record {count} : {title}")
                for x in j_content["productIds"]:
                    j_prod=x
                    if j_prod["productIdType"]=="37b65e79-0392-450d-adc6-e2a1f47de452":
                        sierrabib=j_prod["productId"]
                        del j_prod["productId"]
                        del j_prod['productIdType']
                        j_prod['value']=sierrabib
                        j_prod['identifierTypeId']="5e1c71c5-c4ce-4585-a057-88eb3675f353"
                        #print(x["productId"])
                        identifiers.append(j_prod)
                    else:
                        value=j_prod["productId"]
                        identifierTypeId=j_prod['productIdType']
                        del j_prod["productId"]
                        del j_prod['productIdType']
                        j_prod['value']=value
                        j_prod['identifierTypeId']=identifierTypeId
                        identifiers.append(j_prod)
                        #Title creation                        
                instanceId=str(uuid.uuid4())
                record= {
                            "id": instanceId,
                            "_version": 1,
                            #"hrid": "in00000003038",
                            "source": "FOLIO",
                            "title": title,
                            "alternativeTitles": [],
                            "editions": [],
                            "series": [],
                            "identifiers": identifiers,
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
                            #"statusUpdatedDate": "2021-07-03T03:58:25.410+0000",
                            "tags": {"tagList": []},
                            "holdingsRecords2": [],
                            "natureOfContentTermIds": []
                        }
                instance.append(record)
                faf.printObject(record,"mls\\golive",x,"Update_prod_instance,json",False)
                record={}
                j_content['instanceId']=instanceId
                titOrd.append(j_content)
                faf.printObject(j_content,"mls\\golive",x,"FixTitle_Ordermich_prod_instance,json",False)
                j_content={}        
        f.close()
        inventory['instances']=instance
        orderTitle['titles']=titOrd
        faf.printObject(inventory,"mls\\golive",x,"Fixmich_prod_instance,json",True)
        faf.printObject(orderTitle,"mls\\golive",x,"title_Ordermich_prod_instance,json",True)
    except ValueError as error:
        print("Error: %s" % error)          
        
if __name__ == "__main__":
    #path_dir: str=r"C:\Users\asoto\Documents\EBSCO\Migrations\folio\mls"
    #SearchClient(path_dir)
    
    """This is the Starting point for the script"""
    #customerName="michstate_prod"
    #path_dir: str=r"C:\Users\asoto\Documents\EBSCO\Migrations\folio\mls\golive"
    #content_dir: List[str] = os.listdir(path_dir)
    #readorders_fix()
    
    clientName="michstate_prod"
    #Creating/verifing  folders logs, data, results, download refdata needed
    paths=faf.createdFolderStructure(clientName,False,"categories","acquisitionsUnits","organizations","mtypes","locations","funds","expenseClasses","note-types")
    fileToread="Copy of Copy of LAW FOLIO Base Status o and c June 28 on July 2.xlsx"
    fileToread="Copy of LAW FOLIO STO BASE June 26.xlsx"
    sheetName=""
    spread=True
    readorders(spread,rootpath=paths[0],file_name=fileToread,client=clientName,distinct=['RECORD #(ORDER)'],orderby="",delimiter="",sheetName=sheetName)
    #out_file=open('uuid_michigan3.txt','w')
    #dup=[]
    #for i in range(1, 1000):
    #    uui=str(uuid.uuid4())
    #    if uui in dup:
    #        dup.append(uui)
    #        print("duplicated")
    #    else:
    #        out_file.write(uui+"\n")
    #out_file.close()
    #print(f"list ready")
    #filename="MI_State_ExpenseClass_FundAssignments2.xlsx"
    #funds="funds2.xlsx"
    #path_file: str = path_dir + "/" + filename  
    #typeFile=2 #1 spreadsheet, 2 CSV
    #readOrganizations(f"{path_dir}/{filename}","vendorrecsApril8" ,customerName,typeFile)
    #readorders(f"{path_dir}\{filename}","masterorders" ,customerName)
    #readfunds(path_dir,f"{path_dir}\{filename}",f"{path_dir}\{funds}","masterorders" ,customerName)
    #file_name="organizations_final_mls.xlsx"
    #sheetName=""
    #readOrganizations(path_dir,f"{path_dir}\{file_name}",sheetName,customerName)
    #path_dir: str=r"C:\Users\asoto\Documents\EBSCO\Migrations\folio\mls\goliveTEST"
    #filename="Michigan_State_Test_SO.xlsx"
    #filename="Michigan_State_ongoinOrders.xlsx"
    #readorders(f"{path_dir}",f"{path_dir}\{filename}","masterorders" ,customerName,True)
    #path_dir: str=r"C:\Users\asoto\Documents\EBSCO\Migrations\folio\mls\golive1"
    #filename="FOLIOMSUoandcBase_6_26_21.xlsx"
    #readorders(f"{path_dir}",f"{path_dir}\{filename}","masterorders" ,customerName,False)
    
    
    #filename="FOLIOMSUoandcBase_6_26_21.xlsx"
    #readorders_fix(f"{path_dir}",f"{path_dir}\{filename}","masterorders" ,customerName)