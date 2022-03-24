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
import csv
from typing import List

def sierra_get_items(path_file):
    in_file=open(path_file,"r", encoding="utf-8")    
    #out_file.close()
    count=1
    items=[]
    new_line=""

    x=0
    for a_line in in_file:
        new_line=a_line.replace("\n","")
        tag=a_line[1:4]
        tagtemp=""
        creationdate=""
        tag050=""
        tag090=""
        tag907=""
        tag945=""
        callNumber=""
        if tag=="001":
            print("Record : ",str(count))
            x+=1
            count+=1
            
        elif tag=="008":
            pass
        elif tag=="050":
            tag050=new_line[8:]
            tag050=tag050.replace("$a","")
            tag050=tag050.replace("$b","")
            callNumber=tag050
        elif tag=="090":

            tag090=new_line[7:]
            tag090=new_line[8:]
            tag090=tag090.replace("$a","")
            tag090=tag090.replace("$b","")
            callNumber=tag090
            
        elif tag=="907":
            
            tag907=new_line[7:]            
            tagtemp=tag907.split("$")
            for l in tagtemp:
                if l[0]=="a":
                    RECORDBIBLIO=l[2:]
                # else:
                #     print("error")
                #     with open("C:\\Users\\asoto\\Documents\\EBSCO\\Migrations\\folio\\client_data\\Cairn\\data\\norecordbibloerror.txt","a",encoding="utf-8") as errornobiblio:
                #         errornobiblio.write(new_line+"\n")    
                if l[0]=="c":
                    creationdate=l[1:]
                    
        if tag=="945":

            tag945=new_line[6:]
            if new_line.find("MARCIVE")==-1:
                tagtemp=tag945.split("$")
                print(tagtemp)
                #['\\\\', 'g1', 'i35800000235186', 'lmcrc1', 'o-', 'p{dollar}50.00', 'q-', 'r-', 's-  ', 
                # 't1', 'u0', 'v0', 'w0', 'x0', 'y.i10000021', 'z010530\n']
                #                               INTNOTE,STATUS,IMESSAGE,PRICE,INTNOTE_1,MESSAGE(ITEM),PBARCODE
            # a	Call number part 1
            # b	Call number part 2
            # c	Volume number
            # g	Copy number OK
            # i	Barcode OK
            # l	Location code OK
            # m	Message
            # n	Note
            # o	icode2
            # q	imessage
            # r	opacmsg
            # s 	status
            # t	itype
            # u	total checkouts
            # y	item record number
            # z	item created date
                for i in tagtemp:
                    if i[0]=="a":
                        CALLITEMA=i[2:]#a
                    else:
                        CALLITEMA=callNumber
                    if i[0]=="b":
                        CALLITEMA=i[2:]#b
                    if i[1]=="c":
                        VOLUME=i[2:]#v
                    if str(i[0])=="g":
                        COPY=tagtemp[1][1] #g
                    if i[0]=="i":
                        BARCODE=i[2:] #i
                    if i[0]=="l":
                        LOCATION=i[2:] #l
                    if i[0]=="p":
                    #o
                        PRICE=i[2:] #p
                        PRICE=PRICE.replace("{dollar}","")
                    if i[0]=="q":
                        IMESSAGE=i[2:] #q
                    if i[0]=="s":
                        #r
                        STATUS=i[2:]#s
                    if i[0]=="t":
                        ITYPE=i[2:] #t
                    if i[0]=="u":
                        TOTCHKOUT=i[2:] #u
                    if i[0]=="v":
                        pass
                        print(i)
                        #VOLUME=i[2:]#v
                    if i[0]=="y":
                        RECORDITEM=i[2:] #y
                
                
                
                #RECORDBIBLIO=tagtemp[1][1]
                
                ''' CALLITEM=tagtemp[2][1]
                CALLBIBLIO=tagtemp[2][1]
                
                

                ITYPE=tagtemp[2][1]
                LOCATION=tagtemp[2][1]
                OUTDATE=tagtemp[2][1]
                DUEDATE=tagtemp[2][1]
                TOTRENEW=tagtemp[2][1]
                TOTCHKOUT=tagtemp[2][1]
                INTLUSE=tagtemp[2][1]
                YTDCIRC=tagtemp[2][1]
                LYRCIRC=tagtemp[2][1]
                INVDA=tagtemp[2][1]
                MESSAGEITEM=tagtemp[2][1] '''
            
            
            #items.append([RECORDBIBLIO,RECORDITEM,CALLITEM,CALLBIBLIO,VOLUME,BARCODE,ITYPE,LOCATION,OUTDATE,
            #                               DUEDATE,TOTRENEW,TOTCHKOUT,INTLUSE,YTDCIRC,LYRCIRC,INVDA,MESSAGEITEM,
            #                               INTNOTE,STATUS,IMESSAGE,PRICE,INTNOTE_1,MESSAGE(ITEM),PBARCODE])
        
        with open("C:\\Users\\asoto\\Documents\\EBSCO\\Migrations\\folio\\client_data\\Cairn\\data\\marc.mrk","a",encoding="utf-8") as newmarc:
            newmarc.write(new_line+"\n")        
       
        
    #df_item=pd.DataFrame(items, columns = ['RECORD #(BIBLIO)','RECORD #(ITEM)','CALL #(ITEM)','CALL #(BIBLIO)','VOLUME','BARCODE','I TYPE','LOCATION','OUT DATE',
    #                                       'DUE DATE','TOT RENEW','TOT CHKOUT','INTL USE ','YTDCIRC','LYRCIRC','INVDA','MESSAGEITEM',
    #                                       'INT NOTE','STATUS','IMESSAGE','PRICE','INT NOTE_1','MESSAGE(ITEM)','P BARCODE'])
    
    #df_item.to_csv('C:\\Users\\asoto\\Documents\\EBSCO\\Migrations\\folio\\client_data\\Cairn\\data\\New_items_cairn.tsv', sep = '\t', index=False)


'''items.append([str(row['Z30_REC_KEY_FULL']).strip(),str(newId),str(row['N_SISTEMA']).strip(),str(row['COPIA']).strip(),str(row['Z30_REC_KEY']).strip(),str(row['Z30_BARCODE']).strip(),str(row['z30_barcode-SIN-ESPACIOS']).strip(),str(row['Z30_SUB_LIBRARY']).strip(),str(row['Z30_MATERIAL']).strip(),str(row['Z30_ITEM_STATUS']).strip(),str(row['Z30_ITEM_PROCESS_STATUS']).strip(),row['Z30_OPEN_DATE'],row['Z30_UPDATE_DATE'],str(row['Z30_CATALOGER']).strip(),row['Z30_DATE_LAST_RETURN'],row['Z30_HOUR_LAST_RETURN'],str(row['Z30_IP_LAST_RETURN']).strip(),row['Z30_NO_LOANS'],str(row['Z30_ALPHA']).strip(),str(row['Z30_COLLECTION']).strip(),str(row['Z30_CALL_NO_TYPE']).strip(),clasificacion,str(row['PREFIX']).strip(),str(row['DEWEY']).strip(),str(row['POS']).strip(),str(row['Z30_CALL_NO_KEY']).strip(),str(row['Z30_CALL_NO_2_TYPE']).strip(),str(row['Z30_CALL_NO_2']).strip(),str(row['Z30_CALL_NO_2_KEY']).strip(),str(row['Z30_DESCRIPTION']).strip(),str(row['Z30_NOTE_OPAC']).strip(),str(row['Z30_NOTE_CIRCULATION']).strip(),str(row['Z30_NOTE_INTERNAL']).strip(),str(row['Z30_ORDER_NUMBER']).strip(),str(row['Z30_INVENTORY_NUMBER']).strip(),row['Z30_INVENTORY_NUMBER_DATE'],['Z30_LAST_SHELF_REPORT_DATE'],row['Z30_PRICE'],row['Z30_SHELF_REPORT_NUMBER'],row['Z30_ON_SHELF_DATE'],str(row['Z30_ON_SHELF_SEQ']).strip(),str(row['Z30_REC_KEY_2']).strip(),str(row['Z30_REC_KEY_3']).strip(),str(row['Z30_PAGES']).strip(),row['Z30_ISSUE_DATE'],row['Z30_EXPECTED_ARRIVAL_DATE'],row['Z30_ARRIVAL_DATE'],str(row['Z30_ITEM_STATISTIC']).strip(),str(row['Z30_COPY_ID']).strip(),str(row['Z30_HOL_DOC_NUMBER_X']).strip(),str(row['Z30_TEMP_LOCATION']).strip(),str(row['Z30_ENUMERATION_A']).strip(),str(row['Z30_ENUMERATION_B']).strip(),str(row['Z30_ENUMERATION_C']).strip(),str(row['Z30_ENUMERATION_D']).strip(),str(row['Z30_ENUMERATION_E']).strip(),str(row['Z30_ENUMERATION_F']).strip(),str(row['Z30_ENUMERATION_G']).strip(),str(row['Z30_ENUMERATION_H']).strip(),str(row['Z30_CHRONOLOGICAL_I']).strip(),str(row['Z30_CHRONOLOGICAL_J']).strip(),str(row['Z30_CHRONOLOGICAL_K']).strip(),str(row['Z30_CHRONOLOGICAL_L']).strip(),str(row['Z30_CHRONOLOGICAL_M']).strip(),str(row['Z30_SUPP_INDEX_O']).strip(),str(row['Z30_85X_TYPE']).strip(),str(row['Z30_DEPOSITORY_ID']).strip(),str(row['Z30_LINKING_NUMBER']).strip(),str(row['Z30_GAP_INDICATOR']).strip(),str(row['Z30_MAINTENANCE_COUNT']).strip(),row['Z30_PROCESS_STATUS_DATE'],row['Z30_UPD_TIME_STAMP'],str(row['Z30_IP_LAST_RETURN_V6']).strip()])'''
    
if __name__ == "__main__":
    """This is the Starting point for the script"""
    #replaceItemIPVG()
    #ipvg_001() # reemplazar la 001 del archivo original de Aleph
    pathfile="C:\\Users\\asoto\\Documents\\EBSCO\\Migrations\\folio\\client_data\\Cairn\\data\\foliounsuppressedjan312022.mrk"
    sierra_get_items(pathfile)