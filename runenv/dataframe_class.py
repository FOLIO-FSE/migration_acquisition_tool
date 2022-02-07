import json
import os
import logging
import pandas as pd
import time
from xlsx2csv import Xlsx2csv
from io import StringIO


class dataframe():    
    def __init__(self):

        # Use 3 decimal places in output display
        pd.set_option("display.precision", 3)

        # Don't wrap repr(DataFrame) across additional lines
        pd.set_option("display.expand_frame_repr", False)

        # Set max rows displayed in output to 5
        pd.set_option("display.max_rows", 5)

    def importupla(self, **kwargs):
        try:
            sw = True
            if "dfname" in kwargs:
                self.dfname = kwargs['dfname']
            else:
                self.dfname = kwargs['dfname']
            customcolumns=kwargs['columns']
            self.tupla=kwargs['tupla']
            start_time = time.perf_counter()
            self.df = pd.DataFrame(self.tupla, columns=customcolumns)
            lendf=len(self.df)
            end_time = time.perf_counter()
            total_time= round((end_time - start_time)) 
            print(f"INFO Dataframe <<{self.dfname}>> Execution Time {total_time} seconds, for {lendf} records")
            #print(self.df.to_markdown(tablefmt="grid"))
            return self.df
        
        except pd.errors.EmptyDataError:
            print(f"ERROR DATAFRAME:{pd.errors.EmptyDataError}")
            return None
        
    def importdict(self, **kwargs):
        try:
            data=kwargs['data']
            self.df=pd.DataFrame.from_dict(data)
            return self.df
        except pd.errors.EmptyDataError:
            print(f"ERROR DATAFRAME:{pd.errors.EmptyDataError}")
              
    def importDataFrame(self, file_path, **kwargs):
        try:
            start_time = time.perf_counter()
            file_size = os.path.getsize(file_path)
            print(f"INFO file size: {file_size} bytes")
            sw = True
            if "dfname" in kwargs:
                self.dfname = kwargs['dfname']
            else:
                self.dfname = "Not Name defined"
            #print(f"INFO Uploading Dataframe [[{self.dfname}]]")
            if "orderby" in kwargs: 
                self.orderby = kwargs['orderby']
            else:
                self.orderby = ""
            if "distinct" in kwargs: 
                self.distinct = kwargs['distinct']
            else:
                self.distinct = ""
            if "sheetName" in kwargs: 
                self.sheet_name = kwargs['sheetName']
            else:
                self.sheet_name = ""
            if "mapping_file" in kwargs: 
                self.mapping_file = kwargs['mapping_file']
            else:
                self.mapping_file = ""

            self.filename = r"{}".format(file_path)
            if self.filename[-4:] == ".csv":
                try:
                    self.df = pd.read_csv(self.filename, encoding="utf-8", error_bad_lines=False)
                except pd.errors.EmptyDataError:
                    print(f"ERROR DATAFRAME:{pd.errors.EmptyDataError}")
            elif self.filename[-5:] == ".json":
                try:
                    self.df = pd.read_json(self.filename,encoding="utf-8")
                except pd.errors.EmptyDataError:
                    print(f"ERROR DATAFRAME:{pd.errors.EmptyDataError}")
            elif self.filename[-4:] == ".tsv":
                try:
                    self.df = pd.read_csv(self.filename, sep='\t',encoding="utf-8")
                except pd.errors.EmptyDataError:
                    print(f"ERROR DATAFRAME:{pd.errors.EmptyDataError}")
            elif self.filename[-4:] == ".xls" or self.filename[-5:] == ".xlsx":
                try:
                    if self.sheet_name:
                        self.df = pd.read_excel(self.filename, engine='openpyxl', sheet_name=self.sheet_name)
                    else:
                        #if file_size> 10000:
                        #    self.df=self.exceltodataframe()
                    #        print(f"INFO converting file from xls to tsv")   
                        self.df = pd.read_excel(self.filename, engine='openpyxl')

                except pd.errors.EmptyDataError:
                    print(f"ERROR DATAFRAME:{pd.errors.EmptyDataError}")
            else:
                    print(f"ERROR The \"{self.dfname}\" data file must have one of the following endings: csv, tsv, json, xls, xlsx")
                    sw = False
            if sw:
                lendf = len(self.df)
                if self.filename.find("acquisitionMapping.xlsx")!=-1:
                    self.df.rename(columns={ self.df.columns[1]: "LEGACY SYSTEM" }, inplace=True)
                if self.sheet_name:      
                    print(f"INFO File <<{self.dfname}>>")
                else:
                    print(f"INFO File <<{self.dfname}>>")
                #print(f"INFO columns in the file with legacy system fields Names  {self.df.columns}")
                self.df = self.df.apply(lambda x: x.fillna(""))
                self.df.columns = self.df.columns.str.strip()

                if self.mapping_file:
                    self.df_changed = self.changeColumns()
                    self.df=self.df_changed


                if self.distinct: 
                    if len(self.distinct)>0:
                        try:
                            #print(self.distinct)
                            self.df_unique = self.df.drop_duplicates(subset=self.distinct, keep="first", inplace=False, ignore_index=True)
                            print("INFO Total rows not duplicated records: {0}".format(len(self.df_unique)))
                            self.df=self.df_unique
                        except pd.errors.EmptyDataError:
                            print(f"ERROR DATAFRAME distinct :{pd.errors.EmptyDataError}")

                self.changeDataType()
                end_time = time.perf_counter()
                total_time= round((end_time - start_time)) 
                print(f"INFO Dataframe <<{self.dfname}>> Execution Time {total_time} seconds, for {lendf} records")
                
                return self.df
        except pd.errors.EmptyDataError:
            print(f"ERROR DATAFRAME:{pd.errors.EmptyDataError}")
            return None
        
    def changeDataType(self):
        try:
            if 'poNumber' in self.df:
                self.df['poNumber']=self.df['poNumber'].astype('str')
                self.df['poNumber']=self.df['poNumber'].str.strip()           
            if 'code' in self.df:
                self.df['code']=self.df['code'].astype('str')
                self.df['code']=self.df['code'].str.strip()
            if 'vendor' in self.df:
                self.df['vendor'] = self.df['vendor'].astype('str')
                self.df['vendor'] = self.df['vendor'].str.strip()
            if 'orderType' in self.df:
                self.df['orderType'] = self.df['orderType'].astype('str')
                self.df['orderType'] = self.df['orderType'].str.strip()
            if 'workflowStatus' in self.df:
                self.df['workflowStatus'] = self.df['workflowStatus'].astype('str')
                self.df['workflowStatus'] = self.df['workflowStatus'].str.strip()
            if 'compositePoLines[0].fundDistribution[0].code' in self.df:
                self.df['compositePoLines[0].fundDistribution[0].code'] = self.df['compositePoLines[0].fundDistribution[0].code'].astype('str')
                self.df['compositePoLines[0].fundDistribution[0].code'] = self.df['compositePoLines[0].fundDistribution[0].code'].str.strip()
            if 'compositePoLines[0].fundDistribution[0].expenseClassId' in self.df:
                self.df['compositePoLines[0].fundDistribution[0].expenseClassId'] = self.df['compositePoLines[0].fundDistribution[0].expenseClassId'].astype('str')
                self.df['compositePoLines[0].fundDistribution[0].expenseClassId'] = self.df['compositePoLines[0].fundDistribution[0].expenseClassId'].str.strip()
            if 'compositePoLines[0].locations[0].locationId' in self.df:
                self.df['compositePoLines[0].locations[0].locationId'] = self.df['compositePoLines[0].locations[0].locationId'].astype('str')
                self.df['compositePoLines[0].locations[0].locationId'] = self.df['compositePoLines[0].locations[0].locationId'].str.strip()
            if 'compositePoLines[0].locations[1].locationId' in self.df:
                self.df['compositePoLines[0].locations[1].locationId'] = self.df['compositePoLines[0].locations[1].locationId'].astype('str')
                self.df['compositePoLines[0].locations[1].locationId'] = self.df['compositePoLines[0].locations[1].locationId'].str.strip()
            if 'compositePoLines[0].locations[2].locationId' in self.df:
                self.df['compositePoLines[0].locations[2].locationId'] = self.df['compositePoLines[0].locations[2].locationId'].astype('str')
                self.df['compositePoLines[0].locations[2].locationId'] = self.df['compositePoLines[0].locations[2].locationId'].str.strip()
            if 'compositePoLines[0].locations[3].locationId' in self.df:
                self.df['compositePoLines[0].locations[0].locationId'] = self.df['compositePoLines[0].locations[3].locationId'].astype('str')
                self.df['compositePoLines[0].locations[0].locationId'] = self.df['compositePoLines[0].locations[3].locationId'].str.strip()
            if 'compositePoLines[0].orderFormat' in self.df:
                self.df['compositePoLines[0].orderFormat'] = self.df['compositePoLines[0].orderFormat'].astype('str')
                self.df['compositePoLines[0].orderFormat'] = self.df['compositePoLines[0].orderFormat'].str.strip()
            if 'compositePoLines[0].paymentStatus' in self.df:
                self.df['compositePoLines[0].paymentStatus'] = self.df['compositePoLines[0].paymentStatus'].astype('str')
                self.df['compositePoLines[0].paymentStatus'] = self.df['compositePoLines[0].paymentStatus'].str.strip()
            if 'compositePoLines[0].receiptStatus' in self.df:
                self.df['compositePoLines[0].receiptStatus'] = self.df['compositePoLines[0].receiptStatus'].astype('str')
                self.df['compositePoLines[0].receiptStatus'] = self.df['compositePoLines[0].receiptStatus'].str.strip()
            if 'compositePoLines[0].acquisitionMethod' in self.df:
                self.df['compositePoLines[0].acquisitionMethod'] = self.df['compositePoLines[0].acquisitionMethod'].astype('str')
                self.df['compositePoLines[0].acquisitionMethod'] = self.df['compositePoLines[0].acquisitionMethod'].str.strip()
            if 'compositePoLines[0].eresource.materialType' in self.df:
                self.df['compositePoLines[0].eresource.materialType'] = self.df['compositePoLines[0].eresource.materialType'].astype('str')
                self.df['compositePoLines[0].eresource.materialType'] = self.df['compositePoLines[0].eresource.materialType'].str.strip()                
            if 'compositePoLines[0].physical.materialType' in self.df:
                self.df['compositePoLines[0].physical.materialType'] = self.df['compositePoLines[0].physical.materialType'].astype('str')
                self.df['compositePoLines[0].physical.materialType'] = self.df['compositePoLines[0].physical.materialType'].str.strip() 
            if 'LEGACY SYSTEM' in self.df:
                self.df['LEGACY SYSTEM'] = self.df['LEGACY SYSTEM'].str.strip()
                self.df['LEGACY SYSTEM'] = self.df['LEGACY SYSTEM'].str.strip()
            if 'FOLIO' in self.df:
                self.df['FOLIO'] = self.df['FOLIO'].str.strip()
                self.df['FOLIO'] = self.df['FOLIO'].str.strip()
            '''if 'compositePoLines[0].cost.listUnitPrice' in self.df:
                self.df['compositePoLines[0].cost.listUnitPrice'] = self.df['compositePoLines[0].cost.listUnitPrice'].str.strip()
                self.df['compositePoLines[0].cost.listUnitPrice'] = self.df['compositePoLines[0].cost.listUnitPrice'].replace({'$':''}, regex=True)
            if 'compositePoLines[0].cost.quantityPhysical' in self.df:
                self.df['compositePoLines[0].cost.quantityPhysical'] = self.df['compositePoLines[0].cost.quantityPhysical'].str.strip()
                self.df['compositePoLines[0].cost.quantityPhysical'] = self.df['compositePoLines[0].cost.quantityPhysical'].replace({'$':''}, regex=True)
            if 'compositePoLines[0].cost.quantityElectronic' in self.df:
                self.df['compositePoLines[0].cost.quantityElectronic'] = self.df['compositePoLines[0].cost.quantityElectronic'].str.strip()
                self.df['compositePoLines[0].cost.quantityElectronic'] = self.df['compositePoLines[0].cost.quantityElectronic'].replace({'$':''}, regex=True)'''
        except Exception as ee:
            print(f"ERROR: Change Columns {ee}") 

    def changeColumns(self):
        try:
            self.dfnew=pd.DataFrame()
            f = open(self.mapping_file,encoding='utf-8')
            data = json.load(f)
            try:
                changelist=[]
                for i in data['data']:
                    try:
                        #print(i['legacy_field'])
                        if str(i['legacy_field'])!="Not mapped":
                            if i['legacy_field']:
                                folio_field=i['folio_field']
                                legacy_field=i['legacy_field']
                                self.dfnew[folio_field]=self.df[legacy_field]
                                #print("INFO Dataframe Replacing the following legacy field columns:")
                                changelist.append(f"{legacy_field} => {folio_field}")
                    except Exception as ee:
                        print(f"WARNING: {ee} legacy_field was not described as column Name in the sourceData: check the mapping file {self.dfname}")
            except Exception as ee:
                print(f"ERROR: Change Columns {ee}")  
                #print(changelist)
            print(f"INFO Column has been renamed for <<{self.dfname}>>")
            #print(self.dfnew.to_markdown(tablefmt="grid"))
            for li in changelist:
                print(li)
                #print(self.dfnew)
            return self.dfnew
        except Exception as ee:
            print(f"ERROR: Change Columns {ee}") 
    
    def exportDataFrame(self,df,file_path,**kwargs):
        self.filename=file_path[:-5]
        self.df.to_csv(self.df, index = False)
        return df

    def createDataFrame(self,columns):
        #df = pd.DataFrame(data, label_rows, label_cols)                
        self.df = pd.DataFrame(columns = columns)
        return self.df


    def exceltodataframe(self):
        try:
            buffer = StringIO()
            #if self.sheet_name:
            #    Xlsx2csv(self.filename, outputencoding="utf-8", sheet_name=self.sheet_name).convert(buffer)
            #else:
            Xlsx2csv(self.filename, outputencoding="utf-8").convert(buffer)
            buffer.seek(0)
            self.dftocsv = pd.read_csv(buffer)
            #self.csv_data = buffer.to_csv(f"{self.filename}_.tsv", sep="\t", index=False, header = True, quoting=csv.QUOTE_NONE)
        except pd.errors.EmptyDataError:
            print(f"ERROR DATAFRAME:{pd.errors.EmptyDataError}")
            return None
        
        return self.dftocsv

    def printdataframe(self):
        print(self.df.to_markdown(tablefmt="grid"))
    #def report(self,columnsDataframe):
        #label_rows=
        #df = pd.DataFrame(data, label_rows, label_cols)     
        
        
'''def split_column(self, s_col):
        # Split the header into individual pieces.
        #compositePoLines[0].fundDistribution[0].code
        col = s_col.name.split(",")
        # Clean up any leading or trailing white space.
        col = [x.strip() for x in col]
        # Create a new dataframe from series and column heads.
        data = {col[x]: s_col.to_list() for x in range(len(col))}
        df = pd.DataFrame(data)
        # Create a copy to make changes to the values.
        df_res = df.copy()
        # Go through the column headers, get the first number, then filter and apply bool.
        for col in df.columns:
            value = pd.to_numeric(col[0])
            df_res.loc[df[col] == value, col] = 1
            df_res.loc[df[col] != value, col] = 0
        return df_res

    def splitfield(self, s_col):
        df_full=pd.DataFrame(df.s_col.str.split(',',1).tolist(),columns = [s_col+"1",s_col+"2"])
        return df_full
        
        data = contain_values
        df_full = pd.DataFrame(data)
        df_full = df_full.assign(s_col = ['Lahore','Dehli','New York'])
        print(df_full.columns)
        for c in df_full.columns:
            # Call the function to get the split columns in a new dataframe.
            print(df_full[c])
            df_split = self.split_column(s_col)
            # Join it with the origianl full dataframe but drop the current column.
            df_full = pd.concat([df_full.loc[:, ~df_full.columns.isin([c])], df_split], axis=1)

        print(df_full)
        return df_full'''