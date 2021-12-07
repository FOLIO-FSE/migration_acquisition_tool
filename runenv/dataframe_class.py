import json
import logging
import pandas as pd
import time

class dataframe():    
    def __init__(self):

        # Use 3 decimal places in output display
        pd.set_option("display.precision", 3)

        # Don't wrap repr(DataFrame) across additional lines
        pd.set_option("display.expand_frame_repr", False)

        # Set max rows displayed in output to 5
        pd.set_option("display.max_rows", 5)


    def importDataFrame(self, file_path, **kwargs):
        try:
            start_time = time.perf_counter()
            #print(f"INFO start time: {start_time}")
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
                    self.df = pd.read_csv(self.filename, encoding="utf-8")
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
                        self.df = pd.read_excel(self.filename, engine='openpyxl')
                except pd.errors.EmptyDataError:
                    print(f"ERROR DATAFRAME:{pd.errors.EmptyDataError}")
            else:
                print(f"ERROR The \"{self.dfname}\" data file must have one of the following endings: csv, tsv, json, xls, xlsx")
                sw = False
            if sw:
                lendf = len(self.df)
                if self.sheet_name:      
                    print(f"INFO File <<{self.dfname}>>")
                else:
                    print(f"INFO File <<{self.dfname}>>")
                #print(f"INFO columns in the file with legacy system fields Names  {self.df.columns}")
                self.df = self.df.apply(lambda x: x.fillna(""))
                

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
        except Exception as ee:
            print(f"ERROR DATAFRAME: {ee}")
            return None
        
    def changeDataType(self):
        try:
            if 'poNumber' in self.df:
                self.df['poNumber']=self.df['poNumber'].astype('string')
                self.df['poNumber']=self.df['poNumber'].str.strip()           
            if 'code' in self.df:
                self.df['code']=self.df['code'].astype('string')
                self.df['code']=self.df['code'].str.strip()
            if 'vendor' in self.df:
                self.df['vendor'] = self.df['vendor'].astype('string')
                self.df['vendor'] = self.df['vendor'].str.strip()
            if 'orderType' in self.df:
                self.df['orderType'] = self.df['orderType'].astype('string')
                self.df['orderType'] = self.df['orderType'].str.strip()
            if 'workflowStatus' in self.df:
                self.df['workflowStatus'] = self.df['workflowStatus'].astype('string')
                self.df['workflowStatus'] = self.df['workflowStatus'].str.strip()
            if 'compositePoLines[0].fundDistribution[0].code' in self.df:
                self.df['compositePoLines[0].fundDistribution[0].code'] = self.df['compositePoLines[0].fundDistribution[0].code'].astype('string')
                self.df['compositePoLines[0].fundDistribution[0].code'] = self.df['compositePoLines[0].fundDistribution[0].code'].str.strip()
            if 'compositePoLines[0].fundDistribution[0].expenseClassId' in self.df:
                self.df['compositePoLines[0].fundDistribution[0].expenseClassId'] = self.df['compositePoLines[0].fundDistribution[0].expenseClassId'].astype('string')
                self.df['compositePoLines[0].fundDistribution[0].expenseClassId'] = self.df['compositePoLines[0].fundDistribution[0].expenseClassId'].str.strip()
            if 'compositePoLines[0].locations[0].locationId' in self.df:
                self.df['compositePoLines[0].locations[0].locationId'] = self.df['compositePoLines[0].locations[0].locationId'].astype('string')
                self.df['compositePoLines[0].locations[0].locationId'] = self.df['compositePoLines[0].locations[0].locationId'].str.strip()
            if 'compositePoLines[0].orderFormat' in self.df:
                self.df['compositePoLines[0].orderFormat'] = self.df['compositePoLines[0].orderFormat'].astype('string')
                self.df['compositePoLines[0].orderFormat'] = self.df['compositePoLines[0].orderFormat'].str.strip()
            if 'compositePoLines[0].paymentStatus' in self.df:
                self.df['compositePoLines[0].paymentStatus'] = self.df['compositePoLines[0].paymentStatus'].astype('string')
                self.df['compositePoLines[0].paymentStatus'] = self.df['compositePoLines[0].paymentStatus'].str.strip()
            if 'compositePoLines[0].receiptStatus' in self.df:
                self.df['compositePoLines[0].receiptStatus'] = self.df['compositePoLines[0].receiptStatus'].astype('string')
                self.df['compositePoLines[0].receiptStatus'] = self.df['compositePoLines[0].receiptStatus'].str.strip()
            if 'compositePoLines[0].acquisitionMethod' in self.df:
                self.df['compositePoLines[0].acquisitionMethod'] = self.df['compositePoLines[0].acquisitionMethod'].astype('string')
                self.df['compositePoLines[0].acquisitionMethod'] = self.df['compositePoLines[0].acquisitionMethod'].str.strip()
            if 'compositePoLines[0].eresource.materialType' in self.df:
                self.df['compositePoLines[0].eresource.materialType'] = self.df['compositePoLines[0].eresource.materialType'].astype('string')
                self.df['compositePoLines[0].eresource.materialType'] = self.df['compositePoLines[0].eresource.materialType'].str.strip()                
            if 'compositePoLines[0].physical.materialType' in self.df:
                self.df['compositePoLines[0].physical.materialType'] = self.df['compositePoLines[0].physical.materialType'].astype('string')
                self.df['compositePoLines[0].physical.materialType'] = self.df['compositePoLines[0].physical.materialType'].str.strip() 
            if 'LEGACY SYSTEM' in self.df:
                self.df['LEGACY SYSTEM'] = self.df['LEGACY SYSTEM'].str.strip()
                self.df['LEGACY SYSTEM'] = self.df['LEGACY SYSTEM'].str.strip()
            if 'FOLIO' in self.df:
                self.df['FOLIO'] = self.df['FOLIO'].str.strip()
                self.df['FOLIO'] = self.df['FOLIO'].str.strip()
                
            
                
            
        except pd.errors.EmptyDataError:
            print(f"ERROR DATAFRAME distinct :{pd.errors.EmptyDataError}")

    def changeColumns(self):
        try:
            self.dfnew=pd.DataFrame()
            f = open(self.mapping_file,encoding='utf-8')
            data = json.load(f)
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

            #print(changelist)
            print(f"INFO Column has been renamed for <<{self.dfname}>>")
            for li in changelist:
                print(li)
            #print(self.dfnew)
            return self.dfnew
        except pd.errors.EmptyDataError:
            print(f"ERROR DATAFRAME distinct :{pd.errors.EmptyDataError}")

    def exportDataFrame(self,df,file_path,**kwargs):
        self.filename=file_path[:-5]
        self.df.to_csv(self.df, index = False)
        return df

    def createDataFrame(self,columnsDataframe):
        df = pd.DataFrame(columns = columnsDataframe)
        return df
    
