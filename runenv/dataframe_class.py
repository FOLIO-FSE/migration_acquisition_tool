import json
import logging
import pandas as pd
import time

class dataframe():    
    def __init__(self):
        pass

    def importDataFrame(self,file_path,**kwargs):    
        try:
            start_time = time.perf_counter()
            sw=True
            print("\n"+f"INFO Uploading Dataframe")
            if "orderby" in kwargs: 
                self.orderby=kwargs['orderby']
            else:
                self.orderby=""
            if "distinct" in kwargs: 
                self.distinct=kwargs['distinct']
            else:
                self.distinct=""
            if "sheetName" in kwargs: 
                self.sheet_name=kwargs['sheetName']
            else:
                self.sheet_name=""
            if "mapping_file" in kwargs: 
                self.mapping_file=kwargs['mapping_file']
            else:
                self.mapping_file=""
            self.filename = r"{}".format(file_path)
            if self.filename[-4:] == ".csv":
                self.df = pd.read_csv(self.filename)
            elif self.filename[-5:] == ".json":
                self.df = pd.read_json(self.filename)
            elif self.filename[-4:] == ".tsv":
                self.df = pd.read_csv(self.filename, sep='\t')
            elif self.filename[-4:] == ".xls":            
                if self.sheet_name: 
                    self.df = pd.read_excel(self.filename, engine='openpyxl', sheet_name=self.sheet_name)
                else: 
                    self.df = pd.read_excel(self.filename, engine='openpyxl')
            elif self.filename[-5:] == ".xlsx":            
                if self.sheet_name: 
                    self.df = pd.read_excel(self.filename, engine='openpyxl', sheet_name=self.sheet_name)
                else: 
                    self.df = pd.read_excel(self.filename, engine='openpyxl')
            else:
                print("ERROR there is not a file to read in ../settings file")
                sw=False
            if sw:
                lendf=len(self.df)
                if self.sheet_name:      
                    print(f"INFO File {self.sheet_name} {self.filename} Total rows: {lendf}")
                else:
                    print(f"INFO File {self.filename} Total rows: {lendf}")
                #print(f"INFO columns in the file with legacy system fields Names  {self.df.columns}")
                self.df = self.df.apply(lambda x: x.fillna(""))

                if self.distinct: 
                    if len(self.distinct)>0:
                        #print(self.distinct)
                        self.df_unique =self.df.drop_duplicates(subset =self.distinct, keep="first", inplace=False,ignore_index=True)
                        print("INFO Total rows not duplicated records: {0}".format(len(self.df_unique)))
                        self.df=self.df_unique
                if self.mapping_file:
                    self.df_changed=self.changeColumns()
                    self.df=self.df_changed
                end_time = time.perf_counter()
                print(f"INFO Dataframe Execution Time : {end_time - start_time:0.2f}" )
                return self.df
        except Exception as ee:
            print(f"ERROR: {ee}")
            return None
            
    def changeColumns(self):
        self.dfnew=pd.DataFrame()
        f = open(self.mapping_file,encoding='utf-8')
        data = json.load(f)
        for i in data['data']:
            try:
                #print(i['legacy_field'])
                if str(i['legacy_field'])!="Not mapped":
                    folio_field=i['folio_field']
                    legacy_field=i['legacy_field']
                    self.dfnew[folio_field]=self.df[legacy_field]
            except Exception as ee:
                print(f"WARNING: {ee} legacy_field described was not found in the sourceData as column name, check it in the mapping file")
        print("INFO Column has been renamed"+"\n"+f"{self.dfnew.columns}")
        #print(self.dfnew)
        return self.dfnew
            
    def exportDataFrame(self,df,file_path,**kwargs):
        self.filename=file_path[:-5]
        self.df.to_csv(self.df, index = False)
        return df

    def createDataFrame(self,columnsDataframe):
        df = pd.DataFrame(columns = columnsDataframe)
        return df
    
