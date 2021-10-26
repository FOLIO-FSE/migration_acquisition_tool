import folioAcqfunctions as faf
from tkinter import *
from tkinter import filedialog, messagebox, ttk
################################
# MAIN PURCHASE ORDERS SCRIPT    
################################
def main_acq_erm_migration_tool(**kwargs):
    try:
        sctr=""
        sctr=kwargs['scriptTorun']
        getrefdata=kwargs['getrefdata']
        graphicinterfaces=kwargs['graphicinterfaces']
        customerName=kwargs['customerName']
        print(f"INFO CUSTOMER: {customerName} | SCRIPT  {sctr} | DOWNLOAD ACQ/ERM REFDATA {getrefdata} | GRAPHIC {graphicinterfaces}")
       
        if graphicinterfaces:
            root = Tk()
            e = faf.window(root,"Purchase Orders","1000x500", customerName)
            root.mainloop()
        else:
            if scriptTorun=="p":
                faf.readorders(rootpath=faf.createdFolderStructure(customerName,getrefdata),customerName=customerName)
            elif scriptTorun=="o":
                pass
            elif scriptTorun=="l":
                pass
            elif scriptTorun=="a":
                pass
    except ValueError as error:
        print(f"Error: {error}")    
    
        
if __name__ == "__main__":

    """This is the Starting point for the script"""
    #Insert the customer code here or enter the customer running the code, by default blank
    customerName=""
    #Insert if do you want to get the reference data from server, this function allow download the reference data to acquisitions, by default TRUE
    getrefdata=""#False
    #Insert True/False if you want to use GUI / command line. by default command Line FALSE
    graphicinterfaces=""#False
    #Insert the script to run (org=organizations/ord=orders/lic=licenses/agr=Agreements)
    scriptTorun=""#"o"    
    if customerName=="": 
        print("Client code: ") 
        customerName = str(input())
    if getrefdata=="": 
        print("Do you want to download Acq Ref data: False/True") 
        getrefdata = str(input())        
    if scriptTorun=="":
        print("Enter script to run Organizations=o | Orders=p | Licenses=l | Agreements=a") 
        scriptTorun = str(input())
    if graphicinterfaces=="": 
        print("Do you want command / graphical interface: False/True") 
        graphicinterfaces = str(input())  
        exist=faf.SearchClient(customerName)
    if len(exist)!=0:
        print(f"INFO the customer exits in /runenv/okapi_customer.json file OK")
        main_acq_erm_migration_tool(customerName=customerName,getrefdata=getrefdata,scriptTorun=scriptTorun,graphicinterfaces=graphicinterfaces)        
    else: 
        print(f"INFO Customer okapi parameters should include in the ../runenv/okapi_customer.json"+"\n"+"END")
    #END