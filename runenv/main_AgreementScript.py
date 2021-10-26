import folioAcqfunctions as faf
from tkinter import *
from tkinter import filedialog, messagebox, ttk
################################
# MAIN PURCHASE ORDERS SCRIPT    
################################
        
if __name__ == "__main__":
    """This is the Starting point for the script"""
    customerName="cairn"
    getrefdata=False
    exist=faf.SearchClient(customerName)
    if len(exist)!=0:
        graphicinterfaces=False
        if graphicinterfaces:
            root = Tk()
            e = faf.window(root,"Agreements","1000x500", customerName)
            root.mainloop()
        else:
            faf.readagreements(rootpath=faf.createdFolderStructure(customerName,getrefdata),customerName=customerName)
    else: 
        print(f"INFO Customer okapi parameters should include in the ../runenv/okapi_customer.json"+"\n"+"END")
    #END