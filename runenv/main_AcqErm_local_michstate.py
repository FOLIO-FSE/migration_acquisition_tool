import functions_AcqErm as ma
from tkinter import *
from tkinter import filedialog, messagebox, ttk
import argparse
################################
# MAIN ERM AND ACQUISITIONS MIGRATION TOOLS 
################################
if __name__ == "__main__":
    """This is the Starting point for the script"""
    #Insert the customer c-ode here or enter the customer running the code, by default blank
    customerName="michstate_prod"
    #Insert True if do you want to get the reference data from server, this function allow download the reference data to acquisitions, by default TRUE
    getrefdata=False#False
    #Insert True/False if you want to use GUI / command line. by default command Line FALSE
    graphicinterfaces=False#False
    #Insert the script to run (o=organizations/p=purchase orders/l=licenses/a=Agreements)
    scriptTorun="n"#"o"    
    if customerName=="": 
        print("Client code: ") 
        customerName = str(input())
    if getrefdata=="": 
        print("Do you want to download Acq Ref data: False/True") 
        getrefdata = str(input())        
    if scriptTorun=="":
        print("Enter script to run Organizations=o | Orders=p | Licenses=l | Agreements=a | Notes=n") 
        scriptTorun = str(input())
    if graphicinterfaces=="": 
        print("Do you want command / graphical interface: False/True") 
        graphicinterfaces = str(input())
    client=ma.AcqErm(customerName)
    if client.okapi_customer():
        print(f"INFO Customer found OK")
        client.menu(getrefdata=getrefdata,scriptTorun=scriptTorun,graphicinterfaces=graphicinterfaces)        
    else: 
        print(f"ERROR Customer does not exit in the file: ../runenv/okapi_customer.json")

    print(f"END")
    #END