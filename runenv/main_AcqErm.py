import functions_AcqErm as ma
import agreement_class as ac
from tkinter import *
from tkinter import filedialog, messagebox, ttk
import argparse
################################
# MAIN ERM AND ACQUISITIONS MIGRATION TOOLS 
################################

def get_args():
    parser = argparse.ArgumentParser(description="Supply customer name, shorthand for script to run, and whether to download ref data.'")
    parser.add_argument("client_name", help="Name of the client - must match name in okapi_customers.json")
    parser.add_argument("script_to_run", help="Enter script to run Organizations=o | Orders=p | Licenses=l | Agreements=a | Notes=n")
    parser.add_argument("download_ref", help="Do you want to download Acq Ref data: get_ref/no_ref", default="False")
    return parser.parse_args()


if __name__ == "__main__":
    """This is the Starting point for the script"""
    #Insert True/False if you want to use GUI / command line. by default command Line FALSE
    graphicinterfaces=False#False
    
    # Fetch CLI arguments client_name, script_to_run and download_ref 
    args = get_args()
    customerName = args.client_name
    scriptTorun = args.script_to_run
    getrefdata = True if args.download_ref == "get_ref" else False

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
        print(f"ERROR Customer does not exit in the file: ../runenv/okapi_customer.json"+"\n"+"END")

    print(f"END")
    #END