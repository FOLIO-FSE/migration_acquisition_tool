import tkinter as tk
from tkinter import filedialog, messagebox, ttk

class window:
    def __init__(self, master,titlewin,geometrywin, customerName):
        master.title(titlewin)
        master.geometry(geometrywin)
        self.head=""
        self.frame1 = tk.LabelFrame(master, bd=5,text="Data")
        self.frame1.place(height=250, width=1000)
        # Frame for open file dialog
        self.file_frame = tk.LabelFrame(master, text="Open File")
        self.file_frame.place(height=100, width=600, rely=0.50, relx=0)
        # Frame for mapping
        self.map_frame = tk.LabelFrame(master, text="Mapping")
        self.map_frame.place(height=100, width=400, rely=0.50, relx=0.6)
        
        # Buttons Browse files
        self.button1 = tk.Button(self.file_frame, text="Browse a file", command=lambda: self.File_dialog())
        self.button1.place(rely=0.65, relx=0.30)
        #Load file
        self.button2 = tk.Button(self.file_frame, text="Load File", command=lambda: self.Load_excel_data())
        self.button2.place(rely=0.65, relx=0.65)
        #Transform data
        self.button3 = tk.Button(self.file_frame, text="Transform File", command=lambda: self.read_orders(customerName))
        self.button3.place(rely=0.65, relx=0.85)
        #self.button4 = tk.Button(self.file_frame, text="Mapping", command=lambda: self.readOrganizations())
        #self.button4.place(rely=0.65, relx=0.95)
        
        # The file/file path text
        self.label_file = tk.Label(self.file_frame, text="No File Selected")
        self.label_file.place(rely=0, relx=0)

        ## Treeview Widget
        self.tv1 = ttk.Treeview(self.frame1)

        self.tv1.place(relheight=1, relwidth=1) # set the height and width of the widget to 100% of its container (frame1).

        self.treescrolly = tk.Scrollbar(self.frame1, orient="vertical", command=self.tv1.yview) # command means update the yaxis view of the widget
        self.treescrollx = tk.Scrollbar(self.frame1, orient="horizontal", command=self.tv1.xview) # command means update the xaxis view of the widget
        self.tv1.configure(xscrollcommand=self.treescrollx.set, yscrollcommand=self.treescrolly.set) # assign the scrollbars to the Treeview Widget
        self.treescrollx.pack(side="bottom", fill="x") # make the scrollbar fill the x axis of the Treeview widget
        self.treescrolly.pack(side="right", fill="y") # make the scrollbar fill the y axis of the Treeview widget


    def File_dialog(self):
        """This Function will open the file explorer and assign the chosen file path to label_file"""
        filename = filedialog.askopenfilename(initialdir="/",
                                          title="Select A File",
                                          filetype=(("xlsx files", "*.xlsx"),("All Files", "*.*")))
        self.label_file["text"] = filename
        return None


    def Load_excel_data(self):
        """If the file selected is valid this will load the file into the Treeview"""
        file_path =self.label_file["text"]
        customerName="utm"
        
        
        try:
            excel_filename = r"{}".format(file_path)
            if excel_filename[-4:] == ".csv":
                df = pd.read_csv(excel_filename)
            else:
                df = pd.read_excel(excel_filename)
            
        except ValueError:
            self.messagebox.showerror("Information", "The file you have chosen is invalid")
            return None
        except FileNotFoundError:
            self.messagebox.showerror("Information", f"No such file as {file_path}")
            return None

        self.clear_data()
                #combobox
        #self.vlist=["option 1", "option 2", "option 3"]'''

        self.head=list(df.columns.values)
        self.vlist=self.head
        #ll=len(self.head)
        #for index in ll:
        #    combo=ttk.Combobox(self.map_frame, values=self.vlist)
        #    combo.set("Pick an Option")
        #    combo.pack(padx=5, pady=5)'''
        
        
        self.tv1["column"] = list(df.columns)
        self.tv1["show"] = "headings"
        
        for column in self.tv1["columns"]:
            self.tv1.heading(column, text=column) # let the column heading = column name

        df_rows = df.to_numpy().tolist() # turns the dataframe into a list of lists
        for row in df_rows:
            self.tv1.insert("", "end", values=row) # inserts each list into the treeview. For parameters see https://docs.python.org/3/library/tkinter.ttk.html#tkinter.ttk.Treeview.insert
        
        return None

    def read_orders(self, customerName):
        readorders(rootpath=createdFolderStructure(customerName,False),customerName=customerName)
        
    def clear_data(self):
        self.tv1.delete(*self.tv1.get_children())
        return None