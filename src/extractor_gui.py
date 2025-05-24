import errno
import logging
import os
import sqlite3
import sys
import threading
from tkinter import E, FALSE, N, S, W
from tkinter import IntVar, StringVar
from tkinter import Label, filedialog, ttk
from tkinter.constants import DISABLED, NORMAL
from tkinter.font import nametofont

from commondatafuncs import CommonDataFunction
from dbtofile import DBToFiles
from dependency_extractor import DependencyExtractor
from modelextractor import ModelExtractor
from ruleextractor import RuleExtractor
from tables import tablesData
from uiextractor import UIExtractor


class ExtractorGUI:
    def __init__(self, master, version, logFileName, guiOption):
        """
        ExtractorGUI Constructor. Declaration of all UI related elements.
        :param master: master root TK instance
        :param version: current app version
        :param logFileName: log file location
        :param guiOption: additional gui options
        """
        self.fileFrame = None
        self.jsonFile = None
        self.chooseJSONButton = None
        self.tenantZIPFile = None
        self.destinationDirLabel = None
        self.chooseDirButton = None
        self.workingDirLabel = None
        self.logFileLabelGroup = None
        self.logFileLabel = None
        self.selectExtGroup = None
        self.selectModelCheckBox = None
        self.selectUICheckBox = None
        self.selectDepCheckBox = None
        self.selectXLSXCheckBox = None
        self.startExtractionButton = None
        self.statusLabelForFileFrame = None
        self.getExtractionDataThread = None
        self.progressBarForFileFrame = None
        self.selectCSVCheckBox = None
        self.outputLabel = None
        self.selectMeasureUsageCheckBox = None
        self.destinationDir = StringVar()
        self.logFileDest = StringVar()
        self.zipFile = StringVar()
        self.appStatus = StringVar()
        self.selectModel = IntVar()
        self.selectUI = IntVar()
        self.selectDep = IntVar()
        self.selectXLSX = IntVar()
        self.selectCSV = IntVar()
        self.selectMeasureUsage = IntVar()
        self.extractionStatus = "Error"
        self.note = None
        self.logger = None
        self.master = master
        self.version = version
        self.logFileName = logFileName
        self.guiOption = guiOption
        self.framesList = {}
        self.destDir = None
        self.uiDestDir = None
        self.tenantDataDBName = None
        self.bgColor = "#6b5275"
        self.textColor = "#000000"
        self.master.title("Tenant Extractor")
        # self.master.geometry('1080x720+300+300')
        self.master.configure(bg=self.bgColor)
        self.master.resizable(False, False)
        self.setLogger()
        if self.guiOption["ui"]:
            self.createUIVariables()
            self.createWidgets()
        else:
            master.withdraw()
            self.extractData()
            sys.exit()
        self.originalColor = self.workingDirLabel["background"]
        self.startExtractionButton.configure(state=DISABLED)
        self.default_font = nametofont("TkDefaultFont")
        self.default_font.configure(size=12)
        self.master.option_add("*Font", self.default_font)
        self.style = ttk.Style()
        self.style.configure(
            "red.Horizontal.TProgressbar",
            foreground="#000000",
            troughcolor=self.bgColor,
            background="#000000",
        )
        # self.style.configure('TFrame', background=self.bgColor)
        self.style.configure("Custom.TLabelframe", background=self.bgColor)
        # self.style.configure('TButton', background=self.bgColor, forground='#000000')

    def setLogger(self):
        """
        Get logger for the app.
        :return: null
        """
        self.logger = logging.getLogger("extractor-logger")

    def createUIVariables(self):
        """
        Declaration of all UI variables.
        :return: null
        """
        self.destinationDir.set("")
        self.zipFile.set("")
        self.appStatus.set("")
        self.logFileDest.set(self.logFileName)
        self.selectModel.set(1)
        self.selectUI.set(1)
        self.selectDep.set(1)
        self.selectXLSX.set(0)
        self.selectCSV.set(0)
        self.selectMeasureUsage.set(1)

    def createWidgets(self):
        """
        Create Widgets.
        :return: null
        """
        self.master.option_add("*tearOff", FALSE)
        self.note = ttk.Notebook(self.master)
        self.createFileUploadFrame()
        self.finalizeUI()

    def createFileUploadFrame(self):
        """
        Create all the elements of the file upload frame.
        :return: null
        """
        self.fileFrame = ttk.Frame(self.note)
        # self.style.configure(self.fileFrame, background='#070110')
        # self.fileFrame.configure(bg='#070110')
        self.framesList["File Upload"] = self.fileFrame
        self.fileFrame.grid(column=0, row=0, sticky=(N, W, E, S))
        self.fileFrame.columnconfigure(0, weight=2)
        # JSON file label
        self.jsonFile = ttk.Labelframe(self.fileFrame, text="Tenant ZIP File")
        self.jsonFile.grid(row=0)
        self.chooseJSONButton = ttk.Button(
            self.jsonFile, text="Choose Tenant ZIP File", command=self.chooseJSONClicked
        )
        # zip file check
        self.chooseJSONButton.grid(row=0, padx=10)
        self.tenantZIPFile = Label(self.jsonFile, textvariable=self.zipFile)
        self.tenantZIPFile.grid(row=1, padx=10, pady=10)

        # Destination Directory label
        self.destinationDirLabel = ttk.Labelframe(
            self.fileFrame, text="Destination Directory"
        )
        self.destinationDirLabel.grid(row=1)

        self.chooseDirButton = ttk.Button(
            self.destinationDirLabel,
            text="Choose Destination Directory",
            command=self.chooseDirClicked,
        )

        self.chooseDirButton.grid(row=1, padx=10)
        self.workingDirLabel = Label(
            self.destinationDirLabel, textvariable=self.destinationDir
        )
        self.workingDirLabel.grid(row=2, padx=10, pady=10)

        # Destination Directory label
        self.logFileLabelGroup = ttk.Labelframe(self.fileFrame, text="Log File")
        self.logFileLabelGroup.grid(row=3)
        self.logFileLabel = Label(self.logFileLabelGroup, textvariable=self.logFileDest)
        self.logFileLabel.grid(row=4, padx=10, pady=10)

        # Selection
        self.selectExtGroup = ttk.LabelFrame(self.fileFrame, text="Select Extraction")
        self.selectExtGroup.grid(row=5)
        self.selectModelCheckBox = ttk.Checkbutton(
            self.selectExtGroup, variable=self.selectModel, text="Model"
        )
        self.selectModelCheckBox.grid(row=6, column=1, padx=10, pady=10)

        self.selectUICheckBox = ttk.Checkbutton(
            self.selectExtGroup, variable=self.selectUI, text="UI"
        )
        self.selectUICheckBox.grid(row=6, column=2, padx=10, pady=10)

        self.selectDepCheckBox = ttk.Checkbutton(
            self.selectExtGroup, variable=self.selectDep, text="Dependencies"
        )
        self.selectDepCheckBox.grid(row=6, column=3, padx=10, pady=10)

        self.selectMeasureUsageCheckBox = ttk.Checkbutton(
            self.selectExtGroup, variable=self.selectMeasureUsage, text="Measure Usage"
        )
        self.selectMeasureUsageCheckBox.grid(row=6, column=4, padx=10, pady=10)

        self.outputLabel = ttk.Label(
            self.selectExtGroup, text="Output Options besides DB:"
        )
        self.outputLabel.grid(row=7, column=1, padx=10, pady=10)

        self.selectXLSXCheckBox = ttk.Checkbutton(
            self.selectExtGroup, variable=self.selectXLSX, text="xlsx"
        )
        self.selectXLSXCheckBox.grid(row=7, column=2, padx=10, pady=10)

        self.selectCSVCheckBox = ttk.Checkbutton(
            self.selectExtGroup, variable=self.selectCSV, text="csv"
        )
        self.selectCSVCheckBox.grid(row=7, column=3, padx=10, pady=10)

        self.startExtractionButton = ttk.Button(
            self.fileFrame, text="Start Extraction", command=self.startExtractionClicked
        )
        self.startExtractionButton.grid(row=9)

        self.statusLabelForFileFrame = Label(
            self.fileFrame, textvariable=self.appStatus
        )
        self.statusLabelForFileFrame.grid(row=21)

        # Progressbar is always at the bottom, sometimes tkinter struggles with new widgets. Layout changes are
        # stressful to user.
        self.progressBarForFileFrame = ttk.Progressbar(
            self.fileFrame,
            mode="indeterminate",
            style="red.Horizontal.TProgressbar",
            length=300,
        )
        self.progressBarForFileFrame.grid(row=22)

    def finalizeUI(self):
        """
        Finalize the UI to be able to render on the root window.
        :return: null
        """
        for frameName, frameObj in self.framesList.items():
            self.padFrameWidgets(frameObj)
        for frameName, frameObj in self.framesList.items():
            self.note.add(frameObj, text=frameName)
        self.note.pack()
        ttk.Style().theme_use("alt")

    @staticmethod
    def padFrameWidgets(inputFrame):
        """
        Add padding to the widgets.
        :param inputFrame: input frame
        :return: null
        """
        for child in inputFrame.winfo_children():
            child.grid_configure(padx=10, pady=10)

    def chooseJSONClicked(self):
        """
        On click of Choose Tenant ZIP File button, To select the zip json file.
        :return: null
        """
        self.startExtractionButton.configure(state=DISABLED)
        filePath = filedialog.askopenfilename(filetypes=[("zip files", "*.zip")])
        self.zipFile.set(filePath)

        if not os.path.isfile(self.zipFile.get()):
            self.updateStatus("ZIP File not chosen", "yellow")
            return
        elif os.path.isdir(self.destinationDir.get()) and os.path.isfile(
            self.zipFile.get()
        ):
            self.tenantZIPFile.configure(bg=self.originalColor)
            self.updateStatus("Destination directory and Tenant ZIP File set")
            self.startExtractionButton.configure(state=NORMAL)
        else:
            self.updateStatus("Tenant ZIP file set")

    def chooseDirClicked(self):
        """
        On click of Choose Destination Directory button, to choose a destination directory.
        :return: null
        """
        self.startExtractionButton.configure(state=DISABLED)
        workDir = filedialog.askdirectory()
        self.destinationDir.set(workDir)
        if not os.path.isdir(self.destinationDir.get()):
            self.updateStatus("Destination directory not chosen", "yellow")
        elif os.path.isdir(self.destinationDir.get()) and os.path.isfile(
            self.zipFile.get()
        ):
            self.workingDirLabel.configure(bg=self.originalColor)
            self.updateStatus("Destination directory and Tenant ZIP File set")
            self.startExtractionButton.configure(state=NORMAL)
        else:
            self.updateStatus("Destination directory set")

    def startExtractionClicked(self):
        """
        On click of Start Extraction button, to start the extraction thread.
        :return: null
        """
        self.logger.info("Getting tenant details. Start Extraction")
        self.startExtractionButton.configure(state=DISABLED)
        self.updateStatus("Processing JSON Config file")
        self.getExtractionDataThread = threading.Thread(target=self.extractData)
        self.getExtractionDataThread.start()
        self.startProgressBars()
        self.fileFrame.after(100, self.startExtractionCompletionCheck)

    def startExtractionCompletionCheck(self):
        """
        Check if the getExtractionDataThread is active i.e if the extraction is completed.
        :return: null
        """
        if self.getExtractionDataThread.is_alive():
            self.fileFrame.after(100, self.startExtractionCompletionCheck)
        else:
            self.stopProgressBars()
            if self.extractionStatus == "Success":
                self.updateStatus("Extraction Completed Successfully", "lawn green")
                self.logger.info("Successfully fetched data from ZIP")
            else:
                self.updateStatus(
                    "Error processing the file. Please check logs.", "red"
                )
            self.startExtractionButton.configure(state=NORMAL)

    def extractData(self):
        """
        Read the json file from the zip file and extract data from the json file.
        :return: null
        """
        commonObj = CommonDataFunction()
        if self.guiOption["ui"]:
            commonObj.readJsonFile(self.zipFile.get(), self.destinationDir.get())
        else:
            commonObj.readJsonFile(
                self.guiOption["nonUIZippedJSON"], self.guiOption["nonUIDestDir"]
            )
        self.destDir = commonObj.destDir
        self.uiDestDir = commonObj.uiDestDir
        self.createDB(commonObj.destPath, commonObj.jsonData)

    def createDB(self, location, data):
        """
        Create the database, all the tables in the database and Extract the model, ui and dependencies data.
        :param location: destination location for the database
        :param data: json data as dict
        :return: null
        """
        self.tenantDataDBName = os.path.join(location, data["Tenant"]["Name"] + ".db")
        if not os.path.exists(os.path.dirname(self.tenantDataDBName)):
            try:
                os.makedirs(os.path.dirname(self.tenantDataDBName))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        try:
            # creating a connection
            tenantDataDBConnection = sqlite3.connect(self.tenantDataDBName)
            tenantDataDBConnection.isolation_level = (
                "DEFERRED"  # Set None for auto-commit.
            )
            tenantDataDBConnection.row_factory = sqlite3.Row  # faster access to data
            # tenantDataDBconnection.set_trace_callback(print)  # To debug all sqlite callback
            # tenantDataDBconnection = tenantDataDBconnection.cursor()
            self.logger.info(
                "Tenant Database created successfully at " + self.tenantDataDBName
            )
            tableList = tablesData.split(";")
            for table in tableList:
                table = table.strip()
                try:
                    if table.startswith("DROP") or table.startswith("CREATE TABLE"):
                        tenantDataDBConnection.execute(table)
                except Exception as e:
                    print("Cannot create table: " + str(e))
                    self.logger.error("Cannot create table: " + str(e))
            self.logger.info("All Tables added in the database.")

        except Exception as e:
            self.logger.error("Error creating Tenant Database " + str(e))
            print("Error creating Tenant Database " + str(e))
            return

        isSelectModel = True
        isSelectDep = True
        isSelectUI = True
        if self.guiOption["ui"]:
            isSelectModel = True if self.selectModel.get() == 1 else False
            isSelectDep = True if self.selectDep.get() == 1 else False
            isSelectUI = True if self.selectUI.get() == 1 else False

        # Parse and insert data in the table
        modelExtractor = ModelExtractor(
            tenantDataDBConnection, data, self.selectMeasureUsage.get()
        )
        if isSelectModel or isSelectDep:
            self.logger.info("Extracting Dimensions Data")
            print("Extracting Dimensions Data")
            modelExtractor.createDimTablesInDB()
            self.logger.info("Extracting Graph Data")
            print("Extracting Graph Data")
            modelExtractor.createGraphTablesInDB()
            self.logger.info("Extracting Plans Data")
            print("Extracting Plans Data")
            modelExtractor.createPlanTablesInDB()

        ruleExtractor = RuleExtractor(data, tenantDataDBConnection)
        if isSelectModel or isSelectDep:
            self.logger.info("Extracting Rules Data")
            print("Extracting Rules Data")
            ruleExtractor.extractRules()
            ruleExtractor.extractIBPLRules()

        uiExtractor = UIExtractor(data, tenantDataDBConnection)
        if isSelectUI or isSelectDep:
            self.logger.info("Extracting Widgets Data")
            print("Extracting Widgets Data")
            uiExtractor.createWidgetTablesInDB()
            self.logger.info("Extracting Web Widgets Data")
            print("Extracting Web Widgets Data")
            uiExtractor.createWebLayoutTablesInDB()
            self.logger.info("Extracting Excel Widgets Data")
            print("Extracting Excel Widgets Data")
            uiExtractor.createExcelLayoutTablesInDB()
            self.logger.info("Extracting Translation Data")
            print("Extracting Translation Data")
            uiExtractor.createTranslationTablesInDB()
            self.logger.info("Extracting Action button Data")
            print("Extracting Action button Data")
            uiExtractor.createActionButtonTableInDB()

        # Measure dependencies
        # try:
        if isSelectDep:
            self.logger.info("Extracting Dependencies table")
            print("Extracting Dependencies table")
            DependencyExtractor(tenantDataDBConnection)
            self.logger.info("Done with Dependencies table")
        # except Exception as e:
        #     self.logger.error('Error in Measure Dependencies Generation: ' + str(e))
        #     print('Error in Measure Dependencies Generation: ' + str(e))
        #     return

        dbToFiles = DBToFiles(tenantDataDBConnection, self.destDir, self.uiDestDir)
        if self.selectCSV.get():
            self.logger.info("Creating CSV Files.")
            print("Creating CSV Files.")
            # dbToFiles.generateCSVArrays()
            dbToFiles.createDimCSVArrays()
            dbToFiles.createGraphCSVArrays()
            dbToFiles.createPlanCSVArrays()
            dbToFiles.createActionButtonCSVArrays()
            dbToFiles.createRuleFilesArray()
            dbToFiles.createPluginsCSVArrays()
            dbToFiles.createProceduresFilesArray()
            dbToFiles.createUIFilesArray()
            dbToFiles.createTranslationFileArray()
            dbToFiles.createExcelFilesArray()
            dbToFiles.createDependenciesCSVArray()
            dbToFiles.createDSRulesFile()
        if self.selectXLSX.get():
            dbToFiles.createExcelFromDB()
        self.logger.info("Completed Extraction")
        print("Completed Extraction")
        try:
            tenantDataDBConnection.commit()
            tenantDataDBConnection.close()
        except Exception as e:
            self.logger.error("Unable to Close DB connection " + str(e))
            print("Unable to Close DB connection " + str(e))

        self.extractionStatus = "Success"

    def updateStatus(self, updateText, color="sky blue"):
        """
        Update the UI status.
        :param updateText: Text to be displayed
        :param color: color of the text to indicate the state
        :return: null
        """
        self.appStatus.set(updateText)
        self.statusLabelForFileFrame.configure(bg=color)

    def startProgressBars(self):
        """
        Starts all progress bars.
        """
        self.progressBarForFileFrame.grid()
        self.progressBarForFileFrame.start()

    def stopProgressBars(self):
        """
        Stop all progress bars.
        """
        self.progressBarForFileFrame.stop()
        self.progressBarForFileFrame.grid_remove()
