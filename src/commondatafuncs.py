import logging
import json
import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from tkinter import Tk, messagebox
from zipfile import ZipFile
from requests import get


class CommonDataFunction:
    def __init__(self):
        """
        CommonDataFunction constructor.
        """
        # Common variables
        self.sourceDir = None
        self.destDir = None
        self.uiDestDir = None
        self.jsonData = {}
        self.tenantAttributeIdToDimName = {}
        self.tenantAttributeIdToAttrName = {}
        self.chromeDriver = None
        self.logger = None
        self.destPath = None

    def setLoggingFile(self):
        """
        Set Logging files.
        :return: filename of the log file generated.
        """
        self.logger = logging.getLogger("extractor-logger")
        self.logger.setLevel(logging.DEBUG)
        fileNameForLogging = os.path.join(os.getcwd(), "extractor.log")
        print("setting log file to " + fileNameForLogging)
        # Open file and overwrite it to empty it
        open(fileNameForLogging, "w").close()
        fh = logging.FileHandler(fileNameForLogging)
        fh.setLevel(logging.DEBUG)
        # create formatter and add it to the handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        fh.setFormatter(formatter)
        # Remove existing handlers if any
        for fHandler in self.logger.handlers:
            print("Removing existing file log handler")
            self.logger.removeHandler(fHandler)
        # add the handlers to the logger
        self.logger.addHandler(fh)
        self.logger.info("Logger Initialized")
        return fileNameForLogging

    @staticmethod
    def getAppVerWithReq(_version):
        url = "https://refappusage.firebaseapp.com/tenant-extractor.html"
        res = get(url)
        # print(res.text)
        if res.status_code == 200:
            text = res.text
            startIndex = text.find("Tenant Extractor")
            endIndex = text.find("</p>")
            heading = text[startIndex:endIndex].strip()
            if heading != f"Tenant Extractor {_version}":
                root = Tk()
                root.withdraw()
                messagebox.showwarning(
                    "App Version",
                    f"A newer version {heading} is available for download.",
                )
        else:
            print(res.text)
            root = Tk()
            root.withdraw()
            messagebox.showinfo(
                "App Version", f"Unable to connect to internet and check app version."
            )

    def getAppVer(self, version):
        """
        Check App version.
        :param version: current version
        :return: null
        """
        try:
            chromeDriverPath = os.path.join(sys._MEIPASS, r".\chromedriver.exe")
        except:
            chromeDriverPath = os.path.join(
                os.path.dirname(__file__), r".\chromedriver.exe"
            )
        try:
            chrOptions = Options()
            chrOptions.headless = True
            chrOptions.add_argument("--disable-gpu")
            chrOptions.add_argument("--ignore-certificate-errors")
            chrOptions.add_argument("--ignore-ssl-errors")
            print("\nPlease ignore the chromium messages in next 2 lines\n")
            self.chromeDriver = webdriver.Chrome(chromeDriverPath, options=chrOptions)
            self.chromeDriver.get(
                "https://refappusage.firebaseapp.com/tenant-extractor.html"
            )

            heading = self.chromeDriver.find_element_by_xpath("/html/body/p").text
            if heading != f"Tenant Extractor {version}":
                root = Tk()
                root.withdraw()
                messagebox.showwarning(
                    "App Version",
                    f"A newer version {heading} is available for download",
                )
        except Exception as e:
            print(f"Error: {str(e)}")
            root = Tk()
            root.withdraw()
            messagebox.showinfo(
                "App Version", f"Unable to connect to internet and check app version."
            )

    def closeBrowser(self):
        """
        Close the chrome driver.
        :return: null
        """
        self.chromeDriver.quit()

    def readJsonFile(self, sourceZippedJsonFile, destDirName):
        """
        Read the JSON file and extract the JSON data to a dict.
        :param sourceZippedJsonFile: JSON file location
        :param destDirName: Destination directory where the data is extractor.
        :return: null
        """
        if not sourceZippedJsonFile.lower().endswith(".zip"):
            print(
                "Please provide zipped JSON file. You provided " + sourceZippedJsonFile
            )
            raise ValueError("Please provide zipped JSON file")
        inputZip = ZipFile(sourceZippedJsonFile)
        jsonFilesList = list(
            filter(lambda x: x.lower().endswith(".json"), inputZip.namelist())
        )
        if len(jsonFilesList) == 0:
            print("No JSON file in zip provided -" + sourceZippedJsonFile)
            raise ValueError("No json file in zip provided")
        elif len(jsonFilesList) == 1:
            jsonFileName = jsonFilesList[0]
        elif "_legacy.json" in jsonFilesList:
            jsonFileName = "_legacy.json"
        else:
            raise Exception(
                "Unable to find the correct json file. Contact sp.nexus@o9solutions."
            )

        print("Reading json file " + jsonFileName)
        with inputZip.open(jsonFileName) as dataFile:
            self.jsonData = json.load(dataFile)
            self.destDir = os.path.join(
                destDirName, self.jsonData["Tenant"]["Name"] + "_Models"
            )
            self.uiDestDir = os.path.join(
                destDirName, self.jsonData["Tenant"]["Name"] + "_UIElements"
            )
        print("Destination directory for Models files " + self.destDir)
        print("Destination directory for UI files " + self.uiDestDir)
        self.destPath = destDirName
