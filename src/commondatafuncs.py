import logging
import json
import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from zipfile import ZipFile
from requests import get
# Tkinter is removed


class CommonDataFunction:
    def __init__(self, logger_obj=None):
        """
        CommonDataFunction constructor.
        :param logger_obj: An optional pre-configured logger instance.
        """
        # Common variables
        self.sourceDir = None
        self.destDir = None
        self.uiDestDir = None
        self.jsonData = {}
        self.tenantAttributeIdToDimName = {}
        self.tenantAttributeIdToAttrName = {}
        self.chromeDriver = None
        self.logger = logger_obj
        self.destPath = None

        if self.logger is None:
            self._initialize_logger() # Initialize a default logger if none provided

    def _initialize_logger(self, log_file_name="extractor.log"):
        """
        Initializes a default logger if no logger is provided to __init__.
        """
        self.logger = logging.getLogger("extractor-logger-default")
        if not self.logger.handlers: # Avoid adding handlers multiple times
            self.logger.setLevel(logging.DEBUG)
            log_path = os.path.join(os.getcwd(), log_file_name)
            # Try to open in append mode, fallback to write if issues, though 'w' was original
            try:
                # Ensure log directory exists if log_file_name includes a path
                log_dir = os.path.dirname(log_path)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir, exist_ok=True)
                
                # Open file and overwrite it to empty it (original behavior)
                # For a shared logger, appending 'a' might be better, but sticking to original for now.
                # open(log_path, "w").close() # This was original, but risky if logger is shared.
                                            # Let FileHandler manage file creation/truncation if mode='w'

                fh = logging.FileHandler(log_path, mode='a') # Changed to append 'a'
                fh.setLevel(logging.DEBUG)
                formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
                fh.setFormatter(formatter)
                self.logger.addHandler(fh)
            except Exception as e:
                # Fallback to console logging if file logging fails
                print(f"Failed to initialize file logger at {log_path}: {e}. Using console logger.")
                ch = logging.StreamHandler()
                ch.setLevel(logging.DEBUG)
                formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
                ch.setFormatter(formatter)
                self.logger.addHandler(ch)
        self.logger.info("Logger Initialized (or using provided/existing logger)")
        # Return filename for compatibility if needed, though setLoggingFile was the one returning it
        return log_file_name 

    # setLoggingFile can be kept for backward compatibility or specific use cases
    # but streamlit_app.py now passes the logger to __init__
    def setLoggingFile(self, log_file_name_param="extractor.log"):
        """
        Set Logging files. Ensures logger is set up.
        :return: filename of the log file generated.
        """
        if self.logger and any(isinstance(h, logging.FileHandler) for h in self.logger.handlers):
            # If logger is already set and has a file handler, assume it's configured
            self.logger.info("Logger already configured with a file handler.")
            # Find existing file handler's path if possible
            for h in self.logger.handlers:
                if isinstance(h, logging.FileHandler):
                    return h.baseFilename
            return log_file_name_param # Fallback

        # If no logger or no file handler, initialize it
        self.logger = logging.getLogger("extractor-logger-custom")
        self.logger.setLevel(logging.DEBUG)
        
        # Clear existing handlers to avoid duplication if getLogger got a pre-existing one
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        log_path_to_use = os.path.join(os.getcwd(), log_file_name_param)
        print("Setting log file to " + log_path_to_use)
        
        try:
            log_dir = os.path.dirname(log_path_to_use)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            # Open file and overwrite it to empty it (original behavior)
            # open(log_path_to_use, "w").close() # Risky if logger is shared.
            fh = logging.FileHandler(log_path_to_use, mode='a') # Changed to 'a'
            fh.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)
        except Exception as e:
            print(f"Failed to initialize file logger in setLoggingFile at {log_path_to_use}: {e}. Using console logger.")
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)
            
        self.logger.info("Logger Initialized via setLoggingFile")
        return log_path_to_use

    @staticmethod
    def getAppVerWithReq(self, _version):
        # This method should ideally use self.logger if available
        logger_to_use = self.logger if self.logger else logging.getLogger("common-data-version-check")
        url = "https://refappusage.firebaseapp.com/tenant-extractor.html"
        try:
            res = get(url, timeout=10) # Added timeout
            # logger_to_use.debug(res.text) # Be careful logging full response text
            if res.status_code == 200:
                text = res.text
                startIndex = text.find("Tenant Extractor")
                endIndex = text.find("</p>")
                if startIndex != -1 and endIndex != -1:
                    heading = text[startIndex:endIndex].strip()
                    if heading != f"Tenant Extractor {_version}":
                        logger_to_use.warning(
                            f"App Version: A newer version {heading} is available for download."
                        )
                    else:
                        logger_to_use.info(f"App Version: You are using the latest version: {_version}")
                else:
                    logger_to_use.warning("Could not parse version information from response.")
            else:
                # logger_to_use.warning(f"App Version check failed. Status: {res.status_code}, Response: {res.text}")
                logger_to_use.warning(f"App Version check failed. Status: {res.status_code}")
        except Exception as e:
            logger_to_use.error(f"App Version: Unable to connect to internet and check app version. Error: {e}", exc_info=True)


    def getAppVer(self, version):
        """
        Check App version using Selenium. (Consider removing or making optional due to dependency)
        :param version: current version
        :return: null
        """
        logger_to_use = self.logger if self.logger else logging.getLogger("common-data-selenium-version-check")
        # This method has heavy dependencies (Selenium, ChromeDriver) and might not be suitable for all environments.
        # For now, just replacing UI calls with logging.
        logger_to_use.info("Attempting to check app version via Selenium (this may be slow or fail in some environments).")
        try:
            # Attempt to find ChromeDriver
            if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'): # PyInstaller bundle
                chrome_driver_path = os.path.join(sys._MEIPASS, "chromedriver.exe")
            else: # Running as script
                chrome_driver_path = os.path.join(os.path.dirname(__file__), "chromedriver.exe")

            if not os.path.exists(chrome_driver_path):
                logger_to_use.warning(f"ChromeDriver not found at {chrome_driver_path}. Skipping Selenium version check.")
                # Also try to call the requests based one as a fallback
                self.getAppVerWithReq(version)
                return

            # Selenium specific imports - keep them local to method if it's optional
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options

            chr_options = Options()
            chr_options.headless = True
            chr_options.add_argument("--disable-gpu")
            chr_options.add_argument("--ignore-certificate-errors")
            chr_options.add_argument("--ignore-ssl-errors")
            # chr_options.add_argument("--no-sandbox") # Sometimes needed in restricted environments
            # chr_options.add_argument("--disable-dev-shm-usage") # Sometimes needed in restricted environments
            
            logger_to_use.debug("Initializing ChromeDriver...")
            # Ensure service_log_path is writable or None
            service_log_path = os.path.join(os.getcwd(), "ghostdriver.log") if os.access(os.getcwd(), os.W_OK) else None

            self.chromeDriver = webdriver.Chrome(executable_path=chrome_driver_path, options=chr_options, service_log_path=service_log_path)
            logger_to_use.debug("ChromeDriver initialized. Fetching URL...")
            self.chromeDriver.get("https://refappusage.firebaseapp.com/tenant-extractor.html")
            
            heading_element = self.chromeDriver.find_element_by_xpath("/html/body/p")
            heading = heading_element.text if heading_element else ""

            if heading and heading != f"Tenant Extractor {version}":
                logger_to_use.warning(
                    f"App Version (Selenium): A newer version {heading} is available for download."
                )
            elif heading:
                 logger_to_use.info(f"App Version (Selenium): You are using the latest version: {version}")
            else:
                logger_to_use.warning("App Version (Selenium): Could not retrieve version heading.")

        except Exception as e:
            logger_to_use.error(f"App Version (Selenium): Error checking app version: {e}", exc_info=True)
            # Fallback to requests based check if selenium failed
            logger_to_use.info("Falling back to requests-based version check due to Selenium error.")
            self.getAppVerWithReq(version)
        finally:
            if self.chromeDriver:
                self.closeBrowser()


    def closeBrowser(self):
        """
        Close the chrome driver if it's running.
        :return: null
        """
        if self.chromeDriver:
            try:
                self.chromeDriver.quit()
                if self.logger: self.logger.info("ChromeDriver closed.")
            except Exception as e:
                if self.logger: self.logger.error(f"Error closing ChromeDriver: {e}", exc_info=True)
            finally:
                self.chromeDriver = None


    def readJsonFile(self, sourceZippedJsonFile, destDirName, is_streamlit_mode=False):
        """
        Read the JSON file and extract the JSON data to a dict.
        :param sourceZippedJsonFile: Path to the source ZIP file.
        :param destDirName: Base destination directory. For Streamlit, this is the user-selected output folder.
        :param is_streamlit_mode: Boolean, if True, adjusts path constructions for Streamlit.
        :return: null
        """
        logger_to_use = self.logger if self.logger else logging.getLogger("common-data-readjson")

        if not sourceZippedJsonFile.lower().endswith(".zip"):
            logger_to_use.error(f"File provided is not a ZIP file: {sourceZippedJsonFile}")
            raise ValueError("Please provide a zipped JSON file.")

        try:
            inputZip = ZipFile(sourceZippedJsonFile)
        except FileNotFoundError:
            logger_to_use.error(f"ZIP file not found: {sourceZippedJsonFile}")
            raise
        except Exception as e:
            logger_to_use.error(f"Error opening ZIP file {sourceZippedJsonFile}: {e}", exc_info=True)
            raise

        jsonFilesList = [name for name in inputZip.namelist() if name.lower().endswith(".json")]

        if not jsonFilesList:
            logger_to_use.error(f"No JSON file found in ZIP: {sourceZippedJsonFile}")
            raise ValueError("No JSON file in zip provided.")
        
        jsonFileName = ""
        if len(jsonFilesList) == 1:
            jsonFileName = jsonFilesList[0]
        elif "_legacy.json" in jsonFilesList: # Check if this specific name exists in the list
            jsonFileName = "_legacy.json"
        elif any(f.endswith("_legacy.json") for f in jsonFilesList): # More robust check
             jsonFileName = next(f for f in jsonFilesList if f.endswith("_legacy.json"))
        else:
            # Fallback or error if specific logic for multiple JSONs isn't clearer
            # For now, let's pick the first one if _legacy.json is not found, and log a warning.
            jsonFileName = jsonFilesList[0]
            logger_to_use.warning(f"Multiple JSON files found and no '_legacy.json'. Using first available: {jsonFileName}. This might not be the intended file.")
            # Original code had an Exception here, restoring that if it's critical path.
            # raise Exception("Unable to find the correct json file. Contact sp.nexus@o9solutions.")


        logger_to_use.info(f"Reading json file {jsonFileName} from {sourceZippedJsonFile}")
        try:
            with inputZip.open(jsonFileName) as dataFile:
                self.jsonData = json.load(dataFile)
        except Exception as e:
            logger_to_use.error(f"Error reading or parsing JSON data from {jsonFileName} in {sourceZippedJsonFile}: {e}", exc_info=True)
            raise

        tenant_name = self.jsonData.get("Tenant", {}).get("Name", "UnknownTenant")

        if is_streamlit_mode:
            # destDirName is the absolute base output directory selected by the user.
            # For CSV/XLSX outputs, they go into destDirName directly or a subfolder like "files".
            self.destDir = os.path.abspath(destDirName) # General output like CSV/XLSX
            # For the DB, it goes into a tenant-specific sub-directory.
            self.destPath = os.path.abspath(os.path.join(destDirName, f"{tenant_name}_extracted_db"))
            # For UI map files, they might also go into a specific sub-directory.
            self.uiDestDir = os.path.abspath(os.path.join(destDirName, f"{tenant_name}_UIElements"))
        else:
            # Original behavior:
            self.destDir = os.path.join(destDirName, f"{tenant_name}_Models")
            self.uiDestDir = os.path.join(destDirName, f"{tenant_name}_UIElements")
            self.destPath = destDirName # destPath was the same as destDirName in original context

        # Ensure directories exist (optional, can be created when first writing a file)
        # For robustness, let's ensure the base output directories are created if they don't exist.
        # Individual file writing operations should also handle their specific subdirectories.
        if self.destDir and not os.path.exists(self.destDir):
            os.makedirs(self.destDir, exist_ok=True)
            logger_to_use.info(f"Created general output directory: {self.destDir}")
        if self.destPath and not os.path.exists(self.destPath): # For DB
            os.makedirs(self.destPath, exist_ok=True)
            logger_to_use.info(f"Created database output directory: {self.destPath}")
        if self.uiDestDir and not os.path.exists(self.uiDestDir):
            os.makedirs(self.uiDestDir, exist_ok=True)
            logger_to_use.info(f"Created UI elements output directory: {self.uiDestDir}")

        logger_to_use.info(f"Data source: {sourceZippedJsonFile}")
        logger_to_use.info(f"JSON data for tenant '{tenant_name}' loaded.")
        logger_to_use.info(f"Output base directory (general files e.g. CSV/XLSX): {self.destDir}")
        logger_to_use.info(f"Output directory for DB: {self.destPath}")
        logger_to_use.info(f"Output directory for UI map files: {self.uiDestDir}")
