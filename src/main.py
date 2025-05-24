"""
    Tenant Extractor

"""

import logging
import os
import sys
from tkinter import Tk

from commondatafuncs import CommonDataFunction
from extractor_gui import ExtractorGUI

AppVersion = "v25.1"
rootWindow = Tk()


def mainFunction(argv):
    """
    Main Function.
    :param argv: arguments
    :return: null
    """
    commonObj = CommonDataFunction()
    # Setting logger for all Classes
    try:
        logFileName = commonObj.setLoggingFile()
        logger = logging.getLogger("extractor-logger")
        logger.info(f"Model Extractor {AppVersion}")

    except Exception as e:
        print(e)
        print("Not able to set application logging. Exiting")
        sys.exit()

    # Check for the app version - Removed due to frequent update of chrome browser.
    try:
        commonObj.getAppVerWithReq(AppVersion)
        # commonObj.closeBrowser()
        print("\n")
    except Exception as e:
        print(f"Error in App Version: {str(e)}")
        logger.error("Exception details: " + str(e))

    # Instantiate GUI
    try:
        try:
            iconPath = os.path.join(sys._MEIPASS, "convert.ico")
        except:
            iconPath = os.path.join(os.path.dirname(__file__), r".\convert.ico")
        rootWindow.iconbitmap(iconPath)
        guiOption = {"ui": True, "nonUIZippedJSON": "", "nonUIDestDir": ""}
        try:
            if (
                argv[1] == "--SOURCE"
                and argv[3] == "--DEST"
                and len(argv) >= 4
                and argv[2] != ""
                and argv != ""
            ):
                guiOption = {
                    "ui": False,
                    "nonUIZippedJSON": argv[2],
                    "nonUIDestDir": argv[4],
                }
        except:
            pass
        ExtractorGUI(rootWindow, AppVersion, logFileName, guiOption)
        # ExtractorGUI.chooseDirClicked()

    except Exception as e:
        print(e)
        print("Not able start the GUI. Exiting")
        logger.error("Not able start the GUI. Exiting: ")
        logger.error(str(e))
        sys.exit()


def windowClose():
    """
    To close the GUI created.
    :return: Null
    """
    print("Closing Window")
    rootWindow.destroy()
    sys.exit()


if __name__ == "__main__":
    mainFunction(sys.argv)
    rootWindow.protocol("WM_DELETE_WINDOW", windowClose)
    # root.title('Weather App')
    # root.geometry('890x470+300+300')
    # root.configure(bg='#070110')
    # root.resizable(False, False)
    rootWindow.mainloop()
