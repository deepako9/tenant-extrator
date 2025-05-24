import streamlit as st
import os
import logging
import sys
import tempfile
import sqlite3
import errno
import shutil # Added for shutil.rmtree

# Assuming these are in src/ or PYTHONPATH is set up
from commondatafuncs import CommonDataFunction
# from extractor_gui import ExtractorGUI # We are replacing its UI parts
from dbtofile import DBToFiles
from modelextractor import ModelExtractor
from ruleextractor import RuleExtractor
from uiextractor import UIExtractor
from dependency_extractor import DependencyExtractor
from tables import tablesData

AppVersion = "v25.1_st"
LOG_FILE_NAME = "extractor_streamlit.log"

# --- Utility: Setup Logger (can be called from run_extraction) ---
def setup_logger():
    logger = logging.getLogger("extractor-logger-st")
    logger.setLevel(logging.INFO)
    # Prevent multiple handlers if function is called multiple times
    if not logger.handlers:
        # File handler
        # Ensure the directory for the log file exists
        log_dir = os.path.dirname(LOG_FILE_NAME)
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir)
            except OSError as e:
                # Handle potential race condition if directory is created by another process
                if e.errno != errno.EEXIST:
                    # Log to console if file logging can't be set up
                    print(f"Error creating log directory {log_dir}: {e}")
                    # Fallback to a basic console logger if directory creation fails
                    ch = logging.StreamHandler()
                    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
                    logger.addHandler(ch)
                    return logger

        # Attempt to create file handler
        try:
            fh = logging.FileHandler(LOG_FILE_NAME, mode='a') # Append mode
            fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            logger.addHandler(fh)
        except Exception as e:
            print(f"Error setting up file handler for logging {LOG_FILE_NAME}: {e}")
            # Fallback to a basic console logger if file handler fails
            if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
                ch = logging.StreamHandler()
                ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
                logger.addHandler(ch)
                
    return logger

def create_db_and_extract(logger, tenant_db_name, common_data, selected_options):
    logger.info(f"Attempting to create DB at {tenant_db_name}")
    
    db_dir = os.path.dirname(tenant_db_name)
    if db_dir and not os.path.exists(db_dir): # Ensure db_dir is not empty string
        try:
            os.makedirs(db_dir)
            logger.info(f"Created directory for DB: {db_dir}")
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                logger.error(f"Error creating directory for DB {db_dir}: {exc}")
                return False, f"Error creating DB directory: {exc}", {}
            logger.info(f"Directory for DB {db_dir} already exists.")

    conn = None
    output_files = {}
    try:
        conn = sqlite3.connect(tenant_db_name)
        conn.isolation_level = "DEFERRED"
        conn.row_factory = sqlite3.Row
        logger.info(f"Tenant Database connection established at {tenant_db_name}")
        output_files["db"] = tenant_db_name

        table_list = tablesData.split(";")
        for table_sql in table_list:
            table_sql = table_sql.strip()
            if table_sql.startswith("DROP") or table_sql.startswith("CREATE TABLE"):
                try:
                    conn.execute(table_sql)
                except Exception as e:
                    logger.error(f"Cannot create table with SQL: {table_sql[:50]}... Error: {e}")
        logger.info("All base tables processed in the database.")

    except Exception as e:
        logger.error(f"Error creating Tenant Database: {e}", exc_info=True)
        if conn:
            conn.close()
        return False, f"Error creating Tenant Database: {e}", output_files

    try:
        json_data = common_data['jsonData']
        dest_dir = common_data['destDir']
        ui_dest_dir = common_data['uiDestDir'] # May not be used directly by extractors but by DBToFiles

        # Model Extractor
        if selected_options.get('model') or selected_options.get('dep'):
            logger.info("Extracting Model data...")
            model_extractor = ModelExtractor(conn, json_data, selected_options.get('measure_usage', True))
            model_extractor.createDimTablesInDB()
            model_extractor.createGraphTablesInDB()
            model_extractor.createPlanTablesInDB()
            logger.info("Model data extraction complete.")

        # Rule Extractor
        if selected_options.get('model') or selected_options.get('dep'):
            logger.info("Extracting Rules data...")
            rule_extractor = RuleExtractor(json_data, conn)
            rule_extractor.extractRules()
            rule_extractor.extractIBPLRules()
            logger.info("Rules data extraction complete.")

        # UI Extractor
        if selected_options.get('ui') or selected_options.get('dep'):
            logger.info("Extracting UI data...")
            ui_extractor = UIExtractor(json_data, conn)
            ui_extractor.createWidgetTablesInDB()
            ui_extractor.createWebLayoutTablesInDB()
            ui_extractor.createExcelLayoutTablesInDB()
            ui_extractor.createTranslationTablesInDB()
            ui_extractor.createActionButtonTableInDB()
            logger.info("UI data extraction complete.")

        # Dependency Extractor
        if selected_options.get('dep'):
            logger.info("Extracting Dependencies...")
            # Assuming DependencyExtractor is initialized and then its method is called.
            # Or if it does all work in __init__ based on the conn.
            # Based on ExtractorGUI: depExtObj = DependencyExtractor(self.conn)
            dep_extractor = DependencyExtractor(conn) # If it does work in init
            # If it has a specific method: dep_extractor.extract_dependencies()
            logger.info("Dependency extraction complete.")
            
        db_to_files = DBToFiles(conn, dest_dir, ui_dest_dir) # ui_dest_dir might be used for specific UI related file paths
        
        if selected_options.get('csv'):
            logger.info("Creating CSV files...")
            # Ensure dest_dir exists for CSV outputs
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)
                logger.info(f"Created destination directory for CSVs: {dest_dir}")

            db_to_files.createDimCSVArrays()
            db_to_files.createGraphCSVArrays()
            db_to_files.createPlanCSVArrays()
            db_to_files.createActionButtonCSVArrays()
            db_to_files.createRuleFilesArray()
            db_to_files.createPluginsCSVArrays()
            db_to_files.createProceduresFilesArray()
            db_to_files.createUIFilesArray()
            db_to_files.createTranslationFileArray()
            db_to_files.createExcelFilesArray()
            db_to_files.createDependenciesCSVArray()
            db_to_files.createDSRulesFile()
            # All CSVs are now in the "csv_exports" subdirectory of dest_dir
            csv_bundle_path = os.path.join(dest_dir, "csv_exports")
            logger.info(f"CSV file creation complete in directory: {csv_bundle_path}")
            output_files["csv_export_path"] = csv_bundle_path # This path will be zipped

        if selected_options.get('xlsx'):
            logger.info("Creating XLSX file...")
            # Ensure dest_dir exists for XLSX output
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)
                logger.info(f"Created destination directory for XLSX: {dest_dir}")
            
            # Assuming createExcelFromDB saves file and returns path or True/False
            # And it uses dest_dir internally to construct the path.
            # Let's assume default filename from DBToFiles is "AllSheets.xlsx" or similar in dest_dir
            # If createExcelFromDB returns the full path:
            xlsx_file_path = db_to_files.createExcelFromDB() 
            if xlsx_file_path and os.path.exists(xlsx_file_path):
                 output_files["xlsx"] = xlsx_file_path
                 logger.info(f"XLSX file created: {xlsx_file_path}")
            else:
                # Attempt to find it if True was returned or path was not absolute
                # This part is a bit of a guess based on original code's behavior
                potential_xlsx_name = "AllSheets.xlsx" # A common name often used
                guessed_path = os.path.join(dest_dir, common_data['jsonData']["Tenant"]["Name"] + "_" + potential_xlsx_name)
                if os.path.exists(guessed_path):
                    output_files["xlsx"] = guessed_path
                    logger.info(f"XLSX file found at default location: {guessed_path}")
                else: # Fallback if the above path is not found
                    logger.warning(f"XLSX file path not explicitly returned or found at default. Check DBToFiles output logic. Expected in {dest_dir}")
                    # Try a generic name if specific one not found
                    generic_xlsx_path = os.path.join(dest_dir, "output.xlsx") # Fallback
                    if os.path.exists(generic_xlsx_path):
                         output_files["xlsx"] = generic_xlsx_path
                         logger.info(f"XLSX file found at generic location: {generic_xlsx_path}")
                    else:
                         logger.error(f"Could not determine XLSX output path. DBToFiles.createExcelFromDB returned: {xlsx_file_path}")


        conn.commit()
        logger.info("Database changes committed.")
        return True, "Extraction completed successfully.", output_files

    except Exception as e:
        logger.error(f"Error during data extraction: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False, f"Error during data extraction: {e}", output_files
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")


def run_extraction(logger, uploaded_zip_file_obj, destination_dir_str, selected_options):
    logger.info(f"Starting extraction for: {uploaded_zip_file_obj.name} to {destination_dir_str}")
    
    # Pass the already configured logger to CommonDataFunction
    # CommonDataFunction's setLoggingFile is not called here, assuming logger is sufficient
    commonObj = CommonDataFunction(logger_obj=logger) 
    
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp()
        zip_file_path = os.path.join(temp_dir, uploaded_zip_file_obj.name)
        with open(zip_file_path, "wb") as f:
            f.write(uploaded_zip_file_obj.getbuffer())
        logger.info(f"Uploaded ZIP file saved to temporary path: {zip_file_path}")

        # readJsonFile will set commonObj.destDir, commonObj.uiDestDir, commonObj.jsonData, commonObj.destPath
        # destination_dir_str is the main output directory selected by the user.
        # commonObj.destPath will be a subdirectory within destination_dir_str for the DB.
        # commonObj.destDir will be the same as destination_dir_str for other files like CSV/XLSX.
        commonObj.readJsonFile(zip_file_path, destination_dir_str, is_streamlit_mode=True)
        logger.info(f"JSON file read. Main Dest Dir: {commonObj.destDir}, DB Path (sub-dir): {commonObj.destPath}, UI Files Dest: {commonObj.uiDestDir}")

        if not commonObj.jsonData:
            logger.error("Failed to read or parse JSON data from ZIP.")
            return False, "Failed to read or parse JSON data from ZIP.", {}
        
        # Data for create_db_and_extract
        common_data_for_db = {
            "destPath": commonObj.destPath,  # Specific path for the DB (e.g., .../output_extraction/TenantName_extracted)
            "jsonData": commonObj.jsonData,
            "destDir": commonObj.destDir,    # Base for general outputs like CSV/XLSX (e.g., .../output_extraction)
            "uiDestDir": commonObj.uiDestDir # Path for UI specific files if any (e.g., .../output_extraction/ui_map_files)
        }
        
        db_name_suggestion = os.path.join(commonObj.destPath, commonObj.jsonData["Tenant"]["Name"] + ".db")
        
        success, message, output_files = create_db_and_extract(logger, db_name_suggestion, common_data_for_db, selected_options)
        return success, message, output_files

    except Exception as e:
        logger.error(f"Error in run_extraction: {e}", exc_info=True)
        # Ensure output_files is initialized for return
        return False, f"Error during extraction setup: {e}", {} 
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e_clean:
                logger.error(f"Error cleaning up temp directory {temp_dir}: {e_clean}")


def main():
    st.set_page_config(layout="wide")
    
    # Initialize logger here and pass to functions, or ensure it's globally available if preferred
    # For Streamlit, setting it up once and storing in session_state or passing is fine.
    # The setup_logger function ensures it's initialized once.
    logger = setup_logger() 
    
    st.title(f"Tenant Extractor (Streamlit) {AppVersion}")

    if 'log_file_name' not in st.session_state:
        st.session_state.log_file_name = LOG_FILE_NAME
    # Update log file name in session state in case setup_logger changes it (e.g. due to permissions)
    # This assumes LOG_FILE_NAME is the intended one. If setup_logger modifies it, that needs to be captured.
    # For simplicity, we assume LOG_FILE_NAME is fixed.
    
    # --- Session State Initialization ---
    if 'tenant_name_for_zip' not in st.session_state: # To help name the zip file
        st.session_state.tenant_name_for_zip = "tenant"
    if 'extraction_complete' not in st.session_state:
        st.session_state.extraction_complete = False
    if 'extraction_status_message' not in st.session_state:
        st.session_state.extraction_status_message = ""
    if 'extraction_success' not in st.session_state:
        st.session_state.extraction_success = False
    if 'output_files' not in st.session_state:
        st.session_state.output_files = {}
    if 'destination_dir_val' not in st.session_state: # Persist destination directory input
         st.session_state.destination_dir_val = os.path.abspath("./output_extraction")


    # --- UI Layout ---
    col1, col2 = st.columns([0.4, 0.6]) # Adjust column width ratios

    with col1:
        st.header("Inputs")
        uploaded_zip_file = st.file_uploader("Choose Tenant ZIP File", type="zip", key="zip_uploader")
        
        destination_dir = st.text_input(
            "Destination Directory", 
            value=st.session_state.destination_dir_val, 
            key="dest_dir_input",
            help="Absolute path where outputs will be saved. E.g., C:/Users/YourUser/Desktop/ExtractionOutput"
        )
        # Update session state if user changes the text input
        st.session_state.destination_dir_val = destination_dir


        st.subheader("Extraction Options")
        select_model = st.checkbox("Model", value=True, key="cb_model")
        select_ui = st.checkbox("UI", value=True, key="cb_ui")
        select_dep = st.checkbox("Dependencies", value=True, key="cb_dep")
        select_measure_usage = st.checkbox("Measure Usage (for Model)", value=True, key="cb_measure")

        st.subheader("Output Options (besides DB)")
        select_xlsx = st.checkbox("XLSX", value=True, key="cb_xlsx") # Default True as per common use
        select_csv = st.checkbox("CSV", value=False, key="cb_csv")

        if st.button("Start Extraction", key="btn_start", type="primary"):
            # Reset status from previous runs
            st.session_state.extraction_complete = False
            st.session_state.extraction_status_message = ""
            st.session_state.output_files = {}
            st.session_state.extraction_success = False
            
            if not uploaded_zip_file:
                st.error("Please upload a Tenant ZIP file.")
            elif not destination_dir.strip():
                st.error("Please enter a destination directory.")
            elif not os.path.isabs(destination_dir.strip()):
                st.error("Please enter an absolute path for the destination directory.")
            else:
                selected_options = {
                    'model': select_model,
                    'ui': select_ui,
                    'dep': select_dep,
                    'measure_usage': select_measure_usage,
                    'xlsx': select_xlsx,
                    'csv': select_csv
                }
                # Ensure log file is clear for this session or properly appended.
                # setup_logger is called once, appends by default.
                # Could add a specific log message for new run start.
                logger.info("="*60)
                logger.info(f"New extraction process started via Streamlit UI for {uploaded_zip_file.name}.")
                logger.info(f"Destination: {destination_dir}, Options: {selected_options}")
                
                # Store tenant name for zip file naming, if possible (might need adjustment if commonObj not available here)
                # This is tricky as commonObj is inside run_extraction. For now, use a generic name or pass tenant name out.
                # For simplicity, tenant_name will be fetched from output_files if available or default.
                # commonObj is not directly available here. run_extraction creates it.
                # We can get tenant name from the db_file path later.

                with st.spinner(f"Processing... Logs are being written to {st.session_state.log_file_name}"):
                    success, message, output_files_dict = run_extraction(
                        logger,
                        uploaded_zip_file,
                        destination_dir.strip(),
                        selected_options
                    )
                    st.session_state.extraction_complete = True
                    st.session_state.extraction_status_message = message
                    st.session_state.extraction_success = success
                    st.session_state.output_files = output_files_dict if output_files_dict else {}
                    
                    # Attempt to get tenant name from DB file path for zip naming
                    db_file_path_for_name = st.session_state.output_files.get("db")
                    if db_file_path_for_name:
                        # DB path is like .../output_extraction/TenantName_extracted_db/TenantName.db
                        # We want "TenantName"
                        try:
                            # Get "TenantName.db"
                            db_filename_part = os.path.basename(db_file_path_for_name)
                            # Get "TenantName"
                            st.session_state.tenant_name_for_zip = os.path.splitext(db_filename_part)[0]
                        except Exception:
                            st.session_state.tenant_name_for_zip = "tenant_export" # fallback
                    else:
                        st.session_state.tenant_name_for_zip = "tenant_export" # fallback


                    # --- CSV Zipping Logic ---
                    if success and selected_options['csv'] and "csv_export_path" in st.session_state.output_files:
                        csv_output_dir_to_zip = st.session_state.output_files["csv_export_path"] # e.g. .../dest_dir/csv_exports
                        
                        if os.path.isdir(csv_output_dir_to_zip) and os.listdir(csv_output_dir_to_zip):
                            try:
                                tenant_name_for_file = st.session_state.tenant_name_for_zip
                                # Place the zip file in the main destination_dir
                                zip_base_name = os.path.join(destination_dir.strip(), f"{tenant_name_for_file}_csv_archive")
                                
                                # The directory containing "csv_exports" is destination_dir.strip()
                                # The directory to zip is "csv_exports"
                                root_dir_for_zip = destination_dir.strip() 
                                dir_to_zip_basename = os.path.basename(csv_output_dir_to_zip) # Should be "csv_exports"

                                logger.info(f"Attempting to zip: base_name={zip_base_name}, root_dir={root_dir_for_zip}, base_dir={dir_to_zip_basename}")
                                
                                # Ensure there's no existing file with the same name (shutil.make_archive will append .zip)
                                if os.path.exists(zip_base_name + ".zip"):
                                     os.remove(zip_base_name + ".zip")

                                archived_file_path = shutil.make_archive(
                                    base_name=zip_base_name,
                                    format='zip',
                                    root_dir=root_dir_for_zip,
                                    base_dir=dir_to_zip_basename
                                )
                                st.session_state.output_files["csv_zip"] = archived_file_path
                                logger.info(f"CSV ZIP created: {archived_file_path}")
                            except Exception as e_zip:
                                st.session_state.extraction_status_message += f"\nError creating ZIP for CSV files: {e_zip}"
                                logger.error(f"Could not create ZIP for CSV files: {e_zip}", exc_info=True)
                        elif os.path.isdir(csv_output_dir_to_zip): # Directory exists but is empty
                             logger.warning(f"CSV output directory {csv_output_dir_to_zip} is empty. No ZIP created.")
                             st.session_state.extraction_status_message += f"\nNote: CSV output directory {os.path.basename(csv_output_dir_to_zip)} was empty."
                        else: # Path doesn't exist or is not a directory
                            logger.warning(f"CSV output directory {csv_output_dir_to_zip} not found or not a directory. Cannot create ZIP.")
                            st.session_state.extraction_status_message += f"\nWarning: CSV output path {os.path.basename(csv_output_dir_to_zip)} not found."

                st.experimental_rerun()


    with col2:
        st.header("Status & Outputs")
        if st.session_state.extraction_complete:
            if st.session_state.extraction_success:
                st.success(st.session_state.extraction_status_message)
                
                # DB Download
                db_file = st.session_state.output_files.get("db")
                if db_file and os.path.exists(db_file):
                    with open(db_file, "rb") as fp:
                        st.download_button(
                            label=f"Download Database ({os.path.basename(db_file)})",
                            data=fp,
                            file_name=os.path.basename(db_file),
                            mime="application/x-sqlite3"
                        )
                
                # XLSX Download
                xlsx_file = st.session_state.output_files.get("xlsx")
                if xlsx_file and os.path.exists(xlsx_file):
                    with open(xlsx_file, "rb") as fp:
                        st.download_button(
                            label=f"Download Excel ({os.path.basename(xlsx_file)})",
                            data=fp,
                            file_name=os.path.basename(xlsx_file),
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )

                # CSV ZIP Download
                csv_zip_file = st.session_state.output_files.get("csv_zip")
                if csv_zip_file and os.path.exists(csv_zip_file):
                    with open(csv_zip_file, "rb") as fp:
                        st.download_button(
                            label=f"Download CSVs (ZIP - {os.path.basename(csv_zip_file)})",
                            data=fp,
                            file_name=os.path.basename(csv_zip_file),
                            mime="application/zip"
                        )
                elif selected_options.get('csv'): # If CSV was selected but no zip (e.g., dir was empty)
                    csv_path = st.session_state.output_files.get("csv_export_path")
                    if csv_path: # Path might exist even if zipping failed or dir was empty
                         st.info(f"CSV files were set to export to: {csv_path}. Check logs if archive is missing.")
                    else:
                         st.info("CSV export selected, but no CSV output path was recorded.")


            else: # Extraction failed
                st.error(f"Extraction Failed: {st.session_state.extraction_status_message}")
        
        st.subheader("Logs")
        log_file_to_display = st.session_state.get("log_file_name", LOG_FILE_NAME)
        
        col_log_refresh, col_log_download = st.columns(2)
        with col_log_refresh:
            if st.button("Refresh Logs"):
                # Reruns the script, which will re-read the log file
                st.experimental_rerun() 
        
        if os.path.exists(log_file_to_display):
            with col_log_download:
                try:
                    with open(log_file_to_display, "rb") as fp_log: # Read as bytes for download
                        st.download_button(
                            label="Download Full Log File",
                            data=fp_log,
                            file_name=os.path.basename(log_file_to_display),
                            mime="text/plain"
                        )
                except Exception as e_log_dl:
                    st.warning(f"Could not prepare log for download: {e_log_dl}")

            try:
                with open(log_file_to_display, "r", encoding="utf-8") as f: # Read as text for display
                    log_lines = f.readlines()
                    log_content_to_display = "".join(log_lines[-100:]) 
                    if len(log_lines) > 100:
                        st.caption(f"Displaying last 100 lines of {len(log_lines)} total lines from {log_file_to_display}")
                    else:
                        st.caption(f"Log file: {log_file_to_display}")
                st.text_area("Log Output", log_content_to_display, height=300, key="log_area_display", disabled=True)
            except Exception as e:
                st.warning(f"Could not read log file {log_file_to_display}: {e}")
        else:
            st.info(f"Log file {log_file_to_display} not found yet or no logs generated.")

if __name__ == "__main__":
    # This sys.path adjustment is crucial if you run `streamlit run src/streamlit_app.py`
    # and your modules (commondatafuncs, etc.) are also in `src/`.
    # It ensures that `src` is treated as a package root for imports like `from commondatafuncs import ...`.
    
    # Get the directory containing this script (src/)
    module_dir = os.path.dirname(os.path.abspath(__file__)) 
    
    # If 'src' is not the current working directory (e.g., running from repo root)
    # and 'src' is not on PYTHONPATH, Python might not find modules in 'src'
    # without this.
    # If commondatafuncs.py, etc., are in the SAME directory as streamlit_app.py (i.e., both in src/),
    # then direct imports `from commondatafuncs import ...` should work if `src` is the CWD
    # or if `src` is on sys.path.
    
    # This structure assumes:
    # repo_root/
    #   src/
    #     streamlit_app.py
    #     commondatafuncs.py
    #     ... (other .py files)
    #   requirements.txt
    #   ...
    # If running `streamlit run src/streamlit_app.py` from `repo_root`,
    # then `src` is typically added to sys.path by Streamlit, or the CWD is `repo_root`.
    
    # The most robust way if `streamlit_app.py` is in `src` and other modules are siblings:
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)

    # If modules were one level up (e.g. streamlit_app in src/app and modules in src/)
    # parent_dir = os.path.dirname(module_dir)
    # if parent_dir not in sys.path:
    #     sys.path.insert(0, parent_dir)

    main()
