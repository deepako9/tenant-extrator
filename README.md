Author: PMM Algo COE
Email: pmm_algocoe@o9solutions.com

# Purpose
This program is meant to enable platform users to extract model and UI entities
in a sorted, diffable and text searchable format.

# Running utility

Pre-requisites
- Python
- Python modules 
    * selenium - ```pip install selenium```
    * XlsxWriter - ```pip install XlsxWriter```
    * pandas - ```pip install pandas```

Execute main.py

# Generating single executable

Install pyinstaller and execute command like below.
```sh
pyinstaller -F --add-binary ".\chromedriver.exe;." --add-binary ".\convert.ico;." --name "Tenant_Extractor_v25.1" main.py
```
This will generate an executable in ```src/dist/```

# Limitations
* Only accepts tenant config file in zip format.

# Versions
* v2.1 
    - Added ActionButtonDetails table.
    - Remove default action buttons. 
    - IsTailNode column added for node formatting.
    - GraphName added to NodeCombosConditionalFormatting.
    - SortOrder added to MeasureGrpGranularity.
* v2.6
    - Added WidgetFilterLinkings table.
    - Added WidgetNavigationViews table.
* v3.0
    - Added Roles for workspaces and views.
* v3.1
    - Commit type set to DEFERRED.
    - csv and xlsx option added.
    - Measure usage option added.
* v3.2
    - chrome driver updated.
* v3.3
    - Handle old kibo1 rules with empty dimension name.
* v3.4
    - Python plugin related tables.  
* v3.5
    - Added _WidgetDefinitionPresentationProperties_ Table. 
* v3.6
    - Added _WidgetInfoContext_ Table.
* v3.6.1
    - Error with empty AttributeName in Graph scope fixed.
    - csv files for python plugin added.
    - chromedriver updated to 83
* v3.6.2
    - Added MeasureColumnName for Measures Table.
* v3.6.3
    - Added MeasureAggregates table.
* v3.6.4
    - Fixed for missing measures in model dependencies table.  
* v3.6.5
    - Bug fix & Chromedriver update.
* v3.6.6
    - Added IsVisible for WidgetLevelAttributes table.
* v3.6.7
    - Added table _MeasureGrpExternalConfigs_.
* v3.7
    - Added all PySparks plugin related tables.
* 3.7.1 
    - Removed version verification and updated python output tables.
* 3.7.2
    - Added ApplyConversion & ConversionFormula to Measures tables
* 22.0
    - Enable "Required" column in table WidgetLevelAttributes(https://o9git.visualstudio.com/CoreDev/_workitems/edit/136214/)
* 22.2
    - Tenant Extractor Dependencies Enhancement (https://o9git.visualstudio.com/RefDev/_workitems/edit/156806/)
* 23.1
    - Edge Output from RGenPluginOutputTables added. (https://o9git.visualstudio.com/RefDev/_workitems/edit/192327?src=WorkItemMention&src-action=artifact_link)
* 23.3
    - Measure Group Translation table added.
* 23.4
    - Fix Python Dependencies.
    - Begin getting removed from measure names.
* 23.5
    - UI Layout checks
    - Version verification via request
* 24.1
    - _legacy json fix for config 2.0
* 25.1
    - Added _MeasureGrpExternalConfigs_ table.

# Steps
1. Choose a **config file** and a **destination directory**.
2. Extract the config file.
