import logging
import sys
import json
from re import findall, IGNORECASE


class RuleExtractor:
    def __init__(self, data, dbConnection):
        """
        RuleExtractor Constructor.
        :param data:
        :param dbConnection:
        """
        try:
            self.logger = logging.getLogger("extractor-logger")
        except:
            print("Not able to set application logging. Exiting")
            sys.exit()
        self.data = data
        self.dbConnection = dbConnection
        self.TENANT_NAME = "TenantName"
        self.PLUGIN_NAME = "PluginName"
        self.CONFIG_JSON = "ConfigJson"
        self.SCRIPT_PARAM = "ScriptParams"
        self.DIM_NAME = "DimensionName"
        self.ruleFile = []
        self.scopeLabels = []
        self.tenantName = self.data["Tenant"]["Name"]
        self.ruleFilePositionList = []

    def extractRules(self):
        """
        Extract all rules related data.
        """
        self.createRuleFiles()
        self.createScopeLabels()
        self.createPlugins()
        procFilePosition = 0
        for curRuleFile in self.ruleFile:
            if curRuleFile["Position"] not in self.ruleFilePositionList:
                filePosition = int(curRuleFile["Position"])
            else:
                filePosition = max(self.ruleFilePositionList) + 1
            self.ruleFilePositionList.append(filePosition)

            if curRuleFile["LabelType"] == "ActiveRule":
                finalActiveRuleFiles = [
                    {
                        self.TENANT_NAME: self.tenantName,
                        "RuleFileName": curRuleFile["RuleFileName"],
                        "RuleFileDescription": curRuleFile["LabelDescription"],
                        "RuleFilePosition": filePosition,
                    }
                ]

                activeRuleFilesDataToDb = [
                    (
                        i[self.TENANT_NAME],
                        i["RuleFileName"],
                        i["RuleFileDescription"],
                        i["RuleFilePosition"],
                    )
                    for i in finalActiveRuleFiles
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO ActiveRuleFiles (TenantName, RuleFileName, RuleFileDescription, RuleFilePosition) "
                        " VALUES (?,?,?,?)",
                        activeRuleFilesDataToDb,
                    )
                except Exception as e:
                    self.logger.error(
                        "Unable to insert data into ActiveRuleFiles: " + str(e)
                    )
                    print(e)
                self.constructActiveRuleDB(curRuleFile)
            elif curRuleFile["LabelType"] == "NamedSet":
                self.constructNamedSetDB(curRuleFile)
            elif curRuleFile["LabelType"] == "Procedure":
                procFilePosition = procFilePosition + 1
                self.constructProcedureDB(curRuleFile, procFilePosition)

    def createPlugins(self):
        """
        Insert all plugin related data to plugin tables.
        """
        finalPluginData = []
        finalNonRPluginData = []
        for globalPluginGroup in self.data["GlobalPlugIns"]:
            if globalPluginGroup["ModuleName"] == "o9.GraphCube.Plugins":
                for plugin in globalPluginGroup["ConfiguredGlobalPlugins"]:
                    pluginClass = plugin["ClassName"]
                    pluginClassArray = pluginClass.split(".")
                    pluginClassName = pluginClassArray[-1]
                    finalPluginData = finalPluginData + [
                        {
                            self.TENANT_NAME: self.tenantName,
                            self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                            "PluginType": "Global",
                            "PluginClass": pluginClassName,
                        }
                    ]

                    if (
                        pluginClassName == "BosToInventory"
                        or pluginClassName == "InventoryToBos"
                        or pluginClassName == "EndingOnHandPlan"
                        or pluginClassName == "PeriodToDatePlan"
                        or pluginClassName == "SupplyChainSolver"
                    ):
                        finalNonRPluginData = []
                        for key, value in plugin[self.CONFIG_JSON].items():
                            if isinstance(value, list):
                                grainList = []
                                for i in value:
                                    if self.DIM_NAME in i and "AttributeName" in i:
                                        grainList.append(
                                            "["
                                            + i[self.DIM_NAME]
                                            + "].["
                                            + i["AttributeName"]
                                            + "]"
                                        )
                                grainList = sorted(grainList)
                                value = ", ".join(grainList)

                            finalNonRPluginData = finalNonRPluginData + [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    "PluginClass": pluginClassName,
                                    "ParamName": key,
                                    "ParamValue": value,
                                }
                            ]
                        nonRPluginDataToDB = [
                            (
                                i[self.TENANT_NAME],
                                i[self.PLUGIN_NAME],
                                i["PluginClass"],
                                i["ParamName"],
                                i["ParamValue"],
                            )
                            for i in finalNonRPluginData
                        ]
                        try:
                            self.dbConnection.executemany(
                                "INSERT INTO NonRPluginParams (TenantName, PluginName, PluginClass, ParamName, "
                                "ParamValue) VALUES (?,?,?,?,?)",
                                nonRPluginDataToDB,
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to insert data into NonRPluginParams: " + str(e)
                            )
                            print(e)

                    elif pluginClassName == "RScriptGeneralized":
                        rGenPluginParamsData = []
                        exceptionData = []
                        if self.SCRIPT_PARAM in plugin[self.CONFIG_JSON]:
                            rGenPluginParamsData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    "VariableName": (
                                        x["VariableName"]
                                        if "VariableName" in x
                                        else None
                                    ),
                                    "Value": x["Value"] if "Value" in x else None,
                                }
                                for x in plugin[self.CONFIG_JSON][self.SCRIPT_PARAM]
                            ]
                        if "Exceptions" in plugin[self.CONFIG_JSON]:
                            exceptionData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    "VariableName": "ExceptionMeasureName",
                                    "Value": (
                                        plugin[self.CONFIG_JSON]["Exceptions"][
                                            "measure"
                                        ]
                                        if "measure"
                                        in plugin[self.CONFIG_JSON]["Exceptions"]
                                        else ""
                                    ),
                                }
                            ]
                        rGenPluginParamsData = rGenPluginParamsData + exceptionData
                        rGenPluginParamsDataToDB = [
                            (
                                i[self.TENANT_NAME],
                                i[self.PLUGIN_NAME],
                                i["VariableName"],
                                i["Value"],
                            )
                            for i in rGenPluginParamsData
                        ]
                        try:
                            self.dbConnection.executemany(
                                "INSERT INTO RGenPluginParams (TenantName, PluginName, VariableName, Value) "
                                " VALUES (?,?,?,?)",
                                rGenPluginParamsDataToDB,
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to insert data into RGenPluginParams: " + str(e)
                            )
                            print(e)

                        if "InputMeasures" in plugin[self.CONFIG_JSON]:
                            for inputMeasures in plugin[self.CONFIG_JSON][
                                "InputMeasures"
                            ]:
                                rGenPluginInputTablesData = [
                                    {
                                        self.TENANT_NAME: self.tenantName,
                                        self.PLUGIN_NAME: plugin[
                                            "InstanceName"
                                        ].strip(),
                                        "VariableName": (
                                            inputMeasures["VariableName"]
                                            if "VariableName" in inputMeasures
                                            else None
                                        ),
                                        "MeasureName": i["MeasureName"],
                                    }
                                    for i in inputMeasures["Measures"]
                                ]
                                rGenPluginInputTablesDataToDB = [
                                    (
                                        i[self.TENANT_NAME],
                                        i[self.PLUGIN_NAME],
                                        i["VariableName"],
                                        i["MeasureName"],
                                    )
                                    for i in rGenPluginInputTablesData
                                ]
                                try:
                                    self.dbConnection.executemany(
                                        "INSERT INTO RGenPluginInputTables (TenantName, PluginName, VariableName, "
                                        "MeasureName) VALUES (?,?,?,?)",
                                        rGenPluginInputTablesDataToDB,
                                    )
                                except Exception as e:
                                    self.logger.error(
                                        "Unable to insert data into RGenPluginInputTables: "
                                        + str(e)
                                    )
                                    print(e)

                        if "InputQueries" in plugin[self.CONFIG_JSON]:
                            RGenPluginInputQueriesData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    "VariableName": (
                                        i["VariableName"]
                                        if "VariableName" in i
                                        else None
                                    ),
                                    "Query": i["Query"] if "Query" in i else None,
                                }
                                for i in plugin[self.CONFIG_JSON]["InputQueries"]
                            ]
                            RGenPluginInputQueriesDataToDB = [
                                (
                                    i[self.TENANT_NAME],
                                    i[self.PLUGIN_NAME],
                                    i["VariableName"],
                                    i["Query"],
                                )
                                for i in RGenPluginInputQueriesData
                            ]
                            try:
                                self.dbConnection.executemany(
                                    "INSERT INTO RGenPluginInputQueries (TenantName, PluginName, VariableName, Query) "
                                    " VALUES (?,?,?,?)",
                                    RGenPluginInputQueriesDataToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into RGenPluginInputQueries: "
                                    + str(e)
                                )
                                print(e)

                        if "OutputMeasures" in plugin[self.CONFIG_JSON]:
                            for outputMeasure in plugin[self.CONFIG_JSON][
                                "OutputMeasures"
                            ]:
                                rGenPluginOutputTablesData = []
                                if "Measures" in outputMeasure:
                                    rGenPluginOutputTablesData = [
                                        {
                                            self.TENANT_NAME: self.tenantName,
                                            self.PLUGIN_NAME: plugin[
                                                "InstanceName"
                                            ].strip(),
                                            "VariableName": (
                                                outputMeasure["VariableName"]
                                                if "VariableName" in outputMeasure
                                                else None
                                            ),
                                            "MeasureName": i["MeasureName"],
                                        }
                                        for i in outputMeasure["Measures"]
                                    ]

                                if "EdgeProperties" in outputMeasure:
                                    rGenPluginOutputTablesData = [
                                        {
                                            self.TENANT_NAME: self.tenantName,
                                            self.PLUGIN_NAME: plugin[
                                                "InstanceName"
                                            ].strip(),
                                            "VariableName": (
                                                outputMeasure["VariableName"]
                                                if "VariableName" in outputMeasure
                                                else None
                                            ),
                                            "MeasureName": i["EdgePropertyName"],
                                        }
                                        for i in outputMeasure["EdgeProperties"]
                                    ]
                                rGenPluginOutputTablesDataToDB = [
                                    (
                                        i[self.TENANT_NAME],
                                        i[self.PLUGIN_NAME],
                                        i["VariableName"],
                                        i["MeasureName"],
                                    )
                                    for i in rGenPluginOutputTablesData
                                ]
                                try:
                                    self.dbConnection.executemany(
                                        "INSERT INTO RGenPluginOutputTables (TenantName, PluginName, VariableName, "
                                        "MeasureName) VALUES (?,?,?,?)",
                                        rGenPluginOutputTablesDataToDB,
                                    )
                                except Exception as e:
                                    self.logger.error(
                                        "Unable to insert data into RGenPluginOutputTables: "
                                        + str(e)
                                    )
                                    print(e)

                        if "SliceKeys" in plugin[self.CONFIG_JSON]:
                            rGenPluginSliceTablesData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    self.DIM_NAME: (
                                        i[self.DIM_NAME] if self.DIM_NAME in i else ""
                                    ),
                                    "AttributeName": (
                                        i["AttributeName"]
                                        if "AttributeName" in i
                                        else None
                                    ),
                                }
                                for i in plugin[self.CONFIG_JSON]["SliceKeys"]
                            ]
                            rGenPluginSliceTablesDataToDB = [
                                (
                                    i[self.TENANT_NAME],
                                    i[self.PLUGIN_NAME],
                                    i[self.DIM_NAME],
                                    i["AttributeName"],
                                )
                                for i in rGenPluginSliceTablesData
                            ]
                            try:
                                self.dbConnection.executemany(
                                    "INSERT INTO RGenPluginSliceTables (TenantName, PluginName, DimensionName, "
                                    "AttributeName) VALUES (?,?,?,?)",
                                    rGenPluginSliceTablesDataToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into RGenPluginSliceTables: "
                                    + str(e)
                                )
                                print(e)

                        if "ScriptCode" in plugin[self.CONFIG_JSON]:
                            rGenPluginScriptsData = {
                                self.TENANT_NAME: self.tenantName,
                                self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                "ScriptCode": plugin[self.CONFIG_JSON]["ScriptCode"],
                            }
                            rGenPluginScriptsDataToDB = (
                                rGenPluginScriptsData[self.TENANT_NAME],
                                rGenPluginScriptsData[self.PLUGIN_NAME],
                                rGenPluginScriptsData["ScriptCode"],
                            )
                            try:
                                self.dbConnection.execute(
                                    "INSERT INTO RGenPluginScripts (TenantName, PluginName, ScriptCode) "
                                    " VALUES (?,?,?)",
                                    rGenPluginScriptsDataToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into RGenPluginScripts: "
                                    + str(e)
                                )
                                print(e)

                    elif pluginClassName == "RScriptTimeSeries":
                        rTimePluginParamsData = []
                        exceptionData = []
                        if self.SCRIPT_PARAM in plugin[self.CONFIG_JSON]:
                            for scriptParams in plugin[self.CONFIG_JSON][
                                self.SCRIPT_PARAM
                            ]:
                                rTimePluginParamsData = [
                                    {
                                        self.TENANT_NAME: self.tenantName,
                                        self.PLUGIN_NAME: plugin["InstanceName"],
                                        "Algorithm": scriptParams["Algorithm"],
                                        "ParamName": key,
                                        "ParamValue": value,
                                    }
                                    for key, value in scriptParams.items()
                                ]
                        if "Exceptions" in plugin[self.CONFIG_JSON]:
                            exceptionData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"],
                                    "Algorithm": "Exception",
                                    "ParamName": "ExceptionMeasureName",
                                    "ParamValue": (
                                        plugin[self.CONFIG_JSON]["Exceptions"][
                                            "measure"
                                        ]
                                        if "measure"
                                        in plugin[self.CONFIG_JSON]["Exceptions"]
                                        else ""
                                    ),
                                }
                            ]
                        rTimePluginParamsData = rTimePluginParamsData + exceptionData
                        rTimePluginParamsDataToDB = [
                            (
                                i[self.TENANT_NAME],
                                i[self.PLUGIN_NAME],
                                i["Algorithm"],
                                i["ParamName"],
                                i["ParamValue"],
                            )
                            for i in rTimePluginParamsData
                        ]
                        try:
                            self.dbConnection.executemany(
                                "INSERT INTO RTimePluginParams (TenantName, PluginName, Algorithm, ParamName, "
                                "ParamValue) VALUES (?,?,?,?,?)",
                                rTimePluginParamsDataToDB,
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to insert data into RTimePluginParams: "
                                + str(e)
                            )
                            print(e)

                        if "InputMeasures" in plugin[self.CONFIG_JSON]:
                            rTimePluginInputsData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"],
                                    "MeasureName": x["measure"],
                                    "VariableName": (
                                        x["variable"] if "variable" in x else None
                                    ),
                                    "IsPrimary": x["is_primary"],
                                }
                                for x in plugin[self.CONFIG_JSON]["InputMeasures"]
                            ]
                            rTimePluginInputsDataToDB = [
                                (
                                    i[self.TENANT_NAME],
                                    i[self.PLUGIN_NAME],
                                    i["MeasureName"],
                                    i["VariableName"],
                                    i["IsPrimary"],
                                )
                                for i in rTimePluginInputsData
                            ]
                            try:
                                self.dbConnection.executemany(
                                    "INSERT INTO RTimePluginInputs (TenantName, PluginName, MeasureName, VariableName, "
                                    "IsPrimary) VALUES (?,?,?,?,?)",
                                    rTimePluginInputsDataToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into RTimePluginInputs: "
                                    + str(e)
                                )
                                print(e)

                        if "OutputMeasures" in plugin[self.CONFIG_JSON]:
                            rTimePluginOutputsData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"],
                                    "MeasureName": x["measure"],
                                    "VariableName": (
                                        x["variable"] if "variable" in x else None
                                    ),
                                    "IsHistorical": x["is_historical"],
                                }
                                for x in plugin[self.CONFIG_JSON]["OutputMeasures"]
                            ]
                            rTimePluginOutputsDataToDB = [
                                (
                                    i[self.TENANT_NAME],
                                    i[self.PLUGIN_NAME],
                                    i["MeasureName"],
                                    i["VariableName"],
                                    i["IsHistorical"],
                                )
                                for i in rTimePluginOutputsData
                            ]
                            try:
                                self.dbConnection.executemany(
                                    "INSERT INTO RTimePluginOutputs (TenantName, PluginName, MeasureName, VariableName,"
                                    " IsHistorical) VALUES (?,?,?,?,?)",
                                    rTimePluginOutputsDataToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into RTimePluginOutputs: "
                                    + str(e)
                                )
                                print(e)

                        if "ScriptCode" in plugin[self.CONFIG_JSON]:
                            rTimePluginScriptsData = {
                                self.TENANT_NAME: self.tenantName,
                                self.PLUGIN_NAME: plugin["InstanceName"],
                                "ScriptCode": plugin[self.CONFIG_JSON]["ScriptCode"],
                            }
                            rTimePluginScriptsDataToDB = (
                                rTimePluginScriptsData[self.TENANT_NAME],
                                rTimePluginScriptsData[self.PLUGIN_NAME],
                                rTimePluginScriptsData["ScriptCode"],
                            )
                            try:
                                self.dbConnection.execute(
                                    "INSERT INTO RTimePluginScripts (TenantName, PluginName, ScriptCode) "
                                    " VALUES (?,?,?)",
                                    rTimePluginScriptsDataToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into RTimePluginScripts: "
                                    + str(e)
                                )
                                print(e)

                        if "TimeseriesParams" in plugin[self.CONFIG_JSON]:
                            rTimePluginSeriesParams = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"],
                                    "ParamName": key,
                                    "ParamValue": value,
                                }
                                for key, value in plugin[self.CONFIG_JSON][
                                    "TimeseriesParams"
                                ].items()
                            ]
                            rTimePluginSeriesParamsDataToDB = [
                                (
                                    i[self.TENANT_NAME],
                                    i[self.PLUGIN_NAME],
                                    i["ParamName"],
                                    i["ParamValue"],
                                )
                                for i in rTimePluginSeriesParams
                            ]
                            try:
                                self.dbConnection.executemany(
                                    "INSERT INTO RTimeSeriesParams (TenantName, PluginName, ParamName, ParamValue) "
                                    " VALUES (?,?,?,?)",
                                    rTimePluginSeriesParamsDataToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into RTimeSeriesParams: "
                                    + str(e)
                                )
                                print(e)

                    elif pluginClassName == "PythonScript":
                        pythonPluginParamData = []
                        pythonPluginExceptionData = []
                        if (
                            self.CONFIG_JSON in plugin
                            and self.SCRIPT_PARAM in plugin[self.CONFIG_JSON]
                        ):
                            pythonPluginParamData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    "VariableName": (
                                        x["VariableName"]
                                        if "VariableName" in x
                                        else None
                                    ),
                                    "Value": x["Value"] if "Value" in x else None,
                                }
                                for x in plugin[self.CONFIG_JSON][self.SCRIPT_PARAM]
                            ]
                        if (
                            self.CONFIG_JSON in plugin
                            and "Exceptions" in plugin[self.CONFIG_JSON]
                        ):
                            pythonPluginExceptionData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    "VariableName": "ExceptionMeasureName",
                                    "Value": (
                                        plugin[self.CONFIG_JSON]["Exceptions"][
                                            "measure"
                                        ]
                                        if "measure"
                                        in plugin[self.CONFIG_JSON]["Exceptions"]
                                        else None
                                    ),
                                }
                            ]
                        pythonPluginParamData = (
                            pythonPluginParamData + pythonPluginExceptionData
                        )
                        paramToDB = [
                            (
                                i[self.TENANT_NAME],
                                i[self.PLUGIN_NAME],
                                i["VariableName"],
                                i["Value"],
                            )
                            for i in pythonPluginParamData
                        ]
                        try:
                            self.dbConnection.executemany(
                                "INSERT INTO PythonPluginParams (TenantName, PluginName, VariableName, Value) "
                                " VALUES (?,?,?,?)",
                                paramToDB,
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to add data to PythonPluginParams: " + str(e)
                            )
                            print("Unable to add data to PythonPluginParams: " + str(e))
                        if (
                            self.CONFIG_JSON in plugin
                            and "InputTables" in plugin[self.CONFIG_JSON]
                        ):
                            position = 0
                            for inputTable in plugin[self.CONFIG_JSON]["InputTables"]:
                                position = position + 1
                                pythonPluginInputTablesData = [
                                    {
                                        self.TENANT_NAME: self.tenantName,
                                        self.PLUGIN_NAME: plugin[
                                            "InstanceName"
                                        ].strip(),
                                        "Position": str(position),
                                        "VariableKey": key,
                                        "Value": str(value),
                                    }
                                    for key, value in inputTable.items()
                                ]
                                inputTableToDB = [
                                    (
                                        i[self.TENANT_NAME],
                                        i[self.PLUGIN_NAME],
                                        i["Position"],
                                        i["VariableKey"],
                                        i["Value"],
                                    )
                                    for i in pythonPluginInputTablesData
                                ]
                                try:
                                    self.dbConnection.executemany(
                                        "INSERT INTO PythonPluginInputTables (TenantName, PluginName, Position,"
                                        " VariableKey, Value) VALUES (?,?,?,?,?)",
                                        inputTableToDB,
                                    )
                                except Exception as e:
                                    self.logger.error(
                                        "Unable to insert data into PythonPluginInputTables: "
                                        + str(e)
                                    )
                                    print(
                                        "Unable to insert data into PythonPluginInputTables: "
                                        + str(e)
                                    )

                        if (
                            self.CONFIG_JSON in plugin
                            and "OutputTables" in plugin[self.CONFIG_JSON]
                        ):
                            position = 0
                            for inputTable in plugin[self.CONFIG_JSON]["OutputTables"]:
                                position = position + 1
                                pythonPluginOutputTablesData = [
                                    {
                                        self.TENANT_NAME: self.tenantName,
                                        self.PLUGIN_NAME: plugin[
                                            "InstanceName"
                                        ].strip(),
                                        "Position": str(position),
                                        "VariableKey": key,
                                        "Value": str(value),
                                    }
                                    for key, value in inputTable.items()
                                ]
                                outputTablesToDB = [
                                    (
                                        i[self.TENANT_NAME],
                                        i[self.PLUGIN_NAME],
                                        i["Position"],
                                        i["VariableKey"],
                                        i["Value"],
                                    )
                                    for i in pythonPluginOutputTablesData
                                ]
                                try:
                                    self.dbConnection.executemany(
                                        "INSERT INTO PythonPluginOutputTables (TenantName, PluginName, Position,"
                                        " VariableKey, Value) VALUES (?,?,?,?,?)",
                                        outputTablesToDB,
                                    )
                                except Exception as e:
                                    self.logger.error(
                                        f"Unable to insert data into PythonPluginOutputTables: {str(e)}"
                                    )
                                    print(
                                        f"Unable to insert data into PythonPluginOutputTables: {str(e)}"
                                    )

                        if (
                            self.CONFIG_JSON in plugin
                            and "SliceKeys" in plugin[self.CONFIG_JSON]
                        ):
                            pythonPluginSliceKeysData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    self.DIM_NAME: (
                                        i[self.DIM_NAME] if self.DIM_NAME in i else None
                                    ),
                                    "AttributeName": (
                                        i["AttributeName"]
                                        if "AttributeName" in i
                                        else None
                                    ),
                                }
                                for i in plugin[self.CONFIG_JSON]["SliceKeys"]
                            ]
                            sliceKeysToDB = [
                                (
                                    i[self.TENANT_NAME],
                                    i[self.PLUGIN_NAME],
                                    i[self.DIM_NAME],
                                    i["AttributeName"],
                                )
                                for i in pythonPluginSliceKeysData
                            ]
                            try:
                                self.dbConnection.executemany(
                                    "INSERT INTO PythonPluginSliceKeyTables (TenantName, PluginName, DimensionName, "
                                    "AttributeName) VALUES (?,?,?,?)",
                                    sliceKeysToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into PythonPluginSliceKeyTables: "
                                    + str(e)
                                )
                                print(
                                    "Unable to insert data into PythonPluginSliceKeyTables: "
                                    + str(e)
                                )

                        if (
                            self.CONFIG_JSON in plugin
                            and "ScriptCode" in plugin[self.CONFIG_JSON]
                        ):
                            scriptCode = plugin[self.CONFIG_JSON]["ScriptCode"]
                            pythonPluginScriptsData = {
                                self.TENANT_NAME: self.tenantName,
                                self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                "ScriptCode": scriptCode,
                            }
                            try:
                                self.dbConnection.execute(
                                    "INSERT INTO PythonPluginScripts (TenantName, PluginName, ScriptCode) "
                                    " VALUES (?,?,?)",
                                    (
                                        pythonPluginScriptsData[self.TENANT_NAME],
                                        pythonPluginScriptsData[self.PLUGIN_NAME],
                                        pythonPluginScriptsData["ScriptCode"],
                                    ),
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into PythonPluginScripts: "
                                    + str(e)
                                )
                                print(
                                    "Unable to insert data into PythonPluginScripts: "
                                    + str(e)
                                )

                            startIndex = scriptCode.find(
                                "TENANT EXTRACTOR: OUTPUT MEASURES START"
                            )
                            endIndex = scriptCode.find(
                                "TENANT EXTRACTOR: OUTPUT MEASURES END"
                            )
                            if startIndex != -1 and endIndex != -1:
                                outMeasures = findall(
                                    r"Measure\.\[.*?\]|Edge\.\[.*?\]",
                                    scriptCode[startIndex:endIndex],
                                    flags=IGNORECASE,
                                )
                                outputMeasures = [
                                    {
                                        self.TENANT_NAME: self.tenantName,
                                        self.PLUGIN_NAME: plugin[
                                            "InstanceName"
                                        ].strip(),
                                        "Type": "Edge" if "Edge" in i else "Measure",
                                        "MeasureName": i.split(".[")[-1].strip(),
                                    }
                                    for i in outMeasures
                                ]
                                outputMeasuresToDB = [
                                    (
                                        i[self.TENANT_NAME],
                                        i[self.PLUGIN_NAME],
                                        i["Type"],
                                        (
                                            i["MeasureName"]
                                            if "[" in i["MeasureName"]
                                            else i["MeasureName"].replace("]", "")
                                        ),
                                    )
                                    for i in outputMeasures
                                ]
                                try:
                                    self.dbConnection.executemany(
                                        "INSERT INTO PythonPluginOutputMeasures (TenantName, PluginName, Type, "
                                        "MeasureName) VALUES (?,?,?,?)",
                                        outputMeasuresToDB,
                                    )
                                except Exception as e:
                                    self.logger.error(
                                        f"Unable to insert data into PythonPluginOutputMeasures: {str(e)}"
                                    )
                                    print(
                                        f"Unable to insert data into PythonPluginOutputMeasures: {str(e)}"
                                    )

                    elif pluginClassName == "PySparkScript":
                        pySparkPluginParamData = []
                        pySparkPluginExceptionData = []
                        # PARAMS
                        if (
                            self.CONFIG_JSON in plugin
                            and self.SCRIPT_PARAM in plugin[self.CONFIG_JSON]
                        ):
                            pySparkPluginParamData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    "VariableName": (
                                        x["VariableName"]
                                        if "VariableName" in x
                                        else None
                                    ),
                                    "Value": x["Value"] if "Value" in x else None,
                                }
                                for x in plugin[self.CONFIG_JSON][self.SCRIPT_PARAM]
                            ]
                        # EXCEPTIONS
                        if (
                            self.CONFIG_JSON in plugin
                            and "Exceptions" in plugin[self.CONFIG_JSON]
                        ):
                            pySparkPluginExceptionData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    "VariableName": "ExceptionMeasureName",
                                    "Value": (
                                        plugin[self.CONFIG_JSON]["Exceptions"][
                                            "measure"
                                        ]
                                        if "measure"
                                        in plugin[self.CONFIG_JSON]["Exceptions"]
                                        else None
                                    ),
                                }
                            ]
                        pySparkPluginParamData = (
                            pySparkPluginParamData + pySparkPluginExceptionData
                        )
                        paramToDB = [
                            (
                                i[self.TENANT_NAME],
                                i[self.PLUGIN_NAME],
                                i["VariableName"],
                                i["Value"],
                            )
                            for i in pySparkPluginParamData
                        ]
                        try:
                            self.dbConnection.executemany(
                                "INSERT INTO PySparkPluginParams (TenantName, PluginName, VariableName, Value) "
                                " VALUES (?,?,?,?)",
                                paramToDB,
                            )
                        except Exception as e:
                            self.logger.error(
                                f"Unable to add data to PySparkPluginParams: {str(e)}"
                            )
                            print(
                                f"ERROR: Unable to add data to PySparkPluginParams: {str(e)}"
                            )

                        # INPUT TABLES
                        if (
                            self.CONFIG_JSON in plugin
                            and "InputTables" in plugin[self.CONFIG_JSON]
                        ):
                            position = 0
                            for inputTable in plugin[self.CONFIG_JSON]["InputTables"]:
                                position = position + 1
                                pySparkPluginInputTablesData = [
                                    {
                                        self.TENANT_NAME: self.tenantName,
                                        self.PLUGIN_NAME: plugin[
                                            "InstanceName"
                                        ].strip(),
                                        "Position": str(position),
                                        "VariableKey": key,
                                        "Value": str(value),
                                    }
                                    for key, value in inputTable.items()
                                ]
                                inputTableToDB = [
                                    (
                                        i[self.TENANT_NAME],
                                        i[self.PLUGIN_NAME],
                                        i["Position"],
                                        i["VariableKey"],
                                        i["Value"],
                                    )
                                    for i in pySparkPluginInputTablesData
                                ]
                                try:
                                    self.dbConnection.executemany(
                                        "INSERT INTO PySparkPluginInputTables (TenantName, PluginName, Position,"
                                        " VariableKey, Value) VALUES (?,?,?,?,?)",
                                        inputTableToDB,
                                    )
                                except Exception as e:
                                    self.logger.error(
                                        f"Unable to insert data into PySparkPluginInputTables: {str(e)}"
                                    )
                                    print(
                                        f"Unable to insert data into PySparkPluginInputTables: {str(e)}"
                                    )

                        # OUTPUT TABLES
                        if (
                            self.CONFIG_JSON in plugin
                            and "OutputTables" in plugin[self.CONFIG_JSON]
                        ):
                            pySparkPluginOutputTablesData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    "VariableName": i["VariableName"],
                                    "VariableType": i["VariableType"],
                                }
                                for i in plugin[self.CONFIG_JSON]["OutputTables"]
                            ]
                            outputTablesToDB = [
                                (
                                    i[self.TENANT_NAME],
                                    i[self.PLUGIN_NAME],
                                    i["VariableName"],
                                    i["VariableType"],
                                )
                                for i in pySparkPluginOutputTablesData
                            ]
                            try:
                                self.dbConnection.executemany(
                                    "INSERT INTO PySparkPluginOutputTables (TenantName, PluginName, VariableName,"
                                    " VariableType) VALUES (?,?,?,?)",
                                    outputTablesToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    f"Unable to insert data into PySparkPluginOutputTables: {str(e)}"
                                )
                                print(
                                    f"Unable to insert data into PySparkPluginOutputTables: {str(e)}"
                                )

                        # SLICE KEYS
                        if (
                            self.CONFIG_JSON in plugin
                            and "SliceKeys" in plugin[self.CONFIG_JSON]
                        ):
                            pySparkPluginSliceKeysData = [
                                {
                                    self.TENANT_NAME: self.tenantName,
                                    self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                    self.DIM_NAME: (
                                        i[self.DIM_NAME] if self.DIM_NAME in i else None
                                    ),
                                    "AttributeName": (
                                        i["AttributeName"]
                                        if "AttributeName" in i
                                        else None
                                    ),
                                }
                                for i in plugin[self.CONFIG_JSON]["SliceKeys"]
                            ]
                            sliceKeysToDB = [
                                (
                                    i[self.TENANT_NAME],
                                    i[self.PLUGIN_NAME],
                                    i[self.DIM_NAME],
                                    i["AttributeName"],
                                )
                                for i in pySparkPluginSliceKeysData
                            ]
                            try:
                                self.dbConnection.executemany(
                                    "INSERT INTO PySparkPluginSliceKeys (TenantName, PluginName, DimensionName, "
                                    "AttributeName) VALUES (?,?,?,?)",
                                    sliceKeysToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    f"Unable to insert data into PySparkPluginSliceKeys: {str(e)}"
                                )
                                print(
                                    f"Unable to insert data into PySparkPluginSliceKeys: {str(e)}"
                                )

                        # CODE
                        if (
                            self.CONFIG_JSON in plugin
                            and "ScriptCode" in plugin[self.CONFIG_JSON]
                        ):
                            pythonPluginScriptsData = {
                                self.TENANT_NAME: self.tenantName,
                                self.PLUGIN_NAME: plugin["InstanceName"].strip(),
                                "ScriptCode": plugin[self.CONFIG_JSON]["ScriptCode"],
                            }
                            try:
                                self.dbConnection.execute(
                                    "INSERT INTO PySparkPluginScripts (TenantName, PluginName, ScriptCode) "
                                    " VALUES (?,?,?)",
                                    (
                                        pythonPluginScriptsData[self.TENANT_NAME],
                                        pythonPluginScriptsData[self.PLUGIN_NAME],
                                        pythonPluginScriptsData["ScriptCode"],
                                    ),
                                )
                            except Exception as e:
                                self.logger.error(
                                    f"Unable to insert data into PySparkPluginScripts: {str(e)}"
                                )
                                print(
                                    f"Unable to insert data into PySparkPluginScripts: {str(e)}"
                                )

        for tenantPluginGroup in self.data["TenantPlugIns"]:
            if (
                tenantPluginGroup["Language"] == "Javascript"
                or tenantPluginGroup["Language"] == "Powershell"
            ):
                tenantPluginDetailsData = [
                    {
                        self.TENANT_NAME: self.tenantName,
                        self.PLUGIN_NAME: tenantPluginGroup["ModuleName"].strip(),
                        "PluginClass": (
                            "JavaScriptPlugins"
                            if tenantPluginGroup["Language"] == "Javascript"
                            else "PowerShellPlugins"
                        ),
                        "PluginCode": tenantPluginGroup["Code"],
                        "Description": tenantPluginGroup["Description"],
                    }
                ]
                tenantPluginDetailsDataToDB = [
                    (
                        i[self.TENANT_NAME],
                        i[self.PLUGIN_NAME],
                        i["PluginClass"],
                        i["PluginCode"],
                        i["Description"],
                    )
                    for i in tenantPluginDetailsData
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO TenantPluginDetails (TenantName, PluginName, PluginClass, PluginCode, Description)"
                        " VALUES (?,?,?,?,?)",
                        tenantPluginDetailsDataToDB,
                    )
                except Exception as e:
                    self.logger.error(
                        "Unable to insert data into TenantPluginDetails: " + str(e)
                    )
                    print(e)

            finalPluginData = finalPluginData + [
                {
                    self.TENANT_NAME: self.tenantName,
                    self.PLUGIN_NAME: tenantPluginGroup["ModuleName"].strip(),
                    "PluginType": "Tenant",
                    "PluginClass": (
                        "JavaScriptPlugins"
                        if tenantPluginGroup["Language"] == "Javascript"
                        else "PowerShellPlugins"
                    ),
                }
            ]
        pluginDataToDB = [
            (
                i[self.TENANT_NAME],
                i[self.PLUGIN_NAME],
                i["PluginType"],
                i["PluginClass"],
            )
            for i in finalPluginData
        ]
        try:
            self.dbConnection.executemany(
                "INSERT INTO Plugins (TenantName, PluginName, PluginType, PluginClass) "
                " VALUES (?,?,?,?)",
                pluginDataToDB,
            )
        except Exception as e:
            self.logger.error("Unable to insert data into Plugins: " + str(e))
            print(e)

    def constructProcedureDB(self, procedureRuleFile, procFilePosition):
        """
        Insert all procedure related data to procedure tables.
        :param procedureRuleFile: procedure file name
        :param procFilePosition: file position
        """
        procFilesData = {
            self.TENANT_NAME: self.tenantName,
            "ProcFile": procedureRuleFile["RuleFileName"],
            "ProcFileDescription": procedureRuleFile["LabelDescription"],
            "ProcFilePosition": procFilePosition,
            "JSONProcFilePosition": procedureRuleFile["Position"],
        }
        try:
            self.dbConnection.execute(
                "INSERT INTO ProcFiles (TenantName, ProcFile, ProcFileDescription, ProcFilePosition, "
                "JSONProcFilePosition) VALUES (?,?,?,?,?)",
                (
                    procFilesData[self.TENANT_NAME],
                    procFilesData["ProcFile"],
                    procFilesData["ProcFileDescription"],
                    procFilesData["ProcFilePosition"],
                    procFilesData["JSONProcFilePosition"],
                ),
            )
        except Exception as e:
            self.logger.error("Unable to insert data into ProcFiles: " + str(e))
            print("Unable to insert data into ProcFiles: " + str(e))
        matchingProcedureGroups = sorted(
            (
                x
                for x in self.data["RuleGroups"]
                if (x["RuleGroupLabelId"] == procedureRuleFile["RuleGroupLabelId"])
            ),
            key=lambda x: x["RuleGroupLabelPosition"],
        )
        procedurePosition = 0
        for procedure in matchingProcedureGroups:
            procedurePosition = procedurePosition + 1
            proceduresData = {
                self.TENANT_NAME: self.tenantName,
                "ProcFile": procedureRuleFile["RuleFileName"],
                "ProcName": procedure["RuleGroupName"],
                "ProcDescription": procedure["RuleGroupDescription"],
                "IsParameterized": (
                    procedure["RuleGroupContent"]["IsParameterized"]
                    if "IsParameterized" in procedure["RuleGroupContent"]
                    else None
                ),
                "ProcPosition": procedurePosition,
                "JSONProcPosition": procedure["RuleGroupLabelPosition"],
            }
            try:
                self.dbConnection.execute(
                    "INSERT INTO Procedures (TenantName, ProcFile, ProcName, ProcDescription, IsParameterized, "
                    "ProcPosition, JSONProcPosition) VALUES (?,?,?,?,?,?,?)",
                    (
                        proceduresData[self.TENANT_NAME],
                        proceduresData["ProcFile"],
                        proceduresData["ProcName"],
                        proceduresData["ProcDescription"],
                        proceduresData["IsParameterized"],
                        proceduresData["ProcPosition"],
                        proceduresData["JSONProcPosition"],
                    ),
                )
            except Exception as e:
                self.logger.error("Unable to insert data into Procedures: " + str(e))
                print("Unable to insert data into Procedures: " + str(e))

            if (
                "ParameterJson" in procedure["RuleGroupContent"]
                and procedure["RuleGroupContent"]["ParameterJson"] is not None
                and "properties" in procedure["RuleGroupContent"]["ParameterJson"]
            ):
                procParamsData = [
                    {
                        self.TENANT_NAME: self.tenantName,
                        "ProcName": procedure["RuleGroupName"],
                        "ParamName": key,
                        "ParamType": value["type"],
                        "ItemType": (
                            value["items"]["type"] if value["type"] == "array" else None
                        ),
                    }
                    for key, value in procedure["RuleGroupContent"]["ParameterJson"][
                        "properties"
                    ].items()
                ]
                procParamsDataToDB = [
                    (
                        i[self.TENANT_NAME],
                        i["ProcName"],
                        i["ParamName"],
                        i["ParamType"],
                        i["ItemType"],
                    )
                    for i in procParamsData
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO ProcParams (TenantName, ProcName, ParamName, ParamType, ItemType) "
                        " VALUES (?,?,?,?,?)",
                        procParamsDataToDB,
                    )
                except Exception as e:
                    self.logger.error(
                        "Unable to insert data into ProcParams: " + str(e)
                    )
                    print("Unable to insert data into ProcParams: " + str(e))

            if "RuleGroupText" in procedure["RuleGroupContent"]:
                procCodeData = {
                    self.TENANT_NAME: self.tenantName,
                    "ProcName": procedure["RuleGroupName"],
                    "ProcCode": procedure["RuleGroupContent"]["RuleGroupText"],
                }
                try:
                    self.dbConnection.execute(
                        "INSERT INTO ProcCodes (TenantName, ProcName, ProcCode) "
                        " VALUES (?,?,?)",
                        (
                            procCodeData[self.TENANT_NAME],
                            procCodeData["ProcName"],
                            procCodeData["ProcCode"],
                        ),
                    )
                except Exception as e:
                    self.logger.error("Unable to insert data into ProcCode: " + str(e))
                    print("Unable to insert data into ProcCode: " + str(e))

    def constructNamedSetDB(self, namedSetRuleFile):
        """
        Insert all namedset related data to namedset tables.
        :param namedSetRuleFile: namedset file name
        """
        matchingRuleGroups = sorted(
            (
                x
                for x in self.data["RuleGroups"]
                if (x["RuleGroupLabelId"] == namedSetRuleFile["RuleGroupLabelId"])
            ),
            key=lambda x: x["RuleGroupName"],
        )
        finalNamedSet = [
            {
                self.TENANT_NAME: self.tenantName,
                "RuleFileName": namedSetRuleFile["RuleFileName"],
                "SetName": x["RuleGroupName"],
                "Definition": x["RuleGroupContent"]["RuleGroupText"],
                "Description": x["RuleGroupDescription"],
            }
            for x in matchingRuleGroups
        ]
        namedSetDataToDB = [
            (
                i[self.TENANT_NAME],
                i["RuleFileName"],
                i["SetName"],
                i["Definition"],
                i["Description"],
            )
            for i in finalNamedSet
        ]
        try:
            self.dbConnection.executemany(
                "INSERT INTO NamedSets (TenantName, RuleFileName, SetName, Definition, Description) "
                " VALUES (?,?,?,?,?)",
                namedSetDataToDB,
            )
        except Exception as e:
            self.logger.error("Unable to insert data into NamedSets: " + str(e))
            print(e)

    def constructActiveRuleDB(self, activeRuleFile):
        """
        Insert all active rules related data to active rule tables.
        :param activeRuleFile: active rule file name
        """
        matchingScopeLabels = filter(
            lambda x: (x["RuleGroupLabelId"] == activeRuleFile["RuleGroupLabelId"]),
            self.scopeLabels,
        )

        pluginRuleGroups = filter(
            lambda x: (
                x["RuleGroupLabelId"] == activeRuleFile["RuleGroupLabelId"]
                and x["ScopeLabelId"] == 0
                and x["RuleGroupType"] == "Plugin"
            ),
            self.data["RuleGroups"],
        )

        sortedMatchingScopeLabels = sorted(
            matchingScopeLabels, key=lambda x: x["ScopePosition"]
        )
        sortedPluginRuleGroups = sorted(
            pluginRuleGroups, key=lambda x: x["RuleGroupLabelPosition"]
        )
        scopePosition = 0
        for x in sortedMatchingScopeLabels:
            scopePosition = scopePosition + 1

            finalActiveRuleScopes = [
                {
                    self.TENANT_NAME: self.tenantName,
                    "RuleFileName": activeRuleFile["RuleFileName"],
                    "ScopePosition": scopePosition,
                    "ScopeDescription": x["LabelDescription"],
                    "ScopeType": x["RuleGroupType"],
                    "ScopeExpression": x["ScopeExpression"],
                    "ScopeString": x["ScopeString"],
                    "JSONScopePosition": x["ScopePosition"],
                }
            ]
            self.createActiveRuleFormulaeDB(
                x,
                activeRuleFile["RuleGroupLabelId"],
                x["ScopeLabelId"],
                activeRuleFile["RuleFileName"],
                scopePosition,
            )

            activeRuleScopesDataToDB = [
                (
                    i[self.TENANT_NAME],
                    i["RuleFileName"],
                    i["ScopePosition"],
                    i["ScopeDescription"],
                    i["ScopeType"],
                    i["ScopeString"],
                    i["JSONScopePosition"],
                )
                for i in finalActiveRuleScopes
            ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO ActiveRuleScopeLists (TenantName, RuleFileName, ScopePosition, ScopeDescription, "
                    "ScopeType, ScopeString, JSONScopePosition) VALUES (?,?,?,?,?,?,?)",
                    activeRuleScopesDataToDB,
                )
            except Exception as e:
                self.logger.error(
                    "Unable to insert data into ActiveRuleFiles: " + str(e)
                )
                print(e)

            fromScopeNodes = []
            toScopeNodes = []
            allNodes = []
            for activeRuleScopeGrain in finalActiveRuleScopes:
                if activeRuleScopeGrain["ScopeType"] == "Graph":
                    versionScopeNode = {
                        self.DIM_NAME: (
                            activeRuleScopeGrain["ScopeExpression"]["VersionScope"][
                                self.DIM_NAME
                            ]
                            if self.DIM_NAME
                            in activeRuleScopeGrain["ScopeExpression"]["VersionScope"]
                            else ""
                        ),
                        "LevelAttributeName": (
                            activeRuleScopeGrain["ScopeExpression"]["VersionScope"][
                                "LevelAttributeName"
                            ]
                            if "LevelAttributeName"
                            in activeRuleScopeGrain["ScopeExpression"]["VersionScope"]
                            else ""
                        ),
                        "FilterExpression": (
                            activeRuleScopeGrain["ScopeExpression"]["VersionScope"][
                                "FilterExpression"
                            ]
                            if "FilterExpression"
                            in activeRuleScopeGrain["ScopeExpression"]["VersionScope"]
                            else ""
                        ),
                        "NodeType": "Version",
                    }

                    if "FromScopes" in activeRuleScopeGrain["ScopeExpression"]:
                        fromScopeNodes = [
                            {
                                self.DIM_NAME: (
                                    i[self.DIM_NAME] if self.DIM_NAME in i else ""
                                ),
                                "LevelAttributeName": (
                                    i["LevelAttributeName"]
                                    if "LevelAttributeName" in i
                                    else ""
                                ),
                                "FilterExpression": (
                                    i["FilterExpression"]
                                    if "FilterExpression"
                                    in activeRuleScopeGrain["ScopeExpression"][
                                        "FromScopes"
                                    ]
                                    else ""
                                ),
                                "NodeType": "From",
                            }
                            for i in activeRuleScopeGrain["ScopeExpression"][
                                "FromScopes"
                            ]
                        ]

                    if "ToScopes" in activeRuleScopeGrain["ScopeExpression"]:
                        toScopeNodes = [
                            {
                                self.DIM_NAME: (
                                    i[self.DIM_NAME] if self.DIM_NAME in i else ""
                                ),
                                "LevelAttributeName": (
                                    i["LevelAttributeName"]
                                    if "LevelAttributeName" in i
                                    else ""
                                ),
                                "FilterExpression": (
                                    i["FilterExpression"]
                                    if "FilterExpression"
                                    in activeRuleScopeGrain["ScopeExpression"][
                                        "ToScopes"
                                    ]
                                    else ""
                                ),
                                "NodeType": "To",
                            }
                            for i in activeRuleScopeGrain["ScopeExpression"]["ToScopes"]
                        ]

                    allNodes = fromScopeNodes + toScopeNodes
                    allNodes.append(versionScopeNode)

                    finalActiveRuleGraphGrain = [
                        {
                            self.TENANT_NAME: self.tenantName,
                            "RuleFileName": activeRuleScopeGrain["RuleFileName"],
                            "ScopePosition": scopePosition,
                            self.DIM_NAME: (
                                x[self.DIM_NAME] if self.DIM_NAME in x else ""
                            ),
                            "LevelAttributeName": (
                                x["LevelAttributeName"]
                                if "LevelAttributeName" in x
                                else ""
                            ),
                            "FilterExpression": x["FilterExpression"],
                            "NodeType": x["NodeType"],
                            "RelationshipTypeName": activeRuleScopeGrain[
                                "ScopeExpression"
                            ]["RelationshipTypeName"],
                        }
                        for x in allNodes
                    ]
                    activeRuleGraphGrainDataToDB = [
                        (
                            i[self.TENANT_NAME],
                            i["RuleFileName"],
                            i["ScopePosition"],
                            i[self.DIM_NAME],
                            i["LevelAttributeName"],
                            i["FilterExpression"],
                            i["NodeType"],
                            i["RelationshipTypeName"],
                        )
                        for i in finalActiveRuleGraphGrain
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO ActiveRuleGraphGrains (TenantName, RuleFileName, ScopePosition, DimensionName,"
                            " LevelAttributeName, FilterExpression, NodeType, RelationshipTypeName) "
                            " VALUES (?,?,?,?,?,?,?,?)",
                            activeRuleGraphGrainDataToDB,
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into ActiveRuleGraphGrains: "
                            + str(e)
                        )
                        print(e)

                if "AttributeMemberScopes" in activeRuleScopeGrain["ScopeExpression"]:

                    finalActiveRuleScopeGrains = [
                        {
                            self.TENANT_NAME: self.tenantName,
                            "RuleFileName": activeRuleScopeGrain["RuleFileName"],
                            "ScopePosition": scopePosition,
                            self.DIM_NAME: (
                                x[self.DIM_NAME] if self.DIM_NAME in x else ""
                            ),
                            "LevelAttributeName": (
                                x["LevelAttributeName"]
                                if "LevelAttributeName" in x
                                else ""
                            ),
                            "FilterExpression": (
                                x["FilterExpression"] if "FilterExpression" in x else ""
                            ),
                        }
                        for x in activeRuleScopeGrain["ScopeExpression"][
                            "AttributeMemberScopes"
                        ]
                    ]

                    activeRuleScopeGrainDataToDB = [
                        (
                            i[self.TENANT_NAME],
                            i["RuleFileName"],
                            i["ScopePosition"],
                            i[self.DIM_NAME],
                            i["LevelAttributeName"],
                            i["FilterExpression"],
                        )
                        for i in finalActiveRuleScopeGrains
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO ActiveRuleScopeGrains (TenantName, RuleFileName, ScopePosition, DimensionName,"
                            " LevelAttributeName, FilterExpression) VALUES (?,?,?,?,?,?)",
                            activeRuleScopeGrainDataToDB,
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into ActiveRuleScopeGrains: "
                            + str(e)
                        )
                        print(e)

        pluginPosition = 0
        for x in sortedPluginRuleGroups:
            pluginPosition = pluginPosition + 1
            if x["RuleGroupContent"]["PluginScopes"] is not None:
                scopeGrain = str(
                    [
                        self.concatScopeMembers(i["AttributeMemberScopes"])
                        for i in x["RuleGroupContent"]["PluginScopes"]
                    ]
                )
            else:
                scopeGrain = ""

            finalActivePluginDetails = [
                {
                    self.TENANT_NAME: self.tenantName,
                    "RuleFileName": activeRuleFile["RuleFileName"],
                    "ScopePosition": scopePosition,
                    "PluginPosition": pluginPosition,
                    "PluginText": self.constructPluginString(x),
                    self.PLUGIN_NAME: x["RuleGroupContent"]["PluginInstanceName"],
                    "ScopeGrain": scopeGrain.strip("[").strip("]"),
                    "ArgsJSON": json.dumps(x["RuleGroupContent"]["PluginArguments"]),
                    "JSONPluginPosition": x["RuleGroupLabelPosition"],
                }
            ]
            activePluginRuleDataToDB = [
                (
                    i[self.TENANT_NAME],
                    i["RuleFileName"],
                    i["ScopePosition"],
                    i["PluginPosition"],
                    i["PluginText"],
                    i[self.PLUGIN_NAME],
                    i["ScopeGrain"],
                    i["ArgsJSON"],
                    i["JSONPluginPosition"],
                )
                for i in finalActivePluginDetails
            ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO ActivePluginDetails (TenantName, RuleFileName, ScopePosition, PluginPosition, "
                    "PluginText, PluginName, ScopeGrain, ArgsJSON, JSONPluginPosition) "
                    " VALUES (?,?,?,?,?,?,?,?,?)",
                    activePluginRuleDataToDB,
                )
            except Exception as e:
                self.logger.error(
                    "Unable to insert data into ActivePluginDetails: " + str(e)
                )
                print(e)
            if x["RuleGroupContent"]["PluginScopes"] is not None:
                scopeString = str(
                    [
                        self.concatScopeMembers(i["AttributeMemberScopes"])
                        for i in x["RuleGroupContent"]["PluginScopes"]
                    ]
                )
            else:
                scopeString = ""
            finalActivePluginScopes = [
                {
                    self.TENANT_NAME: self.tenantName,
                    "RuleFileName": activeRuleFile["RuleFileName"],
                    "ScopePosition": scopePosition,
                    "ScopeDescription": x["RuleGroupDescription"],
                    "ScopeType": x["RuleGroupType"],
                    "ScopeString": scopeString.strip("[").strip("'").strip("]"),
                    "JSONScopePosition": x["ScopeLabelPosition"],
                }
            ]
            activePluginScopeDataToDB = [
                (
                    i[self.TENANT_NAME],
                    i["RuleFileName"],
                    i["ScopePosition"],
                    i["ScopeDescription"],
                    i["ScopeType"],
                    i["ScopeString"],
                    i["JSONScopePosition"],
                )
                for i in finalActivePluginScopes
            ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO ActiveRuleScopeLists (TenantName, RuleFileName, ScopePosition, ScopeDescription, "
                    "ScopeType, ScopeString, JSONScopePosition) VALUES (?,?,?,?,?,?,?)",
                    activePluginScopeDataToDB,
                )
            except Exception as e:
                self.logger.error(
                    "Unable to insert data into ActiveRuleFiles: " + str(e)
                )
                print(e)
            # if 'AttributeMemberScopes' in x['RuleGroupContent']['PluginScopes']:
            if x["RuleGroupContent"]["PluginScopes"] is not None:
                for attributeMember in x["RuleGroupContent"]["PluginScopes"]:
                    finalActivePluginScopeGrains = [
                        {
                            self.TENANT_NAME: self.tenantName,
                            "RuleFileName": activeRuleFile["RuleFileName"],
                            "ScopePosition": scopePosition,
                            self.DIM_NAME: (
                                x[self.DIM_NAME] if self.DIM_NAME in x else ""
                            ),
                            "LevelAttributeName": (
                                x["LevelAttributeName"]
                                if "LevelAttributeName" in x
                                else ""
                            ),
                            "FilterExpression": (
                                x["FilterExpression"] if "FilterExpression" in x else ""
                            ),
                        }
                        for x in attributeMember["AttributeMemberScopes"]
                    ]
                    activePluginScopeGrainDataToDB = [
                        (
                            i[self.TENANT_NAME],
                            i["RuleFileName"],
                            i["ScopePosition"],
                            i[self.DIM_NAME],
                            i["LevelAttributeName"],
                            i["FilterExpression"],
                        )
                        for i in finalActivePluginScopeGrains
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO ActiveRuleScopeGrains (TenantName, RuleFileName, ScopePosition, DimensionName, "
                            "LevelAttributeName, FilterExpression) VALUES (?,?,?,?,?,?)",
                            activePluginScopeGrainDataToDB,
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into ActiveRuleScopeGrains: "
                            + str(e)
                        )
                        print(e)

    def concatScopeMembers(self, memberScopes):
        """
        Create scope members in the desire format.
        :param memberScopes: scope data
        :return modified scope string
        """
        try:
            curMemberScope = []
            singleMemberScopeString = ""
            for obj in memberScopes:
                if (("FilterExpression" in obj) == False) or obj[
                    "FilterExpression"
                ] is None:
                    curMemberScopeValue = (
                        "["
                        + obj[self.DIM_NAME]
                        + "]"
                        + "."
                        + "["
                        + obj["LevelAttributeName"]
                        + "]"
                    )
                    curMemberScope = curMemberScope + [curMemberScopeValue]
                else:
                    curMemberScope = curMemberScope + [obj["FilterExpression"]]

            if len(curMemberScope) > 0:
                singleMemberScopeString = " * ".join(sorted(curMemberScope))
            return singleMemberScopeString

        except Exception as e:
            print(e)
            return ""

    def createActiveRuleFormulaeDB(
        self, cScopeLabel, ruleGroupLabelId, scopeLabelId, ruleFileName, scopePosition
    ):
        """
        Insert data for ActiveRuleFormulae table.
        :param cScopeLabel: scope data
        :param ruleGroupLabelId: rule group id
        :param scopeLabelId: scope id
        :param ruleFileName: rule file name
        :param scopePosition: scope position
        """
        matchingRuleGroups = filter(
            lambda x: (
                x["RuleGroupLabelId"] == ruleGroupLabelId
                and x["ScopeLabelId"] == scopeLabelId
            ),
            self.data["RuleGroups"],
        )

        sortedMatchingRuleGroup = sorted(
            matchingRuleGroups, key=lambda x: x["ScopeLabelPosition"]
        )
        formulaPosition = 0
        scopeGrainStr = (
            (
                ""
                if (
                    cScopeLabel["RuleGroupType"] == "Regular"
                    or cScopeLabel["RuleGroupType"] == "Graph"
                )
                else cScopeLabel["RuleGroupType"].lower() + " "
            )
            + "scope: ("
            + cScopeLabel["ScopeString"]
            + ");"
        )
        for x in sortedMatchingRuleGroup:
            formulaPosition = formulaPosition + 1
            finalActiveRuleFormulae = [
                {
                    self.TENANT_NAME: self.tenantName,
                    "RuleFileName": ruleFileName,
                    "ScopePosition": scopePosition,
                    "FormulaPosition": formulaPosition,
                    "FormulaStatement": x["RuleGroupContent"]["RuleGroupText"],
                    "ScopeGrain": scopeGrainStr,
                    "IsEnabled": x["IsEnabled"],
                    "JSONFormulaPosition": x["ScopeLabelPosition"],
                }
            ]
            activeRuleFormulaeDataToDB = [
                (
                    i[self.TENANT_NAME],
                    i["RuleFileName"],
                    i["ScopePosition"],
                    i["FormulaPosition"],
                    i["FormulaStatement"],
                    i["ScopeGrain"],
                    i["IsEnabled"],
                    i["JSONFormulaPosition"],
                )
                for i in finalActiveRuleFormulae
            ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO ActiveRuleFormulae (TenantName, RuleFileName, ScopePosition, FormulaPosition, "
                    "FormulaStatement, ScopeGrain, IsEnabled, JSONFormulaPosition) VALUES (?,?,?,?,?,?,?,?)",
                    activeRuleFormulaeDataToDB,
                )
            except Exception as e:
                self.logger.error(
                    "Unable to insert data into ActiveRuleFormulae: " + str(e)
                )
                print(e)

    def constructPluginString(self, pluginRuleGroup):
        """
        Construction plugin string.
        :param pluginRuleGroup: plugin rule group data
        :return: plugin string
        """
        pluginString = ""
        if pluginRuleGroup["RuleGroupContent"]["PluginScopes"] is not None:
            for curPluginScope in pluginRuleGroup["RuleGroupContent"]["PluginScopes"]:
                curMeasures = ["[" + x + "]" for x in curPluginScope["MeasureNames"]]
                pluginString = (
                    pluginString
                    + "  "
                    + "for Measures {"
                    + ",".join(curMeasures)
                    + "}"
                    + "\n"
                )  # two space indentation
                pluginString = (
                    pluginString
                    + "    "
                    + "using scope ("
                    + self.concatScopeMembers(curPluginScope["AttributeMemberScopes"])
                    + ")"
                    + "\n"
                )  # four space

        if (
            ("PluginArguments" in pluginRuleGroup["RuleGroupContent"]) == True
            and pluginRuleGroup["RuleGroupContent"]["PluginArguments"] is not None
            and len(pluginRuleGroup["RuleGroupContent"]["PluginArguments"].keys()) > 0
        ):
            pluginString = (
                pluginString
                + "  "
                + "arguments"
                + json.dumps(pluginRuleGroup["RuleGroupContent"]["PluginArguments"])
                + "\n"
            )

        if len(pluginRuleGroup["RuleGroupDescription"].strip()) == 0:
            pluginRuleGroup["RuleGroupDescription"] = " "  # single space

        if pluginRuleGroup["RuleGroupContent"]["PluginInstanceName"] is not None:
            pluginString = (
                "/*"
                + pluginRuleGroup["RuleGroupDescription"]
                + "*/"
                + "\n"
                + "declare plugin instance "
                + pluginRuleGroup["RuleGroupContent"]["PluginInstanceName"]
                + "\n"
                + pluginString[0:-1]
                + ";\n"
                + "\n"
            )

        return pluginString

    def createScopeLabels(self):
        """
        Create scope labels.
        """
        self.scopeLabels = [
            {
                "ScopeLabelId": x["Id"],
                "RuleGroupLabelId": x["RuleGroupLabelId"],
                "RuleGroupType": x["RuleGroupType"],
                "ScopeExpression": x["ScopeExpression"],
                "ScopeString": "",
                "LabelDescription": x["LabelDescription"],
                "ScopePosition": x["Position"],
            }
            for x in self.data["RuleGroupScopeLabels"]
        ]
        for curScopeLabel in self.scopeLabels:
            if (
                curScopeLabel["RuleGroupType"] == "Cartesian"
                or curScopeLabel["RuleGroupType"] == "EvaluateMember"
                or curScopeLabel["RuleGroupType"] == "Recurrence"
                or curScopeLabel["RuleGroupType"] == "Spreading"
                or curScopeLabel["RuleGroupType"] == "Block"
                or curScopeLabel["RuleGroupType"] == "Regular"
            ):
                if "AttributeMemberScopes" in curScopeLabel["ScopeExpression"]:
                    curScopeLabel["ScopeString"] = self.concatScopeMembers(
                        curScopeLabel["ScopeExpression"]["AttributeMemberScopes"]
                    )

            elif curScopeLabel["RuleGroupType"] == "Graph":
                VersionScopeString = ""
                if (
                    (
                        "FilterExpression"
                        in curScopeLabel["ScopeExpression"]["VersionScope"]
                    )
                    == False
                ) or (
                    curScopeLabel["ScopeExpression"]["VersionScope"]["FilterExpression"]
                    is None
                ):
                    VersionScopeString = (
                        "["
                        + curScopeLabel["ScopeExpression"]["VersionScope"][
                            self.DIM_NAME
                        ]
                        + "]"
                        + "."
                        + "["
                        + curScopeLabel["ScopeExpression"]["VersionScope"][
                            "LevelAttributeName"
                        ]
                        + "]"
                    )
                else:
                    VersionScopeString = curScopeLabel["ScopeExpression"][
                        "VersionScope"
                    ]["FilterExpression"]

                try:
                    FromScopeString = self.concatScopeMembers(
                        curScopeLabel["ScopeExpression"]["FromScopes"]
                    )
                except:
                    FromScopeString = " -FromScopeNotDefined- "
                try:
                    ToScopeString = self.concatScopeMembers(
                        curScopeLabel["ScopeExpression"]["ToScopes"]
                    )
                except:
                    ToScopeString = " -ToScopeNotDefined- "

                curScopeLabel["ScopeString"] = (
                    "["
                    + curScopeLabel["ScopeExpression"]["RelationshipTypeName"]
                    + "]"
                    + ", "
                    + VersionScopeString
                    + ", "
                    + "from "
                    + FromScopeString
                    + ", "
                    + "to "
                    + ToScopeString
                )

    def createRuleFiles(self):
        """
        Create rule files data in a global variable.
        """
        self.ruleFile = [
            {
                "RuleGroupLabelId": x["Id"],
                "RuleFileName": x["LabelName"],
                "LabelType": x["LabelType"],
                "FinalFileName": x["LabelName"].replace(":", ""),
                "Position": x["Position"],
                "LabelDescription": x["LabelDescription"],
                "ActiveRuleText": "",
                "NamedSetArray": [],
                "ProcedureData": [],
            }
            for x in self.data["RuleGroupLabels"]
        ]

    def extractIBPLRules(self):
        """
        Extract and insert data security data to DataSecurityIBPLRules table.
        """
        self.logger.info("Extracting Data Security Rules.")
        print("Extracting Data Security Rules.")
        ibplRulesList = self.data["IbplRules"]
        for ibplRule in ibplRulesList:
            if "ScriptType" in ibplRule and ibplRule["ScriptType"] == "DataSecurity":
                try:
                    self.dbConnection.execute(
                        "INSERT INTO DataSecurityIBPLRules (TenantName, DataSecurityRuleName, IsActive, ScriptCode) "
                        "VALUES (?,?,?,?)",
                        (
                            self.tenantName,
                            ibplRule["Name"] if "Name" in ibplRule else None,
                            ibplRule["IsActive"] if "IsActive" in ibplRule else None,
                            ibplRule["Script"] if "Script" in ibplRule else None,
                        ),
                    )
                except Exception as e:
                    self.logger.error(
                        "ERROR inserting data into DataSecurityIBPLRules: " + str(e)
                    )
                    print("ERROR inserting data into DataSecurityIBPLRules: " + str(e))
