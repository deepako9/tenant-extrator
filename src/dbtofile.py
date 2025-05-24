import logging
import csv
import sys
import os
import errno
import re
from pandas import ExcelWriter, DataFrame, read_sql


class DBToFiles:
    def __init__(self, dbConnection, modelFilePath, uiFilePath):
        """
        DBToFiles Constructor.
        :param dbConnection: Database connection
        :param modelFilePath: Model files path
        :param uiFilePath: UI files path
        """
        self.dbConn = dbConnection
        self.dbConnection = dbConnection.cursor()
        self.modelFilePath = modelFilePath # This is commonObj.destDir from streamlit_app.py
        self.uiFilePath = uiFilePath
        # Define a specific base path for all general CSV exports
        self.csv_export_base_path = os.path.join(self.modelFilePath, "csv_exports")
        try:
            self.logger = logging.getLogger("extractor-logger")
        except:
            print("Unable to set log file, Exiting.")
            sys.exit()

    def generateCSVArrays(self):
        print("Creating CSV Files...")
        # 'NodeCombos', 'NodeCombosConditionalFormats'
        modelList = [
            "Dimensions",
            "DimAttributes",
            "DimAttrProperties",
            "DimAttrTranslations",
            "DimAttrPropTranslations",
            "DimHierarchies",
            "DimHierLevels",
            "Graphs",
            "GraphEdges",
            "GraphEdgeTranslations",
            "GraphFromNodes",
            "GraphNodeTranslations",
            "GraphToNodes",
            "Plans",
            "Picklists",
            "DimAliases",
            "DimAttrAliases",
            "PickListValues",
            "NamedSets",
        ]
        planList = []
        abList = [
            "ActionButtonBindingsForWeb",
            "ActionButtonDataSource",
            "ActionButtonDetails",
            "ActionButtonFieldBindings",
            "ActionButtonJSRules",
            "ActionButtonRules",
            "ExcelActionButtonsForWidget",
        ]
        uiList = []
        self.dbConnection.execute('SELECT name FROM sqlite_master WHERE type="table";')
        fetchData = self.dbConnection.fetchall()
        for i in fetchData:
            tableName = i["name"]
            df = read_sql(f"SELECT * FROM {tableName};", self.dbConn)
            df_columns = list(df.columns)
            if "TenantName" in df_columns:
                df_columns.remove("TenantName")
            df = df[df_columns]
            df.sort_values(by=df_columns, inplace=True)
            filePath = self.modelFilePath
            if tableName in modelList:
                filePath = self.modelFilePath
            elif tableName in abList:
                filePath = os.path.join(self.modelFilePath, "ActionButtons")
            os.makedirs(filePath, exist_ok=True)
            df.to_csv(f"{filePath}/{tableName}.csv", index=False)

    def createDSRulesFile(self):
        """
        Create data security rule files.
        :return: null
        """
        try:
            self.dbConnection.execute(
                "SELECT * from DataSecurityIBPLRules ORDER BY DataSecurityRuleName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            dsRulesData = [
                {
                    "DataSecurityRuleName": i["DataSecurityRuleName"],
                    "IsActive": i["IsActive"],
                    "ScriptCode": i["ScriptCode"],
                }
                for i in fetchData
            ]
            filename = "DataSecurityRules/DataSecurityIBPLRulesDetail.csv"
            self.createCSV(filename, dsRulesData)
            for dsRule in dsRulesData:
                if dsRule["ScriptCode"]:
                    filename = (
                        "DataSecurityRules/" + dsRule["DataSecurityRuleName"] + ".ibpl"
                    ).replace(":", "")
                    self.createEntityFile(
                        self.replaceNewLine(dsRule["ScriptCode"]), filename
                    )

        except Exception as e:
            self.logger.error("" + str(e))
            print("" + str(e))

    def createExcelFromDB(self):
        """
        Create xlsx files from all the tables in the database.
        :return: null
        """
        self.logger.info("Generating Excel File.")
        print("Generating Excel File.")
        self.dbConnection.execute('SELECT name FROM sqlite_master WHERE type="table";')
        tables = self.dbConnection.fetchall()
        xlFileLocation = self.modelFilePath.split("_Models")[0] + ".xlsx"
        writer = ExcelWriter(xlFileLocation, engine="xlsxwriter")
        workbook = writer.book
        headerFormat = workbook.add_format(
            {
                "bold": True,
                "text_wrap": True,
                "valign": "top",
                "fg_color": "#ABABAB",
                "border": 1,
            }
        )
        for tableData in tables:
            tableName = tableData[0]
            self.dbConnection.execute(
                'SELECT name FROM PRAGMA_TABLE_INFO("' + tableName + '");'
            )
            fetchHeaderData = self.dbConnection.fetchall()
            headerData = [i[0] for i in fetchHeaderData]
            data = []
            self.dbConnection.execute("SELECT * FROM " + tableName + ";")
            fetchRowData = self.dbConnection.fetchall()
            for row in fetchRowData:
                data.append(row)
            df = DataFrame(data)
            if len(tableName) > 31:
                tableName = ""
                if tableData[0] == "MeasureGroupAsGraphGranularities":
                    tableName = "MGAsGrpGranularities"
                    df.to_excel(
                        writer,
                        index=False,
                        sheet_name=tableName,
                        header=False,
                        startrow=1,
                    )
                    worksheet = writer.sheets[tableName]
                elif tableData[0] == "ExcelActionButtonsForWidget":
                    tableName = "XLActionButtons"
                    df.to_excel(
                        writer,
                        index=False,
                        sheet_name=tableName,
                        header=False,
                        startrow=1,
                    )
                    worksheet = writer.sheets[tableName]
                elif tableData[0] == "WidgetDefinitionProperties":
                    tableName = "WidgetPresentation"
                    df.to_excel(
                        writer,
                        index=False,
                        sheet_name=tableName,
                        header=False,
                        startrow=1,
                    )
                    worksheet = writer.sheets[tableName]
                else:
                    print(
                        "Error table name: exceed 31 chars. Add condition for: "
                        + tableData[0]
                    )
                    worksheet = writer.sheets[tableName]
            else:
                df.to_excel(
                    writer, index=False, sheet_name=tableName, header=False, startrow=1
                )
                worksheet = writer.sheets[tableName]

            i = 0
            for hData in headerData:
                worksheet.write(0, i, hData, headerFormat)
                worksheet.freeze_panes(1, 0)
                i = i + 1
        # writer.save()
        writer.close()
        print("File saved: " + xlFileLocation)
        self.logger.info("File saved: " + xlFileLocation)

    def createDimCSVArrays(self):
        """
        Create dimension data related csv files.
        :return: null
        """
        self.logger.info("Creating dimensions CSV array.")
        # Dimensions file
        try:
            self.dbConnection.execute(
                "SELECT * from Dimensions ORDER BY DimensionName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            dimensionData = [
                {
                    "DimensionName": i["DimensionName"],
                    "DimensionDescription": i["DimensionDescription"],
                    "DimensionType": i["DimensionType"],
                }
                for i in fetchData
            ]
            filename = "Dimensions.csv"
            self.createCSV(filename, dimensionData)

        except Exception as e:
            self.logger.error("Error fetching Dimensions data: " + str(e))
            print("Error fetching Dimensions data: ", str(e))

        # DimAliases file
        try:
            self.dbConnection.execute(
                "SELECT * from DimAliases ORDER BY DimensionName ASC, AliasName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            dimAliasesData = [
                {
                    "DimensionName": i["DimensionName"],
                    "AliasName": i["AliasName"],
                    "AliasDescription": i["AliasDescription"],
                }
                for i in fetchData
            ]
            filename = "DimAliases.csv"
            self.createCSV(filename, dimAliasesData)

        except Exception as e:
            self.logger.error("Error fetching DimAliases data: " + str(e))
            print("Error fetching DimAliases data: ", str(e))

        # DimAttribute file
        try:
            self.dbConnection.execute(
                "SELECT * from DimAttributes ORDER BY DimensionName ASC, AttributeName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            dimAttributesData = [
                {
                    "DimensionName": i["DimensionName"],
                    "AttributeName": i["AttributeName"],
                    "Description": i["Description"],
                    "KeyColumnDataType": i["KeyColumnDataType"],
                    "IsKey": "True" if i["IsKey"] == "1" else "False",
                    "SeedTags": "True" if i["SeedTags"] == "1" else "False",
                }
                for i in fetchData
            ]
            filename = "DimAttributes.csv"
            self.createCSV(filename, dimAttributesData)

        except Exception as e:
            self.logger.error("Error fetching DimAttributes data: " + str(e))
            print("Error fetching DimAttributes data: ", str(e))

        # DimAttrProperties file
        try:
            self.dbConnection.execute(
                "SELECT * from DimAttrProperties ORDER BY DimensionName ASC, AttributeName ASC, PropertyName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            dimAttrPropertiesData = [
                {
                    "DimensionName": i["DimensionName"],
                    "AttributeName": i["AttributeName"],
                    "PropertyName": i["PropertyName"],
                    "Description": i["Description"],
                    "KeyColumnDataType": i["KeyColumnDataType"],
                }
                for i in fetchData
            ]
            filename = "DimAttrProperties.csv"
            self.createCSV(filename, dimAttrPropertiesData)

        except Exception as e:
            self.logger.error("Error fetching DimAttrProperties data: " + str(e))
            print("Error fetching DimAttrProperties data: ", str(e))

        # DimAttrTranslations file
        try:
            self.dbConnection.execute(
                "SELECT * from DimAttrTranslations ORDER BY DimensionName ASC, AttributeName ASC, LCID ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            dimAttrTranslationsData = [
                {
                    "DimensionName": i["DimensionName"],
                    "AttributeName": i["AttributeName"],
                    "TranslationName": i["TranslationName"],
                    "Description": i["Description"],
                    "LCID": i["LCID"],
                    "Language": i["Language"],
                }
                for i in fetchData
            ]
            filename = "DimAttrTranslations.csv"
            self.createCSV(filename, dimAttrTranslationsData)

        except Exception as e:
            self.logger.error("Error fetching DimAttrTranslations data: " + str(e))
            print("Error fetching DimAttrTranslations data: ", str(e))

        # DimAttrAliases file
        try:
            self.dbConnection.execute(
                "SELECT * from DimAttrAliases ORDER BY DimensionName ASC, AttributeName ASC, AliasName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            dimAttrAliasesData = [
                {
                    "DimensionName": i["DimensionName"],
                    "AttributeName": i["AttributeName"],
                    "AliasName": i["AliasName"],
                    "AliasDescription": i["AliasDescription"],
                }
                for i in fetchData
            ]
            filename = "DimAttrAliases.csv"
            self.createCSV(filename, dimAttrAliasesData)

        except Exception as e:
            self.logger.error("Error fetching DimAttrAliases data: " + str(e))
            print("Error fetching DimAttrAliases data: ", str(e))

        # DimAttrPropTranslations file
        try:
            self.dbConnection.execute(
                "SELECT * from DimAttrPropTranslations ORDER BY DimensionName ASC, AttributeName ASC, LCID ASC, "
                "PropertyName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            dimAttrPropTranslationsData = [
                {
                    "DimensionName": i["DimensionName"],
                    "AttributeName": i["AttributeName"],
                    "PropertyName": i["PropertyName"],
                    "TranslationName": i["TranslationName"],
                    "Description": i["Description"],
                    "LCID": i["LCID"],
                    "Language": i["Language"],
                }
                for i in fetchData
            ]
            filename = "DimAttrPropTranslations.csv"
            self.createCSV(filename, dimAttrPropTranslationsData)

        except Exception as e:
            self.logger.error("Error fetching DimAttrPropTranslations data: " + str(e))
            print("Error fetching DimAttrPropTranslations data: ", str(e))

        # DimHierarchies file
        try:
            self.dbConnection.execute(
                "SELECT * from DimHierarchies ORDER BY DimensionName ASC, HierarchyName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            dimHierarchiesData = [
                {
                    "DimensionName": i["DimensionName"],
                    "HierarchyName": i["HierarchyName"],
                    "HierarchyDescription": i["HierarchyDescription"],
                }
                for i in fetchData
            ]
            filename = "DimHierarchies.csv"
            self.createCSV(filename, dimHierarchiesData)

        except Exception as e:
            self.logger.error("Error fetching DimHierarchies data: " + str(e))
            print("Error fetching DimHierarchies data: ", str(e))

        # DimHierLevels file
        try:
            self.dbConnection.execute(
                "SELECT * from DimHierLevels ORDER BY DimensionName ASC, HierarchyName ASC, LevelPosition ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            dimHierLevelsData = [
                {
                    "DimensionName": i["DimensionName"],
                    "HierarchyName": i["HierarchyName"],
                    "LevelPosition": i["LevelPosition"],
                    "LevelName": i["LevelName"],
                    "LevelDescription": i["LevelDescription"],
                }
                for i in fetchData
            ]
            filename = "DimHierLevels.csv"
            self.createCSV(filename, dimHierLevelsData)

        except Exception as e:
            self.logger.error("Error fetching DimHierLevels data: " + str(e))
            print("Error fetching DimHierLevels data: ", str(e))

        # Picklists file
        try:
            self.dbConnection.execute(
                "SELECT * from Picklists ORDER BY PickListName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            picklistsData = [
                {
                    "PickListName": i["PickListName"],
                    "PickListDescription": i["PickListDescription"],
                    "DataType": i["DataType"],
                    "IsMultiSelectAllowed": (
                        "True" if i["IsMultiSelectAllowed"] == "1" else "False"
                    ),
                }
                for i in fetchData
            ]
            filename = "Picklists.csv"
            self.createCSV(filename, picklistsData)

        except Exception as e:
            self.logger.error("Error fetching Picklists data: " + str(e))
            print("Error fetching Picklists data: ", str(e))

        # PickListValues file
        try:
            self.dbConnection.execute(
                "SELECT * from PickListValues ORDER BY PickListName ASC, DisplayPosition ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            pickListValuesData = [
                {
                    "PickListName": i["PickListName"],
                    "Value": i["Value"],
                    "DisplayName": i["DisplayName"],
                    "DisplayPosition": i["DisplayPosition"],
                }
                for i in fetchData
            ]
            filename = "PickListValues.csv"
            self.createCSV(filename, pickListValuesData)

        except Exception as e:
            self.logger.error("Error fetching PickListValues data: " + str(e))
            print("Error fetching PickListValues data: ", str(e))

    def createGraphCSVArrays(self):
        """
        Create Graph data related csv files.
        :return: null
        """
        self.logger.info("Creating Graphs CSV array.")
        # Graphs file
        try:
            self.dbConnection.execute(
                "SELECT * from Graphs ORDER BY RelationshipTypeName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            graphData = [
                {
                    "RelationshipTypeName": i["RelationshipTypeName"],
                    "RelationshipTypeDescription": i["RelationshipTypeDescription"],
                }
                for i in fetchData
            ]
            filename = "Graphs.csv"
            self.createCSV(filename, graphData)

        except Exception as e:
            self.logger.error("Error fetching Graphs data: " + str(e))
            print("Error fetching Graphs data: ", str(e))

        # GraphEdgeTranslations file
        try:
            self.dbConnection.execute(
                "SELECT * from GraphEdgeTranslations ORDER BY RelationshipTypeName ASC, EdgeName ASC, LCID ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            graphEdgeTranslationsData = [
                {
                    "RelationshipTypeName": i["RelationshipTypeName"],
                    "EdgeName": i["EdgeName"],
                    "PropertyName": i["PropertyName"],
                    "PropertyDescription": i["PropertyDescription"],
                    "LCID": i["LCID"],
                    "Language": i["Language"],
                }
                for i in fetchData
            ]
            filename = "GraphEdgeTranslations.csv"
            self.createCSV(filename, graphEdgeTranslationsData)

        except Exception as e:
            self.logger.error("Error fetching GraphEdgeTranslations data: " + str(e))
            print("Error fetching GraphEdgeTranslations data: ", str(e))

        # GraphNodeTranslations file
        try:
            self.dbConnection.execute(
                "SELECT * from GraphNodeTranslations ORDER BY RelationshipTypeName ASC, IsTailNode ASC, "
                "DimensionName ASC, LCID ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            graphNodeTranslationsData = [
                {
                    "RelationshipTypeName": i["RelationshipTypeName"],
                    "DimensionName": i["DimensionName"],
                    "AttributeName": i["AttributeName"],
                    "IsTailNode": "True" if i["IsTailNode"] == "1" else "False",
                    "TransAttributeName": i["TransAttributeName"],
                    "Description": i["Description"],
                    "LCID": i["LCID"],
                    "Language": i["Language"],
                }
                for i in fetchData
            ]
            filename = "GraphNodeTranslations.csv"
            self.createCSV(filename, graphNodeTranslationsData)

        except Exception as e:
            self.logger.error("Error fetching GraphNodeTranslations data: " + str(e))
            print("Error fetching GraphNodeTranslations data: ", str(e))

        # GraphFromNodes file
        try:
            self.dbConnection.execute(
                "SELECT * from GraphFromNodes ORDER BY RelationshipTypeName ASC, DimensionName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            graphFromNodesData = [
                {
                    "RelationshipTypeName": i["RelationshipTypeName"],
                    "DimensionName": i["DimensionName"],
                    "AttributeName": i["AttributeName"],
                }
                for i in fetchData
            ]
            filename = "GraphFromNodes.csv"
            self.createCSV(filename, graphFromNodesData)

        except Exception as e:
            self.logger.error("Error fetching GraphFromNodes data: " + str(e))
            print("Error fetching GraphFromNodes data: ", str(e))

        # GraphToNodes file
        try:
            self.dbConnection.execute(
                "SELECT * from GraphToNodes ORDER BY RelationshipTypeName ASC, DimensionName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            graphToNodesData = [
                {
                    "RelationshipTypeName": i["RelationshipTypeName"],
                    "DimensionName": i["DimensionName"],
                    "AttributeName": i["AttributeName"],
                }
                for i in fetchData
            ]
            filename = "GraphToNodes.csv"
            self.createCSV(filename, graphToNodesData)

        except Exception as e:
            self.logger.error("Error fetching GraphToNodes data: " + str(e))
            print("Error fetching GraphToNodes data: ", str(e))

        # GraphEdges file
        try:
            self.dbConnection.execute(
                "SELECT * from GraphEdges ORDER BY RelationshipTypeName ASC, PropertyName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            graphEdgesData = [
                {
                    "RelationshipTypeName": i["RelationshipTypeName"],
                    "PropertyName": i["PropertyName"],
                    "PropertyDescription": i["PropertyDescription"],
                    "PropertyDataType": i["PropertyDataType"],
                    "AggregateFunction": i["AggregateFunction"],
                    "IsEditable": "True" if i["IsEditable"] == "1" else "False",
                    "FormatString": i["FormatString"],
                }
                for i in fetchData
            ]
            filename = "GraphEdges.csv"
            self.createCSV(filename, graphEdgesData)

        except Exception as e:
            self.logger.error("Error fetching GraphEdges data: " + str(e))
            print("Error fetching GraphEdges data: ", str(e))

        # NodeCombosConditionalFormats file
        try:
            self.dbConnection.execute(
                "SELECT * from NodeCombosConditionalFormats ORDER BY StringID ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            nodeCombosConditionalFormatsData = [
                {
                    "StringID": i["StringID"],
                    "PropertyName": i["PropertyName"],
                    "RelationshipTypeName": i["RelationshipTypeName"],
                    "PropertyDescription": i["PropertyDescription"],
                    "PropertyDataType": i["PropertyDataType"],
                    "PropertyDataSize": i["PropertyDataSize"],
                    "PropertyFormula": i["PropertyFormula"],
                    "IsTailNode": i["IsTailNode"],
                }
                for i in fetchData
            ]
            filename = "NodeCombosConditionalFormats.csv"
            self.createCSV(filename, nodeCombosConditionalFormatsData)

        except Exception as e:
            self.logger.error(
                "Error fetching NodeCombosConditionalFormats data: " + str(e)
            )
            print("Error fetching NodeCombosConditionalFormats data: ", str(e))

        # NodeCombos file
        try:
            self.dbConnection.execute(
                "SELECT * from NodeCombos ORDER BY StringID ASC, DimensionName ASC, AttributeName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            nodeCombosData = [
                {
                    "StringID": i["StringID"],
                    "DimensionName": i["DimensionName"],
                    "AttributeName": i["AttributeName"],
                }
                for i in fetchData
            ]
            filename = "NodeCombos.csv"
            self.createCSV(filename, nodeCombosData)

        except Exception as e:
            self.logger.error("Error fetching NodeCombos data: " + str(e))
            print("Error fetching NodeCombos data: ", str(e))

    def createPlanCSVArrays(self):
        """
        Create Plan related csv files.
        :return: null
        """
        self.logger.info("Creating plans CSV array.")
        # Plans file
        plansData = []
        try:
            self.dbConnection.execute("SELECT * from Plans ORDER BY PlanName ASC;")
            fetchData = self.dbConnection.fetchall()
            plansData = [
                {"PlanName": i["PlanName"], "PlanDescription": i["PlanDescription"]}
                for i in fetchData
            ]
            filename = "Plans.csv"
            self.createCSV(filename, plansData)

        except Exception as e:
            self.logger.error("Error fetching Plans data: " + str(e))
            print("Error fetching Plans data: ", str(e))

        planNameList = [i["PlanName"] for i in plansData]

        for plan in planNameList:
            # MeasureGroups file
            try:
                self.dbConnection.execute(
                    "SELECT MeasureGroupName, MeasureGroupDescription, GranularityAsSingleString from MeasureGroups "
                    "WHERE PlanName=? ORDER BY MeasureGroupName;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                measureGroupsData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "MeasureGroupDescription": i["MeasureGroupDescription"],
                        "GranularityAsSingleString": i["GranularityAsSingleString"],
                    }
                    for i in fetchData
                ]
                filename = "Plans/" + plan + "/MeasureGroups." + plan + ".csv"
                self.createCSV(filename, measureGroupsData)

            except Exception as e:
                self.logger.error("Error fetching MeasureGroups data: " + str(e))
                print("Error fetching MeasureGroups data: ", str(e))

            # MeasureGroupTranslations file
            try:
                self.dbConnection.execute(
                    "SELECT MeasureGroupName, MeasureGroupTranslationName, MeasureGroupTranslationDescription, "
                    "Language from MeasureGroupTranslations WHERE PlanName=? ORDER BY MeasureGroupName;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                measureGroupsData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "MeasureGroupTranslationName": i["MeasureGroupTranslationName"],
                        "MeasureGroupTranslationDescription": i[
                            "MeasureGroupTranslationDescription"
                        ],
                        "Language": i["Language"],
                    }
                    for i in fetchData
                ]
                filename = (
                    "Plans/" + plan + "/MeasureGroupTranslations." + plan + ".csv"
                )
                self.createCSV(filename, measureGroupsData)

            except Exception as e:
                self.logger.error(
                    "Error fetching MeasureGroupTranslations data: " + str(e)
                )
                print("Error fetching MeasureGroupTranslations data: ", str(e))

            # MeasureGrpGranularity file
            try:
                self.dbConnection.execute(
                    "SELECT * from MeasureGrpGranularity WHERE PlanName=? ORDER BY MeasureGroupName ASC, "
                    "DimensionName ASC;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                measureGrpGranularityData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "DimensionName": i["DimensionName"],
                        "AttributeName": i["AttributeName"],
                        "SortOrder": i["SortOrder"],
                    }
                    for i in fetchData
                ]
                filename = "Plans/" + plan + "/MeasureGrpGranularity." + plan + ".csv"
                self.createCSV(filename, measureGrpGranularityData)

            except Exception as e:
                self.logger.error(
                    "Error fetching MeasureGrpGranularity data: " + str(e)
                )
                print("Error fetching MeasureGrpGranularity data: ", str(e))

            # MeasureGrpExternalConfigs file
            try:
                self.dbConnection.execute(
                    "SELECT * from MeasureGrpExternalConfigs WHERE PlanName=? ORDER BY MeasureGroupName ASC;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                externalConfigData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "NeedsRedeployment": i["NeedsRedeployment"],
                        "DeploymentStatus": i["DeploymentStatus"],
                        "MaintainLocalCache": i["MaintainLocalCache"],
                        "DeploymentStatusMessage": i["DeploymentStatusMessage"],
                        "ExternalConfigJson": i["ExternalConfigJson"],
                        "DataSourceType": i["DataSourceType"],
                    }
                    for i in fetchData
                ]
                filename = f"Plans/{plan}/MeasureGrpExternalConfigs.{plan}.csv"
                self.createCSV(filename, externalConfigData)

            except Exception as e:
                self.logger.error(
                    f"Error fetching MeasureGrpExternalConfigs data: {str(e)}"
                )
                print(f"Error fetching MeasureGrpExternalConfigs data: {str(e)}")

            # MGrpAsGraphGranularity file
            try:
                self.dbConnection.execute(
                    "SELECT * from MeasureGroupAsGraphGranularities WHERE PlanName=? ORDER BY MeasureGroupName ASC, "
                    "IsTailNode ASC, DimensionName ASC;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                mGrpAsGraphGranularityData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "DimensionName": i["DimensionName"],
                        "AttributeName": i["AttributeName"],
                        "IsTailNode": "True" if i["IsTailNode"] == "1" else "False",
                    }
                    for i in fetchData
                ]
                filename = (
                    "Plans/"
                    + plan
                    + "/MeasureGroupAsGraphGranularities."
                    + plan
                    + ".csv"
                )
                self.createCSV(filename, mGrpAsGraphGranularityData)

            except Exception as e:
                self.logger.error(
                    "Error fetching MeasureGroupAsGraphGranularities data: " + str(e)
                )
                print("Error fetching MeasureGroupAsGraphGranularities data: ", str(e))

            # Measures file
            try:
                self.dbConnection.execute(
                    "SELECT * from Measures WHERE PlanName=? ORDER BY MeasureGroupName ASC, MeasureName ASC;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                measuresData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "MeasureName": i["MeasureName"],
                        "MeasureColumnName": i["MeasureColumnName"],
                        "MeasureDescription": i["MeasureDescription"],
                        "Tags": i["Tags"],
                        "AggregateFunction": i["AggregateFunction"],
                        "DataType": i["DataType"],
                        "FormatString": i["FormatString"],
                        "IsEditable": "True" if i["IsEditable"] == "1" else "False",
                        "IsReportingMeasure": (
                            "True" if i["IsReportingMeasure"] == "1" else "False"
                        ),
                        "MeasureType": i["MeasureType"],
                        "AssociationAsGraph": (
                            "True" if i["AssociationAsGraph"] == "1" else "False"
                        ),
                        "ToolTip": i["ToolTip"],
                        "ValidationFormula": i["ValidationFormula"],
                        "ValidationTooltip": i["ValidationTooltip"],
                        "UsedAsIBPLCount": i["UsedAsIBPLCount"],
                        "UsedAsNonIBPLCount": i["UsedAsNonIBPLCount"],
                        "TotalUsageCount": i["TotalUsageCount"],
                        "ConversionFormula": i["ConversionFormula"],
                        "ApplyConversion": i["ApplyConversion"],
                    }
                    for i in fetchData
                ]
                filename = "Plans/" + plan + "/Measures." + plan + ".csv"
                self.createCSV(filename, measuresData)

            except Exception as e:
                self.logger.error("Error fetching Measures data: " + str(e))
                print("Error fetching Measures data: ", str(e))

            # MeasureConditionalFormats file
            try:
                self.dbConnection.execute(
                    "SELECT * from MeasureConditionalFormats WHERE PlanName=? ORDER BY MeasureGroupName ASC, "
                    "MeasureName ASC;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                measureConditionalFormatsData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "MeasureName": i["MeasureName"],
                        "MeasureDescription": i["MeasureDescription"],
                        "BgColorFormula": i["BgColorFormula"],
                        "FgColorFormula": i["FgColorFormula"],
                        "TrendFormula": i["TrendFormula"],
                        "FormattingViewModel": i["FormattingViewModel"],
                    }
                    for i in fetchData
                ]
                filename = (
                    "Plans/" + plan + "/MeasureConditionalFormats." + plan + ".csv"
                )
                self.createCSV(filename, measureConditionalFormatsData)

            except Exception as e:
                self.logger.error(
                    "Error fetching MeasureConditionalFormats data: " + str(e)
                )
                print("Error fetching MeasureConditionalFormats data: ", str(e))

            # MeasurePickLists file
            try:
                self.dbConnection.execute(
                    "SELECT * from MeasurePickLists WHERE PlanName=? ORDER BY MeasureGroupName ASC, MeasureName ASC;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                measurePickListsData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "MeasureName": i["MeasureName"],
                        "MeasureDescription": i["MeasureDescription"],
                        "AggregateFunction": i["AggregateFunction"],
                        "DataType": i["DataType"],
                        "FormatString": i["FormatString"],
                        "IsEditable": "True" if i["IsEditable"] == "1" else "False",
                        "MeasureType": i["MeasureType"],
                        "PickListName": i["PickListName"],
                    }
                    for i in fetchData
                ]
                filename = "Plans/" + plan + "/MeasurePickLists." + plan + ".csv"
                self.createCSV(filename, measurePickListsData)

            except Exception as e:
                self.logger.error("Error fetching MeasurePickLists data: " + str(e))
                print("Error fetching MeasurePickLists data: ", str(e))

            # MeasureFormulae file
            try:
                self.dbConnection.execute(
                    "SELECT * from MeasureFormulae WHERE PlanName=? ORDER BY MeasureGroupName ASC, MeasureName ASC;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                measureFormulaeData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "MeasureName": i["MeasureName"],
                        "MeasureDescription": i["MeasureDescription"],
                        "AggregateFunction": i["AggregateFunction"],
                        "DataType": i["DataType"],
                        "FormatString": i["FormatString"],
                        "IsEditable": "True" if i["IsEditable"] == "1" else "False",
                        "MeasureType": i["MeasureType"],
                        "MeasureFormula": self.replaceNewLine(i["MeasureFormula"]),
                    }
                    for i in fetchData
                ]
                filename = "Plans/" + plan + "/MeasureFormulae." + plan + ".csv"
                self.createCSV(filename, measureFormulaeData)

            except Exception as e:
                self.logger.error("Error fetching MeasureFormulae data: " + str(e))
                print("Error fetching MeasureFormulae data: ", str(e))

            # MeasureSpreads file
            try:
                self.dbConnection.execute(
                    "SELECT * from MeasureSpreads WHERE PlanName=? ORDER BY MeasureGroupName ASC, MeasureName ASC, "
                    "BasisMeasureName ASC;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                measureSpreadsData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "MeasureName": i["MeasureName"],
                        "BasisMeasureName": i["BasisMeasureName"],
                        "BasisMeasureType": i["BasisMeasureType"],
                        "SpreadingType": i["SpreadingType"],
                    }
                    for i in fetchData
                ]
                filename = "Plans/" + plan + "/MeasureSpreads." + plan + ".csv"
                self.createCSV(filename, measureSpreadsData)

            except Exception as e:
                self.logger.error("Error fetching MeasureSpreads data: " + str(e))
                print("Error fetching MeasureSpreads data: ", str(e))

            # MeasureAggregates file
            try:
                self.dbConnection.execute(
                    "SELECT * from MeasureAggregates WHERE PlanName=? ORDER BY MeasureGroupName ASC, MeasureName ASC, "
                    "OrderNumber ASC;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                measureSpreadsData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "MeasureName": i["MeasureName"],
                        "AggregateFunction": i["AggregateFunction"],
                        "OrderNumber": i["OrderNumber"],
                        "DimensionName": i["DimensionName"],
                    }
                    for i in fetchData
                ]
                filename = "Plans/" + plan + "/MeasureAggregates." + plan + ".csv"
                self.createCSV(filename, measureSpreadsData)

            except Exception as e:
                self.logger.error("Error fetching MeasureSpreads data: " + str(e))
                print("Error fetching MeasureSpreads data: ", str(e))

            # MeasureTranslations file
            try:
                self.dbConnection.execute(
                    "SELECT * from MeasureTranslations WHERE PlanName=? ORDER BY MeasureGroupName ASC, "
                    "MeasureName ASC, LCID ASC;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                measureTranslationsData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "MeasureName": i["MeasureName"],
                        "TranslationName": i["TranslationName"],
                        "TranslationDesc": i["TranslationDesc"],
                        "ToolTip": i["ToolTip"],
                        "LCID": i["LCID"],
                        "Language": i["Language"],
                    }
                    for i in fetchData
                ]
                filename = "Plans/" + plan + "/MeasureTranslations." + plan + ".csv"
                self.createCSV(filename, measureTranslationsData)

            except Exception as e:
                self.logger.error("Error fetching MeasureTranslations data: " + str(e))
                print("Error fetching MeasureTranslations data: ", str(e))

            # MeasureTwins file
            try:
                self.dbConnection.execute(
                    "SELECT * from MeasureTwins WHERE PlanName=? ORDER BY MeasureGroupName ASC, PrimaryMeasureName ASC,"
                    " TwinMeasureName ASC;",
                    (plan,),
                )
                fetchData = self.dbConnection.fetchall()
                measureTwinsData = [
                    {
                        "MeasureGroupName": i["MeasureGroupName"],
                        "PrimaryMeasureName": i["PrimaryMeasureName"],
                        "TwinMeasureName": i["TwinMeasureName"],
                        "TwinToPrimaryFormula": i["TwinToPrimaryFormula"],
                        "ExternalChangeUpdatesPrimary": (
                            "True"
                            if i["ExternalChangeUpdatesPrimary"] == "1"
                            else "False"
                        ),
                    }
                    for i in fetchData
                ]
                filename = "Plans/" + plan + "/MeasureTwins." + plan + ".csv"
                self.createCSV(filename, measureTwinsData)

            except Exception as e:
                self.logger.error("Error fetching MeasureTwins data: " + str(e))
                print("Error fetching MeasureTwins data: ", str(e))

    def createActionButtonCSVArrays(self):
        """
        Create action button related data to csv files.
        :return: null
        """
        # ActionButtonDetails file
        try:
            self.dbConnection.execute(
                "SELECT * from ActionButtonDetails ORDER BY ActionButtonName ASC, Tooltip ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            actionButtonDetailList = [
                {
                    "ActionButtonName": i["ActionButtonName"],
                    "Tooltip": i["Tooltip"],
                    "ActionButtonType": i["ActionButtonType"],
                    "Alignment": i["Alignment"],
                    "IsPopOver": i["IsPopOver"],
                    "IsGlobal": i["IsGlobal"],
                    "ConfigJson": i["ConfigJson"],
                }
                for i in fetchData
            ]
            filename = "ActionButtons/ActionButtonDetails.csv"
            self.createCSV(filename, actionButtonDetailList)

        except Exception as e:
            self.logger.error("Error fetching ActionButtonDetails data: " + str(e))
            print("Error fetching ActionButtonDetails data: ", str(e))

        # ActionButtonRules file
        try:
            self.dbConnection.execute(
                "SELECT * from ActionButtonRules ORDER BY ActionButtonName ASC, IBPLRulePosition ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            actionButtonRulesData = [
                {
                    "ActionButtonName": i["ActionButtonName"],
                    "IBPLRulePosition": i["IBPLRulePosition"],
                    "IBPLRule": i["IBPLRule"],
                }
                for i in fetchData
            ]
            filename = "ActionButtons/ActionButtonRules.csv"
            self.createCSV(filename, actionButtonRulesData)

        except Exception as e:
            self.logger.error("Error fetching ActionButtonRules data: " + str(e))
            print("Error fetching ActionButtonRules data: ", str(e))

        # ActionButtonFieldBindings file
        try:
            self.dbConnection.execute(
                "SELECT * from ActionButtonFieldBindings ORDER BY ActionButtonName ASC, FieldName ASC, "
                "PropertyName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            actionButtonFieldBindingData = [
                {
                    "ActionButtonName": i["ActionButtonName"],
                    "FieldName": i["FieldName"],
                    "PropertyName": i["PropertyName"],
                    "PropertyValue": i["PropertyValue"],
                }
                for i in fetchData
            ]
            filename = "ActionButtons/ActionButtonFieldBindings.csv"
            self.createCSV(filename, actionButtonFieldBindingData)

        except Exception as e:
            self.logger.error(
                "Error fetching ActionButtonFieldBindings data: " + str(e)
            )
            print("Error fetching ActionButtonFieldBindings data: ", str(e))

        # ActionButtonJSRules file
        try:
            self.dbConnection.execute(
                "SELECT * from ActionButtonJSRules ORDER BY ActionButtonName ASC, ModuleName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            actionButtonJSRulesData = [
                {
                    "ActionButtonName": i["ActionButtonName"],
                    "ModuleName": i["ModuleName"],
                    "FunctionName": i["FunctionName"],
                }
                for i in fetchData
            ]
            filename = "ActionButtons/ActionButtonJSRules.csv"
            self.createCSV(filename, actionButtonJSRulesData)

        except Exception as e:
            self.logger.error("Error fetching ActionButtonJSRules data: " + str(e))
            print("Error fetching ActionButtonJSRules data: ", str(e))

        # ActionButtonDataSources file
        try:
            self.dbConnection.execute(
                "SELECT * from ActionButtonDataSources ORDER BY ActionButtonName ASC, DataSourceName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            actionButtonDataSourceData = [
                {
                    "ActionButtonName": i["ActionButtonName"],
                    "DataSourceName": i["DataSourceName"],
                    "IBPLRule": i["IBPLRule"],
                }
                for i in fetchData
            ]
            filename = "ActionButtons/ActionButtonDataSource.csv"
            self.createCSV(filename, actionButtonDataSourceData)

        except Exception as e:
            self.logger.error("Error fetching ActionButtonDataSources data: " + str(e))
            print("Error fetching ActionButtonDataSources data: ", str(e))

        # ActionButtonBindingsForWeb file
        try:
            self.dbConnection.execute(
                "SELECT * from ActionButtonBindingsForWeb ORDER BY WorkSpaceName ASC, PageGroupName ASC, PageName ASC, "
                "ViewName ASC, WidgetName ASC, ActionButtonName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            actionButtonUIBindingsList = [
                {
                    "WorkSpaceName": i["WorkSpaceName"],
                    "PageGroupName": i["PageGroupName"],
                    "PageName": i["PageName"],
                    "ViewName": i["ViewName"],
                    "WidgetName": i["WidgetName"],
                    "WidgetTitle": i["WidgetTitle"],
                    "ActionButtonName": i["ActionButtonName"],
                }
                for i in fetchData
            ]
            filename = "ActionButtons/ActionButtonBindingsForWeb.csv"
            self.createCSV(filename, actionButtonUIBindingsList)

        except Exception as e:
            self.logger.error(
                "Error fetching ActionButtonBindingsForWeb data: " + str(e)
            )
            print("Error fetching ActionButtonBindingsForWeb data: ", str(e))

        # ActionButtonBindingsForExcel file
        try:
            self.dbConnection.execute(
                "SELECT * from ActionButtonBindingsForExcel ORDER BY XLFolder ASC, XLWorkbook ASC, "
                "ActionButtonName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            actionButtonBindingsForExcelList = [
                {
                    "XLFolder": i["XLFolder"],
                    "XLWorkbook": i["XLWorkbook"],
                    "ActionButtonName": i["ActionButtonName"],
                }
                for i in fetchData
            ]
            filename = "ActionButtons/ActionButtonBindingsForExcel.csv"
            self.createCSV(filename, actionButtonBindingsForExcelList)

        except Exception as e:
            self.logger.error(
                "Error fetching ActionButtonBindingsForExcel data: " + str(e)
            )
            print("Error fetching ActionButtonBindingsForExcel data: ", str(e))

        # ActionButtonBindingsForWidget file
        try:
            self.dbConnection.execute(
                "SELECT * from ActionButtonBindingsForWidget ORDER BY WidgetName ASC, ActionButtonName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            actionButtonBindingsForWidgetList = [
                {
                    "WidgetName": i["WidgetName"],
                    "ActionButtonName": i["ActionButtonName"],
                }
                for i in fetchData
            ]
            filename = "ActionButtons/ActionButtonBindingsForWidget.csv"
            self.createCSV(filename, actionButtonBindingsForWidgetList)

        except Exception as e:
            self.logger.error(
                "Error fetching ActionButtonBindingsForWidget data: " + str(e)
            )
            print("Error fetching ActionButtonBindingsForWidget data: ", str(e))

        # ExcelActionButtonsForWidget file
        try:
            self.dbConnection.execute(
                "SELECT * from ExcelActionButtonsForWidget ORDER BY WidgetName ASC, ActionButtonName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            excelActionButtonsForWidget = [
                {
                    "WidgetName": i["WidgetName"],
                    "ActionButtonName": i["ActionButtonName"],
                    "IBPLExpression": i["IBPLExpression"],
                    "IsBackgroundProcess": i["IsBackgroundProcess"],
                }
                for i in fetchData
            ]
            filename = "ActionButtons/ExcelActionButtonsForWidget.csv"
            self.createCSV(filename, excelActionButtonsForWidget)

        except Exception as e:
            self.logger.error(
                "Error fetching ExcelActionButtonsForWidget data: " + str(e)
            )
            print("Error fetching ExcelActionButtonsForWidget data: ", str(e))

    def createRuleFilesArray(self):
        """
        Create rule files from the rules tables in the database.
        :return: null
        """
        self.logger.info("Creating rules files.")
        # NamedSets file
        try:
            self.dbConnection.execute("SELECT * from NamedSets ORDER BY SetName ASC;")
            fetchData = self.dbConnection.fetchall()
            namedSetData = [
                {
                    "RuleFileName": i["RuleFileName"],
                    "SetName": i["SetName"],
                    "Definition": self.replaceNewLine(i["Definition"]),
                    "Description": i["Description"],
                }
                for i in fetchData
            ]
            filename = "NamedSets.csv"
            self.createCSV(filename, namedSetData)

        except Exception as e:
            self.logger.error("Error fetching NamedSets data: " + str(e))
            print("Error fetching NamedSets data: ", str(e))

        # ActiveRuleFiles file
        try:
            self.dbConnection.execute(
                "SELECT * from ActiveRuleFiles ORDER BY RuleFileName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            activeRuleFilesData = [
                {
                    "RuleFileName": i["RuleFileName"],
                    "RuleFileDescription": i["RuleFileDescription"],
                }
                for i in fetchData
            ]
            filename = "ActiveRules/ActiveRuleFiles.csv"
            self.createCSV(filename, activeRuleFilesData)

        except Exception as e:
            self.logger.error("Error fetching ActiveRuleFiles data: " + str(e))
            print("Error fetching ActiveRuleFiles data: ", str(e))

        try:
            self.dbConnection.execute("SELECT RuleFileName FROM ActiveRuleFiles")
            fetchData = self.dbConnection.fetchall()
            ruleFileList = [i["RuleFileName"] for i in fetchData]
            finalData = ""
            for ruleFileName in ruleFileList:
                try:
                    self.dbConnection.execute(
                        "SELECT ScopeDescription, ScopeType, ScopePosition, ScopeString FROM ActiveRuleScopeLists "
                        "WHERE RuleFileName=?;",
                        (ruleFileName,),
                    )
                    fetchData = self.dbConnection.fetchall()
                    scopeData = [
                        {
                            "ScopeComment": i["ScopeDescription"],
                            "ScopeType": i["ScopeType"],
                            "ScopePosition": i["ScopePosition"],
                            "ScopeString": i["ScopeString"],
                        }
                        for i in fetchData
                    ]
                    for scope in scopeData:
                        if scope["ScopeType"] == "Plugin":
                            pass
                        else:
                            if len(scope["ScopeComment"].strip()) == 0:
                                scope["ScopeComment"] = " "
                            finalData = (
                                finalData + "/*" + scope["ScopeComment"] + "*/\n"
                            )

                            if scope["ScopeType"] == "Block":
                                finalData = finalData + "block "
                            elif scope["ScopeType"] == "Cartesian":
                                finalData = finalData + "cartesian "
                            elif scope["ScopeType"] == "EvaluateMember":
                                finalData = finalData + "evaluatemember "
                            elif scope["ScopeType"] == "Recurrence":
                                finalData = finalData + "recurrence "
                            elif scope["ScopeType"] == "Spreading":
                                finalData = finalData + "spread "

                            finalData = (
                                finalData + "scope: ( " + scope["ScopeString"] + " );\n"
                            )
                            try:
                                self.dbConnection.execute(
                                    "SELECT * FROM ActiveRuleFormulae WHERE RuleFileName=? AND ScopePosition=?;",
                                    (ruleFileName, scope["ScopePosition"]),
                                )
                                fetchData = self.dbConnection.fetchall()
                                formulaeData = [
                                    {
                                        "FormulaPosition": i["FormulaPosition"],
                                        "FormulaStatement": i["FormulaStatement"],
                                        "FormulaScope": i["ScopeGrain"],
                                        "IsEnabled": i["IsEnabled"],
                                    }
                                    for i in fetchData
                                ]

                                for formula in formulaeData:
                                    if formula["IsEnabled"] == "1":
                                        finalData = (
                                            finalData
                                            + "  "
                                            + formula["FormulaStatement"]
                                            + "\n"
                                        )
                                    else:
                                        finalData = (
                                            finalData
                                            + "/*"
                                            + formula["FormulaStatement"]
                                            + "*/\n"
                                        )

                            except Exception as e:
                                self.logger.error(
                                    "Error in getting data from ActiveRuleFormulae: "
                                    + str(e)
                                )

                            finalData = finalData + "end scope;\n\n"
                    try:
                        self.dbConnection.execute(
                            "SELECT PluginText FROM ActivePluginDetails WHERE RuleFileName=?;",
                            (ruleFileName,),
                        )
                        fetchData = self.dbConnection.fetchall()
                        plugin = [{"text": i["PluginText"]} for i in fetchData]
                        for x in plugin:
                            finalData = finalData + x["text"]

                    except Exception as e:
                        self.logger.error(
                            "Error in getting data from ActivePluginDetails: " + str(e)
                        )
                    filename = ("ActiveRules/" + ruleFileName + ".ibpl").replace(
                        ":", ""
                    )
                    self.createEntityFile(self.replaceNewLine(finalData), filename)
                    finalData = ""

                except Exception as ex:
                    self.logger.error("Error creating rule file:" + str(ex))

        except Exception as e:
            self.logger.error("Error getting data from ActiveRuleFiles:  " + str(e))

    def createPluginsCSVArrays(self):
        """
        Create plugin related csv files.
        :return: null
        """
        try:
            self.dbConnection.execute(
                "SELECT PluginClass, PluginName from Plugins ORDER BY PluginClass;"
            )
            fetchData = self.dbConnection.fetchall()
            pluginData = [
                {"PluginClass": i["PluginClass"], "PluginName": i["PluginName"]}
                for i in fetchData
            ]

            for plugin in pluginData:
                if plugin["PluginClass"] == "RScriptGeneralized":
                    try:
                        self.dbConnection.execute(
                            "SELECT ScriptCode FROM RGenPluginScripts WHERE PluginName=? ORDER BY ScriptCode ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        scriptCode = [i["ScriptCode"].strip() for i in fetchData]
                        for data in scriptCode:
                            filename = (
                                "Plugins/RScriptGeneralized/"
                                + plugin["PluginName"]
                                + "/"
                                + plugin["PluginName"]
                                + "_Script.txt"
                            )
                            self.createEntityFile(self.replaceNewLine(data), filename)
                    except Exception as e:
                        self.logger.error(
                            "Error generating plugin code for "
                            + str(plugin["PluginName"])
                            + str(e)
                        )
                        print(
                            "Error generating plugin data for ", plugin["PluginName"], e
                        )
                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM RGenPluginParams WHERE PluginName=? ORDER BY VariableName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        rGenPluginParamsData = [
                            {"VariableName": i["VariableName"], "Value": i["Value"]}
                            for i in fetchData
                        ]
                        filename = (
                            "Plugins/RScriptGeneralized/"
                            + plugin["PluginName"]
                            + "/"
                            + plugin["PluginName"]
                            + "_params.csv"
                        )
                        self.createCSV(filename, rGenPluginParamsData)

                    except Exception as e:
                        self.logger.error(
                            "Error generating data from RGenPluginParams " + str(e)
                        )
                        print("Error generating data from RGenPluginParams " + str(e))

                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM RGenPluginInputTables WHERE PluginName=? ORDER BY VariableName ASC, "
                            "MeasureName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        rGenPluginInputTablesData = [
                            {
                                "VariableName": i["VariableName"],
                                "MeasureName": i["MeasureName"],
                            }
                            for i in fetchData
                        ]
                        filename = (
                            "Plugins/RScriptGeneralized/"
                            + plugin["PluginName"]
                            + "/"
                            + plugin["PluginName"]
                            + "_input.csv"
                        )
                        self.createCSV(filename, rGenPluginInputTablesData)

                    except Exception as e:
                        self.logger.error(
                            "Error generating data from RGenPluginInputTables " + str(e)
                        )
                        print(
                            "Error generating data from RGenPluginInputTables " + str(e)
                        )

                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM RGenPluginInputQueries WHERE PluginName=? ORDER BY VariableName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        RGenPluginInputQueriesData = [
                            {"VariableName": i["VariableName"], "Query": i["Query"]}
                            for i in fetchData
                        ]
                        filename = (
                            "Plugins/RScriptGeneralized/"
                            + plugin["PluginName"]
                            + "/"
                            + plugin["PluginName"]
                            + "_inputQueries.csv"
                        )
                        self.createCSV(filename, RGenPluginInputQueriesData)

                    except Exception as e:
                        self.logger.error(
                            "Error generating data from RGenPluginInputQueries "
                            + str(e)
                        )
                        print(
                            "Error generating data from RGenPluginInputQueries "
                            + str(e)
                        )

                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM RGenPluginOutputTables WHERE PluginName=? ORDER BY VariableName ASC, "
                            "MeasureName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        rGenPluginOutputTablesData = [
                            {
                                "VariableName": i["VariableName"],
                                "MeasureName": i["MeasureName"],
                            }
                            for i in fetchData
                        ]
                        filename = (
                            "Plugins/RScriptGeneralized/"
                            + plugin["PluginName"]
                            + "/"
                            + plugin["PluginName"]
                            + "_output.csv"
                        )
                        self.createCSV(filename, rGenPluginOutputTablesData)

                    except Exception as e:
                        self.logger.error(
                            "Error generating data from RGenPluginOutputTables "
                            + str(e)
                        )
                        print(
                            "Error generating data from RGenPluginOutputTables "
                            + str(e)
                        )

                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM RGenPluginSliceTables WHERE PluginName=? ORDER BY DimensionName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        rGenPluginSliceTablesData = [
                            {
                                "DimensionName": i["DimensionName"],
                                "AttributeName": i["AttributeName"],
                            }
                            for i in fetchData
                        ]
                        filename = (
                            "Plugins/RScriptGeneralized/"
                            + plugin["PluginName"]
                            + "/"
                            + plugin["PluginName"]
                            + "_slice.csv"
                        )
                        self.createCSV(filename, rGenPluginSliceTablesData)

                    except Exception as e:
                        self.logger.error(
                            "Error generating data from RGenPluginSliceTables " + str(e)
                        )
                        print(
                            "Error generating data from RGenPluginSliceTables " + str(e)
                        )

                elif plugin["PluginClass"] == "RScriptTimeSeries":
                    try:
                        self.dbConnection.execute(
                            "SELECT ScriptCode FROM RTimePluginScripts WHERE PluginName=? ORDER BY ScriptCode;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        scriptCode = [i["ScriptCode"].strip() for i in fetchData]
                        for data in scriptCode:
                            filename = (
                                "Plugins/RScriptTimeSeries/"
                                + plugin["PluginName"]
                                + "/"
                                + plugin["PluginName"]
                                + "_Script.txt"
                            )
                            self.createEntityFile(self.replaceNewLine(data), filename)
                    except Exception as e:
                        self.logger.error(
                            "Error generating plugin code for "
                            + str(plugin["PluginName"])
                            + str(e)
                        )
                        print(
                            "Error generating plugin code for ", plugin["PluginName"], e
                        )

                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM RTimePluginParams WHERE PluginName=? ORDER BY Algorithm ASC, ParamName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        rTimePluginParamsData = [
                            {
                                "Algorithm": i["Algorithm"],
                                "ParamName": i["ParamName"],
                                "ParamValue": i["ParamValue"],
                            }
                            for i in fetchData
                        ]
                        filename = (
                            "Plugins/RScriptTimeSeries/"
                            + plugin["PluginName"]
                            + "/"
                            + plugin["PluginName"]
                            + "_params.csv"
                        )
                        self.createCSV(filename, rTimePluginParamsData)

                    except Exception as e:
                        self.logger.error(
                            "Error generating data from RTimePluginParams " + str(e)
                        )
                        print("Error generating data from RTimePluginParams " + str(e))

                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM RTimePluginInputs WHERE PluginName=? ORDER BY MeasureName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        rTimePluginInputsData = [
                            {
                                "MeasureName": i["MeasureName"],
                                "VariableName": i["VariableName"],
                                "IsPrimary": i["IsPrimary"],
                            }
                            for i in fetchData
                        ]
                        filename = (
                            "Plugins/RScriptTimeSeries/"
                            + plugin["PluginName"]
                            + "/"
                            + plugin["PluginName"]
                            + "_input.csv"
                        )
                        self.createCSV(filename, rTimePluginInputsData)

                    except Exception as e:
                        self.logger.error(
                            "Error generating data from RTimePluginInputs " + str(e)
                        )
                        print("Error generating data from RTimePluginInputs " + str(e))

                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM RTimePluginOutputs WHERE PluginName=? ORDER BY MeasureName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        rTimePluginOutputsData = [
                            {
                                "MeasureName": i["MeasureName"],
                                "VariableName": i["VariableName"],
                                "IsHistorical": i["IsHistorical"],
                            }
                            for i in fetchData
                        ]
                        filename = (
                            "Plugins/RScriptTimeSeries/"
                            + plugin["PluginName"]
                            + "/"
                            + plugin["PluginName"]
                            + "_output.csv"
                        )
                        self.createCSV(filename, rTimePluginOutputsData)

                    except Exception as e:
                        self.logger.error(
                            "Error generating data from RTimePluginOutputs " + str(e)
                        )
                        print("Error generating data from RTimePluginOutputs " + str(e))

                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM RTimeSeriesParams WHERE PluginName=? ORDER BY ParamName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        rTimeSeriesParamsData = [
                            {"ParamName": i["ParamName"], "ParamValue": i["ParamValue"]}
                            for i in fetchData
                        ]
                        filename = (
                            "Plugins/RScriptTimeSeries/"
                            + plugin["PluginName"]
                            + "/"
                            + plugin["PluginName"]
                            + "_seriesParams.csv"
                        )
                        self.createCSV(filename, rTimeSeriesParamsData)

                    except Exception as e:
                        self.logger.error(
                            "Error generating data from RTimeSeriesParams " + str(e)
                        )
                        print("Error generating data from RTimeSeriesParams " + str(e))

                elif (
                    plugin["PluginClass"] == "BosToInventory"
                    or plugin["PluginClass"] == "InventoryToBos"
                    or plugin["PluginClass"] == "EndingOnHandPlan"
                    or plugin["PluginClass"] == "PeriodToDatePlan"
                    or plugin["PluginClass"] == "SupplyChainSolver"
                ):
                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM NonRPluginParams WHERE PluginName=? ORDER BY ParamName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        nonRPluginParamsData = [
                            {
                                "PluginClass": i["PluginClass"],
                                "PluginName": i["PluginName"],
                                "ParamName": i["ParamName"],
                                "ParamValue": i["ParamValue"],
                            }
                            for i in fetchData
                        ]
                        filename = (
                            "Plugins/"
                            + plugin["PluginClass"]
                            + "Plugins/"
                            + plugin["PluginName"]
                            + "_params.csv"
                        )
                        self.createCSV(filename, nonRPluginParamsData)
                    except Exception as e:
                        self.logger.error(
                            "Error fetching NonRPluginParams data: " + str(e)
                        )
                        print("Error fetching NonRPluginParams data: ", str(e))

                elif (
                    plugin["PluginClass"] == "JavaScriptPlugins"
                    or plugin["PluginClass"] == "PowerShellPlugins"
                ):
                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM TenantPluginDetails WHERE PluginName=?;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        tenantPluginDetailsData = [
                            {
                                "PluginCode": i["PluginCode"],
                                "Description": i["Description"],
                            }
                            for i in fetchData
                        ]
                        filename = ""
                        if plugin["PluginClass"] == "JavaScriptPlugins":
                            filename = (
                                "Plugins/JavaScriptPlugins/"
                                + plugin["PluginName"]
                                + ".js"
                            )

                        elif plugin["PluginClass"] == "PowerShellPlugins":
                            filename = (
                                "Plugins/PowerShellPlugins/"
                                + plugin["PluginName"]
                                + ".ps1"
                            )

                        for scriptCode in tenantPluginDetailsData:
                            self.createEntityFile(
                                self.replaceNewLine(scriptCode["PluginCode"]), filename
                            )

                    except Exception as e:
                        self.logger.error(
                            "Error fetching TenantPluginDetails data: " + str(e)
                        )
                        print("Error fetching TenantPluginDetails data: ", str(e))

                elif plugin["PluginClass"] == "PythonScript":
                    # ScriptCode
                    try:
                        self.dbConnection.execute(
                            "SELECT ScriptCode FROM PythonPluginScripts WHERE PluginName=? ORDER BY ScriptCode ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        scriptCode = [i["ScriptCode"].strip() for i in fetchData]
                        for data in scriptCode:
                            filename = f'Plugins/PythonPlugins/{plugin["PluginName"]}/{plugin["PluginName"]}_Script.txt'
                            self.createEntityFile(self.replaceNewLine(data), filename)
                    except Exception as ex:
                        self.logger.error(
                            f'Error generating plugin code for {str(plugin["PluginName"])}: {str(ex)}'
                        )
                        print(
                            f'Error generating plugin code for {str(plugin["PluginName"])}: {str(ex)}'
                        )
                    # Param
                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM PythonPluginParams WHERE PluginName=? ORDER BY VariableName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        pyPluginParams = [
                            {"VariableName": i["VariableName"], "Value": i["Value"]}
                            for i in fetchData
                        ]
                        filename = f'Plugins/PythonPlugins/{plugin["PluginName"]}/{plugin["PluginName"]}_params.csv'
                        self.createCSV(filename, pyPluginParams)
                    except Exception as ex:
                        self.logger.error(
                            f"Error generating data from PythonPluginParams {str(ex)}"
                        )
                        print(
                            f"Error generating data from PythonPluginParams {str(ex)}"
                        )
                    # PythonPluginInputTables
                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM PythonPluginInputTables WHERE PluginName=? ORDER BY Position ASC, "
                            "VariableKey ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        pyPluginInput = [
                            {
                                "VariableKey": i["VariableKey"],
                                "Value": i["Value"],
                                "Position": i["Position"],
                            }
                            for i in fetchData
                        ]
                        filename = f'Plugins/PythonPlugins/{plugin["PluginName"]}/{plugin["PluginName"]}_inputTables.csv'
                        self.createCSV(filename, pyPluginInput)
                    except Exception as e:
                        self.logger.error(
                            f"Error generating data from PythonPluginInputTables {str(e)}"
                        )
                        print(
                            f"Error generating data from PythonPluginInputTables {str(e)}"
                        )
                    # PythonPluginOutputTables
                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM PythonPluginOutputTables WHERE PluginName=? ORDER BY Position ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        pyPluginOutput = [
                            {
                                "VariableKey": i["VariableKey"],
                                "Value": i["Value"],
                                "Position": i["Position"],
                            }
                            for i in fetchData
                        ]
                        filename = f'Plugins/PythonPlugins/{plugin["PluginName"]}/{plugin["PluginName"]}_outputTables.csv'
                        self.createCSV(filename, pyPluginOutput)
                    except Exception as e:
                        self.logger.error(
                            f"Error generating data from PythonPluginOutputTables {str(e)}"
                        )
                        print(
                            f"Error generating data from PythonPluginOutputTables {str(e)}"
                        )
                    # PythonPluginSliceKeyTables
                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM PythonPluginSliceKeyTables WHERE PluginName=? ORDER BY DimensionName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        pyPluginSlice = [
                            {
                                "DimensionName": i["DimensionName"],
                                "AttributeName": i["AttributeName"],
                            }
                            for i in fetchData
                        ]
                        filename = f'Plugins/PythonPlugins/{plugin["PluginName"]}/{plugin["PluginName"]}_slice.csv'
                        self.createCSV(filename, pyPluginSlice)
                    except Exception as e:
                        self.logger.error(
                            f"Error generating data from PythonPluginSliceKeyTables {str(e)}"
                        )
                        print(
                            f"Error generating data from PythonPluginSliceKeyTables {str(e)}"
                        )

                elif plugin["PluginClass"] == "PySparkScript":
                    # CODE
                    try:
                        self.dbConnection.execute(
                            "SELECT ScriptCode FROM PySparkPluginScripts WHERE PluginName=? ORDER BY ScriptCode ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        scriptCode = [i["ScriptCode"].strip() for i in fetchData]
                        for data in scriptCode:
                            filename = f'Plugins/PySparkPlugins/{plugin["PluginName"]}/{plugin["PluginName"]}_Script.txt'
                            self.createEntityFile(self.replaceNewLine(data), filename)
                    except Exception as ex:
                        self.logger.error(
                            f'Error generating plugin code for {str(plugin["PluginName"])}: {str(ex)}'
                        )
                        print(
                            f'Error generating plugin code for {str(plugin["PluginName"])}: {str(ex)}'
                        )
                    # PARAMS
                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM PySparkPluginParams WHERE PluginName=? ORDER BY VariableName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        pyPluginParams = [
                            {"VariableName": i["VariableName"], "Value": i["Value"]}
                            for i in fetchData
                        ]
                        filename = f'Plugins/PySparkPlugins/{plugin["PluginName"]}/{plugin["PluginName"]}_params.csv'
                        self.createCSV(filename, pyPluginParams)
                    except Exception as ex:
                        self.logger.error(
                            f"Error generating data from PySparkPluginParams {str(ex)}"
                        )
                        print(
                            f"Error generating data from PySparkPluginParams {str(ex)}"
                        )
                    # INPUTS
                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM PySparkPluginInputTables WHERE PluginName=? ORDER BY Position ASC, "
                            "VariableKey ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        pyPluginInput = [
                            {
                                "VariableKey": i["VariableKey"],
                                "Value": i["Value"],
                                "Position": i["Position"],
                            }
                            for i in fetchData
                        ]
                        filename = f'Plugins/PySparkPlugins/{plugin["PluginName"]}/{plugin["PluginName"]}_inputTables.csv'
                        self.createCSV(filename, pyPluginInput)
                    except Exception as e:
                        self.logger.error(
                            f"Error generating data from PySparkPluginInputTables {str(e)}"
                        )
                        print(
                            f"Error generating data from PySparkPluginInputTables {str(e)}"
                        )
                    # OUTPUTS
                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM PySparkPluginOutputTables WHERE PluginName=? ORDER BY VariableName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        pyPluginOutput = [
                            {
                                "VariableName": i["VariableName"],
                                "VariableType": i["VariableType"],
                            }
                            for i in fetchData
                        ]
                        filename = f'Plugins/PySparkPlugins/{plugin["PluginName"]}/{plugin["PluginName"]}_outputTables.csv'
                        self.createCSV(filename, pyPluginOutput)
                    except Exception as e:
                        self.logger.error(
                            f"Error generating data from PySparkPluginOutputTables {str(e)}"
                        )
                        print(
                            f"Error generating data from PySparkPluginOutputTables {str(e)}"
                        )
                    # SLICES
                    try:
                        self.dbConnection.execute(
                            "SELECT * FROM PySparkPluginSliceKeys WHERE PluginName=? ORDER BY DimensionName ASC;",
                            (plugin["PluginName"],),
                        )
                        fetchData = self.dbConnection.fetchall()
                        pyPluginSlice = [
                            {
                                "DimensionName": i["DimensionName"],
                                "AttributeName": i["AttributeName"],
                            }
                            for i in fetchData
                        ]
                        filename = f'Plugins/PySparkPlugins/{plugin["PluginName"]}/{plugin["PluginName"]}_slice.csv'
                        self.createCSV(filename, pyPluginSlice)
                    except Exception as e:
                        self.logger.error(
                            f"Error generating data from PySparkPluginSliceKeys {str(e)}"
                        )
                        print(
                            f"Error generating data from PySparkPluginSliceKeys {str(e)}"
                        )

        except Exception as e:
            self.logger.error("Error fetching Plugins data: " + str(e))
            print("Error fetching Plugins data: ", str(e))

        try:
            self.dbConnection.execute("SELECT * FROM TenantPluginDetails;")
            fetchData = self.dbConnection.fetchall()
            tenantPluginDetailsList = [
                {
                    "PluginName": i["PluginName"],
                    "PluginClass": i["PluginClass"],
                    "PluginCode": i["PluginCode"],
                    "Description": i["Description"],
                }
                for i in fetchData
            ]
            filename = "Plugins/TenantPluginDetails.csv"
            self.createCSV(filename, tenantPluginDetailsList)

        except Exception as e:
            self.logger.error("Error fetching TenantPluginDetails data: " + str(e))
            print("Error fetching TenantPluginDetails data: ", str(e))

    def createProceduresFilesArray(self):
        """
        Create procedure related files from the database.
        :return: null
        """
        try:
            self.dbConnection.execute(
                "SELECT * FROM procedures ORDER by ProcFile ASC, ProcPosition ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            proceduresData = [
                {
                    "ProcFile": i["ProcFile"],
                    "ProcName": i["ProcName"],
                    "ProcDescription": i["ProcDescription"],
                    "IsParameterized": i["IsParameterized"],
                    "ProcPosition": i["ProcPosition"],
                }
                for i in fetchData
            ]
            for procedure in proceduresData:
                try:
                    self.dbConnection.execute(
                        "SELECT ParamName, ParamType, ItemType FROM ProcParams WHERE ProcName=? ORDER BY ParamName;",
                        (procedure["ProcName"],),
                    )
                    fetchData = self.dbConnection.fetchall()
                    procParamsData = [
                        {
                            "ParamName": i["ParamName"],
                            "ParamType": i["ParamType"],
                            "ItemType": i["ItemType"],
                        }
                        for i in fetchData
                    ]
                    filename = (
                        "Procedures/ProcedureGroup_"
                        + procedure["ProcFile"]
                        + "/"
                        + procedure["ProcName"]
                        + "/InputParameters.csv"
                    )
                    self.createCSV(filename, procParamsData)
                    filename = (
                        "Procedures/ProcedureGroup_"
                        + procedure["ProcFile"]
                        + "/"
                        + procedure["ProcName"]
                        + "/ProcComment.ibpl"
                    )
                    self.createEntityFile(
                        self.replaceNewLine(procedure["ProcDescription"]), filename
                    )
                except Exception as e:
                    self.logger.error("Error fetching ProcParams data: " + str(e))
                    print("Error fetching ProcParams data: ", str(e))

                try:
                    self.dbConnection.execute(
                        "SELECT ProcCode FROM ProcCodes WHERE ProcName=?;",
                        (procedure["ProcName"],),
                    )
                    fetchData = self.dbConnection.fetchall()
                    procCodeData = [{"ProcCode": i["ProcCode"]} for i in fetchData]
                    for procCode in procCodeData:
                        filename = (
                            "Procedures/ProcedureGroup_"
                            + procedure["ProcFile"]
                            + "/"
                            + procedure["ProcName"]
                            + "/ProcedureText.ibpl"
                        )
                        self.createEntityFile(
                            self.replaceNewLine(procCode["ProcCode"].strip()), filename
                        )
                except Exception as e:
                    self.logger.error("Error fetching ProcParams data: " + str(e))
                    print("Error fetching ProcParams data: ", str(e))

            filename = "Procedures/Procedures.csv"
            self.createCSV(filename, proceduresData)

        except Exception as e:
            self.logger.error("Error fetching procedures data: " + str(e))
            print("Error fetching procedures data: ", str(e))

    def createUIFilesArray(self):
        """
        Create UI data related csv files.
        :return: null
        """
        try:
            self.dbConnection.execute(
                "SELECT * from WidgetDefinitionProperties ORDER BY WidgetID ASC, PropertyName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            widgetDefinitionPresentationPropertiesData = [
                {
                    "WidgetID": i["WidgetID"],
                    "WidgetName": i["WidgetName"],
                    "IsPrivate": i["IsPrivate"],
                    "WidgetType": i["WidgetType"],
                    "PropertyName": i["PropertyName"],
                    "PropertyValue": i["PropertyValue"],
                }
                for i in fetchData
            ]

            filename = "WidgetDefinitionProperties.csv"
            self.createCSV(filename, widgetDefinitionPresentationPropertiesData, True)

        except Exception as e:
            self.logger.error(
                "Error fetching WidgetDefinitionPresentationProperties data: " + str(e)
            )
            print(
                "Error fetching WidgetDefinitionPresentationProperties data: ", str(e)
            )

        workspaceNameList = []
        try:
            self.dbConnection.execute(
                "SELECT * from Workspaces ORDER BY WorkspaceName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            workspaceData = [
                {
                    "WorkspaceName": i["WorkspaceName"],
                    "WorkspaceTitle": i["WorkspaceTitle"],
                    "WorkspaceIsHidden": i["WorkspaceIsHidden"],
                    "Roles": i["Roles"],
                }
                for i in fetchData
            ]
            workspaceNameList = [x["WorkspaceTitle"] for x in workspaceData]

            filename = "Workspaces.csv"
            self.createCSV(filename, workspaceData, True)

        except Exception as e:
            self.logger.error("Error fetching Workspaces data: " + str(e))
            print("Error fetching Workspaces data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from PageGroups ORDER BY WorkspaceName ASC, PageGroupDisplayOrder ASC, PageGroupName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            pageGroupData = [
                {
                    "WorkspaceName": i["WorkspaceName"],
                    "PageGroupName": i["PageGroupName"],
                    "PageGroupTitle": i["PageGroupTitle"],
                }
                for i in fetchData
            ]
            filename = "PageGroups.csv"
            self.createCSV(filename, pageGroupData, True)

        except Exception as e:
            self.logger.error("Error fetching PageGroups data: " + str(e))
            print("Error fetching PageGroups data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from Pages ORDER BY WorkspaceName ASC, PageGroupName ASC, PageDisplayOrder ASC, "
                "PageTitle ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            pageData = [
                {
                    "WorkspaceName": i["WorkspaceName"],
                    "PageGroupName": i["PageGroupName"],
                    "PageName": i["PageName"],
                    "PageTitle": i["PageTitle"],
                    "PageIsDefault": i["PageIsDefault"],
                }
                for i in fetchData
            ]
            filename = "Pages.csv"
            self.createCSV(filename, pageData, True)

        except Exception as e:
            self.logger.error("Error fetching Pages data: " + str(e))
            print("Error fetching Pages data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from WebLayoutPageWidgets ORDER BY WorkspaceName ASC, PageGroupName ASC, PageName ASC, "
                "Widget ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            webLayoutPageWidgetsData = [
                {
                    "WorkspaceName": i["WorkspaceName"],
                    "PageGroupName": i["PageGroupName"],
                    "PageName": i["PageName"],
                    "Widget": i["Widget"],
                }
                for i in fetchData
            ]
            filename = "WebLayoutPageWidgets.csv"
            self.createCSV(filename, webLayoutPageWidgetsData, True)

        except Exception as e:
            self.logger.error("Error fetching WebLayoutPageWidgets data: " + str(e))
            print("Error fetching WebLayoutPageWidgets data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from Views ORDER BY WorkspaceName ASC, PageGroupName ASC, PageName ASC, ViewPosition ASC, "
                "ViewName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            viewData = [
                {
                    "WorkspaceName": i["WorkspaceName"],
                    "PageGroupName": i["PageGroupName"],
                    "PageName": i["PageName"],
                    "ViewName": i["ViewName"],
                    "ViewTitle": i["ViewTitle"],
                    "ViewIsDefault": i["ViewIsDefault"],
                    "Roles": i["Roles"],
                }
                for i in fetchData
            ]
            filename = "Views.csv"
            self.createCSV(filename, viewData, True)

        except Exception as e:
            self.logger.error("Error fetching Views data: " + str(e))
            print("Error fetching Views data: ", str(e))

        try:
            self.dbConnection.execute("SELECT * from Widgets ORDER BY WidgetName ASC;")
            fetchData = self.dbConnection.fetchall()
            widgetsData = [
                {
                    "WidgetName": i["WidgetName"],
                    "WidgetType": i["WidgetType"],
                    "IsPrivate": i["IsPrivate"],
                    "CreatedUserId": i["CreatedUserId"],
                    "TileUsageCount": i["TileUsageCount"],
                    "ViewUsageCount": i["ViewUsageCount"],
                    "ExcelUsageCount": i["ExcelUsageCount"],
                    "TotalUsageCount": i["TotalUsageCount"],
                }
                for i in fetchData
            ]
            filename = "Widgets.csv"
            self.createCSV(filename, widgetsData, True)

        except Exception as e:
            self.logger.error("Error fetching Widgets data: " + str(e))
            print("Error fetching Widgets data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from Widgets WHERE TotalUsageCount=0 ORDER BY WidgetName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            widgetsNotUsedInWebOrExcelData = [
                {
                    "WidgetName": i["WidgetName"],
                    "WidgetType": i["WidgetType"],
                    "TileUsageCount": i["TileUsageCount"],
                    "ViewUsageCount": i["ViewUsageCount"],
                    "ExcelUsageCount": i["ExcelUsageCount"],
                    "TotalUsageCount": i["TotalUsageCount"],
                }
                for i in fetchData
            ]
            filename = "WidgetsNotUsedInWebOrExcel.csv"
            self.createCSV(filename, widgetsNotUsedInWebOrExcelData, True)

        except Exception as e:
            self.logger.error("Error fetching Widgets data: " + str(e))
            print("Error fetching Widgets data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from WidgetLevelAttributes ORDER BY WidgetName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            widgetLevelAttributesData = [
                {
                    "WidgetName": i["WidgetName"],
                    "DimName": i["DimName"],
                    "AttributeName": i["AttributeName"],
                    "MemberFilterExpression": i["MemberFilterExpression"],
                    "RelationshipType": i["RelationshipType"],
                    "EdgeDirection": i["EdgeDirection"],
                    "IsCurrencyFilter": i["IsCurrencyFilter"],
                    "IsVisible": i["IsVisible"],
                    "IsAttributeRequired": i["IsAttributeRequired"],
                }
                for i in fetchData
            ]
            filename = "WidgetLevelAttributes.csv"
            self.createCSV(filename, widgetLevelAttributesData, True)

        except Exception as e:
            self.logger.error("Error fetching WidgetLevelAttributes data: " + str(e))
            print("Error fetching WidgetLevelAttributes data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from WidgetLevelAttrFilters ORDER BY WidgetName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            widgetFiltersData = [
                {
                    "WidgetName": i["WidgetName"],
                    "DimName": i["DimName"],
                    "AttributeName": i["AttributeName"],
                    "MemberFilterExpression": i["MemberFilterExpression"],
                    "SelectedMembers": i["SelectedMembers"],
                    "IsSingleSelect": i["IsSingleSelect"],
                    "IsCurrencyFilter": i["IsCurrencyFilter"],
                }
                for i in fetchData
            ]
            filename = "WidgetLevelAttrFilters.csv"
            self.createCSV(filename, widgetFiltersData, True)

        except Exception as e:
            self.logger.error("Error fetching WidgetLevelAttrFilters data: " + str(e))
            print("Error fetching WidgetLevelAttrFilters data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from WidgetMeasureFilters ORDER BY WidgetName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            widgetMeasureFiltersData = [
                {
                    "WidgetName": i["WidgetName"],
                    "MeasureFilterExpr": i["MeasureFilterExpr"],
                    "FilterScopeType": i["FilterScopeType"],
                }
                for i in fetchData
            ]
            filename = "WidgetMeasureFilters.csv"
            self.createCSV(filename, widgetMeasureFiltersData, True)

        except Exception as e:
            self.logger.error("Error fetching WidgetMeasureFilters data: " + str(e))
            print("Error fetching WidgetMeasureFilters data: ", str(e))

        # WidgetInterDependentMeasures
        try:
            self.dbConnection.execute(
                "SELECT * from WidgetInterDependentMeasures ORDER BY WidgetName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            widgetInterDependentMeasuresData = [
                {
                    "WidgetName": i["WidgetName"],
                    "VersionDependentFilter": i["VersionDependentFilter"],
                    "InterDependentMeasureName": i["InterDependentMeasureName"],
                }
                for i in fetchData
            ]
            filename = "WidgetInterDependentMeasures.csv"
            self.createCSV(filename, widgetInterDependentMeasuresData, True)

        except Exception as e:
            self.logger.error(
                "Error fetching WidgetInterDependentMeasures data: " + str(e)
            )
            print("Error fetching WidgetInterDependentMeasures data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from WidgetAssociationMeasures ORDER BY WidgetName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            widgetAssociationMeasuresData = [
                {
                    "WidgetName": i["WidgetName"],
                    "AssocMeasureExpr": i["AssocMeasureExpr"],
                }
                for i in fetchData
            ]
            filename = "WidgetAssociationMeasures.csv"
            self.createCSV(filename, widgetAssociationMeasuresData, True)

        except Exception as e:
            self.logger.error(
                "Error fetching WidgetAssociationMeasures data: " + str(e)
            )
            print("Error fetching WidgetAssociationMeasures data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from WidgetNamedSets ORDER BY WidgetName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            widgetNamedSetsData = [
                {
                    "WidgetName": i["WidgetName"],
                    "DimName": i["DimName"],
                    "AvailableNamedSetName": i["AvailableNamedSetName"],
                    "AvailableNamedSetDisplayName": i["AvailableNamedSetDisplayName"],
                    "IsDefault": i["IsDefault"],
                }
                for i in fetchData
            ]
            filename = "WidgetNamedSets.csv"
            self.createCSV(filename, widgetNamedSetsData, True)

        except Exception as e:
            self.logger.error("Error fetching WidgetNamedSets data: " + str(e))
            print("Error fetching WidgetNamedSets data: ", str(e))

        # WidgetNavigationViews
        try:
            self.dbConnection.execute(
                "SELECT * from WidgetNavigationViews ORDER BY Workspace ASC, PageGroup ASC, Page ASC, View ASC, "
                "WidgetName ASC, NavTargetViewName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            widgetNavigationViewsList = [
                {
                    "Workspace": i["Workspace"],
                    "PageGroup": i["PageGroup"],
                    "Page": i["Page"],
                    "View": i["View"],
                    "WidgetName": i["WidgetName"],
                    "NavTargetWorkSpaceName": i["NavTargetWorkSpaceName"],
                    "NavTargetPageGroupName": i["NavTargetPageGroupName"],
                    "NavTargetPageName": i["NavTargetPageName"],
                    "NavTargetViewName": i["NavTargetViewName"],
                }
                for i in fetchData
            ]
            filename = "WidgetNavigationViews.csv"
            self.createCSV(filename, widgetNavigationViewsList, True)

        except Exception as e:
            self.logger.error("Error fetching WidgetNamedSets data: " + str(e))
            print("Error fetching WidgetNamedSets data: ", str(e))

        for workspace in workspaceNameList:
            try:
                self.dbConnection.execute(
                    "SELECT * FROM WebLayoutViewWidgets WHERE Workspace=? ORDER BY Pagegroup ASC, Page ASC, View ASC, "
                    "WidgetName ASC;",
                    (workspace,),
                )
                fetchData = self.dbConnection.fetchall()
                webLayoutViewWidgetsData = [
                    {
                        "Pagegroup": i["Pagegroup"],
                        "Page": i["Page"],
                        "View": i["View"],
                        "WidgetName": i["WidgetName"],
                        "WidgetTitle": i["WidgetTitle"],
                        "IsAnchor": i["IsAnchor"],
                    }
                    for i in fetchData
                ]
                filename = "WorkspaceWebWidgets/" + workspace + ".WebWidgets.csv"
                self.createCSV(filename, webLayoutViewWidgetsData, True)

            except Exception as e:
                self.logger.error("Error fetching WebLayoutViewWidgets data: " + str(e))
                print("Error fetching WebLayoutViewWidgets data: ", str(e))

            try:
                self.dbConnection.execute(
                    "SELECT \
                        vw.Pagegroup, \
                        vw.Page, \
                        vw.View, \
                        ml.WidgetName, \
                        ml.Type, \
                        ml.MeasureName, \
                        ml.IsVisible, \
                        ml.Formula,\
                        ml.Color\
                    from WebLayoutViewWidgets as vw \
                    INNER JOIN WidgetMeasuresList as ml on vw.WidgetName = ml.WidgetName \
                    WHERE vw.Workspace = ? \
                    ORDER BY vw.Pagegroup ASC, vw.Page ASC, vw.View ASC, ml.WidgetName ASC, ml.MeasureName ASC;",
                    (workspace,),
                )
                fetchData = self.dbConnection.fetchall()
                measureListData = [
                    {
                        "Pagegroup": i["Pagegroup"],
                        "Page": i["Page"],
                        "View": i["View"],
                        "Widget": i["WidgetName"],
                        "Type": i["Type"],
                        "MeasureName": i["MeasureName"],
                        "IsVisible": i["IsVisible"],
                        "Formula": i["Formula"],
                        "Color": i["Color"],
                    }
                    for i in fetchData
                ]
                filename = (
                    "WorkspaceWidgetMeasuresList/"
                    + workspace
                    + ".WidgetMeasuresList.csv"
                )
                self.createCSV(filename, measureListData, True)

            except Exception as e:
                self.logger.error("Error fetching WidgetMeasuresList data: " + str(e))
                print("Error fetching WidgetMeasuresList data: ", str(e))

            try:
                self.dbConnection.execute(
                    "SELECT vw.Pagegroup, vw.Page, vw.View, el.WidgetName, el.GraphName, el.EdgeName from "
                    "WebLayoutViewWidgets as vw INNER JOIN WidgetGraphEdgesList as el on vw.WidgetName = el.WidgetName "
                    "WHERE vw.Workspace = ? ORDER BY vw.Pagegroup ASC, vw.Page ASC, vw.View ASC, el.WidgetName ASC, "
                    "el.GraphName ASC, el.EdgeName ASC;",
                    (workspace,),
                )
                fetchData = self.dbConnection.fetchall()
                graphListData = [
                    {
                        "Pagegroup": i["Pagegroup"],
                        "Page": i["Page"],
                        "View": i["View"],
                        "Widget": i["WidgetName"],
                        "GraphName": i["GraphName"],
                        "EdgeName": i["EdgeName"],
                    }
                    for i in fetchData
                ]
                filename = (
                    "WorkspaceWidgetGraphEdgesList/"
                    + workspace
                    + ".WidgetGraphEdgesList.csv"
                )
                self.createCSV(filename, graphListData, True)

            except Exception as e:
                self.logger.error("Error fetching WidgetGraphEdgesList data: " + str(e))
                print("Error fetching WidgetGraphEdgesList data: ", str(e))

            try:
                self.dbConnection.execute(
                    "SELECT * FROM WidgetFilterSharings WHERE WorkspaceName=? ORDER BY PageGroupName ASC, PageName ASC,"
                    " ViewName ASC, WidgetName ASC, DimName ASC;",
                    (workspace,),
                )
                fetchData = self.dbConnection.fetchall()
                widgetFilterSharingsData = [
                    {
                        "PageGroupName": i["PageGroupName"],
                        "PageName": i["PageName"],
                        "ViewName": i["ViewName"],
                        "WidgetName": i["WidgetName"],
                        "DimName": i["DimName"],
                        "AttributeName": i["AttributeName"],
                        "Scope": i["Scope"],
                        "MemberFilterExpression": i["MemberFilterExpression"],
                    }
                    for i in fetchData
                ]
                filename = (
                    "WorkspaceWidgetFilterSharings/"
                    + workspace
                    + ".WidgetFilterSharings.csv"
                )
                self.createCSV(filename, widgetFilterSharingsData, True)

            except Exception as e:
                self.logger.error("Error fetching WidgetFilterSharings data: " + str(e))
                print("Error fetching WidgetFilterSharings data: ", str(e))

            # WidgetFilterLinkings
            try:
                self.dbConnection.execute(
                    "SELECT * FROM WidgetFilterLinkings WHERE WorkspaceName=? ORDER BY PageGroupName ASC, PageName ASC,"
                    " ViewName ASC, WidgetName ASC, DimName ASC;",
                    (workspace,),
                )
                fetchData = self.dbConnection.fetchall()
                widgetFilterLinkingsData = [
                    {
                        "PageGroupName": i["PageGroupName"],
                        "PageName": i["PageName"],
                        "ViewName": i["ViewName"],
                        "WidgetName": i["WidgetName"],
                        "DimName": i["DimName"],
                        "AttributeName": i["AttributeName"],
                        "Scope": i["Scope"],
                    }
                    for i in fetchData
                ]
                filename = (
                    "WorkspaceWidgetFilterLinkings/"
                    + workspace
                    + ".WidgetFilterLinkings.csv"
                )
                self.createCSV(filename, widgetFilterLinkingsData, True)

            except Exception as e:
                self.logger.error("Error fetching WidgetFilterLinkings data: " + str(e))
                print("Error fetching WidgetFilterLinkings data: ", str(e))

            # WidgetInfoContext
            try:
                self.dbConnection.execute(
                    "SELECT * FROM WidgetInfoContext WHERE WorkspaceName=? ORDER BY PageGroupName ASC, "
                    "PageName ASC, ViewName ASC, WidgetName ASC, Title ASC;",
                    (workspace,),
                )
                fetchData = self.dbConnection.fetchall()
                widgetInfoContextData = [
                    {
                        "PageGroupName": i["PageGroupName"],
                        "PageName": i["PageName"],
                        "ViewName": i["ViewName"],
                        "WidgetName": i["WidgetName"],
                        "MemberInfo": i["MemberInfo"],
                        "Title": i["Title"],
                        "UnreadOnly": i["UnreadOnly"],
                        "MemberIndicator": i["MemberIndicator"],
                        "LastNDays": i["LastNDays"],
                        "Folders": i["Folders"],
                        "IsShowTask": i["IsShowTask"],
                        "TaskIndicator": i["TaskIndicator"],
                    }
                    for i in fetchData
                ]
                filename = (
                    "WorkspaceWidgetInfoContext/" + workspace + ".WidgetInfoContext.csv"
                )
                self.createCSV(filename, widgetInfoContextData, True)

            except Exception as e:
                self.logger.error("Error fetching WidgetInfoContext data: " + str(e))
                print("Error fetching WidgetInfoContext data: ", str(e))

    def createTranslationFileArray(self):
        """
        Create translation data related csv files.
        :return: null
        """
        try:
            self.dbConnection.execute(
                "SELECT * from WorkspaceTranslations ORDER BY Name ASC, LCId ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            workspaceTranslationsData = [
                {
                    "Name": i["Name"],
                    "GId": i["GId"],
                    "LCId": i["LCId"],
                    "TranslatedName": i["TranslatedName"],
                }
                for i in fetchData
            ]
            filename = "Translations/WorkspaceTranslations.csv"
            self.createCSV(filename, workspaceTranslationsData, True)

        except Exception as e:
            self.logger.error("Error fetching WorkspaceTranslations data: " + str(e))
            print("Error fetching WorkspaceTranslations data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from PageGroupTranslations ORDER BY Name ASC, LCId ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            pageGroupTranslationsData = [
                {
                    "Name": i["Name"],
                    "GId": i["GId"],
                    "LCId": i["LCId"],
                    "TranslatedName": i["TranslatedName"],
                }
                for i in fetchData
            ]
            filename = "Translations/PageGroupTranslations.csv"
            self.createCSV(filename, pageGroupTranslationsData, True)

        except Exception as e:
            self.logger.error("Error fetching PageGroupTranslations data: " + str(e))
            print("Error fetching PageGroupTranslations data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from PageTranslations ORDER BY Name ASC, LCId ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            pageTranslationsData = [
                {
                    "Name": i["Name"],
                    "GId": i["GId"],
                    "LCId": i["LCId"],
                    "TranslatedName": i["TranslatedName"],
                }
                for i in fetchData
            ]
            filename = "Translations/PageTranslations.csv"
            self.createCSV(filename, pageTranslationsData, True)

        except Exception as e:
            self.logger.error("Error fetching PageTranslations data: " + str(e))
            print("Error fetching PageTranslations data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from ViewTranslations ORDER BY Name ASC, LCId ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            viewTranslationsData = [
                {
                    "Name": i["Name"],
                    "GId": i["GId"],
                    "LCId": i["LCId"],
                    "TranslatedName": i["TranslatedName"],
                }
                for i in fetchData
            ]
            filename = "Translations/ViewTranslations.csv"
            self.createCSV(filename, viewTranslationsData, True)

        except Exception as e:
            self.logger.error("Error fetching ViewTranslations data: " + str(e))
            print("Error fetching ViewTranslations data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from ViewWidgetTranslations ORDER BY Name ASC, LCId Asc;"
            )
            fetchData = self.dbConnection.fetchall()
            viewWidgetTranslationsData = [
                {
                    "Name": i["Name"],
                    "GId": i["GId"],
                    "LCId": i["LCId"],
                    "TranslatedName": i["TranslatedName"],
                }
                for i in fetchData
            ]
            filename = "Translations/ViewWidgetTranslations.csv"
            self.createCSV(filename, viewWidgetTranslationsData, True)

        except Exception as e:
            self.logger.error("Error fetching ViewWidgetTranslations data: " + str(e))
            print("Error fetching ViewWidgetTranslations data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from PageWidgetTranslations ORDER BY Name ASC, LCId Asc;"
            )
            fetchData = self.dbConnection.fetchall()
            pageWidgetTranslationsData = [
                {
                    "Name": i["Name"],
                    "GId": i["GId"],
                    "LCId": i["LCId"],
                    "TranslatedName": i["TranslatedName"],
                }
                for i in fetchData
            ]
            filename = "Translations/PageWidgetTranslations.csv"
            self.createCSV(filename, pageWidgetTranslationsData, True)

        except Exception as e:
            self.logger.error("Error fetching PageWidgetTranslations data: " + str(e))
            print("Error fetching PageWidgetTranslations data: ", str(e))

    def createExcelFilesArray(self):
        """
        Create excel data related csv files.
        :return: null
        """
        folderList = []
        try:
            self.dbConnection.execute(
                "SELECT * from ExcelFolders ORDER BY FolderName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            excelFoldersData = [
                {
                    "FolderName": i["FolderName"],
                    "IsPublished": i["IsPublished"],
                    "IsPrivate": i["IsPrivate"],
                    "Roles": i["Roles"],
                }
                for i in fetchData
            ]
            folderList = [i["FolderName"] for i in excelFoldersData]
            filename = "ExcelFolders.csv"
            self.createCSV(filename, excelFoldersData, True)

        except Exception as e:
            self.logger.error("Error fetching ExcelFolders data: " + str(e))
            print("Error fetching ExcelFolders data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from ExcelWorkbooksInFolders ORDER BY XLFolder ASC, DisplayOrder ASC, XLWorkbook ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            excelWorkbookInFoldersData = [
                {
                    "XLFolder": i["XLFolder"],
                    "XLWorkbook": i["XLWorkbook"],
                    "IsPublished": i["IsPublished"],
                }
                for i in fetchData
            ]
            filename = "ExcelWorkbooksInFolders.csv"
            self.createCSV(filename, excelWorkbookInFoldersData, True)

        except Exception as e:
            self.logger.error("Error fetching ExcelWorkbooksInFolders data: " + str(e))
            print("Error fetching ExcelWorkbooksInFolders data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * from ExcelLayoutWidgets ORDER BY XLFolder ASC, XLWorkbook ASC, Widget ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            excelLayoutWidgetsData = [
                {
                    "XLFolder": i["XLFolder"],
                    "XLWorkbook": i["XLWorkbook"],
                    "Widget": i["Widget"],
                }
                for i in fetchData
            ]
            filename = "ExcelLayoutWidgets.csv"
            self.createCSV(filename, excelLayoutWidgetsData, True)

        except Exception as e:
            self.logger.error("Error fetching ExcelLayoutWidgets data: " + str(e))
            print("Error fetching ExcelLayoutWidgets data: ", str(e))

        for folder in folderList:
            try:
                self.dbConnection.execute(
                    "SELECT ExcelLayoutWidgets.XLWorkbook, WidgetMeasuresList.WidgetName, WidgetMeasuresList.Type, "
                    "WidgetMeasuresList.MeasureName, WidgetMeasuresList.IsVisible, WidgetMeasuresList.Formula "
                    "from ExcelLayoutWidgets INNER JOIN WidgetMeasuresList on "
                    "ExcelLayoutWidgets.Widget = WidgetMeasuresList.WidgetName WHERE ExcelLayoutWidgets.XLFolder = ? "
                    "ORDER BY XLWorkbook ASC, WidgetName ASC, MeasureName ASC;",
                    (folder,),
                )
                fetchData = self.dbConnection.fetchall()
                excelMeasureListData = [
                    {
                        "XLWorkbook": i["XLWorkbook"],
                        "WidgetName": i["WidgetName"],
                        "Type": i["Type"],
                        "MeasureName": i["MeasureName"],
                        "IsVisible": i["IsVisible"],
                        "Formula": i["Formula"],
                    }
                    for i in fetchData
                ]
                filename = "XLFolderMeasureList/" + folder + ".MeasureList.csv"
                self.createCSV(filename, excelMeasureListData, True)

            except Exception as e:
                self.logger.error("Error fetching ExcelMeasureList data: " + str(e))
                print("Error fetching ExcelMeasureList data: ", str(e))

            try:
                self.dbConnection.execute(
                    "SELECT \
                        ExcelLayoutWidgets.XLWorkbook, \
                        WidgetGraphEdgesList.WidgetName, \
                        WidgetGraphEdgesList.GraphName, \
                        WidgetGraphEdgesList.EdgeName \
                    from ExcelLayoutWidgets \
                    INNER JOIN WidgetGraphEdgesList on ExcelLayoutWidgets.Widget = WidgetGraphEdgesList.WidgetName\
                    WHERE ExcelLayoutWidgets.XLFolder = ? \
                    ORDER BY XLWorkbook ASC, WidgetName ASC, GraphName ASC;",
                    (folder,),
                )
                fetchData = self.dbConnection.fetchall()
                excelGraphListData = [
                    {
                        "XLWorkbook": i["XLWorkbook"],
                        "WidgetName": i["WidgetName"],
                        "GraphName": i["GraphName"],
                        "EdgeName": i["EdgeName"],
                    }
                    for i in fetchData
                ]
                filename = "XLFolderGraphList/" + folder + ".GraphList.csv"
                self.createCSV(filename, excelGraphListData, True)

            except Exception as e:
                self.logger.error("Error fetching ExcelGraphList data: " + str(e))
                print("Error fetching ExcelGraphList data: ", str(e))

    def createDependenciesCSVArray(self):
        """
        Create dependencies data related csv files from the database.
        :return: null
        """
        self.logger.info("Creating dependency CSV array.")
        # PluginInvocation file
        try:
            self.dbConnection.execute(
                "SELECT * FROM PluginInvocation ORDER BY EntityName ASC, PluginName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            pluginInvocationList = [
                {
                    "EntityType": i["EntityType"],
                    "EntityName": i["EntityName"],
                    "PluginName": i["PluginName"],
                    "PluginCode": i["PluginCode"],
                }
                for i in fetchData
            ]
            filename = "ModelDependencies/PluginInvocation.csv"
            self.createCSV(filename, pluginInvocationList)

        except Exception as e:
            self.logger.error("Error fetching PluginInvocation data: " + str(e))
            print("Error fetching PluginInvocation data: ", str(e))

        # ProcInvocation file
        try:
            self.dbConnection.execute(
                "SELECT * FROM ProcInvocation ORDER BY EntityName ASC, ProcName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            procInvocationList = [
                {
                    "EntityType": i["EntityType"],
                    "EntityName": i["EntityName"],
                    "ProcName": i["ProcName"],
                }
                for i in fetchData
            ]
            filename = "ModelDependencies/ProcInvocation.csv"
            self.createCSV(filename, procInvocationList)

        except Exception as e:
            self.logger.error("Error fetching ProcInvocation data: " + str(e))
            print("Error fetching ProcInvocation data: ", str(e))

        # DependenciesDetails file
        try:
            self.dbConnection.execute(
                "SELECT * FROM ModelDependencies ORDER BY LHS ASC, RHS ASC, EntityName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            dependenciesDetailsList = [
                {
                    "LHSType": i["LHSType"],
                    "LHS": i["LHS"],
                    "RHSType": i["RHSType"],
                    "RHS": i["RHS"],
                    "EntityType": i["EntityType"],
                    "EntityName": i["EntityName"],
                    "Scope": i["Scope"],
                    "Formula": i["Formula"],
                }
                for i in fetchData
            ]
            filename = "ModelDependencies/ModelDependenciesDetails.csv"
            self.createCSV(filename, dependenciesDetailsList)

        except Exception as e:
            self.logger.error("Error fetching ModelDependenciesDetails data: " + str(e))
            print("Error fetching ModelDependenciesDetails data: ", str(e))

        try:
            self.dbConnection.execute(
                "SELECT * FROM UIDependencies ORDER BY RHS ASC, EntityName ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            uiDependenciesDetailsList = [
                {
                    "RHSType": i["RHSType"],
                    "RHS": i["RHS"],
                    "EntityType": i["EntityType"],
                    "EntityName": i["EntityName"],
                    "DependencyType": i["DependencyType"],
                    "Formula": i["Formula"],
                }
                for i in fetchData
            ]
            filename = "UIDependencies/UIDependenciesDetails.csv"
            self.createCSV(filename, uiDependenciesDetailsList, True)
        except Exception as e:
            self.logger.error("Error fetching UIDependencies data: " + str(e))
            print("Error fetching UIDependencies data: ", str(e))

        # Formulae file
        try:
            self.dbConnection.execute(
                "SELECT DISTINCT LHSType, LHS, EntityType, EntityName, Scope, Formula from ModelDependencies ORDER BY "
                "LHS ASC, EntityName ASC, Scope ASC, Formula ASC;"
            )
            fetchData = self.dbConnection.fetchall()
            formulaeList = [
                {
                    "LHSType": i["LHSType"],
                    "LHS": i["LHS"],
                    "EntityType": i["EntityType"],
                    "EntityName": i["EntityName"],
                    "Scope": i["Scope"],
                    "Formula": i["Formula"],
                }
                for i in fetchData
            ]
            filename = "ModelDependencies/Formulae.csv"
            self.createCSV(filename, formulaeList)

        except Exception as e:
            self.logger.error(
                "Error fetching ModelDependencies Formulae data: " + str(e)
            )
            print("Error fetching ModelDependencies Formulae data: ", str(e))

    def createCSV(self, fileName, data, ui=False):
        """
        Create CSV files.
        :param fileName: filename
        :param data: data to be added to the csv file
        :param ui: boolean value to represent if the file is model or ui related
        :return: null
        """
        if len(data) > 0:
            if ui is True: # UI related files go to uiFilePath
                dirName = os.path.join(self.uiFilePath, fileName)
            else: # Non-UI files (general CSVs, entity files) go to the csv_export_base_path
                dirName = os.path.join(self.csv_export_base_path, fileName)
            
            # Ensure the full path to the file exists
            target_dir = os.path.dirname(dirName)
            if not os.path.exists(target_dir):
                try:
                    os.makedirs(target_dir)
                    os.makedirs(os.path.dirname(dirName))
                except OSError as exc:  # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise
            with open(dirName, "w", newline="", encoding="utf-8") as csvFile:
                if data == []:
                    return
                fileWriter = csv.writer(
                    csvFile, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
                )
                fileWriter.writerow(data[0].keys())
                for value in data:
                    fileWriter.writerow(value.values())
            self.logger.info("Data Written in file: " + fileName)
            return

    def createEntityFile(self, dataBody, fileName, ui=False):
        """
        Create entity based files.
        :param dataBody: file data body
        :param fileName: filename
        :param ui: if to be added to ui or model folder
        :return: null
        """
        if ui: # UI related files go to uiFilePath
            dirName = os.path.join(self.uiFilePath, fileName)
        else: # Non-UI files (general CSVs, entity files) go to the csv_export_base_path
            dirName = os.path.join(self.csv_export_base_path, fileName)

        # Ensure the full path to the file exists
        target_dir = os.path.dirname(dirName)
        if not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir)
                os.makedirs(os.path.dirname(dirName))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        with open(dirName, "w", newline="", encoding="utf-8") as txtFile:
            txtFile.write(dataBody)

        self.logger.info("Data Written in file: " + fileName)
        return

    @staticmethod
    def replaceNewLine(inputString):
        """
        Replace all line feed with carriage return and line feed for windows compatibility.
        :param inputString: input line
        :return: new line with CRLF
        """
        if re.search("\\r\\n", inputString):
            return inputString
        else:
            return inputString.replace("\n", "\r\n")
