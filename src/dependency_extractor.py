import logging
import re
from tables import insertData


class DependencyExtractor:
    def __init__(self, dbConnection):
        """
        DependencyExtractor Constructor.
        :param dbConnection: database connection
        """
        self.dbConnection = dbConnection.cursor()
        self.logger = logging.getLogger("extractor-logger")
        self.insertOutputParameterData()
        self.processMeasureConditionalFormats()
        self.activeRuleMeasureDependencies()
        self.procMeasureDependencies()
        self.actionButtonMeasureDependencies()
        self.pluginInvocationForJSRule()
        self.processMeasureTwins()
        self.processMeasureFormulae()
        self.processMeasureSpread()
        self.processRGenPluginInputQueries()
        self.processRGenPluginInputTables()
        self.processRGenPluginOutputTables()
        self.processPythonPluginInputTables()
        self.processTenantPluginDetails()
        self.processNonRPluginParams()
        self.processWidgetDependencies()
        self.cleanDependenciesTable()

    def insertOutputParameterData(self):
        """
        Insert the output parameters data and update the param type in the NonRPluginParams table.
        """
        insertList = insertData.split(";")
        for insert in insertList:
            insert = insert.strip()
            try:
                if insert.startswith("INSERT"):
                    self.dbConnection.execute(insert)
            except Exception as e:
                print("Cannot insert row: " + str(e))
                self.logger.error("Cannot insert row: " + str(e))
        self.dbConnection.execute(
            'UPDATE NonRPluginParams SET ParamType=(SELECT "Output" FROM PluginOutParameterListing '
            "WHERE NonRPluginParams.PluginClass = PluginOutParameterListing.PluginClassName AND "
            "NonRPluginParams.ParamName = PluginOutParameterListing.OutputParameterName);"
        )
        self.dbConnection.execute(
            'UPDATE NonRPluginParams SET ParamType="Input" WHERE ParamType IS NULL;'
        )

    def cleanDependenciesTable(self):
        """
        Clean dependencies tables.
        """
        self.logger.info("Clean up Model Dependency table.")
        self.dbConnection.execute(
            "INSERT INTO TEMPModelDependencies SELECT DISTINCT * From ModelDependencies;"
        )
        self.dbConnection.execute("DELETE FROM ModelDependencies;")
        self.dbConnection.execute(
            "INSERT INTO ModelDependencies SELECT * FROM TEMPModelDependencies;"
        )
        self.dbConnection.execute("DROP TABLE IF EXISTS TEMPModelDependencies;")
        self.logger.info("Clean up UI Dependency table.")
        self.dbConnection.execute(
            "INSERT INTO TEMPUIDependencies SELECT DISTINCT * From UIDependencies;"
        )
        self.dbConnection.execute("DELETE FROM UIDependencies;")
        self.dbConnection.execute(
            "INSERT INTO UIDependencies SELECT * FROM TEMPUIDependencies;"
        )
        self.dbConnection.execute("DROP TABLE IF EXISTS TEMPUIDependencies;")

    def processWidgetDependencies(self):
        """
        Process widget measure list dependencies.
        :return:
        """
        self.logger.info("Process Widget Measure List.")
        # Regular Measures
        self.dbConnection.execute(
            "INSERT INTO UIDependencies (TenantName, RHSType, RHS, EntityType, EntityName, DependencyType) "
            'SELECT TenantName, "Measure", MeasureName, "Widget", WidgetName, Type FROM WidgetMeasuresList '
            'WHERE Type="Regular";'
        )
        # Transient Measure
        self.dbConnection.execute(
            'SELECT * FROM WidgetMeasuresList WHERE Type="Transient";'
        )
        fetchData = self.dbConnection.fetchall()
        uiDepData = []
        for i in fetchData:
            if i["Formula"]:
                rhsMeasureList = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]", i["Formula"], flags=re.IGNORECASE
                )
                for rhsMeasure in rhsMeasureList:
                    rhs, rhsType = self.parseDependencyString(rhsMeasure)
                    uiDepData = uiDepData + [
                        {
                            "TenantName": i["TenantName"],
                            "RHSType": rhsType,
                            "RHS": rhs,
                            "EntityType": "Widget",
                            "EntityName": i["WidgetName"],
                            "DependencyType": i["Type"],
                            "Formula": i["Formula"],
                        }
                    ]
        self.insertIntoUIDependencyTable(uiDepData)

        # Filters Measure
        self.dbConnection.execute("SELECT * from WidgetMeasureFilters;")
        fetchData = self.dbConnection.fetchall()
        uiDepData = []
        for i in fetchData:
            if i["MeasureFilterExpr"]:
                rhsMeasureList = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]",
                    i["MeasureFilterExpr"],
                    flags=re.IGNORECASE,
                )
                for rhsMeasure in rhsMeasureList:
                    rhs, rhsType = self.parseDependencyString(rhsMeasure)
                    uiDepData = uiDepData + [
                        {
                            "TenantName": i["TenantName"],
                            "RHSType": rhsType,
                            "RHS": rhs,
                            "EntityType": "Widget",
                            "EntityName": i["WidgetName"],
                            "DependencyType": i["FilterScopeType"],
                            "Formula": i["MeasureFilterExpr"],
                        }
                    ]
        self.insertIntoUIDependencyTable(uiDepData)

        # Interdependent Measure
        self.dbConnection.execute(
            "INSERT INTO UIDependencies (TenantName, RHSType, RHS, EntityType, EntityName, DependencyType) "
            'SELECT TenantName, "Measure", InterDependentMeasureName, "Widget", WidgetName, "InterDependentMeasure" '
            "FROM WidgetInterdependentMeasures WHERE InterDependentMeasureName IS NOT NULL;"
        )

        # ValidationFormula Measure
        self.dbConnection.execute(
            "SELECT * FROM Measures WHERE ValidationFormula IS NOT NULL;"
        )
        fetchData = self.dbConnection.fetchall()
        uiDepData = []
        for i in fetchData:
            if i["ValidationFormula"]:
                rhsMeasureList = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]",
                    i["ValidationFormula"],
                    flags=re.IGNORECASE,
                )
                for rhsMeasure in rhsMeasureList:
                    rhs, rhsType = self.parseDependencyString(rhsMeasure)
                    uiDepData = uiDepData + [
                        {
                            "TenantName": i["TenantName"],
                            "RHSType": rhsType,
                            "RHS": rhs,
                            "EntityType": "Measure",
                            "EntityName": i["MeasureName"],
                            "DependencyType": "Validation Measure",
                            "Formula": i["ValidationFormula"],
                        }
                    ]
        self.insertIntoUIDependencyTable(uiDepData)

        # WidgetAssociationMeasures
        self.dbConnection.execute("SELECT * from WidgetAssociationMeasures;")
        fetchData = self.dbConnection.fetchall()
        uiDepData = []
        for i in fetchData:
            if i["AssocMeasureExpr"]:
                rhsMeasureList = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]",
                    i["AssocMeasureExpr"],
                    flags=re.IGNORECASE,
                )
                for rhsMeasure in rhsMeasureList:
                    rhs, rhsType = self.parseDependencyString(rhsMeasure)
                    uiDepData = uiDepData + [
                        {
                            "TenantName": i["TenantName"],
                            "RHSType": rhsType,
                            "RHS": rhs,
                            "EntityType": "Widget",
                            "EntityName": i["WidgetName"],
                            "DependencyType": "Association Measure",
                            "Formula": i["AssocMeasureExpr"],
                        }
                    ]
        self.insertIntoUIDependencyTable(uiDepData)

    def processNonRPluginParams(self):
        """
        Process non R plugin parameters and add it to the ModelDependencies table.
        """
        self.logger.info("Process NonRPluginParams Dependencies.")
        # Non R Plugins with param values as MeasureName.
        self.dbConnection.execute(
            "INSERT INTO ModelDependencies (TenantName, LHSType, LHS, EntityType, EntityName) "
            'SELECT n.TenantName, "Measure", n.ParamValue, n.PluginClass, n.PluginName '
            'FROM NonRPluginParams as n, Measures as m where n.ParamValue=m.MeasureName AND n.ParamType="Output";'
        )
        self.dbConnection.execute(
            "INSERT INTO ModelDependencies (TenantName, RHSType, RHS, EntityType, EntityName) "
            'SELECT n.TenantName, "Measure", n.ParamValue, n.PluginClass, n.PluginName '
            'FROM NonRPluginParams as n, Measures as m where n.ParamValue=m.MeasureName AND n.ParamType="Input";'
        )

        # Non R Plugins with param values as EdgeName.
        self.dbConnection.execute(
            "INSERT INTO ModelDependencies (TenantName, LHSType, LHS, EntityType, EntityName) "
            'SELECT n.TenantName, "Edge", n.ParamValue, n.PluginClass, n.PluginName FROM NonRPluginParams as n, '
            'GraphEdges as g where n.ParamValue=g.PropertyName AND n.ParamType="Output";'
        )
        self.dbConnection.execute(
            "INSERT INTO ModelDependencies (TenantName, RHSType, RHS, EntityType, EntityName) "
            'SELECT n.TenantName, "Edge", n.ParamValue, n.PluginClass, n.PluginName FROM NonRPluginParams as n, '
            'GraphEdges as g where n.ParamValue=g.PropertyName AND n.ParamType="Input";'
        )

    def processTenantPluginDetails(self):
        """
        Process tenant plugin dependencies.
        """
        self.logger.info("Process TenantPluginDetails Dependencies.")
        self.dbConnection.execute("SELECT * FROM TenantPluginDetails;")
        fetchData = self.dbConnection.fetchall()
        depData = []
        for i in fetchData:
            if i["PluginCode"]:
                rhsMeasureList = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]",
                    i["PluginCode"],
                    flags=re.IGNORECASE,
                )
                for rhsMeasure in rhsMeasureList:
                    rhs, rhsType = self.parseDependencyString(rhsMeasure)
                    depData = depData + [
                        {
                            "TenantName": i["TenantName"],
                            "LHSType": rhsType,
                            "LHS": rhs,
                            "RHSType": rhsType,
                            "RHS": rhs,
                            "EntityType": i["PluginClass"],
                            "EntityName": i["PluginName"],
                            "Scope": None,
                            "Formula": "Refer Plugin code",
                            "NamedSets": None,
                        }
                    ]
        self.insertIntoDependencyTable(depData)

    def processRGenPluginOutputTables(self):
        """
        Process R generalized plugins output table.
        """
        self.logger.info("Process RGenPluginOutputTables Dependencies.")
        self.dbConnection.execute("SELECT * FROM RGenPluginOutputTables;")
        fetchData = self.dbConnection.fetchall()
        depData = [
            {
                "TenantName": i["TenantName"],
                "LHSType": "Measure",
                "LHS": i["MeasureName"],
                "RHSType": None,
                "RHS": None,
                "EntityType": "RGenPluginOutputTables",
                "EntityName": i["PluginName"],
                "Scope": None,
                "Formula": None,
                "NamedSets": None,
            }
            for i in fetchData
        ]
        self.insertIntoDependencyTable(depData)

    def processRGenPluginInputTables(self):
        """
        Process R generalized plugin input tables.
        """
        self.logger.info("Process RGenPluginInputTables Dependencies.")
        self.dbConnection.execute("SELECT * FROM RGenPluginInputTables;")
        fetchData = self.dbConnection.fetchall()
        depData = [
            {
                "TenantName": i["TenantName"],
                "LHSType": None,
                "LHS": None,
                "RHSType": "Measure",
                "RHS": i["MeasureName"],
                "EntityType": "RGenPluginInputTables",
                "EntityName": i["PluginName"],
                "Scope": None,
                "Formula": None,
                "NamedSets": None,
            }
            for i in fetchData
        ]
        self.insertIntoDependencyTable(depData)

    def processRGenPluginInputQueries(self):
        """
        Process R generalized plugin input queries.
        """
        self.logger.info("Process RGenPluginInputQueries Dependencies.")
        self.dbConnection.execute("SELECT * FROM RGenPluginInputQueries;")
        fetchData = self.dbConnection.fetchall()
        depData = []
        for i in fetchData:
            if i["Query"]:
                rhsMeasureList = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]", i["Query"], flags=re.IGNORECASE
                )
                for rhsMeasure in rhsMeasureList:
                    rhs, rhsType = self.parseDependencyString(rhsMeasure)
                    depData = depData + [
                        {
                            "TenantName": i["TenantName"],
                            "LHSType": None,
                            "LHS": None,
                            "RHSType": rhsType,
                            "RHS": rhs,
                            "EntityType": "RGenPluginInputQueries",
                            "EntityName": i["PluginName"],
                            "Scope": None,
                            "Formula": i["Query"],
                            "NamedSets": None,
                        }
                    ]
        self.insertIntoDependencyTable(depData)

    def processPythonPluginInputTables(self):
        """
        Process Python plugin input tables.
        """
        self.logger.info("Process PythonPluginInputTables Dependencies.")
        self.dbConnection.execute(
            "SELECT i.TenantName, i.PluginName, i.Value, i.VariableKey FROM PythonPluginInputTables as i "
            'WHERE i.VariableKey="Query";'
        )
        fetchData = self.dbConnection.fetchall()
        depData = []
        for i in fetchData:
            if i["VariableKey"] == "Query":
                rhsMeasureList = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]", i["Value"], flags=re.IGNORECASE
                )
                for rhsMeasure in rhsMeasureList:
                    rhs, rhsType = self.parseDependencyString(rhsMeasure)
                    depData = depData + [
                        {
                            "TenantName": i["TenantName"],
                            "LHSType": None,
                            "LHS": None,
                            "RHSType": rhsType,
                            "RHS": rhs,
                            "EntityType": "PythonPluginInputTables",
                            "EntityName": i["PluginName"],
                            "Scope": None,
                            "Formula": i["Value"],
                            "NamedSets": None,
                        }
                    ]
        self.insertIntoDependencyTable(depData)
        #
        # self.dbConnection.execute(
        #     'SELECT i.TenantName, i.PluginName, i.Value, i.VariableKey, o.Type, o.MeasureName FROM '
        #     'PythonPluginInputTables as i, PythonPluginOutputMeasures as o WHERE i.VariableKey="Query" '
        #     'and i.PluginName==o.PluginName;'
        # )
        # fetchData = self.dbConnection.fetchall()
        # depData = []
        # for i in fetchData:
        #     if i['VariableKey'] == 'Query':
        #         rhsMeasureList = re.findall(
        #             r'Measure\.\[.*?\]|Edge\.\[.*?\]', i['Value'], flags=re.IGNORECASE
        #         )
        #         for rhsMeasure in rhsMeasureList:
        #             rhs, rhsType = self.parseDependencyString(rhsMeasure)
        #             depData = depData + [
        #                 {
        #                     'TenantName': i['TenantName'],
        #                     'LHSType': i['Type'],
        #                     'LHS': i['MeasureName'],
        #                     'RHSType': rhsType,
        #                     'RHS': rhs,
        #                     'EntityType': 'PythonPluginInputTables',
        #                     'EntityName': i['PluginName'],
        #                     'Scope': None,
        #                     'Formula': i['Value'],
        #                     'NamedSets': None
        #                 }
        #             ]
        # self.insertIntoDependencyTable(depData)

    def processMeasureSpread(self):
        """
        Process measure spread table for ModelDependencies table.
        """
        self.logger.info("Process MeasureSpreads Dependencies.")
        self.dbConnection.execute(
            "INSERT INTO ModelDependencies (TenantName, LHSType, LHS, RHSType, RHS, EntityType, EntityName)"
            'SELECT TenantName, "Measure", MeasureName, "Measure", BasisMeasureName, "Spreading", BasisMeasureName '
            '|| "-" || SpreadingType FROM MeasureSpreads;'
        )

    def processMeasureFormulae(self):
        """
        Process measure formulae for ModelDependencies table.
        """
        self.logger.info("Process MeasureFormulae Dependencies.")
        self.dbConnection.execute("SELECT * FROM MeasureFormulae;")
        fetchData = self.dbConnection.fetchall()
        measureFormulaeList = [
            {
                "TenantName": i["TenantName"],
                "MeasureName": i["MeasureName"],
                "MeasureFormula": i["MeasureFormula"],
            }
            for i in fetchData
        ]
        depData = []
        for measureFormula in measureFormulaeList:
            if measureFormula["MeasureFormula"]:
                rhsMeasureList = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]",
                    measureFormula["MeasureFormula"],
                    flags=re.IGNORECASE,
                )
                for rhsMeasure in rhsMeasureList:
                    rhs, rhsType = self.parseDependencyString(rhsMeasure)
                    depData = depData + [
                        {
                            "TenantName": measureFormula["TenantName"],
                            "LHSType": "Measure",
                            "LHS": measureFormula["MeasureName"],
                            "RHSType": rhsType,
                            "RHS": rhs,
                            "EntityType": "ComputedAggregation",
                            "EntityName": None,
                            "Scope": None,
                            "Formula": measureFormula["MeasureFormula"],
                            "NamedSets": None,
                        }
                    ]
        self.insertIntoDependencyTable(depData)

    def processMeasureTwins(self):
        """
        Process model twins for ModelDependencies table.
        """
        self.logger.info("Process Measure Twins Dependencies.")
        self.dbConnection.execute("SELECT * FROM MeasureTwins;")
        fetchData = self.dbConnection.fetchall()
        measureTwinList = [
            {
                "TenantName": i["TenantName"],
                "PrimaryMeasureName": i["PrimaryMeasureName"],
                "TwinMeasureName": i["TwinMeasureName"],
                "TwinToPrimaryFormula": i["TwinToPrimaryFormula"],
            }
            for i in fetchData
        ]
        depData = []
        for twinData in measureTwinList:
            if twinData["TwinToPrimaryFormula"]:
                rhsMeasureList = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]",
                    twinData["TwinToPrimaryFormula"],
                    flags=re.IGNORECASE,
                )
                for rhsMeasure in rhsMeasureList:
                    rhs, rhsType = self.parseDependencyString(rhsMeasure)
                    depData = depData + [
                        {
                            "TenantName": twinData["TenantName"],
                            "LHSType": "Measure",
                            "LHS": twinData["PrimaryMeasureName"],
                            "RHSType": rhsType,
                            "RHS": rhs,
                            "EntityType": "MeasureTwins",
                            "EntityName": None,
                            "Scope": None,
                            "Formula": twinData["TwinToPrimaryFormula"],
                            "NamedSets": None,
                        }
                    ]
        self.insertIntoDependencyTable(depData)

    def processMeasureConditionalFormats(self):
        """
        Process measure conditional formats for ModelDependencies table.
        """
        self.logger.info("Process Measure Conditional Formats Dependencies.")
        self.dbConnection.execute("SELECT * FROM MeasureConditionalFormats;")
        fetchData = self.dbConnection.fetchall()
        mcfData = [
            {
                "TenantName": i["TenantName"],
                "MeasureName": i["MeasureName"],
                "BgColorFormula": i["BgColorFormula"],
                "FgColorFormula": i["FgColorFormula"],
                "TrendFormula": i["TrendFormula"],
            }
            for i in fetchData
        ]
        depData = []
        for data in mcfData:
            fgFormula = data["FgColorFormula"]
            bgFormula = data["BgColorFormula"]
            trendFormulae = data["TrendFormula"]
            rhsMeasureListFG = []
            rhsMeasureListBG = []
            trendFormulaList = []
            if fgFormula:
                rhsMeasureListFG = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]", fgFormula, flags=re.IGNORECASE
                )
            if bgFormula:
                rhsMeasureListBG = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]", bgFormula, flags=re.IGNORECASE
                )
            if trendFormulae:
                trendFormulaList = re.findall(
                    r"Measure\.\[.*?\]|Edge\.\[.*?\]",
                    trendFormulae,
                    flags=re.IGNORECASE,
                )
            for rhsMeasureFG in rhsMeasureListFG:
                rhs, rhsType = self.parseDependencyString(rhsMeasureFG)
                depData = depData + [
                    {
                        "TenantName": data["TenantName"],
                        "RHSType": rhsType,
                        "RHS": rhs,
                        "EntityType": "ConditionalFormats",
                        "EntityName": data["MeasureName"],
                        "DependencyType": "Foreground Formula",
                        "Formula": fgFormula,
                    }
                ]
            for rhsMeasureBG in rhsMeasureListBG:
                rhs, rhsType = self.parseDependencyString(rhsMeasureBG)
                depData = depData + [
                    {
                        "TenantName": data["TenantName"],
                        "RHSType": rhsType,
                        "RHS": rhs,
                        "EntityType": "ConditionalFormats",
                        "EntityName": data["MeasureName"],
                        "DependencyType": "Background Formula",
                        "Formula": bgFormula,
                    }
                ]
            for trendFormula in trendFormulaList:
                rhs, rhsType = self.parseDependencyString(trendFormula)
                depData = depData + [
                    {
                        "TenantName": data["TenantName"],
                        "RHSType": rhsType,
                        "RHS": rhs,
                        "EntityType": "ConditionalFormats",
                        "EntityName": data["MeasureName"],
                        "DependencyType": "Trend Formula",
                        "Formula": trendFormulae,
                    }
                ]
        self.insertIntoUIDependencyTable(depData)

    @staticmethod
    def parseDependencyString(stringData):
        """
        Parse dependencies string.
        :param stringData: incoming string
        :return: return rhsMeasure and rhsType
        """
        rhsType = None
        if "measure" in stringData.lower():
            rhsType = "Measure"
        elif "edge" in stringData.lower():
            rhsType = "Edge"
        rhsMeasure = re.sub(
            re.compile(r"Measure\.\[|Edge\.\[", re.IGNORECASE), "", stringData
        )
        rhsMeasure = rhsMeasure.replace("]", "")
        return rhsMeasure, rhsType

    def pluginInvocationForJSRule(self):
        """
        Process ActionButtonJSRules for PluginInvocation table.
        """
        self.dbConnection.execute("SELECT * FROM ActionButtonJSRules;")
        fetchData = self.dbConnection.fetchall()
        jsRuleData = [
            {
                "TenantName": x["TenantName"],
                "EntityType": "ActionButton",
                "EntityName": x["ActionButtonName"],
                "PluginName": x["ModuleName"],
                "PluginCode": None,
            }
            for x in fetchData
        ]
        self.insertIntoPluginInvocationTable(jsRuleData)

    def activeRuleMeasureDependencies(self):
        """
        Extract active rule measures for the ModelDependencies table.
        :return:
        """
        self.logger.info("Extracting Active Rule measures dependencies.")
        try:
            self.dbConnection.execute("SELECT * FROM ActiveRuleFormulae ")
            fetchData = self.dbConnection.fetchall()
            formulaeData = [
                {
                    "TenantName": i["TenantName"],
                    "RuleFileName": i["RuleFileName"],
                    "FormulaStatement": i["FormulaStatement"],
                    "FormulaScope": i["ScopeGrain"],
                    "IsEnabled": i["IsEnabled"],
                }
                for i in fetchData
            ]
            for formula in formulaeData:
                if formula["IsEnabled"] == "1":
                    measuresList = formula["FormulaStatement"].strip().split("=", 1)
                    lhsType = ""
                    if "measure" in measuresList[0].lower():
                        lhsType = "Measure"
                    elif "edge" in measuresList[0].lower():
                        lhsType = "Edge"
                    lhsMeasure = re.sub(
                        re.compile(r"Measure\.\[|Edge\.\[", re.IGNORECASE),
                        "",
                        measuresList[0],
                    )
                    lhsMeasure = lhsMeasure.replace("]", "")

                    rhsMeasureList = re.findall(
                        r"Measure\.\[.*?\]|Edge\.\[.*?\]",
                        measuresList[1],
                        flags=re.IGNORECASE,
                    )
                    scope = formula["FormulaScope"]
                    namedSets = re.findall(
                        r"&[^ ]*",
                        scope,
                        flags=re.IGNORECASE,
                    )

                    rhsType = ""
                    measureDependenciesData = []
                    if len(rhsMeasureList) == 0:
                        measureDependenciesData = measureDependenciesData + [
                            {
                                "TenantName": formula["TenantName"],
                                "LHS": lhsMeasure.strip(),
                                "LHSType": lhsType,
                                "RHS": None,
                                "RHSType": None,
                                "EntityType": "ActiveRule",
                                "EntityName": formula["RuleFileName"],
                                "Scope": scope,
                                "Formula": formula["FormulaStatement"],
                                "NamedSets": ", ".join(namedSets),
                            }
                        ]
                    for rhsMeasure in rhsMeasureList:
                        if "measure" in rhsMeasure.lower():
                            rhsType = "Measure"
                        elif "edge" in rhsMeasure.lower():
                            rhsType = "Edge"
                        rhsMeasure = re.sub(
                            re.compile(r"Measure\.\[|Edge\.\[", re.IGNORECASE),
                            "",
                            rhsMeasure,
                        )
                        rhsMeasure = rhsMeasure.replace("]", "")
                        measureDependenciesData = measureDependenciesData + [
                            {
                                "TenantName": formula["TenantName"],
                                "LHS": lhsMeasure.strip(),
                                "LHSType": lhsType,
                                "RHS": rhsMeasure.strip(),
                                "RHSType": rhsType,
                                "EntityType": "ActiveRule",
                                "EntityName": formula["RuleFileName"],
                                "Scope": scope,
                                "Formula": formula["FormulaStatement"],
                                "NamedSets": ", ".join(namedSets),
                            }
                        ]
                    self.insertIntoDependencyTable(measureDependenciesData)

        except Exception as e:
            self.logger.error("Error generating data from ActiveRuleFormulae " + str(e))
            print("Error generating data from ActiveRuleFormulae " + str(e))

    def procMeasureDependencies(self):
        """
        Process procCodes table for measures for ModelDependencies table.
        :return:
        """
        self.logger.info("Extracting Procedures measures dependencies.")
        try:
            self.dbConnection.execute("SELECT * FROM ProcCodes;")
            fetchData = self.dbConnection.fetchall()
            procCodeData = [
                {
                    "TenantName": i["TenantName"],
                    "ProcName": i["ProcName"],
                    "ProcCode": i["ProcCode"],
                }
                for i in fetchData
            ]
            for proc in procCodeData:
                depObj = self.getDependencies(proc["ProcCode"])
                if "MeasureDep" in depObj and len(depObj["MeasureDep"]) > 0:
                    for x in depObj["MeasureDep"]:
                        measureDependenciesData = []
                        scope = x["Scope"]
                        namedSets = re.findall(r"&[^ ]*", scope, flags=re.IGNORECASE)
                        if len(x["RHS"]) > 0:
                            for i in x["RHS"]:
                                rhsType = None
                                if "measure" in i.lower():
                                    rhsType = "Measure"
                                elif "edge" in i.lower():
                                    rhsType = "Edge"
                                rhsMeasure = re.sub(
                                    re.compile(r"Measure\.\[|Edge\.\[", re.IGNORECASE),
                                    "",
                                    i,
                                )
                                rhsMeasure = rhsMeasure.replace("]", "")
                                measureDependenciesData = measureDependenciesData + [
                                    {
                                        "TenantName": proc["TenantName"],
                                        "LHS": x["LHS"].strip(),
                                        "LHSType": x["LHSType"],
                                        "RHS": rhsMeasure.strip(),
                                        "RHSType": rhsType,
                                        "EntityType": "Procedures",
                                        "EntityName": proc["ProcName"],
                                        "Scope": scope,
                                        "Formula": x["Formula"],
                                        "NamedSets": ", ".join(namedSets),
                                    }
                                ]
                        else:
                            measureDependenciesData = measureDependenciesData + [
                                {
                                    "TenantName": proc["TenantName"],
                                    "LHS": x["LHS"].strip(),
                                    "LHSType": x["LHSType"],
                                    "RHS": None,
                                    "RHSType": None,
                                    "EntityType": "Procedures",
                                    "EntityName": proc["ProcName"],
                                    "Scope": scope,
                                    "Formula": x["Formula"],
                                    "NamedSets": ", ".join(namedSets),
                                }
                            ]
                        self.insertIntoDependencyTable(measureDependenciesData)
                if "PluginInvocation" in depObj and len(depObj["PluginInvocation"]) > 0:
                    pluginInvocationData = [
                        {
                            "TenantName": proc["TenantName"],
                            "EntityType": "Procedures",
                            "EntityName": proc["ProcName"],
                            "PluginName": x["PluginName"],
                            "PluginCode": x["PluginCode"],
                        }
                        for x in depObj["PluginInvocation"]
                    ]
                    self.insertIntoPluginInvocationTable(pluginInvocationData)
                if "ProcInvocation" in depObj and len(depObj["ProcInvocation"]) > 0:
                    procInvocationData = [
                        {
                            "TenantName": proc["TenantName"],
                            "EntityType": "Procedures",
                            "EntityName": proc["ProcName"],
                            "ProcName": y["ProcName"],
                        }
                        for y in depObj["ProcInvocation"]
                    ]
                    self.insertIntoProcInvocationTable(procInvocationData)

        except Exception as e:
            self.logger.error("Error generating data from ProcCodes " + str(e))
            print("Error generating data from ProcCodes " + str(e))

    def actionButtonMeasureDependencies(self):
        """
        Process ActionButtonRules measures for ModelDependencies table.
        """
        self.logger.info("Extracting Action Button measures dependencies.")
        try:
            self.dbConnection.execute("SELECT * FROM ActionButtonRules;")
            fetchData = self.dbConnection.fetchall()
            actionButtonData = [
                {
                    "TenantName": i["TenantName"],
                    "ActionButtonName": i["ActionButtonName"],
                    "IBPLRule": i["IBPLRule"],
                }
                for i in fetchData
            ]
            for actionButton in actionButtonData:
                depObj = self.getDependencies(actionButton["IBPLRule"])
                for x in depObj["MeasureDep"]:
                    measureDependenciesData = []
                    scope = x["Scope"]
                    namedSets = re.findall(r"&[^ ]*", scope, flags=re.IGNORECASE)

                    if len(x["RHS"]) > 0:
                        for i in x["RHS"]:
                            rhsType = None
                            if "measure" in i.lower():
                                rhsType = "Measure"
                            elif "edge" in i.lower():
                                rhsType = "Edge"
                            rhsMeasure = re.sub(
                                re.compile(r"Measure\.\[|Edge\.\[", re.IGNORECASE),
                                "",
                                i,
                            )
                            rhsMeasure = rhsMeasure.replace("]", "")
                            measureDependenciesData = measureDependenciesData + [
                                {
                                    "TenantName": actionButton["TenantName"],
                                    "LHS": x["LHS"].strip(),
                                    "LHSType": x["LHSType"],
                                    "RHS": rhsMeasure.strip(),
                                    "RHSType": rhsType,
                                    "EntityType": "ActionButton",
                                    "EntityName": actionButton["ActionButtonName"],
                                    "Scope": scope,
                                    "Formula": x["Formula"],
                                    "NamedSets": ", ".join(namedSets),
                                }
                            ]
                    else:
                        measureDependenciesData = measureDependenciesData + [
                            {
                                "TenantName": actionButton["TenantName"],
                                "LHS": x["LHS"].strip(),
                                "LHSType": x["LHSType"],
                                "RHS": None,
                                "RHSType": None,
                                "EntityType": "ActionButton",
                                "EntityName": actionButton["ActionButtonName"],
                                "Scope": scope,
                                "Formula": x["Formula"],
                                "NamedSets": ", ".join(namedSets),
                            }
                        ]
                    self.insertIntoDependencyTable(measureDependenciesData)
                if "PluginInvocation" in depObj and len(depObj["PluginInvocation"]) > 0:
                    pluginInvocationData = [
                        {
                            "TenantName": actionButton["TenantName"],
                            "EntityType": "ActionButton",
                            "EntityName": actionButton["ActionButtonName"],
                            "PluginName": y["PluginName"],
                            "PluginCode": y["PluginCode"],
                        }
                        for y in depObj["PluginInvocation"]
                    ]
                    self.insertIntoPluginInvocationTable(pluginInvocationData)
                if "ProcInvocation" in depObj and len(depObj["ProcInvocation"]) > 0:
                    procInvocationData = [
                        {
                            "TenantName": actionButton["TenantName"],
                            "EntityType": "ActionButton",
                            "EntityName": actionButton["ActionButtonName"],
                            "ProcName": y["ProcName"],
                        }
                        for y in depObj["ProcInvocation"]
                    ]
                    self.insertIntoProcInvocationTable(procInvocationData)
        except Exception as e:
            self.logger.error("Error generating data from ActionButtonRules " + str(e))
            print("Error generating data from ActionButtonRules " + str(e))

    @staticmethod
    def getDependencies(codeText):
        """
        Parse dependencies data from code text.
        :param codeText: incoming code text
        :return: dependencies object
        """
        measureDep = []
        pluginInvocationObj = []
        procInvocationObj = []
        # removing comments
        codeText = codeText.strip()
        codeText = re.sub(re.compile(r"//.*?\n"), "", codeText)
        codeText = re.sub(re.compile(r"/\*.*?\*/", re.DOTALL), "", codeText)
        codeText = re.sub(
            re.compile(r"end\s*scope\s*;", re.IGNORECASE), "end scope;", codeText
        )
        # codeText = re.sub(re.compile(r"begin", re.IGNORECASE), '', codeText)
        codeText = re.sub(r"begin(?![^\[]*\])", "", codeText, flags=re.IGNORECASE)

        # Split with end scope
        for code in codeText.split("end scope;"):
            curScope = ""
            for line in code.strip().split(";"):
                line = re.sub(
                    re.compile(r"exec\s*plugin", re.IGNORECASE), "EXEC plugin", line
                )
                line = re.sub(
                    re.compile(r"exec\s*powershell\s*plugin", re.IGNORECASE),
                    "EXEC powershell plugin",
                    line,
                )
                line = re.sub(
                    re.compile(r"exec\s*procedure", re.IGNORECASE),
                    "EXEC procedure",
                    line,
                )

                if line.strip().lower().startswith("exec plugin"):
                    findName = re.findall(
                        r"instance \[.*?\]|instance .*? ", line, flags=re.IGNORECASE
                    )
                    if len(findName) > 0:
                        pluginName = (
                            findName[0]
                            .split(" ", 1)[1]
                            .replace("[", "")
                            .replace("]", "")
                        ).strip()
                        if len(pluginName) > 0:
                            pluginInvocationObj = pluginInvocationObj + [
                                {"PluginName": pluginName, "PluginCode": line.strip()}
                            ]
                if line.strip().lower().startswith("exec powershell plugin"):
                    findName = re.findall(
                        r"powershell plugin \[.*?\]|powershell plugin .*? ",
                        line,
                        flags=re.IGNORECASE,
                    )
                    if len(findName) > 0:
                        pluginName = (
                            findName[0]
                            .split(" ", 2)[-1]
                            .replace("[", "")
                            .replace("]", "")
                        ).strip()
                        if len(pluginName) > 0:
                            pluginInvocationObj = pluginInvocationObj + [
                                {"PluginName": pluginName, "PluginCode": line.strip()}
                            ]
                elif line.strip().lower().startswith("exec procedure"):
                    findName = re.findall(r"procedure .*? ", line, flags=re.IGNORECASE)
                    if len(findName) > 0:
                        procName = (
                            findName[0]
                            .split(" ", 1)[-1]
                            .replace("[", "")
                            .replace("]", "")
                        ).strip()
                        if len(procName) > 0:
                            procInvocationObj = procInvocationObj + [
                                {"ProcName": procName, "ProcCode": line.strip()}
                            ]

                elif "recurrence" in line.strip().lower():
                    curScope = line[line.lower().index("recurrence") :].strip()

                elif "evaluatemember" in line.strip().lower():
                    curScope = line[line.lower().index("evaluatemember") :].strip()

                elif "spread" in line.strip().lower():
                    curScope = line[line.lower().index("spread") :].strip()

                elif "cartesian" in line.strip().lower():
                    curScope = line[line.lower().index("cartesian") :].strip()

                elif "block" in line.strip().lower():
                    curScope = line[line.lower().index("block") :].strip()

                elif "scope" in line.strip().lower():
                    curScope = line[line.lower().index("scope") :].strip()

                elif line.strip().lower().startswith("measure"):
                    measureDetail = line.strip().split("=", 1)
                    lhsType = ""
                    if "measure" in measureDetail[0].lower():
                        lhsType = "Measure"
                    elif "edge" in measureDetail[0].lower():
                        lhsType = "Edge"
                    lhsMeasure = re.sub(
                        re.compile(r"Measure\.\[|Edge\.\[", re.IGNORECASE),
                        "",
                        measureDetail[0],
                    )
                    lhsMeasure = lhsMeasure.replace("]", "")
                    rhsMeasureList = re.findall(
                        r"Measure\.\[.*?\]|Edge\.\[.*?\]",
                        measureDetail[1].strip(),
                        flags=re.IGNORECASE,
                    )
                    measureDep = measureDep + [
                        {
                            "Scope": curScope + ";",
                            "LHS": lhsMeasure.strip(),
                            "LHSType": lhsType,
                            "RHS": rhsMeasureList,
                            "Formula": line.strip() + ";",
                        }
                    ]
        depObj = {
            "MeasureDep": measureDep,
            "PluginInvocation": pluginInvocationObj,
            "ProcInvocation": procInvocationObj,
        }
        return depObj

    def insertIntoDependencyTable(self, measureDependenciesData):
        """
        Insert into dependencies table.
        :param measureDependenciesData: measure dependencies data.
        """
        measureDependenciesDataToDB = []
        for i in measureDependenciesData:
            lshMeasure = i["LHS"]
            if lshMeasure is None:
                uploadType = "Derived"
            elif ("ERP" in lshMeasure) or ("External" in lshMeasure):
                uploadType = "Upload"
            else:
                uploadType = "Derived"
            measureDependenciesDataToDB.append(
                (
                    i["TenantName"],
                    i["LHSType"],
                    i["LHS"],
                    i["RHSType"],
                    i["RHS"],
                    i["EntityType"],
                    i["EntityName"],
                    i["Scope"],
                    i["Formula"],
                    i["NamedSets"],
                    uploadType,
                )
            )
        # measureDependenciesDataToDB = [
        #     (
        #         i['TenantName'],
        #         i['LHSType'],
        #         i['LHS'],
        #         i['RHSType'],
        #         i['RHS'],
        #         i['EntityType'],
        #         i['EntityName'],
        #         i['Scope'],
        #         i['Formula'],
        #         i['NamedSets'],
        #         'Derived' if (i['LHSType'] is None) or ('ERP' not in i['LHSType']) or ('External' not in i['LHSType']) else 'Upload'
        #     )
        #     for i in measureDependenciesData
        # ]
        try:
            self.dbConnection.executemany(
                "INSERT INTO ModelDependencies (TenantName, LHSType, LHS, RHSType, RHS, EntityType, EntityName, Scope, "
                "Formula, NamedSets, DataUploadType) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                measureDependenciesDataToDB,
            )
        except Exception as e:
            self.logger.error("Error generating data from ModelDependencies " + str(e))
            print("Error generating data from ModelDependencies " + str(e))

    def insertIntoUIDependencyTable(self, measureDependenciesData):
        """
        Insert into UI dependencies table.
        :param measureDependenciesData: measure dependencies data
        """
        measureDependenciesDataToDB = [
            (
                i["TenantName"],
                i["RHSType"],
                i["RHS"],
                i["EntityType"],
                i["EntityName"],
                i["DependencyType"],
                i["Formula"],
            )
            for i in measureDependenciesData
        ]
        try:
            self.dbConnection.executemany(
                "INSERT INTO UIDependencies (TenantName, RHSType, RHS, EntityType, EntityName, DependencyType, "
                "Formula) VALUES (?,?,?,?,?,?,?)",
                measureDependenciesDataToDB,
            )
        except Exception as e:
            self.logger.error("Error generating data from UIDependencies " + str(e))
            print("Error generating data from UIDependencies " + str(e))

    def insertIntoPluginInvocationTable(self, pluginData):
        """
        Insert into plugin invocation table.
        :param pluginData: plugin data
        """
        pluginDataToDB = [
            (
                i["TenantName"],
                i["EntityType"],
                i["EntityName"],
                i["PluginName"],
                i["PluginCode"],
            )
            for i in pluginData
        ]
        try:
            self.dbConnection.executemany(
                "INSERT INTO PluginInvocation (TenantName, EntityType, EntityName, PluginName, PluginCode) \
                VALUES (?,?,?,?,?)",
                pluginDataToDB,
            )
        except Exception as e:
            self.logger.error("Error generating data from PluginInvocation " + str(e))
            print("Error generating data from PluginInvocation " + str(e))

    def insertIntoProcInvocationTable(self, procData):
        """
        Insert into procedure invocation table.
        :param procData: procedure data
        """
        procDataToDB = [
            (
                i["TenantName"],
                i["EntityType"],
                i["EntityName"],
                i["ProcName"],
            )
            for i in procData
        ]
        try:
            self.dbConnection.executemany(
                "INSERT INTO ProcInvocation (TenantName, EntityType, EntityName, ProcName) \
                VALUES (?,?,?,?)",
                procDataToDB,
            )
        except Exception as e:
            self.logger.error("Error generating data from ProcInvocation " + str(e))
            print("Error generating data from ProcInvocation " + str(e))
