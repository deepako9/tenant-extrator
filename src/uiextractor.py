import logging
import sys
import json


class UIExtractor:

    def __init__(self, data, dbConnection):
        """
        UIExtractor Constructor.
        :param data: json data
        :param dbConnection: database connection
        """
        try:
            self.logger = logging.getLogger("extractor-logger")
        except:
            print("Not able to set application logging. Exiting")
            sys.exit()
        self.data = data
        self.dbConnection = dbConnection
        self.finalTenantWidgetsArray = []
        self.tenantWidgetIdToName = {}
        self.tenantName = self.data["Tenant"]["Name"]

    def createWidgetTablesInDB(self):
        """
        Insert data for widget related tables in the database.
        """
        if "Layout" in self.data:
            tenantWidgetsArray = self.data["Layout"]["WidgetDefinitions"]
            for widget in tenantWidgetsArray:
                tenantWidgetIdToNameTemp = {
                    widget["Id"]: widget["Name"] if "Name" in widget else None
                }
                self.tenantWidgetIdToName.update(tenantWidgetIdToNameTemp)

                widgetMeasuresVisibilityList = []
                if (
                    "ConfigJson" in widget
                    and "Presentation" in widget["ConfigJson"]
                    and "MeasureCollections" in widget["ConfigJson"]["Presentation"]
                ):
                    for x in widget["ConfigJson"]["Presentation"]["MeasureCollections"]:
                        if "Measures" in x:
                            widgetMeasuresVisibilityList = (
                                widgetMeasuresVisibilityList
                                + [
                                    {
                                        "MeasureName": (
                                            y["Name"] if "Name" in y else None
                                        ),
                                        "IsVisible": (
                                            y["IsVisible"]
                                            if "IsVisible" in y
                                            else (
                                                y["Visible"] if "Visible" in y else True
                                            )
                                        ),
                                        "Color": y["Color"] if "Color" in y else None,
                                    }
                                    for y in x["Measures"]
                                ]
                            )
                if (
                    "ConfigJson" in widget
                    and "Widget" in widget["ConfigJson"]
                    and "ActionButtonBindings" in widget["ConfigJson"]["Widget"]
                    and widget["ConfigJson"]["Widget"]["ActionButtonBindings"]
                    is not None
                ):
                    for abInWidget in widget["ConfigJson"]["Widget"][
                        "ActionButtonBindings"
                    ]:
                        try:
                            self.dbConnection.execute(
                                "INSERT INTO ActionButtonBindingsForWidget (TenantName, WidgetName, ActionButtonName) "
                                "VALUES (?,?,?)",
                                (
                                    self.tenantName,
                                    widget["Name"] if "Name" in widget else None,
                                    (
                                        abInWidget["Name"]
                                        if "Name" in abInWidget
                                        else None
                                    ),
                                ),
                            )
                        except Exception as ex:
                            self.logger.error(
                                "Unable to insert data into ActionButtonBindingsForWidget: "
                                + str(ex)
                            )
                            print(ex)

                if (
                    "ConfigJson" in widget
                    and "Widget" in widget["ConfigJson"]
                    and "ExcelActionButtons" in widget["ConfigJson"]["Widget"]
                    and widget["ConfigJson"]["Widget"]["ExcelActionButtons"] is not None
                ):
                    for excelAB in widget["ConfigJson"]["Widget"]["ExcelActionButtons"]:
                        try:
                            self.dbConnection.execute(
                                "INSERT INTO ExcelActionButtonsForWidget (TenantName, WidgetName, ActionButtonName, "
                                "IBPLExpression, IsBackgroundProcess) VALUES (?,?,?,?,?)",
                                (
                                    self.tenantName,
                                    widget["Name"] if "Name" in widget else None,
                                    excelAB["Name"] if "Name" in excelAB else None,
                                    excelAB["IBPLExpression"],
                                    excelAB["IsBackgroundProcess"],
                                ),
                            )
                        except Exception as ex:
                            self.logger.error(
                                "Unable to insert data into ExcelActionButtonsForWidget: "
                                + str(ex)
                            )
                            print(ex)

                measureData = []
                matchingWidgetModel = list(
                    filter(
                        lambda x: x["Id"] == widget["WidgetModelId"],
                        self.data["Layout"]["WidgetModels"],
                    )
                )[0]

                if "ConfigJson" in widget and "Presentation" in widget["ConfigJson"]:
                    widgetDefinitionPresentationData = [
                        {
                            "TenantName": self.tenantName,
                            "WidgetID": matchingWidgetModel["Id"],
                            "WidgetName": widget["Name"] if "Name" in widget else None,
                            "IsPrivate": (
                                widget["IsPrivate"] if "IsPrivate" in widget else None
                            ),
                            "WidgetType": (
                                widget["WidgetType"] if "WidgetType" in widget else None
                            ),
                            "PropertyName": key,
                            "PropertyValue": value,
                        }
                        for key, value in widget["ConfigJson"]["Presentation"].items()
                        if isinstance(value, (float, int, str, bool, tuple))
                    ]
                    toDB = [
                        (
                            i["TenantName"],
                            i["WidgetID"],
                            i["WidgetName"],
                            i["IsPrivate"],
                            i["WidgetType"],
                            i["PropertyName"],
                            i["PropertyValue"],
                        )
                        for i in widgetDefinitionPresentationData
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO WidgetDefinitionProperties (TenantName, WidgetID, WidgetName, "
                            "IsPrivate, WidgetType, PropertyName, PropertyValue) VALUES (?,?,?,?,?,?,?)",
                            toDB,
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into WidgetDefinitionProperties: "
                            + str(e)
                        )
                        print(
                            "Unable to insert data into WidgetDefinitionProperties: "
                            + str(e)
                        )

                widgetModelConfig = matchingWidgetModel["ConfigJson"]

                if "LevelAttributes" in widgetModelConfig:
                    attributeList = list(
                        (x for x in widgetModelConfig["LevelAttributes"])
                    )

                    for x in attributeList:
                        selectedMembersList = []
                        if "Dimension" in x and x["Dimension"] != "Measure":
                            if "IsFilter" in x and x["IsFilter"]:
                                for y in x["SelectedMembers"]:
                                    selectedMembersList.append(
                                        y["Name"] if "Name" in y else ""
                                    )
                                selectedMembersString = ", ".join(
                                    sorted(selectedMembersList)
                                )
                                filterData = {
                                    "TenantName": self.tenantName,
                                    "WidgetId": widget["Id"],
                                    "WidgetName": (
                                        widget["Name"] if "Name" in widget else None
                                    ),
                                    "DimName": x["Dimension"],
                                    "AttributeName": x["AttributeName"],
                                    "MemberFilterExpression": (
                                        x["MemberFilterExpression"]
                                        if "MemberFilterExpression" in x
                                        else ""
                                    ),
                                    "SelectedMembers": selectedMembersString,
                                    "IsSingleSelect": (
                                        x["IsSingleSelect"]
                                        if "IsSingleSelect" in x
                                        else None
                                    ),
                                    "IsCurrencyFilter": (
                                        x["IsCurrencyFilter"]
                                        if "IsCurrencyFilter" in x
                                        else None
                                    ),
                                }

                                try:
                                    self.dbConnection.execute(
                                        "INSERT INTO WidgetLevelAttrFilters (TenantName, WidgetId, WidgetName, DimName, "
                                        "AttributeName, MemberFilterExpression, SelectedMembers, IsSingleSelect, "
                                        "IsCurrencyFilter) VALUES (?,?,?,?,?,?,?,?,?)",
                                        (
                                            filterData["TenantName"],
                                            filterData["WidgetId"],
                                            filterData["WidgetName"],
                                            filterData["DimName"],
                                            filterData["AttributeName"],
                                            filterData["MemberFilterExpression"],
                                            filterData["SelectedMembers"],
                                            filterData["IsSingleSelect"],
                                            filterData["IsCurrencyFilter"],
                                        ),
                                    )
                                except Exception as e:
                                    self.logger.error(
                                        "Unable to insert data into WidgetLevelAttrFilters: "
                                        + str(e)
                                    )
                                    print(e)

                            else:
                                attributeListData = {
                                    "TenantName": self.tenantName,
                                    "WidgetId": widget["Id"],
                                    "WidgetName": (
                                        widget["Name"] if "Name" in widget else None
                                    ),
                                    "DimName": x["Dimension"],
                                    "AttributeName": x["AttributeName"],
                                    "MemberFilterExpression": (
                                        x["MemberFilterExpression"]
                                        if "MemberFilterExpression" in x
                                        else ""
                                    ),
                                    "RelationshipType": (
                                        x["RelationshipType"]
                                        if "RelationshipType" in x
                                        else None
                                    ),
                                    "EdgeDirection": (
                                        x["EdgeDirection"]
                                        if "EdgeDirection" in x
                                        else None
                                    ),
                                    "IsCurrencyFilter": (
                                        x["IsCurrencyFilter"]
                                        if "IsCurrencyFilter" in x
                                        else None
                                    ),
                                    "IsVisible": (
                                        x["IsVisible"] if "IsVisible" in x else None
                                    ),
                                    "IsAttributeRequired": (
                                        x["IsAttributeRequired"]
                                        if "IsAttributeRequired" in x
                                        else None
                                    ),
                                }

                                try:
                                    self.dbConnection.execute(
                                        "INSERT INTO WidgetLevelAttributes (TenantName, WidgetId, WidgetName, DimName, "
                                        "AttributeName, MemberFilterExpression, RelationshipType, EdgeDirection, "
                                        "IsCurrencyFilter, IsVisible, IsAttributeRequired) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                        (
                                            attributeListData["TenantName"],
                                            attributeListData["WidgetId"],
                                            attributeListData["WidgetName"],
                                            attributeListData["DimName"],
                                            attributeListData["AttributeName"],
                                            attributeListData["MemberFilterExpression"],
                                            attributeListData["RelationshipType"],
                                            attributeListData["EdgeDirection"],
                                            attributeListData["IsCurrencyFilter"],
                                            attributeListData["IsVisible"],
                                            attributeListData["IsAttributeRequired"],
                                        ),
                                    )
                                except Exception as e:
                                    self.logger.error(
                                        "Unable to insert data into WidgetLevelAttributes: "
                                        + str(e)
                                    )
                                    print(e)

                if "NamedSets" in widgetModelConfig:
                    namedSetList = list((x for x in widgetModelConfig["NamedSets"]))
                    for namedSet in namedSetList:
                        defaultName = (
                            namedSet["SelectedNamedSet"]["Name"]
                            if "SelectedNamedSet" in namedSet
                            and "Name" in namedSet["SelectedNamedSet"]
                            else ""
                        )
                        availableNamedSets = []
                        if "AvailableNamedSets" in namedSet:
                            availableNamedSets = namedSet["AvailableNamedSets"]
                        namedSetData = [
                            {
                                "TenantName": self.tenantName,
                                "WidgetID": matchingWidgetModel["Id"],
                                "WidgetName": (
                                    widget["Name"] if "Name" in widget else None
                                ),
                                "DimName": namedSet["DimensionName"],
                                "AvailableNamedSetName": (
                                    x["Name"] if "Name" in x else None
                                ),
                                "AvailableNamedSetDisplayName": x["DisplayName"],
                                "IsDefault": (
                                    "Yes" if x["Name"] == defaultName else "No"
                                ),
                            }
                            for x in availableNamedSets
                        ]
                        namedSetDataToDB = [
                            (
                                i["TenantName"],
                                i["WidgetID"],
                                i["WidgetName"],
                                i["DimName"],
                                i["AvailableNamedSetName"],
                                i["AvailableNamedSetDisplayName"],
                                i["IsDefault"],
                            )
                            for i in namedSetData
                        ]
                        try:
                            self.dbConnection.executemany(
                                "INSERT INTO WidgetNamedSets (TenantName, WidgetID, WidgetName, DimName, "
                                "AvailableNamedSetName, AvailableNamedSetDisplayName, IsDefault) "
                                " VALUES (?,?,?,?,?,?,?)",
                                namedSetDataToDB,
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to insert data into WidgetNamedSets: " + str(e)
                            )
                            print(e)

                if "GraphRelations" in widgetModelConfig:
                    graphList = list((x for x in widgetModelConfig["GraphRelations"]))

                    for x in graphList:
                        edgeProperties = []
                        if "EdgeProperties" in x:
                            edgeProperties = list(x["EdgeProperties"])
                        graphData = [
                            {
                                "TenantName": self.tenantName,
                                "WidgetID": matchingWidgetModel["Id"],
                                "WidgetName": (
                                    widget["Name"] if "Name" in widget else None
                                ),
                                "GraphName": x["Name"] if "Name" in x else None,
                                "EdgeName": edge["Name"] if "Name" in edge else None,
                            }
                            for edge in edgeProperties
                        ]
                        graphListDataToDB = [
                            (
                                i["TenantName"],
                                i["WidgetID"],
                                i["WidgetName"],
                                i["GraphName"],
                                i["EdgeName"],
                            )
                            for i in graphData
                        ]

                        try:
                            self.dbConnection.executemany(
                                "INSERT INTO WidgetGraphEdgesList (TenantName, WidgetID, WidgetName, GraphName, EdgeName) "
                                " VALUES (?,?,?,?,?)",
                                graphListDataToDB,
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to insert data into WidgetGraphEdgesList: "
                                + str(e)
                            )
                            print(e)

                if "AssociationMeasures" in widgetModelConfig:
                    measureFilterList = list(
                        x for x in widgetModelConfig["AssociationMeasures"]
                    )
                    for measureFilter in measureFilterList:
                        if (
                            "MeasureFilterIBPLExpression" in measureFilter
                            and measureFilter["MeasureFilterIBPLExpression"] != ""
                        ):
                            measureFilterData = {
                                "TenantName": self.tenantName,
                                "WidgetID": matchingWidgetModel["Id"],
                                "WidgetName": (
                                    widget["Name"] if "Name" in widget else None
                                ),
                                "MeasureFilterExpr": measureFilter[
                                    "MeasureFilterIBPLExpression"
                                ],
                                "FilterScopeType": (
                                    measureFilter["MeasureFilterScope"]
                                    if "MeasureFilterScope" in measureFilter
                                    else ""
                                ),
                            }
                            try:
                                self.dbConnection.execute(
                                    "INSERT INTO WidgetMeasureFilters (TenantName, WidgetID, WidgetName, MeasureFilterExpr,"
                                    " FilterScopeType) VALUES (?,?,?,?,?)",
                                    (
                                        measureFilterData["TenantName"],
                                        measureFilterData["WidgetID"],
                                        measureFilterData["WidgetName"],
                                        measureFilterData["MeasureFilterExpr"],
                                        measureFilterData["FilterScopeType"],
                                    ),
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into WidgetMeasureFilters: "
                                    + str(e)
                                )
                                print(
                                    "Unable to insert data into WidgetMeasureFilters: ",
                                    str(e),
                                )

                if "FilterProperties" in widgetModelConfig:
                    if (
                        "VersionDependentFilter"
                        in widgetModelConfig["FilterProperties"]
                    ):
                        interDependentMeasureName = None
                        if (
                            "InterDependentMeasure"
                            in widgetModelConfig["FilterProperties"]
                        ):
                            if (
                                "Name"
                                in widgetModelConfig["FilterProperties"][
                                    "InterDependentMeasure"
                                ]
                            ):
                                interDependentMeasureName = widgetModelConfig[
                                    "FilterProperties"
                                ]["InterDependentMeasure"]["Name"]

                        interdependentMeasureData = {
                            "TenantName": self.tenantName,
                            "WidgetID": matchingWidgetModel["Id"],
                            "WidgetName": widget["Name"] if "Name" in widget else None,
                            "VersionDependentFilter": widgetModelConfig[
                                "FilterProperties"
                            ]["VersionDependentFilter"],
                            "InterDependentMeasureName": interDependentMeasureName,
                        }
                        try:
                            self.dbConnection.execute(
                                "INSERT INTO WidgetInterDependentMeasures (TenantName, WidgetID, WidgetName, "
                                "VersionDependentFilter, InterDependentMeasureName) VALUES (?,?,?,?,?)",
                                (
                                    interdependentMeasureData["TenantName"],
                                    interdependentMeasureData["WidgetID"],
                                    interdependentMeasureData["WidgetName"],
                                    interdependentMeasureData["VersionDependentFilter"],
                                    interdependentMeasureData[
                                        "InterDependentMeasureName"
                                    ],
                                ),
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to insert data into WidgetInterDependentMeasures: "
                                + str(e)
                            )
                            print(
                                "Unable to insert data into WidgetInterDependentMeasures: ",
                                str(e),
                            )

                if "AssociationMeasureExpressions" in widgetModelConfig:
                    associationMeasureList = list(
                        x for x in widgetModelConfig["AssociationMeasureExpressions"]
                    )
                    for associationMeasure in associationMeasureList:
                        if (
                            "Expression" in associationMeasure
                            and associationMeasure["Expression"] != ""
                        ):
                            associationMeasureData = {
                                "TenantName": self.tenantName,
                                "WidgetID": matchingWidgetModel["Id"],
                                "WidgetName": (
                                    widget["Name"] if "Name" in widget else None
                                ),
                                "AssocMeasureExpr": associationMeasure["Expression"],
                            }
                            try:
                                self.dbConnection.execute(
                                    "INSERT INTO WidgetAssociationMeasures (TenantName, WidgetID, WidgetName, "
                                    "AssocMeasureExpr) VALUES (?,?,?,?)",
                                    (
                                        associationMeasureData["TenantName"],
                                        associationMeasureData["WidgetID"],
                                        associationMeasureData["WidgetName"],
                                        associationMeasureData["AssocMeasureExpr"],
                                    ),
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into WidgetAssociationMeasures: "
                                    + str(e)
                                )
                                print(
                                    "Unable to insert data into WidgetAssociationMeasures: ",
                                    str(e),
                                )

                regMeasList = []
                if "RegularMeasures" in widgetModelConfig:
                    for x in widgetModelConfig["RegularMeasures"]:
                        if "Name" in x:
                            regMeasList.append(x)

                transMeasList = []
                if "TransientMeasures" in widgetModelConfig:
                    for x in widgetModelConfig["TransientMeasures"]:
                        if "Name" in x:
                            transMeasList.append(x)

                for x in regMeasList:
                    isVisible = True
                    color = None
                    for i in widgetMeasuresVisibilityList:
                        if i["MeasureName"] == x["Name"]:
                            isVisible = i["IsVisible"]
                            color = i["Color"]

                    measureData = measureData + [
                        {
                            "TenantName": self.tenantName,
                            "WidgetID": matchingWidgetModel["Id"],
                            "WidgetName": widget["Name"] if "Name" in widget else None,
                            "Type": "Regular",
                            "MeasureName": x["Name"] if "Name" in x else None,
                            "IsVisible": isVisible,
                            "Formula": None,
                            "Color": color,
                        }
                    ]

                for x in transMeasList:
                    isVisible = True
                    color = None
                    for i in widgetMeasuresVisibilityList:
                        if i["MeasureName"] == x["Name"]:
                            isVisible = i["IsVisible"]
                            color = i["Color"]
                    measureData = measureData + [
                        {
                            "TenantName": self.tenantName,
                            "WidgetID": matchingWidgetModel["Id"],
                            "WidgetName": widget["Name"] if "Name" in widget else None,
                            "Type": "Transient",
                            "MeasureName": x["Name"] if "Name" in x else None,
                            "IsVisible": isVisible,
                            "Formula": x["Formula"] if "Formula" in x else None,
                            "Color": color,
                        }
                    ]

                measureDataToDB = [
                    (
                        i["TenantName"],
                        i["WidgetID"],
                        i["WidgetName"],
                        i["Type"],
                        i["MeasureName"],
                        i["IsVisible"],
                        i["Formula"],
                        i["Color"],
                    )
                    for i in measureData
                ]

                try:
                    self.dbConnection.executemany(
                        "INSERT INTO WidgetMeasuresList (TenantName, WidgetID, WidgetName, Type, MeasureName, IsVisible, "
                        "Formula, Color) VALUES (?,?,?,?,?,?,?,?)",
                        measureDataToDB,
                    )
                except Exception as e:
                    self.logger.error(
                        "Unable to insert data into WidgetMeasuresList: " + str(e)
                    )
                    print(e)

            self.finalTenantWidgetsArray = [
                {
                    "TenantName": self.tenantName,
                    "WidgetID": x["Id"],
                    "WidgetName": x["Name"] if "Name" in x else None,
                    "WidgetType": x["WidgetType"],
                    "IsPrivate": x["IsPrivate"],
                    "CreatedUserId": x["CreatedUserId"],
                    "TileUsageCount": 0,
                    "ViewUsageCount": 0,
                    "ExcelUsageCount": 0,
                    "TotalUsageCount": 0,
                }
                for x in tenantWidgetsArray
            ]

    def createWebLayoutTablesInDB(self):
        """
        Insert data in the web layout tables in the database.
        """
        if "Layout" in self.data:
            tenantWorkspaceIdToName = {}
            tenantPageGroupIdToName = {}
            tenantPageIdToName = {}
            tenantWorkspacesArray = self.data["Layout"]["Workspaces"]
            viewListWithId = []
            workspacePosition = 0
            for workspace in tenantWorkspacesArray:
                workspacePosition = workspacePosition + 1
                tenantWorkspaceIdToNameTemp = {workspace["Id"]: workspace["Title"]}
                tenantWorkspaceIdToName.update(tenantWorkspaceIdToNameTemp)
                wsRoles = ""
                if "Roles" in workspace:
                    wsRoles = ", ".join(workspace["Roles"])
                try:
                    self.dbConnection.execute(
                        "INSERT INTO Workspaces (TenantName, WorkspaceName, WorkspaceTitle, WorkspacePosition, "
                        "WorkspaceIsHidden, JSONWorkspacePosition, Roles) VALUES (?,?,?,?,?,?,?)",
                        (
                            self.tenantName,
                            workspace["Name"],
                            workspace["Title"],
                            workspacePosition,
                            workspace["IsHidden"],
                            workspace["Position"],
                            wsRoles,
                        ),
                    )
                except Exception as e:
                    self.logger.error(
                        "Unable to insert data into Workspaces: " + str(e)
                    )
                    print(e)

                for pageGroup in workspace["PageGroups"]:
                    tenantPageGroupIdToNameTemp = {pageGroup["Id"]: pageGroup["Name"]}
                    tenantPageGroupIdToName.update(tenantPageGroupIdToNameTemp)

                    try:
                        self.dbConnection.execute(
                            "INSERT INTO PageGroups (TenantName, WorkspaceName, PageGroupName, PageGroupTitle, "
                            "PageGroupDisplayOrder) VALUES (?,?,?,?,?)",
                            (
                                self.tenantName,
                                workspace["Title"],
                                pageGroup["Name"],
                                pageGroup["Title"],
                                pageGroup["DisplayOrder"],
                            ),
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into PageGroups: " + str(e)
                        )
                        print(e)

                for page in workspace["Pages"]:
                    tenantPageIdToNameTemp = {page["Id"]: page["Title"]}
                    tenantPageIdToName.update(tenantPageIdToNameTemp)
                    pgNameList = list(
                        filter(
                            lambda x: x["Id"] == page["PageGroupId"],
                            workspace["PageGroups"],
                        )
                    )
                    pgName = "-NoPageGroup-"
                    if len(pgNameList) == 1:
                        pgName = pgNameList[0]["Name"]
                    if len(pgNameList) > 1:
                        pgName = "-MultiplePageGroup-"
                    try:
                        self.dbConnection.execute(
                            "INSERT INTO Pages (TenantName, WorkspaceName, PageGroupName, PageName, PageTitle, "
                            "PageDisplayOrder, PageIsDefault) VALUES (?,?,?,?,?,?,?)",
                            (
                                self.tenantName,
                                workspace["Title"],
                                pgName,
                                page["Name"],
                                page["Title"],
                                page["DisplayOrder"],
                                page["IsDefault"],
                            ),
                        )
                    except Exception as e:
                        self.logger.error("Unable to insert data into Pages: " + str(e))
                        print(e)

                    sortedPageWidgetList = sorted(
                        (
                            x
                            for x in page["Widgets"]
                            if (x["WidgetDefinitionId"] is not None)
                        ),
                        key=lambda x: x["Rank"],
                    )

                    tenantWebLayoutPageWidgetsArray = [
                        {
                            "TenantName": self.tenantName,
                            "WorkspaceName": workspace["Title"],
                            "PageGroupName": pgName,
                            "PageName": page["Title"],
                            "Widget": self.tenantWidgetIdToName[
                                x["WidgetDefinitionId"]
                            ],
                        }
                        for x in sortedPageWidgetList
                    ]
                    tenantWebLayoutPageWidgetsArrayDataToDB = [
                        (
                            i["TenantName"],
                            i["WorkspaceName"],
                            i["PageGroupName"],
                            i["PageName"],
                            i["Widget"],
                        )
                        for i in tenantWebLayoutPageWidgetsArray
                    ]

                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO WebLayoutPageWidgets (TenantName, WorkspaceName, PageGroupName, PageName, Widget )"
                            " VALUES (?,?,?,?,?)",
                            tenantWebLayoutPageWidgetsArrayDataToDB,
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into WebLayoutPageWidgets: " + str(e)
                        )
                        print(e)
                    for view in page["Views"]:
                        viewListWithId.append(
                            {
                                "ViewId": view["ViewId"],
                                "ViewName": view["Name"],
                                "WorkspaceName": workspace["Title"],
                                "PageGroupName": pgName,
                                "PageName": page["Title"],
                            }
                        )

                    for view in page["Views"]:
                        viewRoles = ""
                        if "Roles" in view:
                            viewRoles = ", ".join(view["Roles"])
                        try:
                            self.dbConnection.execute(
                                "INSERT INTO Views (TenantName, WorkspaceName, PageGroupName, PageName, ViewName, "
                                "ViewTitle, ViewPosition, ViewIsDefault, Roles) VALUES (?,?,?,?,?,?,?,?,?)",
                                (
                                    self.tenantName,
                                    workspace["Title"],
                                    pgName,
                                    page["Title"],
                                    view["Name"],
                                    view["Title"],
                                    view["Position"],
                                    view["IsDefault"],
                                    viewRoles,
                                ),
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to insert data into Views: " + str(e)
                            )
                            print(e)

                        for viewWidget in view["ViewWidgetDefinitions"]:
                            matchingViewWidget = list(
                                filter(
                                    lambda y: y["WidgetID"]
                                    == viewWidget["WidgetDefinitionId"],
                                    self.finalTenantWidgetsArray,
                                )
                            )[0]
                            matchingViewWidget["ViewUsageCount"] = (
                                matchingViewWidget["ViewUsageCount"] + 1
                            )
                            tenantWebLayoutViewWidgetsArray = [
                                {
                                    "TenantName": self.tenantName,
                                    "Workspace": workspace["Title"],
                                    "Pagegroup": pgName,
                                    "Page": page["Title"],
                                    "View": view["Title"],
                                    "WidgetName": self.tenantWidgetIdToName[
                                        viewWidget["WidgetDefinitionId"]
                                    ],
                                    "WidgetTitle": (
                                        viewWidget["Name"]
                                        if "Name" in viewWidget
                                        else None
                                    ),
                                    "IsAnchor": (
                                        viewWidget["IsPrimary"]
                                        if "IsPrimary" in viewWidget
                                        else None
                                    ),
                                }
                            ]
                            tenantWebLayoutViewWidgetsArrayDataToDB = [
                                (
                                    i["TenantName"],
                                    i["Workspace"],
                                    i["Pagegroup"],
                                    i["Page"],
                                    i["View"],
                                    i["WidgetName"],
                                    i["WidgetTitle"],
                                    i["IsAnchor"],
                                )
                                for i in tenantWebLayoutViewWidgetsArray
                            ]
                            try:
                                self.dbConnection.executemany(
                                    "INSERT INTO WebLayoutViewWidgets (TenantName, Workspace, Pagegroup, Page, View, "
                                    "WidgetName, WidgetTitle, IsAnchor) VALUES (?,?,?,?,?,?,?,?)",
                                    tenantWebLayoutViewWidgetsArrayDataToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into WebLayoutViewWidgets: "
                                    + str(e)
                                )
                                print(e)

                            if (
                                "ConfigJson" in viewWidget
                                and "FilterSharings" in viewWidget["ConfigJson"]
                            ):
                                filterSharingData = [
                                    {
                                        "TenantName": self.tenantName,
                                        "WorkspaceName": workspace["Title"],
                                        "PageGroupName": pgName,
                                        "PageName": page["Title"],
                                        "ViewName": view["Title"],
                                        "WidgetName": self.tenantWidgetIdToName[
                                            viewWidget["WidgetDefinitionId"]
                                        ],
                                        "DimName": (
                                            x["LevelAttribute"]["Dimension"]
                                            if "Dimension" in x["LevelAttribute"]
                                            else ""
                                        ),
                                        "AttributeName": (
                                            x["LevelAttribute"]["AttributeName"]
                                            if "AttributeName" in x["LevelAttribute"]
                                            else ""
                                        ),
                                        "MemberFilterExpression": (
                                            x["MemberFilterExpression"]
                                            if "MemberFilterExpression" in x
                                            else ""
                                        ),
                                        "Scope": x["Scope"] if "Scope" in x else "",
                                    }
                                    for x in viewWidget["ConfigJson"]["FilterSharings"]
                                ]
                                filterSharingDataToDB = [
                                    (
                                        i["TenantName"],
                                        i["WorkspaceName"],
                                        i["PageGroupName"],
                                        i["PageName"],
                                        i["ViewName"],
                                        i["WidgetName"],
                                        i["DimName"],
                                        i["AttributeName"],
                                        i["Scope"],
                                        i["MemberFilterExpression"],
                                    )
                                    for i in filterSharingData
                                ]
                                try:
                                    self.dbConnection.executemany(
                                        "INSERT INTO WidgetFilterSharings (TenantName, WorkspaceName, PageGroupName, "
                                        "PageName, ViewName, WidgetName, DimName, AttributeName, Scope, "
                                        "MemberFilterExpression) VALUES (?,?,?,?,?,?,?,?,?,?)",
                                        filterSharingDataToDB,
                                    )
                                except Exception as e:
                                    self.logger.error(
                                        "Unable to insert data into WidgetFilterSharings: "
                                        + str(e)
                                    )
                                    print(e)

                            if (
                                "ConfigJson" in viewWidget
                                and "FilterScopes" in viewWidget["ConfigJson"]
                            ):
                                filterLinkingData = [
                                    {
                                        "TenantName": self.tenantName,
                                        "WorkspaceName": workspace["Title"],
                                        "PageGroupName": pgName,
                                        "PageName": page["Title"],
                                        "ViewName": view["Title"],
                                        "WidgetName": self.tenantWidgetIdToName[
                                            viewWidget["WidgetDefinitionId"]
                                        ],
                                        "DimName": (
                                            x["LevelAttribute"]["Dimension"]
                                            if "Dimension" in x["LevelAttribute"]
                                            else ""
                                        ),
                                        "AttributeName": (
                                            x["LevelAttribute"]["AttributeName"]
                                            if "AttributeName" in x["LevelAttribute"]
                                            else ""
                                        ),
                                        "Scope": x["Scope"] if "Scope" in x else "",
                                    }
                                    for x in viewWidget["ConfigJson"]["FilterScopes"]
                                ]
                                filterLinkingDataToDB = [
                                    (
                                        i["TenantName"],
                                        i["WorkspaceName"],
                                        i["PageGroupName"],
                                        i["PageName"],
                                        i["ViewName"],
                                        i["WidgetName"],
                                        i["DimName"],
                                        i["AttributeName"],
                                        i["Scope"],
                                    )
                                    for i in filterLinkingData
                                ]
                                try:
                                    self.dbConnection.executemany(
                                        "INSERT INTO WidgetFilterLinkings (TenantName, WorkspaceName, PageGroupName, "
                                        "PageName, ViewName, WidgetName, DimName, AttributeName, Scope) "
                                        "VALUES (?,?,?,?,?,?,?,?,?)",
                                        filterLinkingDataToDB,
                                    )
                                except Exception as e:
                                    self.logger.error(
                                        "Unable to insert data into WidgetFilterLinkings: "
                                        + str(e)
                                    )
                                    print(e)

                            if (
                                "ConfigJson" in viewWidget
                                and "ShowInfoContext" in viewWidget["ConfigJson"]
                            ):
                                widgetInfoContext = viewWidget["ConfigJson"][
                                    "ShowInfoContext"
                                ]
                                folderList = []
                                if (
                                    "Pulse" in widgetInfoContext
                                    and widgetInfoContext["Pulse"] is not None
                                    and "Folders" in widgetInfoContext["Pulse"]
                                    and widgetInfoContext["Pulse"]["Folders"]
                                    is not None
                                    and isinstance(
                                        widgetInfoContext["Pulse"]["Folders"], list
                                    )
                                ):
                                    for folder in widgetInfoContext["Pulse"]["Folders"]:
                                        folderList.append(folder["Name"])
                                infoContextData = [
                                    {
                                        "TenantName": self.tenantName,
                                        "WorkspaceName": workspace["Title"],
                                        "PageGroupName": pgName,
                                        "PageName": page["Title"],
                                        "ViewName": view["Title"],
                                        "WidgetName": self.tenantWidgetIdToName[
                                            viewWidget["WidgetDefinitionId"]
                                        ],
                                        "MemberInfo": (
                                            widgetInfoContext["MemberInfo"]
                                            if "MemberInfo" in widgetInfoContext
                                            else None
                                        ),
                                        "Title": (
                                            widgetInfoContext["Pulse"]["Title"]
                                            if "Pulse" in widgetInfoContext
                                            and widgetInfoContext["Pulse"] is not None
                                            and "Title" in widgetInfoContext["Pulse"]
                                            else None
                                        ),
                                        "UnreadOnly": (
                                            widgetInfoContext["Pulse"]["PostIndicator"][
                                                "UnreadOnly"
                                            ]
                                            if "Pulse" in widgetInfoContext
                                            and widgetInfoContext["Pulse"] is not None
                                            and "PostIndicator"
                                            in widgetInfoContext["Pulse"]
                                            and widgetInfoContext["Pulse"][
                                                "PostIndicator"
                                            ]
                                            is not None
                                            and "UnreadOnly"
                                            in widgetInfoContext["Pulse"][
                                                "PostIndicator"
                                            ]
                                            else None
                                        ),
                                        "MemberIndicator": (
                                            widgetInfoContext["Pulse"]["PostIndicator"][
                                                "MemberIndicator"
                                            ]
                                            if "Pulse" in widgetInfoContext
                                            and widgetInfoContext["Pulse"] is not None
                                            and "PostIndicator"
                                            in widgetInfoContext["Pulse"]
                                            and widgetInfoContext["Pulse"][
                                                "PostIndicator"
                                            ]
                                            is not None
                                            and "MemberIndicator"
                                            in widgetInfoContext["Pulse"][
                                                "PostIndicator"
                                            ]
                                            else None
                                        ),
                                        "LastNDays": (
                                            widgetInfoContext["Pulse"]["PostIndicator"][
                                                "LastNDays"
                                            ]
                                            if "Pulse" in widgetInfoContext
                                            and widgetInfoContext["Pulse"] is not None
                                            and "PostIndicator"
                                            in widgetInfoContext["Pulse"]
                                            and widgetInfoContext["Pulse"][
                                                "PostIndicator"
                                            ]
                                            is not None
                                            and "LastNDays"
                                            in widgetInfoContext["Pulse"][
                                                "PostIndicator"
                                            ]
                                            else None
                                        ),
                                        "Folders": ", ".join(folderList),
                                        "IsShowTask": (
                                            True
                                            if "TaskManagement" in widgetInfoContext
                                            else False
                                        ),
                                        "TaskIndicator": (
                                            True
                                            if "TaskManagement" in widgetInfoContext
                                            and widgetInfoContext["TaskManagement"]
                                            is not None
                                            and "TaskIndicator"
                                            in widgetInfoContext["TaskManagement"]
                                            and widgetInfoContext["TaskManagement"][
                                                "TaskIndicator"
                                            ]
                                            is not None
                                            else False
                                        ),
                                    }
                                ]
                                infoContextDataToDB = [
                                    (
                                        i["TenantName"],
                                        i["WorkspaceName"],
                                        i["PageGroupName"],
                                        i["PageName"],
                                        i["ViewName"],
                                        i["WidgetName"],
                                        i["MemberInfo"],
                                        i["Title"],
                                        i["UnreadOnly"],
                                        i["MemberIndicator"],
                                        i["LastNDays"],
                                        i["Folders"],
                                        i["IsShowTask"],
                                        i["TaskIndicator"],
                                    )
                                    for i in infoContextData
                                ]

                                try:
                                    self.dbConnection.executemany(
                                        "INSERT INTO WidgetInfoContext (TenantName, WorkspaceName, PageGroupName, "
                                        "PageName, ViewName, WidgetName, MemberInfo, Title, UnreadOnly, "
                                        "MemberIndicator, LastNDays, Folders, IsShowTask, TaskIndicator) "
                                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                        infoContextDataToDB,
                                    )
                                except Exception as e:
                                    self.logger.error(
                                        "Unable to insert data into WidgetInfoContext: "
                                        + str(e)
                                    )
                                    print(
                                        "Unable to insert data into WidgetInfoContext: "
                                        + str(e)
                                    )

            for workspace in tenantWorkspacesArray:
                for page in workspace["Pages"]:
                    pgNameList = list(
                        filter(
                            lambda x: x["Id"] == page["PageGroupId"],
                            workspace["PageGroups"],
                        )
                    )
                    pgName = "-NoPageGroup-"
                    if len(pgNameList) == 1:
                        pgName = pgNameList[0]["Name"]
                    for view in page["Views"]:
                        for viewWidget in view["ViewWidgetDefinitions"]:
                            if (
                                "ConfigJson" in viewWidget
                                and "Navigations" in viewWidget["ConfigJson"]
                                and "RowNavigationList"
                                in viewWidget["ConfigJson"]["Navigations"]
                            ):
                                for navigation in viewWidget["ConfigJson"][
                                    "Navigations"
                                ]["RowNavigationList"]["Views"]:
                                    navigationList = list(
                                        filter(
                                            lambda x: x["ViewId"]
                                            == navigation["ViewId"],
                                            viewListWithId,
                                        )
                                    )
                                    if len(navigationList) > 0:
                                        self.dbConnection.execute(
                                            "INSERT INTO WidgetNavigationViews (TenantName, Workspace, PageGroup, Page, "
                                            "View, WidgetName, NavTargetWorkSpaceName, NavTargetPageGroupName, "
                                            "NavTargetPageName, NavTargetViewName) "
                                            "VALUES (?,?,?,?,?,?,?,?,?,?)",
                                            (
                                                self.tenantName,
                                                workspace["Title"],
                                                pgName,
                                                page["Title"],
                                                view["Title"],
                                                self.tenantWidgetIdToName[
                                                    viewWidget["WidgetDefinitionId"]
                                                ],
                                                navigationList[0]["WorkspaceName"],
                                                navigationList[0]["PageGroupName"],
                                                navigationList[0]["PageName"],
                                                navigationList[0]["ViewName"],
                                            ),
                                        )

            for workSpace in tenantWorkspacesArray:
                if "ActionButtonBindings" in workSpace:
                    for actionButtonInWS in workSpace["ActionButtonBindings"]:
                        self.dbConnection.execute(
                            "INSERT INTO ActionButtonBindingsForWeb (TenantName, WorkSpaceName, PageGroupName, PageName, "
                            "ViewName, WidgetName, WidgetTitle, ActionButtonName) VALUES (?,?,?,?,?,?,?,?)",
                            (
                                self.tenantName,
                                workSpace["Title"],
                                None,
                                None,
                                None,
                                None,
                                None,
                                actionButtonInWS["ActionButtonName"],
                            ),
                        )
                for page in workSpace["Pages"]:
                    pgNameList = list(
                        filter(
                            lambda x: x["Id"] == page["PageGroupId"],
                            workSpace["PageGroups"],
                        )
                    )
                    pgName = "-NoPageGroup-"
                    if len(pgNameList) > 0:
                        pgName = pgNameList[0]["Name"]
                    if "ActionButtonBindings" in page:
                        for actionButtonInPage in page["ActionButtonBindings"]:
                            self.dbConnection.execute(
                                "INSERT INTO ActionButtonBindingsForWeb (TenantName, WorkSpaceName, PageGroupName, "
                                "PageName, ViewName, WidgetName, WidgetTitle, ActionButtonName) VALUES (?,?,?,?,?,?,?,?)",
                                (
                                    self.tenantName,
                                    workSpace["Title"],
                                    pgName,
                                    page["Title"],
                                    None,
                                    None,
                                    None,
                                    actionButtonInPage["ActionButtonName"],
                                ),
                            )
                    for view in page["Views"]:
                        if "ActionButtonBindings" in view:
                            for actionButtonInView in view["ActionButtonBindings"]:
                                self.dbConnection.execute(
                                    "INSERT INTO ActionButtonBindingsForWeb (TenantName, WorkSpaceName, PageGroupName, "
                                    "PageName, ViewName, WidgetName, WidgetTitle, ActionButtonName) "
                                    "VALUES (?,?,?,?,?,?,?,?)",
                                    (
                                        self.tenantName,
                                        workSpace["Title"],
                                        pgName,
                                        page["Title"],
                                        view["Title"],
                                        None,
                                        None,
                                        actionButtonInView["ActionButtonName"],
                                    ),
                                )
                        for widget in view["ViewWidgetDefinitions"]:
                            widgetNameList = list(
                                filter(
                                    lambda x: x["Id"] == widget["WidgetDefinitionId"],
                                    self.data["Layout"]["WidgetDefinitions"],
                                )
                            )
                            widgetName = None
                            if len(widgetNameList) > 0:
                                widgetName = widgetNameList[0]["Name"]
                            if (
                                "ConfigJson" in widget
                                and "ActionButtonBindings" in widget["ConfigJson"]
                            ):
                                for actionButtonInWidget in widget["ConfigJson"][
                                    "ActionButtonBindings"
                                ]:
                                    self.dbConnection.execute(
                                        "INSERT INTO ActionButtonBindingsForWeb (TenantName, WorkSpaceName, PageGroupName, "
                                        "PageName, ViewName, WidgetName, WidgetTitle, ActionButtonName) "
                                        "VALUES (?,?,?,?,?,?,?,?)",
                                        (
                                            self.tenantName,
                                            workSpace["Title"],
                                            pgName,
                                            page["Title"],
                                            view["Title"],
                                            widgetName,
                                            widget["Name"],
                                            actionButtonInWidget["ActionButtonName"],
                                        ),
                                    )

    def createExcelLayoutTablesInDB(self):
        """
        Insert data in the excel layout tables in the database.
        """
        tenantXLWorkbookIdToName = {}

        if "XLWorkbooks" in self.data:
            for workbook in self.data["XLWorkbooks"]:
                tenantXLWorkbookIdToNameTemp = {
                    workbook["Id"]: workbook["WorkbookName"]
                }
                tenantXLWorkbookIdToName.update(tenantXLWorkbookIdToNameTemp)

        if "XLFolders" in self.data:
            for folder in self.data["XLFolders"]:
                rolesString = ", ".join(sorted(folder["Roles"]))
                try:
                    self.dbConnection.execute(
                        "INSERT INTO ExcelFolders (TenantName, FolderName, IsPublished, DisplayOrder, IsPrivate, Roles, "
                        "CreatedUserEmail, ModifiedUserEmail) VALUES (?,?,?,?,?,?,?,?)",
                        (
                            self.tenantName,
                            folder["FolderName"],
                            folder["IsPublished"],
                            folder["DisplayOrder"],
                            folder["IsPrivate"],
                            rolesString,
                            folder["CreatedUserEmail"],
                            folder["ModifiedUserEmail"],
                        ),
                    )
                except Exception as e:
                    self.logger.error(
                        "Unable to insert data into ExcelFolders: " + str(e)
                    )
                    print("Unable to insert data into ExcelFolders: " + str(e))

                if (
                    "ConfigJson" in folder
                    and "ActionButtonBindings" in folder["ConfigJson"]
                ):
                    for abInFolder in folder["ConfigJson"]["ActionButtonBindings"]:
                        try:
                            self.dbConnection.execute(
                                "INSERT INTO ActionButtonBindingsForExcel (TenantName, XLFolder, XLWorkbook, "
                                "ActionButtonName) VALUES (?,?,?,?)",
                                (
                                    self.tenantName,
                                    folder["FolderName"],
                                    None,
                                    (
                                        abInFolder["Name"]
                                        if "Name" in abInFolder
                                        else None
                                    ),
                                ),
                            )
                        except Exception as ex:
                            self.logger.error(
                                "Unable to insert data into ActionButtonBindingsForExcel: "
                                + str(ex)
                            )
                            print(
                                "Unable to insert data into ActionButtonBindingsForExcel: "
                                + str(ex)
                            )

                workbooksInFolders = list(
                    (
                        widget
                        for widget in self.data["XLWorkbookInFolders"]
                        if (widget["XLFolderId"] == folder["Id"])
                    )
                )
                for workbook in workbooksInFolders:
                    curWorkBookName = tenantXLWorkbookIdToName[workbook["XLWorkbookId"]]
                    try:
                        self.dbConnection.execute(
                            "INSERT INTO ExcelWorkbooksInFolders (TenantName, XLFolder, XLWorkbook, DisplayOrder, "
                            "IsPublished) VALUES (?,?,?,?,?)",
                            (
                                self.tenantName,
                                folder["FolderName"],
                                curWorkBookName,
                                workbook["DisplayOrder"],
                                workbook["IsPublished"],
                            ),
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into ExcelWorkbooksInFolders: "
                            + str(e)
                        )
                        print(
                            "Unable to insert data into ExcelWorkbooksInFolders: "
                            + str(e)
                        )
                    workbookDef = list(
                        filter(
                            lambda x: x["Id"] == workbook["XLWorkbookId"],
                            self.data["XLWorkbooks"],
                        )
                    )[0]
                    if (
                        "ConfigJson" in workbookDef
                        and "ActionButtonBindings" in workbookDef["ConfigJson"]
                    ):
                        for abInWorkbook in workbookDef["ConfigJson"][
                            "ActionButtonBindings"
                        ]:
                            try:
                                self.dbConnection.execute(
                                    "INSERT INTO ActionButtonBindingsForExcel (TenantName, XLFolder, XLWorkbook, "
                                    "ActionButtonName) VALUES (?,?,?,?)",
                                    (
                                        self.tenantName,
                                        folder["FolderName"],
                                        curWorkBookName,
                                        (
                                            abInWorkbook["Name"]
                                            if "Name" in abInWorkbook
                                            else None
                                        ),
                                    ),
                                )
                            except Exception as ex:
                                self.logger.error(
                                    "Unable to insert data into ActionButtonBindingsForExcel: "
                                    + str(ex)
                                )
                                print(
                                    "Unable to insert data into ActionButtonBindingsForExcel: "
                                    + str(ex)
                                )

                    xlWidgetList = list(
                        (
                            widget
                            for widget in self.data["XLWidgetInWorkbooks"]
                            if (widget["XLWorkbookId"] == workbook["XLWorkbookId"])
                        )
                    )
                    for widget in xlWidgetList:
                        matchingXLWidget = list(
                            filter(
                                lambda y: y["WidgetID"] == widget["WidgetDefinitionId"],
                                self.finalTenantWidgetsArray,
                            )
                        )[0]
                        matchingXLWidget["ExcelUsageCount"] = (
                            matchingXLWidget["ExcelUsageCount"] + 1
                        )

                        tenantExcelLayoutArray = {
                            "TenantName": self.tenantName,
                            "XLFolder": folder["FolderName"],
                            "XLWorkbook": curWorkBookName,
                            "Widget": self.tenantWidgetIdToName[
                                widget["WidgetDefinitionId"]
                            ],
                        }
                        try:
                            self.dbConnection.execute(
                                "INSERT INTO ExcelLayoutWidgets (TenantName, XLFolder, XLWorkbook, Widget) "
                                " VALUES (?,?,?,?)",
                                (
                                    tenantExcelLayoutArray["TenantName"],
                                    tenantExcelLayoutArray["XLFolder"],
                                    tenantExcelLayoutArray["XLWorkbook"],
                                    tenantExcelLayoutArray["Widget"],
                                ),
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to insert data into ExcelLayoutWidgets: "
                                + str(e)
                            )
                            print("Unable to insert data into ExcelLayoutWidgets: ", e)

            for x in self.finalTenantWidgetsArray:
                x["TotalUsageCount"] = (
                    x["TileUsageCount"] + x["ViewUsageCount"] + x["ExcelUsageCount"]
                )

            tenantWidgetsArrayDataToDB = []
            userData = self.data["Users"]
            for i in self.finalTenantWidgetsArray:
                createdUserId = None
                if i["IsPrivate"]:
                    createdUserId = userData[str(i["CreatedUserId"])]

                tenantWidgetsArrayDataToDB = tenantWidgetsArrayDataToDB + [
                    (
                        i["TenantName"],
                        i["WidgetID"],
                        i["WidgetName"],
                        i["WidgetType"],
                        i["IsPrivate"],
                        createdUserId,
                        i["TileUsageCount"],
                        i["ViewUsageCount"],
                        i["ExcelUsageCount"],
                        i["TotalUsageCount"],
                    )
                ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO Widgets (TenantName, WidgetID, WidgetName, WidgetType, IsPrivate, CreatedUserId, "
                    "TileUsageCount, ViewUsageCount, ExcelUsageCount, TotalUsageCount) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    tenantWidgetsArrayDataToDB,
                )
            except Exception as e:
                self.logger.error("Unable to insert data into Widgets: " + str(e))
                print(e)

    def createTranslationTablesInDB(self):
        """
        Insert data in the translation tables in the database.
        :return:
        """
        entityTypes = []
        WorkspaceGIdToName = {}
        PageGroupGIdToName = {}
        PageGIdToName = {}
        PageWidgetDefinitionsGIdToName = {}
        ViewGIdToName = {}
        ViewWidgetDefinitionGIdToName = {}

        if "Translations" in self.data and "Layout" in self.data:
            for obj in self.data["Translations"]:
                entityTypes.append(obj["EntityType"])

            uniEntityTypes = list(set(entityTypes))
            self.logger.info(
                "\nentity types for which translations are given in the tenant are:"
            )
            self.logger.info(uniEntityTypes)

            for workspace in self.data["Layout"]["Workspaces"]:
                WorkspaceGIdToNameTemp = {workspace["WorkspaceId"]: workspace["Title"]}
                WorkspaceGIdToName.update(WorkspaceGIdToNameTemp)

                for pagegroup in workspace["PageGroups"]:
                    PageGroupGIdToNameTemp = {
                        pagegroup["PageGroupId"]: pagegroup["Title"]
                    }
                    PageGroupGIdToName.update(PageGroupGIdToNameTemp)

                for page in workspace["Pages"]:
                    PageGIdToNameTemp = {page["PageId"]: page["Title"]}
                    PageGIdToName.update(PageGIdToNameTemp)

                    for PageWidgetDefinition in page["PageWidgetDefinitions"]:
                        PageWidgetDefinitionsGIdtToNameTemp = {
                            PageWidgetDefinition[
                                "PageWidgetDefinitionId"
                            ]: PageWidgetDefinition["Name"]
                        }
                        PageWidgetDefinitionsGIdToName.update(
                            PageWidgetDefinitionsGIdtToNameTemp
                        )

                    for view in page["Views"]:
                        ViewGIdToNameTemp = {view["ViewId"]: view["Title"]}
                        ViewGIdToName.update(ViewGIdToNameTemp)

                        for ViewWidgetDefinition in view["ViewWidgetDefinitions"]:
                            ViewWidgetDefinitionGIdToNameTemp = {
                                ViewWidgetDefinition[
                                    "ViewWidgetDefinitionId"
                                ]: ViewWidgetDefinition["Name"]
                            }
                            ViewWidgetDefinitionGIdToName.update(
                                ViewWidgetDefinitionGIdToNameTemp
                            )

            WorkspaceEntities = list(
                filter(
                    lambda y: (
                        y["EntityType"] == "Workspace"
                        and y["EntityId"] in WorkspaceGIdToName.keys()
                    ),
                    self.data["Translations"],
                )
            )

            PageGroupEntities = list(
                filter(
                    lambda y: (
                        y["EntityType"] == "PageGroup"
                        and y["EntityId"] in PageGroupGIdToName.keys()
                    ),
                    self.data["Translations"],
                )
            )

            PageEntities = list(
                filter(
                    lambda y: (
                        y["EntityType"] == "Page"
                        and y["EntityId"] in PageGIdToName.keys()
                    ),
                    self.data["Translations"],
                )
            )

            ViewEntities = list(
                filter(
                    lambda y: (
                        y["EntityType"] == "View"
                        and y["EntityId"] in ViewGIdToName.keys()
                    ),
                    self.data["Translations"],
                )
            )

            ViewWidgetDefinitionEntities = list(
                filter(
                    lambda y: (
                        y["EntityType"] == "ViewWidgetDefinition"
                        and y["EntityId"] in ViewWidgetDefinitionGIdToName.keys()
                    ),
                    self.data["Translations"],
                )
            )

            PageWidgetDefinitionEntities = list(
                filter(
                    lambda y: (
                        y["EntityType"] == "PageWidgetDefinitions"
                        and y["EntityId"] in PageWidgetDefinitionsGIdToName.keys()
                    ),
                    self.data["Translations"],
                )
            )

            WorkspaceEntityArray = list(
                [
                    {
                        "TenantName": self.tenantName,
                        "Name": WorkspaceGIdToName[x["EntityId"]],
                        "GId": x["EntityId"],
                        "LCId": x["LCID"],
                        "TranslatedName": (
                            list(x["Config"].values())[0]
                            if x["Config"].values()
                            else "--"
                        ),
                    }
                    for x in WorkspaceEntities
                ]
            )

            PageGroupEntityArray = list(
                [
                    {
                        "TenantName": self.tenantName,
                        "Name": PageGroupGIdToName[x["EntityId"]],
                        "GId": x["EntityId"],
                        "LCId": x["LCID"],
                        "TranslatedName": (
                            list(x["Config"].values())[0]
                            if x["Config"].values()
                            else "--"
                        ),
                    }
                    for x in PageGroupEntities
                ]
            )

            PageEntityArray = list(
                [
                    {
                        "TenantName": self.tenantName,
                        "Name": PageGIdToName[x["EntityId"]],
                        "GId": x["EntityId"],
                        "LCId": x["LCID"],
                        "TranslatedName": (
                            list(x["Config"].values())[0]
                            if x["Config"].values()
                            else "--"
                        ),
                    }
                    for x in PageEntities
                ]
            )

            ViewEntityArray = list(
                [
                    {
                        "TenantName": self.tenantName,
                        "Name": ViewGIdToName[x["EntityId"]],
                        "GId": x["EntityId"],
                        "LCId": x["LCID"],
                        "TranslatedName": (
                            list(x["Config"].values())[0]
                            if x["Config"].values()
                            else "--"
                        ),
                    }
                    for x in ViewEntities
                ]
            )

            ViewWidgetEntityArray = list(
                [
                    {
                        "TenantName": self.tenantName,
                        "Name": ViewWidgetDefinitionGIdToName[x["EntityId"]],
                        "GId": x["EntityId"],
                        "LCId": x["LCID"],
                        "TranslatedName": (
                            list(x["Config"].values())[0]
                            if x["Config"].values()
                            else "--"
                        ),
                    }
                    for x in ViewWidgetDefinitionEntities
                ]
            )

            PageWidgetEntityArray = list(
                [
                    {
                        "TenantName": self.tenantName,
                        "Name": PageWidgetDefinitionsGIdToName[x["EntityId"]],
                        "GId": x["EntityId"],
                        "LCId": x["LCID"],
                        "TranslatedName": (
                            list(x["Config"].values())[0]
                            if x["Config"].values()
                            else "--"
                        ),
                    }
                    for x in PageWidgetDefinitionEntities
                ]
            )
            workspaceEntityArrayDataToDB = [
                (i["TenantName"], i["Name"], i["GId"], i["LCId"], i["TranslatedName"])
                for i in WorkspaceEntityArray
            ]
            pageGroupEntityArrayDataToDB = [
                (i["TenantName"], i["Name"], i["GId"], i["LCId"], i["TranslatedName"])
                for i in PageGroupEntityArray
            ]
            pageEntityArrayDataToDB = [
                (i["TenantName"], i["Name"], i["GId"], i["LCId"], i["TranslatedName"])
                for i in PageEntityArray
            ]
            viewEntityArrayDataToDB = [
                (i["TenantName"], i["Name"], i["GId"], i["LCId"], i["TranslatedName"])
                for i in ViewEntityArray
            ]
            viewWidgetEntityArrayDataToDB = [
                (i["TenantName"], i["Name"], i["GId"], i["LCId"], i["TranslatedName"])
                for i in ViewWidgetEntityArray
            ]
            pageWidgetEntityArrayDataToDB = [
                (i["TenantName"], i["Name"], i["GId"], i["LCId"], i["TranslatedName"])
                for i in PageWidgetEntityArray
            ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO WorkspaceTranslations (TenantName, Name, GId, LCId, TranslatedName) "
                    " VALUES (?,?,?,?,?)",
                    workspaceEntityArrayDataToDB,
                )
                self.dbConnection.executemany(
                    "INSERT INTO PageGroupTranslations (TenantName, Name, GId, LCId, TranslatedName) "
                    " VALUES (?,?,?,?,?)",
                    pageGroupEntityArrayDataToDB,
                )
                self.dbConnection.executemany(
                    "INSERT INTO PageTranslations (TenantName, Name, GId, LCId, TranslatedName) "
                    " VALUES (?,?,?,?,?)",
                    pageEntityArrayDataToDB,
                )
                self.dbConnection.executemany(
                    "INSERT INTO ViewTranslations (TenantName, Name, GId, LCId, TranslatedName) "
                    " VALUES (?,?,?,?,?)",
                    viewEntityArrayDataToDB,
                )
                self.dbConnection.executemany(
                    "INSERT INTO ViewWidgetTranslations (TenantName, Name, GId, LCId, TranslatedName) "
                    " VALUES (?,?,?,?,?)",
                    viewWidgetEntityArrayDataToDB,
                )
                self.dbConnection.executemany(
                    "INSERT INTO PageWidgetTranslations (TenantName, Name, GId, LCId, TranslatedName) "
                    " VALUES (?,?,?,?,?)",
                    pageWidgetEntityArrayDataToDB,
                )
            except Exception as e:
                self.logger.error("Error in Translation Tables: " + str(e))
                print(e)
        else:
            print("\nNo Translations are available")

    def createActionButtonTableInDB(self):
        """
        Insert data in the action button tables in the database.
        """
        if "Layout" in self.data:
            allActionButtonList = self.data["Layout"]["ActionButtons"]
            actionButtonList = [
                x
                for x in allActionButtonList
                if (
                    x["Name"] != "UPost"
                    and x["Name"] != "UTask"
                    and x["Name"] != "UProfile"
                )
            ]
            for actionButton in actionButtonList:
                try:
                    actionButtonDetails = {
                        "TenantName": self.tenantName,
                        "ActionButtonName": (
                            actionButton["Name"] if "Name" in actionButton else None
                        ),
                        "Tooltip": (
                            actionButton["Tooltip"]
                            if "Tooltip" in actionButton
                            else None
                        ),
                        "ActionButtonType": actionButton["ActionButtonType"],
                        "Alignment": actionButton["Alignment"],
                        "IsPopOver": actionButton["IsPopOver"],
                        "IsGlobal": actionButton["IsGlobal"],
                        "ConfigJson": json.dumps(actionButton["ConfigJson"], indent=4),
                    }
                    self.dbConnection.execute(
                        "INSERT INTO ActionButtonDetails (TenantName, ActionButtonName, Tooltip, "
                        "ActionButtonType, Alignment, IsPopOver, IsGlobal, ConfigJson) VALUES (?,?,?,?,?,?,?,?)",
                        (
                            actionButtonDetails["TenantName"],
                            actionButtonDetails["ActionButtonName"],
                            actionButtonDetails["Tooltip"],
                            actionButtonDetails["ActionButtonType"],
                            actionButtonDetails["Alignment"],
                            actionButtonDetails["IsPopOver"],
                            actionButtonDetails["IsGlobal"],
                            actionButtonDetails["ConfigJson"],
                        ),
                    )
                except Exception as e:
                    self.logger.error("Error inserting actionButtonDetails: " + str(e))
                    print("Error inserting actionButtonDetails: " + str(e))

                if len(actionButton["ConfigJson"]) <= 0:
                    continue

                if (
                    "IBPLRules" in actionButton["ConfigJson"]
                    and len(actionButton["ConfigJson"]["IBPLRules"]) > 0
                ):
                    rulePosition = 0
                    for rule in actionButton["ConfigJson"]["IBPLRules"]:
                        rulePosition = rulePosition + 1
                        finalTenantActionButtonRule = [
                            {
                                "TenantName": self.tenantName,
                                "ActionButtonName": (
                                    actionButton["Name"]
                                    if "Name" in actionButton
                                    else None
                                ),
                                "IBPLRulePosition": rulePosition,
                                "IBPLRule": (
                                    rule["template"] if "template" in rule else ""
                                ),
                            }
                        ]
                        tenantActionButtonRuleDataToDB = [
                            (
                                i["TenantName"],
                                i["ActionButtonName"],
                                i["IBPLRulePosition"],
                                i["IBPLRule"],
                            )
                            for i in finalTenantActionButtonRule
                        ]
                        try:
                            self.dbConnection.executemany(
                                "INSERT INTO ActionButtonRules (TenantName, ActionButtonName, IBPLRulePosition, "
                                "IBPLRule) VALUES (?, ?, ?, ?)",
                                tenantActionButtonRuleDataToDB,
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to insert data into ActionButtonRules: "
                                + str(e)
                            )
                            print(e)

                if (
                    "FieldBindings" in actionButton["ConfigJson"]
                    and len(actionButton["ConfigJson"]["FieldBindings"]) > 0
                ):
                    for fieldBinding in actionButton["ConfigJson"]["FieldBindings"]:
                        finalTenantFieldBinding = [
                            {
                                "TenantName": self.tenantName,
                                "ActionButtonName": (
                                    actionButton["Name"]
                                    if "Name" in actionButton
                                    else None
                                ),
                                "FieldName": fieldBinding["fieldName"],
                                "PropertyName": key,
                                "PropertyValue": value,
                            }
                            for key, value in fieldBinding.items()
                            if (key != "validation")
                        ]
                        tenantFieldBindingDataToDB = [
                            (
                                i["TenantName"],
                                i["ActionButtonName"],
                                i["FieldName"],
                                i["PropertyName"],
                                str(i["PropertyValue"]),
                            )
                            for i in finalTenantFieldBinding
                        ]
                        try:
                            self.dbConnection.executemany(
                                "INSERT INTO ActionButtonFieldBindings (TenantName, ActionButtonName, FieldName, "
                                "PropertyName, PropertyValue) VALUES (?, ?, ?, ?, ?)",
                                tenantFieldBindingDataToDB,
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to insert data into ActionButtonFieldBindings: "
                                + str(e)
                            )
                            print(e)

                if (
                    "DataSources" in actionButton["ConfigJson"]
                    and len(actionButton["ConfigJson"]["DataSources"]) > 0
                ):
                    for dataSources in actionButton["ConfigJson"]["DataSources"]:
                        for ibplRules in dataSources["IBPLRules"]:
                            finalTenantDataSource = [
                                {
                                    "TenantName": self.tenantName,
                                    "ActionButtonName": (
                                        actionButton["Name"]
                                        if "Name" in actionButton
                                        else None
                                    ),
                                    "DataSourceName": (
                                        dataSources["Name"]
                                        if "Name" in dataSources
                                        else None
                                    ),
                                    "IBPLRule": (
                                        ibplRules["template"]
                                        if "template" in ibplRules
                                        else ""
                                    ),
                                }
                            ]
                            tenantDataSourceDataToDB = [
                                (
                                    i["TenantName"],
                                    i["ActionButtonName"],
                                    i["DataSourceName"],
                                    i["IBPLRule"],
                                )
                                for i in finalTenantDataSource
                            ]
                            try:
                                self.dbConnection.executemany(
                                    "INSERT INTO ActionButtonDataSources (TenantName, ActionButtonName, DataSourceName,"
                                    " IBPLRule) VALUES (?, ?, ?, ?)",
                                    tenantDataSourceDataToDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into ActionButtonDataSource: "
                                    + str(e)
                                )
                                print(e)

                if (
                    "JavascriptRules" in actionButton["ConfigJson"]
                    and len(actionButton["ConfigJson"]["JavascriptRules"]) > 0
                ):
                    jsRuleData = [
                        {
                            "TenantName": self.tenantName,
                            "ActionButtonName": (
                                actionButton["Name"] if "Name" in actionButton else None
                            ),
                            "ModuleName": (
                                jsRule["modulename"] if "modulename" in jsRule else None
                            ),
                            "FunctionName": (
                                jsRule["functionname"]
                                if "functionname" in jsRule
                                else None
                            ),
                        }
                        for jsRule in actionButton["ConfigJson"]["JavascriptRules"]
                    ]
                    jsRuleDataToDB = [
                        (
                            i["TenantName"],
                            i["ActionButtonName"],
                            i["ModuleName"],
                            i["FunctionName"],
                        )
                        for i in jsRuleData
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO ActionButtonJSRules (TenantName, ActionButtonName, ModuleName, FunctionName)"
                            " VALUES (?,?,?,?)",
                            jsRuleDataToDB,
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into ActionButtonJSRules: " + str(e)
                        )
                        print(
                            "Unable to insert data into ActionButtonJSRules: " + str(e)
                        )
