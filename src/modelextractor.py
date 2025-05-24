import logging
import re
import sys
import json
from collections import Counter


class ModelExtractor:
    def __init__(self, dbConnection, data, measureUsage):
        """
        ModelExtractor Constructor.
        :param dbConnection: database connection
        :param data: json data
        :param measureUsage: boolean to check if measure usage is to be extracted.
        """
        try:
            self.logger = logging.getLogger("extractor-logger")
        except Exception as e:
            print(e)
            print("Not able to set application logging. Exiting")
            sys.exit()
        self.dbConnection = dbConnection
        self.data = data
        self.measureUsage = measureUsage
        # Get the tenantName
        self.tenantName = data["Tenant"]["Name"]
        self.tenantAttributeIdToDimName = {}
        self.tenantAttributeIdToAttrName = {}
        self.measureAsIBPLCount = {}
        self.measureAsNotIBPLCount = {}
        if measureUsage:
            measureAsIBPL = re.findall(
                r"Measure\.\[.*?]", json.dumps(self.data), re.IGNORECASE
            )
            self.measureAsIBPLCount = dict(Counter(measureAsIBPL))
            measureAsNotIBPL = re.findall(
                r': ".*?"', json.dumps(self.data), re.IGNORECASE
            )
            self.measureAsNotIBPLCount = dict(Counter(measureAsNotIBPL))

    def createDimTablesInDB(self):
        """
        Insert corresponding data for all dimension related tables.
        """
        # Extract Picklist data and insert into Picklist table.
        allTenantPickList = self.data["PickLists"]
        tenantPickLists = [
            x
            for x in allTenantPickList
            if (
                x["PickListName"] != "Version Category List"
                and x["PickListName"] != "FulfilmentPolicy"
            )
        ]
        finalTenantPickLists = [
            {
                "TenantName": self.tenantName,
                "PickListName": x["PickListName"],
                "PickListDescription": x["PickListDescription"],
                "DataType": x["DataType"],
                "IsMultiSelectAllowed": x["IsMultiSelectAllowed"],
            }
            for x in tenantPickLists
        ]
        tenantPickListDataToDB = [
            (
                i["TenantName"],
                i["PickListName"],
                i["PickListDescription"],
                i["DataType"],
                i["IsMultiSelectAllowed"],
            )
            for i in finalTenantPickLists
        ]
        try:
            self.dbConnection.executemany(
                "INSERT INTO Picklists (TenantName, PickListName, PickListDescription, DataType, IsMultiSelectAllowed) "
                "VALUES (?, ?, ?, ?, ?)",
                tenantPickListDataToDB,
            )
        except Exception as e:
            self.logger.error("Unable to insert into Picklists: " + str(e))
            print(e)

        for pickList in tenantPickLists:
            pickListName = pickList["PickListName"]
            finalTenantPickListValues = [
                {
                    "TenantName": self.tenantName,
                    "PickListName": pickListName,
                    "Value": x["Value"],
                    "DisplayName": x["DisplayName"],
                    "DisplayPosition": x["DisplayPosition"],
                }
                for x in pickList["PickListValues"]
            ]
            tenantPickListValuesDataToDB = [
                (
                    i["TenantName"],
                    i["PickListName"],
                    i["Value"],
                    i["DisplayName"],
                    i["DisplayPosition"],
                )
                for i in finalTenantPickListValues
            ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO PickListValues (TenantName, PickListName, Value, DisplayName, DisplayPosition) "
                    "VALUES (?, ?, ?, ?, ?)",
                    tenantPickListValuesDataToDB,
                )
            except Exception as e:
                self.logger.error(
                    "Unable to insert data into PickListValues: " + str(e)
                )
                print(e)

        # Code for Dimension data
        allTenantDimensions = self.data["Dimensions"]
        tenantDimensions = [
            x
            for x in allTenantDimensions
            if (
                x["DimensionName"] != "Algorithm"
                and x["DimensionName"] != "Personnel"
                and x["DimensionName"] != "DimPlugin"
                and x["DimensionName"] != "Version"
                and x["DimensionName"] != "RootCause"
                and x["DimensionName"] != "_SchemaPlan"
                and x["DimensionName"] != "_SchemaDimension"
                and x["DimensionName"] != "_SchemaRelationship"
            )
        ]
        skippedTenantDimensions = [
            x
            for x in allTenantDimensions
            if (
                x["DimensionName"] != "_SchemaPlan"
                and x["DimensionName"] != "_SchemaDimension"
                and x["DimensionName"] != "_SchemaRelationship"
            )
        ]

        # Extract dimensions data and insert into Dimensions table.
        finalTenantDimensions = [
            {
                "TenantName": self.tenantName,
                "DimensionName": x["DimensionName"],
                "DimensionDescription": x["DimensionDescription"],
                "DimensionType": x["DimensionType"],
            }
            for x in tenantDimensions
        ]
        tenantDimensionsDataToDB = [
            (
                i["TenantName"],
                i["DimensionName"],
                i["DimensionDescription"],
                i["DimensionType"],
            )
            for i in finalTenantDimensions
        ]

        try:
            self.dbConnection.executemany(
                "INSERT INTO Dimensions (TenantName, DimensionName, DimensionDescription, DimensionType) "
                " VALUES (?, ?, ?, ?)",
                tenantDimensionsDataToDB,
            )
        except Exception as e:
            self.logger.error("Unable to insert data into Dimensions: " + str(e))
            print(e)

        for dimensionObj in tenantDimensions:
            if "DimensionAliases" in dimensionObj:
                for dimAlias in dimensionObj["DimensionAliases"]:
                    try:
                        self.dbConnection.execute(
                            "INSERT INTO DimAliases (TenantName, DimensionName, AliasName, AliasDescription) "
                            "VALUES (?,?,?,?)",
                            (
                                self.tenantName,
                                dimensionObj["DimensionName"],
                                dimAlias["AliasName"],
                                dimAlias["AliasDescription"],
                            ),
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into DimAliases: " + str(e)
                        )
                        print("Unable to insert data into DimAliases: " + str(e))

        # Code for Dimension attribute
        for dimension in skippedTenantDimensions:
            dimensionName = dimension["DimensionName"]
            if (
                dimensionName != "Algorithm"
                and dimensionName != "Personnel"
                and dimensionName != "DimPlugin"
                and dimensionName != "Version"
                and dimensionName != "RootCause"
            ):
                finalTenantDimAttributes = [
                    {
                        "TenantName": self.tenantName,
                        "DimensionName": dimensionName,
                        "AttributeName": x["AttributeName"],
                        "Description": x["Description"],
                        "KeyColumnDataType": x["KeyColumnDataType"],
                        "IsKey": x["IsKey"],
                        "SeedTags": x["SeedTags"] if "SeedTags" in x else None,
                    }
                    for x in dimension["DimensionAttributes"]
                ]
                tenantDimAttributesDataToDB = [
                    (
                        i["TenantName"],
                        i["DimensionName"],
                        i["AttributeName"],
                        i["Description"],
                        i["KeyColumnDataType"],
                        i["IsKey"],
                        i["SeedTags"],
                    )
                    for i in finalTenantDimAttributes
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO DimAttributes (TenantName, DimensionName, AttributeName, Description, "
                        "KeyColumnDataType, IsKey, SeedTags) VALUES (?,?,?,?,?,?,?)",
                        tenantDimAttributesDataToDB,
                    )
                except Exception as e:
                    self.logger.error(
                        "Unable to insert data into DimAttributes: " + str(e)
                    )
                    print(e)

            # code for translation
            tenantAttributes = dimension["DimensionAttributes"]
            for attributeTranslation in tenantAttributes:
                curAttributeName = attributeTranslation["AttributeName"]

                tenantAttributeIdToDimNameTemp = {
                    attributeTranslation["Id"]: dimensionName
                }
                self.tenantAttributeIdToDimName.update(tenantAttributeIdToDimNameTemp)
                tenantAttributeIdToAttrNameTemp = {
                    attributeTranslation["Id"]: curAttributeName
                }
                self.tenantAttributeIdToAttrName.update(tenantAttributeIdToAttrNameTemp)

                if (
                    dimensionName != "Algorithm"
                    and dimensionName != "Personnel"
                    and dimensionName != "DimPlugin"
                    and dimensionName != "Version"
                    and dimensionName != "RootCause"
                ):
                    # Extract dimension translation data and insert into DimAttrTranslations table.
                    finalTenantDimTranslation = [
                        {
                            "TenantName": self.tenantName,
                            "DimensionName": dimensionName,
                            "AttributeName": curAttributeName,
                            "TranslationName": x["AttributeName"],
                            "Description": x["Description"],
                            "LCID": x["LCID"],
                            "Language": x["Language"],
                        }
                        for x in attributeTranslation["DimensionAttributeTranslations"]
                    ]
                    tenantDimTranslationDataToDB = [
                        (
                            i["TenantName"],
                            i["DimensionName"],
                            i["AttributeName"],
                            i["TranslationName"],
                            i["Description"],
                            i["LCID"],
                            i["Language"],
                        )
                        for i in finalTenantDimTranslation
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO DimAttrTranslations (TenantName, DimensionName, AttributeName, "
                            "TranslationName, Description, LCID, Language) VALUES (?,?,?,?,?,?,?)",
                            tenantDimTranslationDataToDB,
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into DimAttrTranslations: " + str(e)
                        )
                        print(e)

                    # Code for Alias
                    if (
                        "DimensionAttributeAliases" in attributeTranslation
                        and len(attributeTranslation["DimensionAttributeAliases"]) > 0
                    ):
                        tenantDimAttrAlias = [
                            {
                                "TenantName": self.tenantName,
                                "DimensionName": dimensionName,
                                "AttributeName": curAttributeName,
                                "AliasName": i["AliasName"],
                                "AliasDescription": i["AliasDescription"],
                            }
                            for i in attributeTranslation["DimensionAttributeAliases"]
                        ]
                        tenantDimAttrAliasToDB = [
                            (
                                i["TenantName"],
                                i["DimensionName"],
                                i["AttributeName"],
                                i["AliasName"],
                                i["AliasDescription"],
                            )
                            for i in tenantDimAttrAlias
                        ]
                        try:
                            self.dbConnection.executemany(
                                "INSERT INTO DimAttrAliases (TenantName, DimensionName, AttributeName, AliasName, "
                                "AliasDescription) VALUES (?,?,?,?,?)",
                                tenantDimAttrAliasToDB,
                            )
                        except Exception as e:
                            self.logger.error(
                                "Unable to insert data into DimAttrAliases: " + str(e)
                            )
                            print(e)

                    self.addToDimAttrPropertiesTable(
                        attributeTranslation["Properties"],
                        dimensionName,
                        curAttributeName,
                    )
                else:
                    self.addToDimAttrPropertiesTable(
                        attributeTranslation["Properties"],
                        dimensionName,
                        curAttributeName,
                    )

                tenantAttrProperty = attributeTranslation["Properties"]
                for attrProperty in tenantAttrProperty:
                    attrPropertyName = attrProperty["AttributeName"]

                    # Extract attribute property translation data and insert into DimAttrPropTranslations table.
                    finalTenantAttrPropertyTranslation = [
                        {
                            "TenantName": self.tenantName,
                            "DimensionName": dimensionName,
                            "AttributeName": curAttributeName,
                            "PropertyName": attrPropertyName,
                            "TranslationName": x["AttributeName"],
                            "Description": x["Description"],
                            "LCID": x["LCID"],
                            "Language": x["Language"],
                        }
                        for x in attrProperty["DimensionAttributeTranslations"]
                    ]
                    tenantAttrPropertyTranslationDataToDB = [
                        (
                            i["TenantName"],
                            i["DimensionName"],
                            i["AttributeName"],
                            i["PropertyName"],
                            i["TranslationName"],
                            i["Description"],
                            i["LCID"],
                            i["Language"],
                        )
                        for i in finalTenantAttrPropertyTranslation
                    ]

                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO DimAttrPropTranslations (TenantName, DimensionName, AttributeName, "
                            "PropertyName, TranslationName, Description, LCID, Language) VALUES (?,?,?,?,?,?,?,?)",
                            tenantAttrPropertyTranslationDataToDB,
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into DimAttrPropTranslations: "
                            + str(e)
                        )
                        print(e)

            if (
                dimensionName != "Algorithm"
                and dimensionName != "Personnel"
                and dimensionName != "DimPlugin"
                and dimensionName != "Version"
                and dimensionName != "RootCause"
            ):
                tenantHierarchies = dimension["Hierarchies"]

                # Extract hierarchy data and insert into DimHierarchies
                finalTenantHierarchy = [
                    {
                        "TenantName": self.tenantName,
                        "DimensionName": dimensionName,
                        "HierarchyName": x["HierarchyName"],
                        "HierarchyDescription": x["HierarchyDescription"],
                    }
                    for x in tenantHierarchies
                ]
                tenantHierarchyDataToDB = [
                    (
                        i["TenantName"],
                        i["DimensionName"],
                        i["HierarchyName"],
                        i["HierarchyDescription"],
                    )
                    for i in finalTenantHierarchy
                ]

                try:

                    self.dbConnection.executemany(
                        "INSERT INTO DimHierarchies (TenantName, DimensionName, HierarchyName, HierarchyDescription) "
                        " VALUES (?,?,?,?)",
                        tenantHierarchyDataToDB,
                    )
                except Exception as e:
                    self.logger.error(
                        "Unable to insert data into DimHierarchies: " + str(e)
                    )
                    print(e)

                for hierarchy in tenantHierarchies:
                    hierarchyName = hierarchy["HierarchyName"]

                    # Extract hierarchy level data and insert into DimHierLevels table.
                    finalTenantHierarchyLevel = [
                        {
                            "TenantName": self.tenantName,
                            "DimensionName": dimensionName,
                            "HierarchyName": hierarchyName,
                            "LevelPosition": x["LevelPosition"],
                            "LevelName": x["LevelName"],
                            "LevelDescription": x["LevelDescription"],
                        }
                        for x in hierarchy["Levels"]
                    ]
                    tenantHierarchyLevelDataToDB = [
                        (
                            i["TenantName"],
                            i["DimensionName"],
                            i["HierarchyName"],
                            i["LevelPosition"],
                            i["LevelName"],
                            i["LevelDescription"],
                        )
                        for i in finalTenantHierarchyLevel
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO DimHierLevels (TenantName, DimensionName, HierarchyName, LevelPosition, "
                            "LevelName, LevelDescription) VALUES (?,?,?,?,?,?)",
                            tenantHierarchyLevelDataToDB,
                        )
                    except Exception as e:
                        self.logger.error(
                            "Unable to insert data into DimHierLevels: " + str(e)
                        )
                        print(e)

    def createGraphTablesInDB(self):
        """
        Insert corresponding data for all graph related tables.
        """
        graphNodePropertyList = []
        # Get Member relationship types data(
        jMemberRelationshipTypes = self.data["MemberRelationshipTypes"]

        # Extract tenant graph data and insert into graph table.
        finalTenantGraph = [
            {
                "TenantName": self.tenantName,
                "RelationshipTypeName": x["RelationshipTypeName"],
                "RelationshipTypeDescription": x["RelationshipTypeDescription"],
            }
            for x in jMemberRelationshipTypes
        ]
        tenantGraphDataToDB = [
            (
                i["TenantName"],
                i["RelationshipTypeName"],
                i["RelationshipTypeDescription"],
            )
            for i in finalTenantGraph
        ]
        try:
            self.dbConnection.executemany(
                "INSERT INTO Graphs (TenantName, RelationshipTypeName, RelationshipTypeDescription) "
                " VALUES (?,?,?)",
                tenantGraphDataToDB,
            )
        except Exception as e:
            self.logger.error("Unable to insert data" + str(e))
            print(e)

        for curGraph in jMemberRelationshipTypes:
            curGraphName = curGraph["RelationshipTypeName"]
            graphNodes = curGraph["MemberRelationshipNodeAttributeElements"]
            graphFromNodes = [x for x in graphNodes if (not x["IsTailNode"])]
            graphToNodes = [x for x in graphNodes if (x["IsTailNode"])]
            graphEdges = curGraph["MemberRelationshipProperties"]
            graphNodePropertyObj = {
                "GraphName": curGraphName,
                "GraphFromNodeIds": sorted(
                    [x["DimensionAttributeId"] for x in graphFromNodes]
                ),
                "GraphToNodeIds": sorted(
                    [x["DimensionAttributeId"] for x in graphToNodes]
                ),
            }
            graphNodePropertyList.append(graphNodePropertyObj)

            for curEdge in graphEdges:
                # Extract tenant graph edge translations data  and insert into GraphEdgeTranslations table.
                finalTenantGraphEdgeTranslations = [
                    {
                        "TenantName": self.tenantName,
                        "RelationshipTypeName": curGraphName,
                        "EdgeName": curEdge["PropertyName"],
                        "PropertyName": x["PropertyName"],
                        "PropertyDescription": x["PropertyDescription"],
                        "LCID": x["LCID"],
                        "Language": x["Language"],
                    }
                    for x in curEdge["MemberRelationPropertyTranslations"]
                ]
                tenantGraphEdgeTranslationsToDB = [
                    (
                        i["TenantName"],
                        i["RelationshipTypeName"],
                        i["EdgeName"],
                        i["PropertyName"],
                        i["PropertyDescription"],
                        i["LCID"],
                        i["Language"],
                    )
                    for i in finalTenantGraphEdgeTranslations
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO GraphEdgeTranslations (TenantName, RelationshipTypeName, EdgeName, PropertyName, "
                        "PropertyDescription, LCID, Language) VALUES (?,?,?,?,?,?,?)",
                        tenantGraphEdgeTranslationsToDB,
                    )
                except Exception as e:
                    self.logger.error("Unable to insert data" + str(e))
                    print(e)

            combinedGraphNodes = graphFromNodes + graphToNodes
            for curNode in combinedGraphNodes:
                # Extract graph node translation data and insert into GraphNodeTranslations Table.
                finalTenantGraphNodeTranslations = [
                    {
                        "TenantName": self.tenantName,
                        "RelationshipTypeName": curGraphName,
                        "DimensionName": curNode["DimensionName"],
                        "AttributeName": curNode["AttributeName"],
                        "IsTailNode": curNode["IsTailNode"],
                        "TransAttributeName": x["AttributeName"],
                        "Description": x["Description"],
                        "LCID": x["LCID"],
                        "Language": x["Language"],
                    }
                    for x in curNode["MemberRelNodeTranslations"]
                ]
                tenantGraphNodeTranslationsToDB = [
                    (
                        i["TenantName"],
                        i["RelationshipTypeName"],
                        i["DimensionName"],
                        i["AttributeName"],
                        i["IsTailNode"],
                        i["TransAttributeName"],
                        i["Description"],
                        i["LCID"],
                        i["Language"],
                    )
                    for i in finalTenantGraphNodeTranslations
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO GraphNodeTranslations (TenantName, RelationshipTypeName, DimensionName, "
                        "AttributeName, IsTailNode, TransAttributeName, Description, LCID, Language) "
                        " VALUES (?,?,?,?,?,?,?,?,?)",
                        tenantGraphNodeTranslationsToDB,
                    )
                except Exception as e:
                    self.logger.error("Unable to insert data" + str(e))
                    print(e)

            # Extract graph from nodes data and insert into GraphFromNodes table.
            finalTenantGraphFromNodes = [
                {
                    "TenantName": self.tenantName,
                    "RelationshipTypeName": curGraphName,
                    "DimensionName": x["DimensionName"],
                    "AttributeName": x["AttributeName"],
                }
                for x in graphFromNodes
            ]

            tenantGraphFromNodesDataToDB = [
                (
                    i["TenantName"],
                    i["RelationshipTypeName"],
                    i["DimensionName"],
                    i["AttributeName"],
                )
                for i in finalTenantGraphFromNodes
            ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO GraphFromNodes (TenantName, RelationshipTypeName, DimensionName, AttributeName)"
                    " VALUES (?,?,?,?)",
                    tenantGraphFromNodesDataToDB,
                )
            except Exception as e:
                self.logger.error("Unable to insert data" + str(e))
                print(e)

            # Extract graph to nodes data and insert into GraphToNodes table.
            finalTenantGraphToNodes = [
                {
                    "TenantName": self.tenantName,
                    "RelationshipTypeName": curGraphName,
                    "DimensionName": x["DimensionName"],
                    "AttributeName": x["AttributeName"],
                }
                for x in graphToNodes
            ]

            tenantGraphToNodesDataToDB = [
                (
                    i["TenantName"],
                    i["RelationshipTypeName"],
                    i["DimensionName"],
                    i["AttributeName"],
                )
                for i in finalTenantGraphToNodes
            ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO GraphToNodes (TenantName, RelationshipTypeName, DimensionName, AttributeName)"
                    " VALUES (?,?,?,?)",
                    tenantGraphToNodesDataToDB,
                )
            except Exception as e:
                self.logger.error("Unable to insert data" + str(e))
                print(e)

            # Extract graph edges data and insert into GraphEdges table.
            finalTenantGraphEdges = [
                {
                    "TenantName": self.tenantName,
                    "RelationshipTypeName": curGraphName,
                    "PropertyName": x["PropertyName"],
                    "PropertyDescription": x["PropertyDescription"],
                    "PropertyDataType": x["PropertyDataType"],
                    "AggregateFunction": x["AggregateFunction"],
                    "IsEditable": x["IsEditable"],
                    "FormatString": x["FormatString"],
                }
                for x in graphEdges
            ]

            tenantGraphEdgesDataToDB = [
                (
                    i["TenantName"],
                    i["RelationshipTypeName"],
                    i["PropertyName"],
                    i["PropertyDescription"],
                    i["PropertyDataType"],
                    i["AggregateFunction"],
                    i["IsEditable"],
                    i["FormatString"],
                )
                for i in finalTenantGraphEdges
            ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO GraphEdges (TenantName, RelationshipTypeName, PropertyName, PropertyDescription, \
                        PropertyDataType, AggregateFunction, IsEditable, FormatString)"
                    " VALUES (?,?,?,?,?,?,?,?)",
                    tenantGraphEdgesDataToDB,
                )
            except Exception as e:
                self.logger.error("Unable to insert data" + str(e))
                print(e)

        nodeProperties = self.data["MemberRelNodeProperties"]
        for curNodeProperty in nodeProperties:
            curNodePropertyId = curNodeProperty["Id"]
            stringIDList = []
            graphNodePropertyIds = []
            curGraphName = ""
            isTailNode = False
            for x in curNodeProperty["MemberRelNodePropertyAttributes"]:
                stringIDList.append(self.tenantAttributeIdToDimName[x])
                graphNodePropertyIds.append(x)
            stringIDList = sorted(stringIDList)
            stringIDList.append(curNodeProperty["PropertyName"])
            stringID = "-".join(stringIDList)

            for i in graphNodePropertyList:
                if i["GraphFromNodeIds"] == sorted(graphNodePropertyIds):
                    curGraphName = i["GraphName"]
                    isTailNode = False
                    break
                elif i["GraphToNodeIds"] == sorted(graphNodePropertyIds):
                    curGraphName = i["GraphName"]
                    isTailNode = True
                    break

            finalTenantGraphNodeProperties = {
                "TenantName": self.tenantName,
                "NodePropertyId": curNodeProperty["Id"],
                "PropertyName": curNodeProperty["PropertyName"],
                "RelationshipTypeName": curGraphName,
                "PropertyDescription": curNodeProperty["PropertyDescription"],
                "PropertyDataType": curNodeProperty["PropertyDataType"],
                "PropertyDataSize": curNodeProperty["PropertyDataSize"],
                "PropertyFormula": curNodeProperty["PropertyFormula"],
                "IsTailNode": isTailNode,
                "StringID": stringID,
            }
            try:
                self.dbConnection.execute(
                    "INSERT INTO NodeCombosConditionalFormats (TenantName, NodePropertyId, PropertyName, "
                    "RelationshipTypeName, PropertyDescription, PropertyDataType, PropertyDataSize, PropertyFormula, "
                    "IsTailNode, StringID) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        finalTenantGraphNodeProperties["TenantName"],
                        finalTenantGraphNodeProperties["NodePropertyId"],
                        finalTenantGraphNodeProperties["PropertyName"],
                        finalTenantGraphNodeProperties["RelationshipTypeName"],
                        finalTenantGraphNodeProperties["PropertyDescription"],
                        finalTenantGraphNodeProperties["PropertyDataType"],
                        finalTenantGraphNodeProperties["PropertyDataSize"],
                        finalTenantGraphNodeProperties["PropertyFormula"],
                        finalTenantGraphNodeProperties["IsTailNode"],
                        finalTenantGraphNodeProperties["StringID"],
                    ),
                )
            except Exception as e:
                self.logger.error(
                    "Unable to insert data into NodeCombosConditionalFormats: " + str(e)
                )
                print(e)

            for curNodePropertyAttribute in curNodeProperty[
                "MemberRelNodePropertyAttributes"
            ]:
                # Extract graph node property attributes and insert into NodeCombos Tables
                finalTenantGraphNodePropAttributes = [
                    {
                        "TenantName": self.tenantName,
                        "NodePropertyId": curNodePropertyId,
                        "DimensionName": self.tenantAttributeIdToDimName[
                            curNodePropertyAttribute
                        ],
                        "AttributeName": self.tenantAttributeIdToAttrName[
                            curNodePropertyAttribute
                        ],
                        "StringID": stringID,
                    }
                ]
                tenantGraphNodePropAttributesDataToDB = [
                    (
                        i["TenantName"],
                        i["NodePropertyId"],
                        i["DimensionName"],
                        i["AttributeName"],
                        i["StringID"],
                    )
                    for i in finalTenantGraphNodePropAttributes
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO NodeCombos (TenantName, NodePropertyId, DimensionName, AttributeName, StringID) "
                        " VALUES (?,?,?,?,?)",
                        tenantGraphNodePropAttributesDataToDB,
                    )
                except Exception as e:
                    self.logger.error(
                        "Unable to insert data into NodeCombos: " + str(e)
                    )
                    print(e)

    def createPlanTablesInDB(self):
        """
        Insert corresponding data for all plans related tables.
        """
        tenantPickListIdToName = {}
        finalMeasureStaticProperties = [] # To store extracted static properties

        for curPickList in self.data["PickLists"]:
            tenantPickListIdToNameTemp = {
                curPickList["Id"]: curPickList["PickListName"]
            }
            tenantPickListIdToName.update(tenantPickListIdToNameTemp)
        allPlans = self.data["Plans"]
        jPlans = [x for x in allPlans if x["PlanName"] != "Algorithm Parameters"]

        # Extract Plans data and insert into Plans table.
        finalTenantPlans = [
            {
                "TenantName": self.tenantName,
                "PlanName": x["PlanName"],
                "PlanDescription": x["PlanDescription"],
            }
            for x in jPlans
        ]
        tenantPlansDataToDB = [
            (i["TenantName"], i["PlanName"], i["PlanDescription"])
            for i in finalTenantPlans
        ]
        try:
            self.dbConnection.executemany(
                "INSERT INTO Plans (TenantName, PlanName, PlanDescription) VALUES (?, ?, ?)",
                tenantPlansDataToDB,
            )
        except Exception as ex:
            self.logger.error("Unable to insert data" + str(ex))
            print(ex)

        for curPlan in jPlans:
            curPlanName = curPlan["PlanName"]

            if curPlanName == "Algorithm" or curPlanName == "DimPlugin":
                continue
            jMeasureGroups = curPlan["MeasureGroups"]
            if len(jMeasureGroups) <= 0:
                continue

            curPlanMeasureGroups = []
            for curMeasureGroup in jMeasureGroups:
                curMeasureGroupName = curMeasureGroup["MeasureGroupName"]
                if (
                    "MeasureGroupTranslations" in curMeasureGroup
                    and len(curMeasureGroup["MeasureGroupTranslations"]) > 0
                ):
                    curMGTranslation = [
                        {
                            "TenantName": self.tenantName,
                            "PlanName": curPlanName,
                            "MeasureGroupName": curMeasureGroupName,
                            "MeasureGroupTranslationName": i["MeasureGroupName"],
                            "MeasureGroupTranslationDescription": i[
                                "MeasureGroupDescription"
                            ],
                            "Language": i["Language"],
                        }
                        for i in curMeasureGroup["MeasureGroupTranslations"]
                    ]
                    MGTranslationToDB = [
                        (
                            i["TenantName"],
                            i["PlanName"],
                            i["MeasureGroupName"],
                            i["MeasureGroupTranslationName"],
                            i["MeasureGroupTranslationDescription"],
                            i["Language"],
                        )
                        for i in curMGTranslation
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO MeasureGroupTranslations (TenantName, PlanName, MeasureGroupName, "
                            "MeasureGroupTranslationName, MeasureGroupTranslationDescription, Language) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            MGTranslationToDB,
                        )
                    except Exception as ex:
                        self.logger.error(
                            "Unable to insert data into MeasureGroupTranslations: "
                            + str(ex)
                        )
                        print(ex)

                if (
                    "MeasureGroupExternalConfigs" in curMeasureGroup
                    and len(curMeasureGroup["MeasureGroupExternalConfigs"]) > 0
                ):
                    externalConfigMG = [
                        {
                            "TenantName": self.tenantName,
                            "PlanName": curPlanName,
                            "MeasureGroupName": curMeasureGroupName,
                            "NeedsRedeployment": _x["NeedsRedeployment"],
                            "DeploymentStatus": _x["DeploymentStatus"],
                            "MaintainLocalCache": _x["MaintainLocalCache"],
                            "DeploymentStatusMessage": _x["DeploymentStatusMessage"],
                            "ExternalConfigJson": _x["ExternalConfigJson"],
                            "DataSourceType": _x["DataSourceType"],
                        }
                        for _x in curMeasureGroup["MeasureGroupExternalConfigs"]
                    ]
                    externalConfigMGToDB = [
                        (
                            i["TenantName"],
                            i["PlanName"],
                            i["MeasureGroupName"],
                            i["NeedsRedeployment"],
                            i["DeploymentStatus"],
                            i["MaintainLocalCache"],
                            i["DeploymentStatusMessage"],
                            i["ExternalConfigJson"],
                            i["DataSourceType"],
                        )
                        for i in externalConfigMG
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO MeasureGrpExternalConfigs (TenantName, PlanName, MeasureGroupName, "
                            "NeedsRedeployment, DeploymentStatus, MaintainLocalCache, DeploymentStatusMessage, "
                            "ExternalConfigJson, DataSourceType) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            externalConfigMGToDB,
                        )
                    except Exception as ex:
                        self.logger.error(
                            "Unable to insert data into MeasureGrpExternalConfigs: "
                            + str(ex)
                        )
                        print(ex)

                granularity = curMeasureGroup["DimensionUsages"]
                granularityList = []
                for i in granularity:
                    granularityList.append(
                        "[" + i["DimensionName"] + "].[" + i["AttributeName"] + "]"
                    )
                curPlanMeasureGroups = curPlanMeasureGroups + [
                    {
                        "TenantName": self.tenantName,
                        "PlanName": curPlanName,
                        "MeasureGroupName": curMeasureGroup["MeasureGroupName"],
                        "MeasureGroupDescription": curMeasureGroup[
                            "MeasureGroupDescription"
                        ],
                        "granularityString": " * ".join(sorted(granularityList)),
                    }
                ]

                finalCurPlanMeasureGrpGranularity = [
                    {
                        "TenantName": self.tenantName,
                        "PlanName": curPlanName,
                        "MeasureGroupName": curMeasureGroupName,
                        "DimensionName": x["DimensionName"],
                        "AttributeName": x["AttributeName"],
                        "SortOrder": x["SortOrder"],
                    }
                    for x in granularity
                ]
                planMeasureGrpGranularityDataToDB = [
                    (
                        i["TenantName"],
                        i["PlanName"],
                        i["MeasureGroupName"],
                        i["DimensionName"],
                        i["AttributeName"],
                        i["SortOrder"],
                    )
                    for i in finalCurPlanMeasureGrpGranularity
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO MeasureGrpGranularity (TenantName, PlanName, MeasureGroupName, DimensionName, "
                        "AttributeName, SortOrder) VALUES (?, ?, ?, ?, ?, ?)",
                        planMeasureGrpGranularityDataToDB,
                    )
                except Exception as ex:
                    self.logger.error(
                        "Unable to insert data into MeasureGrpGranularity: " + str(ex)
                    )
                    print(ex)

                curPlanMGrpAsGraphGranularity = [
                    {
                        "TenantName": self.tenantName,
                        "PlanName": curPlanName,
                        "MeasureGroupName": curMeasureGroupName,
                        "DimensionName": self.tenantAttributeIdToDimName[
                            x["DimensionAttributeId"]
                        ],
                        "AttributeName": self.tenantAttributeIdToAttrName[
                            x["DimensionAttributeId"]
                        ],
                        "IsTailNode": x["IsTailNode"],
                    }
                    for x in curMeasureGroup["MeasureGroupAssociationGraphAttributes"]
                ]

                planMGrpAsGraphGranularityDataToDB = [
                    (
                        i["TenantName"],
                        i["PlanName"],
                        i["MeasureGroupName"],
                        i["DimensionName"],
                        i["AttributeName"],
                        i["IsTailNode"],
                    )
                    for i in curPlanMGrpAsGraphGranularity
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO MeasureGroupAsGraphGranularities (TenantName, PlanName, MeasureGroupName, "
                        "DimensionName, AttributeName, IsTailNode) VALUES (?, ?, ?, ?, ?, ?)",
                        planMGrpAsGraphGranularityDataToDB,
                    )
                except Exception as ex:
                    self.logger.error(
                        "Unable to insert data into MeasureGroupAsGraphGranularities: "
                        + str(ex)
                    )
                    print(ex)

                if len(curMeasureGroup["Measures"]) <= 0:
                    # Skip next stuff if there are no measures in this measure-group
                    continue

                jMeasures = curMeasureGroup["Measures"]

                # Split description into (description, tag)
                for x in jMeasures:
                    measureDescription = re.search(
                        r".*?(?=Tags|$)", x["MeasureDescription"]
                    )
                    measureTags = re.search(
                        r"(?<=Tags\[)(.*)(?=\])", x["MeasureDescription"]
                    )
                    if measureDescription:
                        x["MeasureDescription"] = measureDescription.group().strip()
                    else:
                        x["MeasureDescription"] = ""

                    if measureTags:
                        x["Tags"] = measureTags.group().strip()
                    else:
                        x["Tags"] = ""

                finalCurPlanMeasures = []
                for x in jMeasures:
                    usedAsIBPLCount = None
                    usedAsNotIBPLCount = None
                    if self.measureUsage:
                        try:
                            usedAsIBPLCount = self.measureAsIBPLCount[
                                "Measure.[" + x["MeasureName"] + "]"
                            ]
                        except:
                            usedAsIBPLCount = None
                        try:
                            usedAsNotIBPLCount = (
                                self.measureAsNotIBPLCount[
                                    ': "' + x["MeasureName"] + '"'
                                ]
                                - 1
                            )
                        except:
                            usedAsNotIBPLCount = None
                    validationFormula = None
                    validationTooltip = None
                    if "MeasureProperties" in x:
                        for i in x["MeasureProperties"]:
                            if i["PropertyType"] == "Validation_Status":
                                validationFormula = i["PropertyFormula"]
                                validationTooltip = i["ToolTip"]
                    if "MeasureAggregates" in x:
                        measureAggregateData = [
                            {
                                "TenantName": self.tenantName,
                                "PlanName": curPlanName,
                                "MeasureGroupName": curMeasureGroupName,
                                "MeasureName": x["MeasureName"],
                                "AggregateFunction": (
                                    i["AggregateFunction"]
                                    if "AggregateFunction" in i
                                    else None
                                ),
                                "OrderNumber": i["Order"] if "Order" in i else None,
                                "DimensionName": (
                                    i["DimensionName"] if "DimensionName" in i else None
                                ),
                            }
                            for i in x["MeasureAggregates"]
                        ]
                        toDB = [
                            (
                                i["TenantName"],
                                i["PlanName"],
                                i["MeasureGroupName"],
                                i["MeasureName"],
                                i["AggregateFunction"],
                                i["OrderNumber"],
                                i["DimensionName"],
                            )
                            for i in measureAggregateData
                        ]
                        if len(toDB) > 0:
                            try:
                                self.dbConnection.executemany(
                                    "INSERT INTO MeasureAggregates (TenantName, PlanName, MeasureGroupName, MeasureName, "
                                    "AggregateFunction, OrderNumber, DimensionName) VALUES (?,?,?,?,?,?,?);",
                                    toDB,
                                )
                            except Exception as e:
                                self.logger.error(
                                    "Unable to insert data into MeasureAggregates: "
                                    + str(e)
                                )
                                print(e)

                    # Extract specific MeasureStaticProperties
                    if "MeasureStaticProperties" in x and isinstance(x["MeasureStaticProperties"], list):
                        for static_prop in x["MeasureStaticProperties"]:
                            if static_prop.get("PropertyName") == "IsInputOutputInterface":
                                property_value = static_prop.get("PropertyValue")
                                finalMeasureStaticProperties.append({
                                    "TenantName": self.tenantName,
                                    "PlanName": curPlanName,
                                    "MeasureGroupName": curMeasureGroupName,
                                    "MeasureName": x["MeasureName"],
                                    "PropertyName": "IsInputOutputInterface", # Storing the specific property name
                                    "PropertyValue": property_value
                                })
                                break # Assuming we only need the first occurrence of this property

                    finalCurPlanMeasures = finalCurPlanMeasures + [
                        {
                            "TenantName": self.tenantName,
                            "PlanName": curPlanName,
                            "MeasureGroupName": curMeasureGroupName,
                            "MeasureName": x["MeasureName"],
                            "MeasureColumnName": (
                                x["MeasureColumnName"]
                                if "MeasureColumnName" in x
                                else None
                            ),
                            "MeasureDescription": x["MeasureDescription"],
                            "Tags": x["Tags"],
                            "AggregateFunction": x["AggregateFunction"],
                            "DataType": x["DataType"],
                            "FormatString": x["FormatString"],
                            "IsEditable": x["IsEditable"],
                            "IsReportingMeasure": (
                                x["IsReportingMeasure"]
                                if "IsReportingMeasure" in x
                                else None
                            ),
                            "MeasureType": x["MeasureType"],
                            "AssociationAsGraph": x["AssociationMeasure"],
                            "ToolTip": x["ToolTip"] if "ToolTip" in x else None,
                            "ValidationFormula": validationFormula,
                            "ValidationTooltip": validationTooltip,
                            "UsedAsIBPLCount": usedAsIBPLCount,
                            "UsedAsNonIBPLCount": usedAsNotIBPLCount,
                            "TotalUsageCount": None,
                            "ConversionFormula": (
                                x["ConversionFormula"]
                                if "ConversionFormula" in x
                                else None
                            ),
                            "ApplyConversion": (
                                x["ApplyConversion"] if "ApplyConversion" in x else None
                            ),
                        }
                    ]

                planMeasuresDataToDB = [
                    (
                        i["TenantName"],
                        i["PlanName"],
                        i["MeasureGroupName"],
                        i["MeasureName"],
                        i["MeasureColumnName"],
                        i["MeasureDescription"],
                        i["Tags"],
                        i["AggregateFunction"],
                        i["DataType"],
                        i["FormatString"],
                        i["IsEditable"],
                        i["IsReportingMeasure"],
                        i["MeasureType"],
                        i["AssociationAsGraph"],
                        i["ToolTip"],
                        i["ValidationFormula"],
                        i["ValidationTooltip"],
                        i["UsedAsIBPLCount"],
                        i["UsedAsNonIBPLCount"],
                        i["TotalUsageCount"],
                        i["ConversionFormula"],
                        i["ApplyConversion"],
                    )
                    for i in finalCurPlanMeasures
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO Measures (TenantName, PlanName, MeasureGroupName, MeasureName, MeasureColumnName, "
                        "MeasureDescription, Tags, AggregateFunction, DataType, FormatString, IsEditable, "
                        "IsReportingMeasure, MeasureType, AssociationAsGraph, ToolTip, ValidationFormula, "
                        "ValidationTooltip, UsedAsIBPLCount, UsedAsNonIBPLCount, TotalUsageCount, ConversionFormula, "
                        "ApplyConversion) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        planMeasuresDataToDB,
                    )
                except Exception as ex:
                    self.logger.error("Unable to insert data into Measures: " + str(ex))
                    print(ex)
                if self.measureUsage:
                    try:
                        self.dbConnection.execute(
                            "UPDATE Measures SET TotalUsageCount = IFNULL(UsedAsIBPLCount, 0) + "
                            "IFNULL(UsedAsNonIBPLCount, 0);"
                        )
                    except Exception as ex:
                        self.logger.error(
                            "Unable to Update data TotalUsageCount Measures: " + str(ex)
                        )
                        print(ex)
                measuresWithFormatting = filter(
                    lambda x: (
                        (x["BgColorFormula"] is not None and x["BgColorFormula"] != "")
                        or (
                            x["FgColorFormula"] is not None
                            and x["FgColorFormula"] != ""
                        )
                        or (x["TrendFormula"] is not None and x["TrendFormula"] != "")
                        or (
                            x["FormattingViewModel"] is not None
                            and x["FormattingViewModel"] != ""
                        )
                    ),
                    jMeasures,
                )

                finalCurPlanMeasureFormatting = [
                    {
                        "TenantName": self.tenantName,
                        "PlanName": curPlanName,
                        "MeasureGroupName": curMeasureGroupName,
                        "MeasureName": x["MeasureName"],
                        "MeasureDescription": x["MeasureDescription"],
                        "BgColorFormula": x["BgColorFormula"],
                        "FgColorFormula": x["FgColorFormula"],
                        "TrendFormula": x["TrendFormula"],
                        "FormattingViewModel": x["FormattingViewModel"],
                    }
                    for x in measuresWithFormatting
                ]
                planMeasureFormattingDataToDB = [
                    (
                        i["TenantName"],
                        i["PlanName"],
                        i["MeasureGroupName"],
                        i["MeasureName"],
                        i["MeasureDescription"],
                        i["BgColorFormula"],
                        i["FgColorFormula"],
                        i["TrendFormula"],
                        i["FormattingViewModel"],
                    )
                    for i in finalCurPlanMeasureFormatting
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO MeasureConditionalFormats (TenantName, PlanName, MeasureGroupName, MeasureName, "
                        "MeasureDescription, BgColorFormula, FgColorFormula, TrendFormula, FormattingViewModel) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        planMeasureFormattingDataToDB,
                    )
                except Exception as ex:
                    self.logger.error(
                        "Unable to insert data into MeasureConditionalFormats: "
                        + str(ex)
                    )
                    print(ex)

                measuresWithPickLists = filter(
                    lambda x: (x["PickListId"] is not None), jMeasures
                )

                finalCurPlanMeasurePickLists = [
                    {
                        "TenantName": self.tenantName,
                        "PlanName": curPlanName,
                        "MeasureGroupName": curMeasureGroupName,
                        "MeasureName": x["MeasureName"],
                        "MeasureDescription": x["MeasureDescription"],
                        "AggregateFunction": x["AggregateFunction"],
                        "DataType": x["DataType"],
                        "FormatString": x["FormatString"],
                        "IsEditable": x["IsEditable"],
                        "MeasureType": x["MeasureType"],
                        "PickListName": tenantPickListIdToName[x["PickListId"]],
                    }
                    for x in measuresWithPickLists
                ]
                planMeasurePickListsDataToDB = [
                    (
                        i["TenantName"],
                        i["PlanName"],
                        i["MeasureGroupName"],
                        i["MeasureName"],
                        i["MeasureDescription"],
                        i["AggregateFunction"],
                        i["DataType"],
                        i["FormatString"],
                        i["IsEditable"],
                        i["MeasureType"],
                        i["PickListName"],
                    )
                    for i in finalCurPlanMeasurePickLists
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO MeasurePickLists (TenantName, PlanName, MeasureGroupName, MeasureName, "
                        "MeasureDescription, AggregateFunction, DataType, FormatString, IsEditable, MeasureType, "
                        "PickListName) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        planMeasurePickListsDataToDB,
                    )
                except Exception as ex:
                    self.logger.error(
                        "Unable to insert data into MeasurePickLists: " + str(ex)
                    )
                    print(ex)

                measuresWithFormula = filter(
                    lambda x: (
                        x["MeasureFormula"] != "" and x["MeasureFormula"] is not None
                    ),
                    jMeasures,
                )

                finalCurPlanMeasureFormulae = [
                    {
                        "TenantName": self.tenantName,
                        "PlanName": curPlanName,
                        "MeasureGroupName": curMeasureGroupName,
                        "MeasureName": x["MeasureName"],
                        "MeasureDescription": x["MeasureDescription"],
                        "AggregateFunction": x["AggregateFunction"],
                        "DataType": x["DataType"],
                        "FormatString": x["FormatString"],
                        "IsEditable": x["IsEditable"],
                        "MeasureType": x["MeasureType"],
                        "MeasureFormula": x["MeasureFormula"],
                    }
                    for x in measuresWithFormula
                ]
                planMeasureFormulaeDataToDB = [
                    (
                        i["TenantName"],
                        i["PlanName"],
                        i["MeasureGroupName"],
                        i["MeasureName"],
                        i["MeasureDescription"],
                        i["AggregateFunction"],
                        i["DataType"],
                        i["FormatString"],
                        i["IsEditable"],
                        i["MeasureType"],
                        i["MeasureFormula"],
                    )
                    for i in finalCurPlanMeasureFormulae
                    if i["AggregateFunction"] == "Computed"
                ]
                try:
                    self.dbConnection.executemany(
                        "INSERT INTO MeasureFormulae (TenantName, PlanName, MeasureGroupName, MeasureName, "
                        "MeasureDescription, AggregateFunction, DataType, FormatString, IsEditable, MeasureType, "
                        "MeasureFormula) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        planMeasureFormulaeDataToDB,
                    )
                except Exception as ex:
                    self.logger.error(
                        "Unable to insert data into MeasureFormulae: " + str(ex)
                    )
                    print(ex)

                for iMeasure in jMeasures:
                    finalCurPlanMeasureSpreads = [
                        {
                            "TenantName": self.tenantName,
                            "PlanName": curPlanName,
                            "MeasureGroupName": curMeasureGroupName,
                            "MeasureName": iMeasure["MeasureName"],
                            "BasisMeasureName": x["BasisMeasureName"],
                            "BasisMeasureType": x["BasisMeasureType"],
                            "SpreadingType": x["SpreadingType"],
                        }
                        for x in iMeasure["MeasureSpreads"]
                    ]
                    planMeasureSpreadsDataToDB = [
                        (
                            i["TenantName"],
                            i["PlanName"],
                            i["MeasureGroupName"],
                            i["MeasureName"],
                            i["BasisMeasureName"],
                            i["BasisMeasureType"],
                            i["SpreadingType"],
                        )
                        for i in finalCurPlanMeasureSpreads
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO MeasureSpreads (TenantName, PlanName, MeasureGroupName, MeasureName, "
                            "BasisMeasureName, BasisMeasureType, SpreadingType) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            planMeasureSpreadsDataToDB,
                        )
                    except Exception as ex:
                        self.logger.error(
                            "Unable to insert data into MeasureSpreads: " + str(ex)
                        )
                        print(ex)

                    finalCurPlanMeasureTranslations = [
                        {
                            "TenantName": self.tenantName,
                            "PlanName": curPlanName,
                            "MeasureGroupName": curMeasureGroupName,
                            "MeasureName": iMeasure["MeasureName"],
                            "TranslationName": x["MeasureName"],
                            "TranslationDesc": x["MeasureDescription"],
                            "ToolTip": x["ToolTip"],
                            "LCID": x["LCID"],
                            "Language": x["Language"],
                        }
                        for x in iMeasure["MeasureTranslations"]
                    ]
                    planMeasureTranslationsDataToDB = [
                        (
                            i["TenantName"],
                            i["PlanName"],
                            i["MeasureGroupName"],
                            i["MeasureName"],
                            i["TranslationName"],
                            i["TranslationDesc"],
                            i["ToolTip"],
                            i["LCID"],
                            i["Language"],
                        )
                        for i in finalCurPlanMeasureTranslations
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO MeasureTranslations (TenantName, PlanName, MeasureGroupName, MeasureName, "
                            "TranslationName, TranslationDesc, ToolTip, LCID, Language) VALUES (?,?,?,?,?,?,?,?,?)",
                            planMeasureTranslationsDataToDB,
                        )
                    except Exception as ex:
                        self.logger.error(
                            "Unable to insert data into MeasureTranslations: " + str(ex)
                        )
                        print(ex)

                    finalCurPlanMeasureTwins = [
                        {
                            "TenantName": self.tenantName,
                            "PlanName": curPlanName,
                            "MeasureGroupName": curMeasureGroupName,
                            "PrimaryMeasureName": x["PrimaryMeasureName"],
                            "TwinMeasureName": x["TwinMeasureName"],
                            "TwinToPrimaryFormula": x["TwinToPrimaryFormula"],
                            "ExternalChangeUpdatesPrimary": x[
                                "ExternalChangeUpdatesPrimary"
                            ],
                        }
                        for x in iMeasure["MeasureTwins"]
                    ]
                    planMeasureTwinsDataToDB = [
                        (
                            i["TenantName"],
                            i["PlanName"],
                            i["MeasureGroupName"],
                            i["PrimaryMeasureName"],
                            i["TwinMeasureName"],
                            i["TwinToPrimaryFormula"],
                            i["ExternalChangeUpdatesPrimary"],
                        )
                        for i in finalCurPlanMeasureTwins
                    ]
                    try:
                        self.dbConnection.executemany(
                            "INSERT INTO MeasureTwins (TenantName, PlanName, MeasureGroupName, PrimaryMeasureName, "
                            "TwinMeasureName, TwinToPrimaryFormula, ExternalChangeUpdatesPrimary) \
                                VALUES (?, ?, ?, ?, ?, ?, ?)",
                            planMeasureTwinsDataToDB,
                        )
                    except Exception as ex:
                        self.logger.error(
                            "Unable to insert data into MeasureTwins: " + str(ex)
                        )
                        print(ex)

            planMeasureGroupsDataToDB = [
                (
                    i["TenantName"],
                    i["PlanName"],
                    i["MeasureGroupName"],
                    i["MeasureGroupDescription"],
                    i["granularityString"],
                )
                for i in curPlanMeasureGroups
            ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO MeasureGroups (TenantName, PlanName, MeasureGroupName, MeasureGroupDescription, "
                    "GranularityAsSingleString) VALUES (?, ?, ?, ?, ?)",
                    planMeasureGroupsDataToDB,
                )
            except Exception as ex:
                self.logger.error(
                    "Unable to insert data into MeasureGroups: " + str(ex)
                )
                print(ex)

        # Insert collected MeasureStaticProperties into a new table
        if finalMeasureStaticProperties:
            measureStaticPropertiesDataToDB = [
                (
                    item["TenantName"],
                    item["PlanName"],
                    item["MeasureGroupName"],
                    item["MeasureName"],
                    item["PropertyName"],
                    item["PropertyValue"],
                )
                for item in finalMeasureStaticProperties
            ]
            try:
                self.dbConnection.executemany(
                    "INSERT INTO MeasureStaticPropertiesInfo (TenantName, PlanName, MeasureGroupName, MeasureName, "
                    "PropertyName, PropertyValue) VALUES (?, ?, ?, ?, ?, ?)",
                    measureStaticPropertiesDataToDB,
                )
                self.logger.info(f"Successfully inserted {len(measureStaticPropertiesDataToDB)} records into MeasureStaticPropertiesInfo.")
            except Exception as ex:
                self.logger.error(
                    "Unable to insert data into MeasureStaticPropertiesInfo: " + str(ex)
                )
                print(ex)

    def addToDimAttrPropertiesTable(
        self, dimAttrPropertiesObj, dimensionName, curAttributeName
    ):
        """
        Extract attribute property data and insert into DimAttrProperties table.
        :param dimAttrPropertiesObj: dimension attribute properties data object
        :param dimensionName: dimension name
        :param curAttributeName: current attribute name
        :return:
        """
        finalTenantAttrProperty = []
        if dimensionName == "Personnel":
            if curAttributeName == "Email":
                for x in dimAttrPropertiesObj:
                    if (
                        x["AttributeName"] != "User Name"
                        and x["AttributeName"] != "User Id"
                        and x["AttributeName"] != "Is MPower User"
                        and x["AttributeName"] != "Image"
                    ):
                        finalTenantAttrProperty = finalTenantAttrProperty + [
                            {
                                "TenantName": self.tenantName,
                                "DimensionName": dimensionName,
                                "AttributeName": curAttributeName,
                                "PropertyName": x["AttributeName"],
                                "Description": x["Description"],
                                "KeyColumnDataType": x["KeyColumnDataType"],
                            }
                        ]
            else:
                finalTenantAttrProperty = finalTenantAttrProperty + [
                    {
                        "TenantName": self.tenantName,
                        "DimensionName": dimensionName,
                        "AttributeName": curAttributeName,
                        "PropertyName": x["AttributeName"],
                        "Description": x["Description"],
                        "KeyColumnDataType": x["KeyColumnDataType"],
                    }
                    for x in dimAttrPropertiesObj
                ]
        elif dimensionName == "Version":
            if curAttributeName == "Version Name":
                for x in dimAttrPropertiesObj:
                    if (
                        x["AttributeName"] != "Version Creation Timestamp"
                        and x["AttributeName"] != "Version Creation Date"
                        and x["AttributeName"] != "Version Creation Date Level"
                        and x["AttributeName"] != "Is Official"
                        and x["AttributeName"] != "Is Latest"
                        and x["AttributeName"] != "Is Target"
                        and x["AttributeName"] != "Version Archival Date"
                        and x["AttributeName"] != "Version Expiry Date"
                        and x["AttributeName"] != "Version Archival Policy"
                        and x["AttributeName"] != "Version Expiration Policy"
                        and x["AttributeName"] != "Category"
                        and x["AttributeName"] != "Access Role"
                        and x["AttributeName"] != "Is Tuple Merged"
                        and x["AttributeName"] != "Source Name"
                        and x["AttributeName"] != "Proxy Time Stamp"
                        and x["AttributeName"] != "Current Time For Version"
                        and x["AttributeName"] != "Source Key"
                        and x["AttributeName"] != "$ScopedScenarioMetadata"
                        and x["AttributeName"] != "Parent Key"
                        and x["AttributeName"] != "Parent Name"
                        and x["AttributeName"] != "Version Created By"
                        and x["AttributeName"] != "External Replace Parent Data"
                    ):
                        finalTenantAttrProperty = finalTenantAttrProperty + [
                            {
                                "TenantName": self.tenantName,
                                "DimensionName": dimensionName,
                                "AttributeName": curAttributeName,
                                "PropertyName": x["AttributeName"],
                                "Description": x["Description"],
                                "KeyColumnDataType": x["KeyColumnDataType"],
                            }
                        ]
            else:
                finalTenantAttrProperty = finalTenantAttrProperty + [
                    {
                        "TenantName": self.tenantName,
                        "DimensionName": dimensionName,
                        "AttributeName": curAttributeName,
                        "PropertyName": x["AttributeName"],
                        "Description": x["Description"],
                        "KeyColumnDataType": x["KeyColumnDataType"],
                    }
                    for x in dimAttrPropertiesObj
                ]
        else:
            finalTenantAttrProperty = finalTenantAttrProperty + [
                {
                    "TenantName": self.tenantName,
                    "DimensionName": dimensionName,
                    "AttributeName": curAttributeName,
                    "PropertyName": x["AttributeName"],
                    "Description": x["Description"],
                    "KeyColumnDataType": x["KeyColumnDataType"],
                }
                for x in dimAttrPropertiesObj
            ]
        tenantAttrPropertyDataToDB = [
            (
                i["TenantName"],
                i["DimensionName"],
                i["AttributeName"],
                i["PropertyName"],
                i["Description"],
                i["KeyColumnDataType"],
            )
            for i in finalTenantAttrProperty
        ]

        try:
            self.dbConnection.executemany(
                "INSERT INTO DimAttrProperties (TenantName, DimensionName, AttributeName, PropertyName, Description, "
                "KeyColumnDataType) VALUES (?,?,?,?,?,?)",
                tenantAttrPropertyDataToDB,
            )
        except Exception as e:
            self.logger.error("Unable to insert data into DimAttrProperties: " + str(e))
            print(e)
