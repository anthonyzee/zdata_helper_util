import json

def convertConditionCode(sConditionCode):
    if sConditionCode == '0':
        return 'AND'
    elif sConditionCode == '1':
        return 'OR'

def convertOperatorCode(sOperatorCode):
    if sOperatorCode == '0':
        return 'eq'
    elif sOperatorCode == '1':
        return 'ne'
    elif sOperatorCode == '2':
        return 'eq'
    elif sOperatorCode == '3':
        return 'ge'
    elif sOperatorCode == '4':
        return 'le'
    elif sOperatorCode == '5':
        return 'gt'
    elif sOperatorCode == '6':
        return 'lt'
    elif sOperatorCode == '7':
        return 'eq'
    elif sOperatorCode == '8':
        return 'eq'

def convertFieldName(sFieldName, oDataResponse):
    
    # initialize variable
    oSchemeObject = json.loads(oDataResponse['d']['results'][0]['schemajson'])
    
    for sSchemaObject in oSchemeObject:
        if sFieldName in sSchemaObject:
            return sSchemaObject
    
    return sFieldName

def prepareDynQueryString(oFieldList, oDataResponse, oConditionlist):
    

    # initialize variable
    sDynQueryString = ''

    # preparing
    for oFieldObject in oFieldList:
        for sFieldObject in oFieldObject:
            last_character = sFieldObject[-1]
            second_to_last_character = sFieldObject[-2]
            sFieldName = sFieldObject[0:-2]
            
            sSingleDynQueryString = convertFieldName(sFieldName, oDataResponse) + ' ' + convertOperatorCode(last_character) + ' \'' + oFieldObject[sFieldObject] + '\'' 
            
            if sDynQueryString == '':
                sDynQueryString = sSingleDynQueryString
            else:
                sDynQueryString = sDynQueryString + ' ' + sNextCondition + ' ' + sSingleDynQueryString
                
            sNextCondition = convertConditionCode(second_to_last_character)
    
    return sDynQueryString

def getZDataSqlCommandObject(oEventParamObject, oDataResponse, oConditionlist):
    
    # initialize variable
    oSqlCommandObject = {"TableName": "","DbUser":"","DbPassword":"","DbServer":"","DbName":"",
                "SqlFilter": "","SqlSelect":"*","SqlTop":"","SqlOrderBy":"","SqlLineCount":"","SqlSkip":"", "SqlScope": "", "SqlFilterList": []}
    
    # parse zapi query to oDynQueryObject
    if oEventParamObject != None:
        for sQueryString in oEventParamObject:
            
            if sQueryString == 'f':
                # count or filter
                oSqlCommandObject['Type'] = oEventParamObject[sQueryString]
            elif sQueryString == 'id':
                oSqlCommandObject['TableName'] = oEventParamObject[sQueryString]
            elif sQueryString == 'k':
                oSqlCommandObject['Key'] = oEventParamObject[sQueryString]
            elif sQueryString == 'l':
                oSqlCommandObject['SqlTop'] = oEventParamObject[sQueryString]
            elif sQueryString == 'o':
                oSqlCommandObject['SqlSkip'] = oEventParamObject[sQueryString]
            elif sQueryString == '_grb':
                oSqlCommandObject['SqlGroupBy'] = oEventParamObject[sQueryString]
            elif sQueryString == '_orb':
                oSqlCommandObject['SqlOrderBy'] = oEventParamObject[sQueryString]
            else:
                # where
                oZfieldObject = {}
                oZfieldObject[sQueryString] = oEventParamObject[sQueryString]
                oSqlCommandObject['SqlFilterList'].append(oZfieldObject)    
                
        oSqlCommandObject['OdataFilter'] = prepareDynQueryString(oSqlCommandObject['SqlFilterList'], oDataResponse, oConditionlist)
        
    return oSqlCommandObject
