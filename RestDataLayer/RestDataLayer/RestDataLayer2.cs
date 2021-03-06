﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using org.iringtools.library;
using org.iringtools.adapter;
using org.iringtools.utility;
using System.IO;
using System.Net;
using System.Xml.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Data;
using log4net;
//using WebClient = System.Net.Http;


namespace Bechtel.DataLayer
{
    public class RestDataLayer2 : BaseDataLayer
    {
        private DataDictionary _dataDictionary = null;
        //   private DatabaseDictionary _dictionary = null;

        private string _applicationName = string.Empty;
        private string _projectName = string.Empty;
        private string _xmlPath = string.Empty;
        private string _baseDirectory = string.Empty;
        private string _keyDelimiter;

        string _authToken = string.Empty;
        string _appKey = string.Empty;
        string _endPointUrl = string.Empty;
        string _baseUrl = string.Empty;
        string _schemaUrl = string.Empty;
        
        IWebClient _webClient = null;

        private ILog _logger = LogManager.GetLogger(typeof(RestDataLayer2));


        private Dictionary<string, string> _objectyDictionary = null;
        private IDictionary<string, string> _selfUrl = null;

        public RestDataLayer2(AdapterSettings settings)
            : base(settings)
        {
            _xmlPath = _settings["xmlPath"];
            _projectName = _settings["projectName"];
            _applicationName = _settings["applicationName"];
            _baseDirectory = _settings["BaseDirectoryPath"];
            _authToken = _settings["AuthToken"];
            _appKey = _settings["AppKey"];
            _endPointUrl = _settings["EndPointUrl"];
            _baseUrl = _settings["BaseUrl"];
            _keyDelimiter = Convert.ToString(_settings["DefaultKeyDelimiter"]) ?? string.Empty;

            //_webClient = new WebClient(_baseUrl, _appKey, _authToken);
             _webClient = new IringWebClient(_baseUrl, _appKey, _authToken);
            //_webClient = new MockWebClient(_baseUrl, _appKey, _authToken);

           
             LoadEndPointSettings();
             _selfUrl = GetSelfUrlList(); 
            
        }

        public override DataDictionary GetDictionary()
        {

            string Connectionstring = string.Empty;

            string path = String.Format("{0}{1}DataDictionary.{2}.{3}.xml", _baseDirectory, _xmlPath, _projectName, _applicationName);
            try
            {
                if ((File.Exists(path)))
                {
                    dynamic DataDictionary = Utility.Read<DataDictionary>(path);
                    _dataDictionary = Utility.Read<DataDictionary>(path);
                    return _dataDictionary;
                }
                else
                {

                    _dataDictionary = CreateDataDictionary();

                    DatabaseDictionary _databaseDictionary = new DatabaseDictionary();
                    _databaseDictionary.dataObjects = _dataDictionary.dataObjects;
                    _databaseDictionary.ConnectionString = EncryptionUtility.Encrypt(Connectionstring);
                    _databaseDictionary.Provider = "dummy";
                    _databaseDictionary.SchemaName = "dummy";

                    Utility.Write<DatabaseDictionary>(_databaseDictionary, String.Format("{0}{1}DataBaseDictionary.{2}.{3}.xml", _baseDirectory, _xmlPath, _projectName, _applicationName));
                    Utility.Write<DataDictionary>(_dataDictionary, String.Format("{0}{1}DataDictionary.{2}.{3}.xml", _baseDirectory, _xmlPath, _projectName, _applicationName));
                    return _dataDictionary;
                }
            }
            catch
            {
                string error = "Error in getting dictionary";
                //  _logger.Error(error);
                throw new ApplicationException(error);
            }
        }

        public override IList<IDataObject> Get(string objectType, DataFilter filter, int limit, int start)
        {
            int lStart = start;
            int lLimit = limit;
            IList<IDataObject> dataObjects = null;

            try
            {

                string url = GenerateUrl(objectType, filter, limit, start);

                string filterUrl = filter.ToFilterExpression(_dataDictionary, objectType);
                if (!String.IsNullOrEmpty(filterUrl))
                {
                    url = url + "&" + filterUrl;
                }

                string jsonString = GetJsonResponseFrom(url);
                DataTable dataTable = GetDataTableFrom(jsonString, objectType);


                //Sorting 
                if (filter != null && filter.OrderExpressions != null && filter.OrderExpressions.Count > 0)
                {
                    string orderExpression = filter.ToOrderExpression(_dataDictionary, objectType);
                    dataTable.DefaultView.Sort = orderExpression;
                    dataTable = dataTable.DefaultView.ToTable();

                    dataObjects = ToDataObjects(dataTable, objectType);

                    if (lStart >= dataObjects.Count)
                        lStart = dataObjects.Count;

                    if (lLimit == 0 || (lLimit + lStart) >= dataObjects.Count)
                        lLimit = dataObjects.Count - lStart;

                    dataObjects = ((List<IDataObject>)dataObjects).GetRange(lStart, lLimit);

                }
                else
                {
                    dataObjects = ToDataObjects(dataTable, objectType);
                }

                return dataObjects;
            }
            catch (Exception ex)
            {
                _logger.Error("Error get data table: " + ex);
                throw ex;
            }
        }

        public override IList<IDataObject> Get(string objectType, IList<string> identifiers)
        {
            try
            {
                DataTable datatable = datatable = new DataTable();
                foreach (string identifier in identifiers)
                {
                    string url = GenerateUrl(objectType, identifier);
                    string jsonString = GetJsonResponseFrom(url);
                    DataTable dt = GetDataTableFrom(jsonString, objectType);
                    datatable.Merge(dt);
                }

                // Remove duplicate data from dataTable
                DataView dView = new DataView(datatable);
                string[] arrColumns = new string[datatable.Columns.Count];
                
                for(int i=0;i<datatable.Columns.Count;i++)
                    arrColumns[i]=datatable.Columns[i].ColumnName;
                
                datatable = dView.ToTable(true, arrColumns);
                //------

                IList<IDataObject> dataObjects = ToDataObjects(datatable, objectType);
                return dataObjects;
            }
            catch (Exception ex)
            {
                _logger.Error("Error in GetList: " + ex);
                throw new Exception("Error while getting a list of data objects of type [" + objectType + "].", ex);
            }
        }

        public override long GetCount(string objectType, DataFilter filter)
        {
            try
            {
                try
                {
                    return GetObjectCount(objectType, filter);
                }
                catch
                {
                    IList<IDataObject> dataObjects = Get(objectType, filter, 0, 0);
                    return dataObjects.Count();
                }
            }
            catch (Exception ex)
            {
                _logger.Error("Error in GetCount: " + ex);
                throw new Exception("Error while getting a count of type [" + objectType + "].", ex);
            }
        }

        public override IList<string> GetIdentifiers(string objectType, DataFilter filter)
        {
            List<string> identifiers = null;
            try
            {
                identifiers = new List<string>();

                DataObject objDef = _dataDictionary.dataObjects.Find(p => p.objectName.ToUpper() == objectType.ToUpper());

                //IList<string> keyCols = GetKeyColumns(objDef);

                //NOTE: pageSize of 0 indicates that all rows should be returned.
                IList<IDataObject> dataObjects = Get(objectType, filter, 0, 0);
                foreach (IDataObject dataObject in dataObjects)
                {
                    identifiers.Add(Convert.ToString(dataObject.GetPropertyValue(objDef.keyProperties[0].keyPropertyName)));
                }
            }
            catch (Exception ex)
            {
                _logger.ErrorFormat("Error while getting a filtered list of identifiers of type [{0}]: {1}", objectType, ex);
                throw new Exception("Error while getting a filtered list of identifiers of type [" + objectType + "].", ex);
            }

            return identifiers;
        }

        public override IList<IDataObject> GetRelatedObjects(IDataObject dataObject, string relatedObjectType)
        {
            return GetRelatedObjects(dataObject, relatedObjectType, 0, 0);
        }

        public override long GetRelatedCount(IDataObject dataObject, string relatedObjectType)
        {
            return GetRelatedObjects(dataObject, relatedObjectType, 0, 0).Count;
        }

        public override IList<IDataObject> GetRelatedObjects(IDataObject dataObject, string relatedObjectType, int pageSize, int startIndex)
        {
            string objectType = dataObject.GetType().Name;

            IList<IDataObject> dataObjects = null;

            if (objectType == typeof(GenericDataObject).Name)
            {
                objectType = ((GenericDataObject)dataObject).ObjectType;
            }

            try
            {
                DataObject parentDataObject = _dataDictionary.dataObjects.Find(x => x.objectName.ToUpper() == objectType.ToUpper());

                if (parentDataObject == null)
                    throw new Exception("Parent data object [" + objectType + "] not found.");

                DataObject relatedObjectDefinition = _dataDictionary.dataObjects.Find(x => x.objectName.ToUpper() == relatedObjectType.ToUpper());

                if (relatedObjectDefinition == null)
                    throw new Exception("Related data object [" + relatedObjectType + "] not found.");

                DataRelationship dataRelationship = parentDataObject.dataRelationships.Find(c => c.relatedObjectName.ToLower() == relatedObjectDefinition.objectName.ToLower());
                if (dataRelationship == null)
                    throw new Exception("Relationship between data object [" + objectType + "] and related data object [" + relatedObjectType + "] not found.");

                DataFilter filter = null;
                foreach (PropertyMap propertyMap in dataRelationship.propertyMaps)
                {
                    filter = new DataFilter();
                    string keyFieldValue = Convert.ToString(dataObject.GetPropertyValue(propertyMap.dataPropertyName));

                    Expression expression = new Expression();
                    expression.LogicalOperator = LogicalOperator.And;
                    expression.RelationalOperator = RelationalOperator.EqualTo;

                    expression.PropertyName = propertyMap.relatedPropertyName;
                    expression.Values = new Values() { keyFieldValue };

                    filter.Expressions.Add(expression);
                }

                dataObjects = Get(relatedObjectType, filter, 0, 0);

            }
            catch (Exception ex)
            {
                _logger.ErrorFormat("Error while geting related data objects", ex);
                throw new Exception("Error while geting related data objects", ex);
            }


            return dataObjects;
        }

        public override Response Post(IList<IDataObject> dataObjects)
        {
            Response response = new Response();
            string objectType = String.Empty;
            bool isNew = false;
            string identifier = String.Empty;

            objectType = ((GenericDataObject)dataObjects.FirstOrDefault()).ObjectType;
            DataObject objDef = _dataDictionary.dataObjects.Find(p => p.objectName.ToUpper() == objectType.ToUpper());

            if (dataObjects == null || dataObjects.Count == 0)
            {
                Status status = new Status();
                status.Level = StatusLevel.Warning;
                status.Messages.Add("Data object list provided is empty.");
                response.Append(status);
                return response;
            }

            try
            {

                foreach (IDataObject dataObject in dataObjects)
                {
                    identifier = String.Empty;
                    Status status = new Status();
                    string message = String.Empty;

                    try
                    {
                        String objectString = FormJsonObjectString(dataObject);
                        foreach (KeyProperty dataProperty in objDef.keyProperties)
                        {
                            string value = Convert.ToString(dataObject.GetPropertyValue(dataProperty.keyPropertyName));
                            if (String.IsNullOrEmpty(value))
                                isNew = true;
                            else
                                identifier = value;
                            break;
                        }
                        if (!String.IsNullOrEmpty(identifier))
                        {
                            int count = Get(objectType, new List<string>() { identifier }).Count;
                            if (count > 0)
                                isNew = false;
                            else
                                isNew = true;
                        }

                        string url = GenerateUrl(objectType);
                        if (isNew) ///Post data
                        {
                            _webClient.MakePostRequest(url, objectString);
                        }
                        else ///put data
                        {
                            _webClient.MakePutRequest(url, objectString);
                        }

                        message = String.Format("Data object [{0}] posted successfully.", identifier);
                        status.Messages.Add(message);
                        response.Append(status);

                    }
                    catch (Exception ex)
                    {
                        message = String.Format("Error while posting data object [{0}].", identifier);
                        status.Messages.Add(message);
                        response.Append(status);
                    }

                }

            }
            catch (Exception ex)
            {
                _logger.ErrorFormat("Error while processing a list of data objects of type [{0}]: {1}", objectType, ex);
                throw new Exception("Error while processing a list of data objects of type [" + objectType + "].", ex);
            }

            return response;
        }

        private string FormJsonObjectString(IDataObject dataObject)
        {
            string objectType = ((GenericDataObject)dataObject).ObjectType;
            DataObject objDef = _dataDictionary.dataObjects.Find(p => p.objectName.ToUpper() == objectType.ToUpper());

            StringBuilder sb = new StringBuilder();
            StringWriter sw = new StringWriter(sb);
            if (objDef != null)
            {


                JsonWriter jsonWriter = new JsonTextWriter(sw);
                jsonWriter.Formatting = Formatting.Indented;
                jsonWriter.WriteStartObject();

                foreach (var entry in ((GenericDataObject)dataObject).Dictionary)
                {
                    jsonWriter.WritePropertyName(entry.Key);
                    jsonWriter.WriteValue(entry.Value);
                }
                jsonWriter.WriteEndObject();
                jsonWriter.Close();
                sw.Close();
            }
            return sw.ToString();
        }

        public override Response Delete(string objectType, DataFilter filter)
        {
            try
            {
                IList<string> identifiers = GetIdentifiers(objectType, filter);
                Response response = Delete(objectType, identifiers);
                return response;
            }
            catch (Exception ex)
            {
                _logger.ErrorFormat("Error while deleting a list of data objects of type [{0}]: {1}", objectType, ex);
                throw new Exception("Error while deleting a list of data objects of type [" + objectType + "].", ex);
            }
        }

        public override Response Delete(string objectType, IList<string> identifiers)
        {
            Response response = new Response();

            if (identifiers == null || identifiers.Count == 0)
            {
                Status status = new Status();
                status.Level = StatusLevel.Warning;
                status.Messages.Add("Nothing to delete.");
                response.Append(status);
                return response;
            }

            try
            {
                foreach (string identifier in identifiers)
                {
                    Status status = new Status();
                    status.Identifier = identifier;
                    string message = String.Empty;
                    try
                    {
                        if (String.IsNullOrWhiteSpace(identifier))
                            throw new ApplicationException("Identifier can not be blank or null.");

                        string url = GenerateUrl(objectType, identifier);
                        _webClient.MakeDeleteRequest(url);

                        message = String.Format("DataObject [{0}] deleted successfully.", identifier);
                        status.Messages.Add(message);
                        response.Append(status);
                    }
                    catch
                    {
                        message = String.Format("Error while deleting dataObject [{0}].", identifier);
                        status.Messages.Add(message);
                        response.Append(status);
                    }
                }

            }
            catch (Exception ex)
            {
                _logger.ErrorFormat("Error while deleting a list of data objects of type [{0}]: {1}", objectType, ex);
                throw new Exception("Error while deleting a list of data objects of type [" + objectType + "].", ex);
            }

            return response;
        }

     
        #region Private function

        /// <summary>
        /// It will Load configration detail in a Dictionary object.
        /// </summary>
        private void LoadEndPointSettings()
        {
            
            string json = _webClient.MakeGetRequest(_endPointUrl);

            JObject mainObject = JObject.Parse(json);

            //--- Add all resources and there url into globle dictionary object
            if (_objectyDictionary == null)
            {
                _objectyDictionary = new Dictionary<string, string>();
                    
                foreach (JObject obj in (JArray)mainObject["resources"])
                {
                    string resources = obj["resource"].Value<string>();
                    string url = obj["url"].Value<string>();
                    _objectyDictionary.Add(resources, url);
                }
            }

            //--
             _schemaUrl =  mainObject["schemaurl"].Value<string>();
             _baseUrl = mainObject["baseurl"].Value<string>();
           

        }

       
        private DataDictionary CreateDataDictionary()
        {
            try
            {
                
                DataObject _dataObject = null;
                KeyProperty _keyproperties = new KeyProperty();
                DataProperty _dataproperties = new DataProperty();
                DataDictionary _dataDictionary = new DataDictionary();
              
                foreach (var dic in _objectyDictionary)
                {
                    string objectName = dic.Key;
                    bool isFirstRecored = false;
                    string url = _schemaUrl.Replace("{resource}", dic.Key);
                    string json = GetJsonResponseFrom(url);

                    JObject schemaObject = JObject.Parse(json);

                    foreach (JProperty propery in schemaObject.Properties())
                    {
                        string propertyName = propery.Name;
                        if (propertyName != "links")
                        {
                            if (!isFirstRecored)
                            {
                                isFirstRecored = true;
                                _dataObject = new DataObject();

                                _dataObject.objectName = objectName;
                                _dataObject.tableName = objectName;
                                _dataObject.keyDelimeter = _keyDelimiter;

                            }

                            _dataproperties = new DataProperty();
                            _dataproperties.propertyName = propertyName;
                            _dataproperties.columnName = propertyName;
                            _dataproperties.keyType = KeyType.unassigned;
                            _dataproperties.isNullable = true;

                            foreach(JProperty p in ((JObject)propery.Value).Properties())
                            {
                                if (p.Name == "type")
                                {
                                    _dataproperties.dataType = ResolveDataType(p.Value.ToString());
                                }
                                else if(p.Name == "size")
                                {
                                    if (p.Value.ToString() == "string")
                                        _dataproperties.dataLength = Convert.ToInt32(p.Value.ToString());
                                }
                            }

                            _dataObject.dataProperties.Add(_dataproperties);
                        }
                    }

                    foreach (JProperty propery in schemaObject.Properties())
                    {
                        if (propery.Name == "links")
                        {
                            _selfUrl.Add(objectName, schemaObject["links"]["self"].Value<string>());

                            JArray keyList = (JArray)schemaObject["links"]["key"];
                            for (int i = 0; i < keyList.Count; i++)
                            {
                                DataProperty property = _dataObject.dataProperties.Single<DataProperty>(x => x.propertyName == keyList[i].ToString());
                                property.keyType =  KeyType.assigned;
                                property.isNullable = false;
                                
                                _dataObject.keyProperties.Add(new KeyProperty() { keyPropertyName = property.propertyName });
                            }


                        }
                    }

                    _dataDictionary.dataObjects.Add(_dataObject);
                }

                return _dataDictionary;
            }
            catch (Exception ex)
            {
                _logger.Error("Error in loading data dictionary : " + ex);
                throw ex;
            }
          
        }

        private IDictionary<string,string> GetSelfUrlList()
        {
            IDictionary<string, string> selfUrlList = new Dictionary<string, string>();
            try
            {
                foreach (var dic in _objectyDictionary)
                {
                    string objectName = dic.Key;
                    string url = _schemaUrl.Replace("{resource}", dic.Key);
                    string json = GetJsonResponseFrom(url);
                    JObject schemaObject = JObject.Parse(json);

                    foreach (JProperty propery in schemaObject.Properties())
                    {
                        if (propery.Name == "links")
                        {
                            selfUrlList.Add(objectName, schemaObject["links"]["self"].Value<string>());
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Error("Error in loading data dictionary : " + ex);
                throw ex;
            }

            return selfUrlList;

        }

        /// <summary>
        /// It returns url for restfull service of specified object
        /// </summary>
        /// <param name="objectName">object name/table name</param>
        /// <returns></returns>
        private string GetObjectUrl(string objectName)
        {
            var url = (from dicEntry in _objectyDictionary
                       where dicEntry.Key.ToUpper() == objectName.ToUpper()
                       select dicEntry.Value).SingleOrDefault<string>();

            return url;
        }

        /// <summary>
        /// It will make a request on URL and retuen json string.
        /// </summary>
        private string GetJsonResponseFrom(string url)
        {
            try
            {
                return _webClient.MakeGetRequest(url);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private DataTable GetDataTableFrom(string jsonString, string objectType, string collectionName = "Items")
        {
           // if (jsonString.IndexOf("{\"status_text\":\"Record Not Found.\",\"status_code\":\"202\"}") >= 0)
            if (jsonString.IndexOf("{\"total\":0,\"limit\":0,\"Items\":[]}") >= 0)
            
            {
                return GetDataTableSchema(objectType);
            }
            else
            {
                JObject o = JObject.Parse(jsonString);
                JArray items = (JArray)o[collectionName];
                DataTable dt = items.ToObject<DataTable>();
                dt.TableName = objectType;
                return dt;
            }
        }

        private DataTable GetDataTableSchema(string objectType)
        {
            DataObject objDef = _dataDictionary.dataObjects.Find(p => p.objectName.ToUpper() == objectType.ToUpper());
            DataTable dataTable = new DataTable();
            dataTable.TableName = objectType;
            foreach (DataProperty property in objDef.dataProperties)
            {
                DataColumn dataColumn = new DataColumn();
                dataColumn.ColumnName = property.columnName;
                dataColumn.DataType = Type.GetType("System." + property.dataType.ToString());
                dataTable.Columns.Add(dataColumn);
            }


            return dataTable;
        }

        private string GenerateUrl(string objectType, DataFilter filter, int limit, int start)
        {
            string url = GetObjectUrl(objectType);
            if ((limit == 0) || (filter != null && filter.OrderExpressions != null && filter.OrderExpressions.Count > 0))
            {
                url = url + @"?start=" + Convert.ToString(0) + @"&limit=" + Convert.ToString(10000000);
            }
            else
            {
                url = url + @"?start=" + Convert.ToString(start) + @"&limit=" + Convert.ToString(limit);
            }

            return url;
        }

        private string GenerateUrl(string objectType, string identifier)
        {
            //string url = GetObjectUrl(objectType);

            string url = (from dicEntry in _selfUrl
                       where dicEntry.Key.ToUpper() == objectType.ToUpper()
                       select dicEntry.Value).SingleOrDefault<string>();

            DataObject objDef = _dataDictionary.dataObjects.Find(p => p.objectName.ToUpper() == objectType.ToUpper());
            
            string[] identifierArray = identifier.Split(_keyDelimiter.ToCharArray());

            if (identifierArray.Count() != objDef.keyProperties.Count)
                throw new Exception("key fields are not matching with their values.");

            for(int i=0;i<objDef.keyProperties.Count;i++)
            {
               url=  url.Replace("{" + objDef.keyProperties[i].keyPropertyName + "}", identifierArray[i]);
            }

            return url;
        }

        private string GenerateUrl(string objectType)
        {
            return GetObjectUrl(objectType);
        }

        private string GenerateReletedUrl(string parentObject, string pId, string relatedObject)
        {
            string url = GetObjectUrl(parentObject);
            url = url + "//" + pId + "//" + relatedObject;
            return url;
        }

        private string GenerateReletedUrl(string parentObject, string pId, string relatedObject, int limit, int start)
        {
            string url = GenerateReletedUrl(parentObject, pId, relatedObject);

            if (limit == 0)
            {
                url = url + @"?start=" + Convert.ToString(0) + @"&limit=" + Convert.ToString(10000000);
            }
            else
            {
                url = url + @"?start=" + Convert.ToString(start) + @"&limit=" + Convert.ToString(limit);
            }

            return url;
        }

        private long GetObjectCount(string objectType, DataFilter filter)
        {
            string url = GetObjectUrl(objectType);
            url = url + @"?start=0&limit=1";
            string jsonString = GetJsonResponseFrom(url);

            if (jsonString.IndexOf("{\"status_text\":\"Record Not Found.\",\"status_code\":\"202\"}") >= 0)
            {
                return 0;
            }
            else
            {
                JObject o = JObject.Parse(jsonString);
                long count = Convert.ToInt64(o["total"].ToString());

                return count;
            }



        }

        private DataType ResolveDataType(string type)
        {
            switch (type)
            {
                case "number":
                    return DataType.Int32;
                case "date":
                    return DataType.DateTime;
                case "string":
                default:
                    return DataType.String;
            }

        }

        private IList<IDataObject> ToDataObjects(DataTable dataTable, string objectType)
        {
            return ToDataObjects(dataTable, objectType, false);
        }

        private IList<IDataObject> ToDataObjects(DataTable dataTable, string objectType, bool createsIfEmpty)
        {
            IList<IDataObject> dataObjects = new List<IDataObject>();
            // DataObject objectDefinition = GetObjectDefinition(objectType);
            DataObject objectDefinition = _dataDictionary.dataObjects.Find(p => p.objectName.ToUpper() == objectType.ToUpper());
            IDataObject dataObject = null;

            if (objectDefinition != null && dataTable.Rows != null)
            {
                if (dataTable.Rows.Count > 0)
                {
                    foreach (DataRow dataRow in dataTable.Rows)
                    {
                        try
                        {
                            dataObject = ToDataObject(dataRow, objectDefinition);
                        }
                        catch (Exception ex)
                        {
                            _logger.Error("Error converting data row to data object: " + ex);
                            throw ex;
                        }

                        if (dataObjects != null)
                        {
                            dataObjects.Add(dataObject);
                        }
                    }
                }
                else if (createsIfEmpty)
                {
                    dataObject = ToDataObject(null, objectDefinition);
                    dataObjects.Add(dataObject);
                }
            }

            return dataObjects;
        }

        private IDataObject ToDataObject(DataRow dataRow, DataObject objectDefinition)
        {
            IDataObject dataObject = null;

            if (dataRow != null)
            {
                try
                {
                    dataObject = new GenericDataObject() { ObjectType = objectDefinition.objectName };
                }
                catch (Exception ex)
                {
                    _logger.Error("Error instantiating data object: " + ex);
                    throw ex;
                }

                if (dataObject != null && objectDefinition.dataProperties != null)
                {
                    foreach (DataProperty objectProperty in objectDefinition.dataProperties)
                    {
                        try
                        {
                            if (dataRow.Table.Columns.Contains(objectProperty.columnName))
                            {
                                object value = dataRow[objectProperty.columnName];

                                if (value.GetType() == typeof(System.DBNull))
                                {
                                    value = null;
                                }

                                dataObject.SetPropertyValue(objectProperty.propertyName, value);
                            }
                            else
                            {
                                _logger.Warn(String.Format("Value for column [{0}] not found in data row of table [{1}]",
                                  objectProperty.columnName, objectDefinition.tableName));
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.Error("Error getting data row value: " + ex);
                            throw ex;
                        }
                    }
                }
            }
            else
            {
                dataObject = new GenericDataObject() { ObjectType = objectDefinition.objectName };

                foreach (DataProperty objectProperty in objectDefinition.dataProperties)
                {
                    dataObject.SetPropertyValue(objectProperty.propertyName, null);
                }
            }

            return dataObject;
        }

        #endregion
    }

  
}
