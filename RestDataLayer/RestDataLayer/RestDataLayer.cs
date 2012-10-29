using System;
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

namespace Bechtel.DataLayer
{
    public class RestDataLayer : BaseSQLDataLayer
    {
        private DataDictionary _dataDictionary = null;
        private string _applicationName = string.Empty;
        private string _projectName = string.Empty;
        private string _xmlPath = string.Empty;
        private string _baseDirectory = string.Empty;
        private DatabaseDictionary _dictionary = null;
        private ILog _logger = LogManager.GetLogger(typeof(RestDataLayer));

        private Dictionary<string, string> _configDictionary = null;


        public RestDataLayer(AdapterSettings settings)
            : base(settings)
        {
            _settings = settings;
            _xmlPath = _settings["xmlPath"];
            _projectName = _settings["projectName"];
            _applicationName = _settings["applicationName"];
            _baseDirectory = _settings["BaseDirectoryPath"];
            _configDictionary = LoadConfigrationDetailsInDictionary();
        }

        

        /// <summary>
        /// It will give you the DataDictionary if present in the App_Data Folder else It will create it.
        /// </summary>
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

                    _dataDictionary = LoadDataObjects();

                    DatabaseDictionary _databaseDictionary = new DatabaseDictionary();
                    _databaseDictionary.dataObjects = _dataDictionary.dataObjects;
                    _databaseDictionary.ConnectionString = EncryptionUtility.Encrypt(Connectionstring);
                    _databaseDictionary.Provider = "Oracle11g";
                    _databaseDictionary.SchemaName = "dbo";

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

        /// <summary>
        /// It will convert the datatable into list of IDataObject which is the desired form for Iring.
        /// </summary>
        /// <param name="objectType">name of the object</param>
        /// <param name="identifiers">list of identifiers based on this datatable will be produced</param>
        public override IList<IDataObject> Get(string objectType, IList<string> identifiers)
        {
            try
            {
                string url = GetObjectUrl(objectType);

                if (identifiers != null)
                {
                    foreach (string id in identifiers)
                    {
                        url = url + @"/" + id;
                        break;
                    }
                }

                string jsonString = GetJsonResponseFrom(url);

                DataTable datatable = GetDataTableFrom(jsonString);

                IList<IDataObject> dataObjects = ToDataObjects(datatable, objectType);

                return dataObjects;
            }
            catch (Exception ex)
            {
                _logger.Error("Error in GetList: " + ex);
                throw new Exception("Error while getting a list of data objects of type [" + objectType + "].", ex);
            }
        }

        /// <summary>
        /// Returns the list of IDataObject which is expected for Iring.
        /// </summary>
        /// <param name="objectType">name of the object</param>
        /// <param name="filter">filter to get the desired rows</param>
        /// <param name="limit">no. of rows to be choosen</param>
        /// <param name="start">starting point of the rows from the table</param>
        public override IList<IDataObject> Get(string objectType, DataFilter filter, int limit, int start)
        {
            long lStart = start;
            long lLimit = limit;

            _dataFilter = filter;
            try
            {
                string url = GetObjectUrl(objectType);

                if (lLimit == 0)
                { lLimit = 10000000; }

                url = url + @"?start=" + Convert.ToString(lStart) + @"&limit=" + Convert.ToString(lLimit);

                string jsonString = GetJsonResponseFrom(url);
                DataTable dataTable = GetDataTableFrom(jsonString);
                
                IList<IDataObject> dataObjects = ToDataObjects(dataTable, objectType);
                return dataObjects;
            }
            catch (Exception ex)
            {
                _logger.Error("Error get data table: " + ex);
                throw ex;
            }
        }

        public override DatabaseDictionary GetDatabaseDictionary()
        {
            _dictionary = Utility.Read<DatabaseDictionary>(String.Format("{0}{1}DataBaseDictionary.{2}.{3}.xml", _baseDirectory, _xmlPath, _projectName, _applicationName));
            return _dictionary;
        }

        public override IList<string> GetIdentifiers(string objectType, DataFilter filter)
        {
            List<string> identifiers=null;
            try
            {
                identifiers  = new List<string>();

                DataObject objDef = _dbDictionary.dataObjects.Find(p => p.objectName.ToUpper() == objectType.ToUpper());
                IList<string> keyCols = GetKeyColumns(objDef);

                //NOTE: pageSize of 0 indicates that all rows should be returned.
                IList<IDataObject> dataObjects = Get(objectType, filter, 0, 0);
                foreach (IDataObject dataObject in dataObjects)
                {
                    identifiers.Add((string)dataObject.GetPropertyValue(keyCols[0]));
                }
            }
            catch (Exception ex)
            {
                _logger.ErrorFormat("Error while getting a filtered list of identifiers of type [{0}]: {1}", objectType, ex);
                throw new Exception("Error while getting a filtered list of identifiers of type [" + objectType + "].", ex);
            }

            return identifiers;
        }


        #region Not implemented Methods

        public override System.Data.DataTable GetDataTable(string tableName, string whereClause, long start, long limit)
        {
            throw new NotImplementedException();
        }

        public override System.Data.DataTable GetDataTable(string tableName, IList<string> identifiers)
        {
            throw new NotImplementedException();

        }

        public override long GetCount(string tableName, string whereClause)
        {
            throw new NotImplementedException();
        }
        
        public override IList<string> GetIdentifiers(string tableName, string whereClause)
        {
            throw new NotImplementedException();
        }

        public override long GetRelatedCount(System.Data.DataRow dataRow, string relatedTableName)
        {
            throw new NotImplementedException();
        }

        public override System.Data.DataTable GetRelatedDataTable(System.Data.DataRow dataRow, string relatedTableName, long start, long limit)
        {
            throw new NotImplementedException();
        }

        public override System.Data.DataTable GetRelatedDataTable(System.Data.DataRow dataRow, string relatedTableName)
        {
            throw new NotImplementedException();
        }


        public override System.Data.DataTable CreateDataTable(string tableName, IList<string> identifiers)
        {
            throw new NotImplementedException();
        }

        public override Response DeleteDataTable(string tableName, IList<string> identifiers)
        {
            throw new NotImplementedException();
        }

        public override Response DeleteDataTable(string tableName, string whereClause)
        {
            throw new NotImplementedException();
        }
        
        public override Response PostDataTables(IList<System.Data.DataTable> dataTables)
        {
            throw new NotImplementedException();
        }

        #endregion

        public override Response RefreshDataTable(string tableName)
        {
           // throw new NotImplementedException();
            return new Response();
        }

        #region Private function

        /// <summary>
        /// It will Load configration detail in a Dictionary object.
        /// </summary>
        private Dictionary<string, string> LoadConfigrationDetailsInDictionary()
        {
            Dictionary<string, string> dict = null;

            try
            {
                string configPath = String.Format("Configuration.{0}.{1}.xml", _projectName, _applicationName);
                XDocument doc = XDocument.Load(_baseDirectory + _xmlPath + configPath);

                dict = doc.Descendants("add").ToDictionary(x => x.Attribute("key").Value, x => x.Attribute("value").Value);

            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message);
            }
            return dict;

        }

        /// <summary>
        /// It will Parse json string and then fill a list with their properties
        /// </summary>
        private void FillDataPropertiesFrom(string jsonString, List<DataProp> dataPrpCollection, string objectName)
        {

            List<DataProp> dataPrpCollectionTemp = new List<DataProp>();


            JObject o = JObject.Parse(jsonString);
            JArray items = (JArray)o["Items"];
            JObject item = (JObject)items[0];
            bool isKeyAssigned = false;

            foreach (var jp in item.Properties())
            {
                DataProp dp = new DataProp();
                dp.Object_Name = objectName;
                dp.columnName = jp.Name;
                dp.propertyName = jp.Name;
                dp.keyType = "unassigned";
                dp.isNullable = "false";

                if (dp.columnName.ToUpper() == "ID" && isKeyAssigned == false)
                {
                    isKeyAssigned = true;
                    dp.isKey = true;

                }

                dp.dataType = ResolveDataType(jp.Value.Type);
                dp.dataLength = GetDefaultSize(jp.Value.Type).ToString();

                dataPrpCollectionTemp.Add(dp);

            }


            if (isKeyAssigned == false)
            {
                foreach (var dp in dataPrpCollectionTemp)
                {
                    if (dp.columnName.ToUpper().EndsWith("_ID"))
                    {
                        isKeyAssigned = true;
                        dp.isKey = true;

                    }

                }
            }

            if (isKeyAssigned == false)
            {
                isKeyAssigned = true;
                dataPrpCollectionTemp[0].isKey = true;
            }


            foreach (var dp in dataPrpCollectionTemp)
            {
                dataPrpCollection.Add(dp);
            }





        }

        private DataDictionary LoadDataObjects()
        {
            try
            {
                string Object_Name = string.Empty;
                DataObject _dataObject = new DataObject();
                KeyProperty _keyproperties = new KeyProperty();
                DataProperty _dataproperties = new DataProperty();
                DataDictionary _dataDictionary = new DataDictionary();

                List<DataProp> dataPrpCollection = new List<DataProp>();

                var objectList = (from obj in _configDictionary
                                  where obj.Key.StartsWith(Constants.OBJECT_PREFIX) == true
                                  select obj).ToList();

                foreach (var dic in objectList)
                {
                    string objectName = dic.Key.Split('_')[1];
                    string url = dic.Value;
                    string jsonString = GetJsonResponseFrom(url);
                    FillDataPropertiesFrom(jsonString, dataPrpCollection, objectName);
                }

                foreach (DataProp dp in dataPrpCollection)
                {
                    if (Object_Name != dp.Object_Name)
                    {
                        if (!string.IsNullOrEmpty(Object_Name))
                            _dataDictionary.dataObjects.Add(_dataObject);
                        _dataObject = new DataObject();
                        Object_Name = dp.Object_Name;
                        _dataObject.objectName = Object_Name;
                        _dataObject.tableName = Object_Name;
                        _dataObject.keyDelimeter = Constants.DELIMITER_CHAR;
                    }

                    _dataproperties = new DataProperty();
                    _dataproperties.columnName = dp.columnName;

                    if (dp.isKey)
                    {
                        KeyProperty keyProperty = new KeyProperty();
                        keyProperty.keyPropertyName = dp.columnName;
                        _dataObject.keyProperties.Add(keyProperty);

                        _dataproperties.keyType = KeyType.assigned;
                        _dataproperties.isNullable = false;
                    }
                    else
                    {
                        _dataproperties.keyType = KeyType.unassigned;
                        _dataproperties.isNullable = true;
                    }


                    _dataproperties.propertyName = dp.propertyName;
                    _dataproperties.dataLength = Convert.ToInt32(dp.dataLength);

                    _dataproperties.dataType = dp.dataType;



                    _dataObject.dataProperties.Add(_dataproperties);
                }
                _dataDictionary.dataObjects.Add(_dataObject);


                return _dataDictionary;
            }
            catch (Exception ex)
            {
                _logger.Error("Error in loading data dictionary : " + ex);
                throw ex;
            }
            finally
            {
                //Disconnect();
            }
        }

        /// <summary>
        /// It returns url for restfull service of specified object
        /// </summary>
        /// <param name="objectName">object name/table name</param>
        /// <returns></returns>
        private string GetObjectUrl(string objectName)
        {
            var url = (from dicEntry in _configDictionary
                       where dicEntry.Key.ToUpper() == (Constants.OBJECT_PREFIX + objectName).ToUpper()
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
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);

                string authToken = _configDictionary["AuthToken"];
                string appKey = _configDictionary["AppKey"];

                request.Accept = "application/json";
                request.Headers.Add("Authorization", authToken);
                request.Headers.Add("X-myPSN-AppKey", appKey);

                WebRequest.DefaultWebProxy.Credentials = CredentialCache.DefaultCredentials;
                request.Credentials = CredentialCache.DefaultCredentials;

                string responseFromServer = string.Empty;
                using (WebResponse response = request.GetResponse())
                {
                    using (Stream responseStream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(responseStream))
                        {
                            responseFromServer = reader.ReadToEnd();
                        }
                    }

                }

                return responseFromServer;
            }
            catch
            {
                throw new ApplicationException("Error in connectiong rest server");
            }
        }

        private  DataTable GetDataTableFrom(string jsonString)
        {
            JObject o = JObject.Parse(jsonString);
            JArray items = (JArray)o["Items"];
            DataTable dt = items.ToObject<DataTable>();
            return dt;
        }

        private  DataType ResolveDataType(JTokenType type)
        {
            switch (type)
            {
                case JTokenType.Integer:
                    return DataType.Int32;
                case JTokenType.Date:
                    return DataType.DateTime;
                case JTokenType.String:
                    return DataType.String;
                case JTokenType.Float:
                    return DataType.Double;
                case JTokenType.Boolean:
                    return DataType.Boolean;
                case JTokenType.Bytes:
                    return DataType.Byte;
                default:
                    return DataType.String;
            }

        }

        private  int GetDefaultSize(JTokenType type)
        {
            switch (type)
            {
                case JTokenType.Integer:
                    return 16;
                case JTokenType.Date:
                    return 50;
                case JTokenType.String:
                    return 128;
                case JTokenType.Float:
                    return 32;
                case JTokenType.Boolean:
                    return 1;
                default:
                    return 128;
            }

        }

        #endregion
    }

    public class DataProp
    {
        public string Object_Name;
        public string columnName;
        public string propertyName;
        public DataType dataType;
        public string dataLength;
        public string isNullable;
        public string keyType;
        public bool isKey;
    }
}
