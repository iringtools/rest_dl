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
        private void FillDataPropertiesFrom(string jsonString, List<DataProp> dataPrpCollection,string objectName)
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
                dp.propertyName= jp.Name;
                dp.keyType = "unassigned";
                dp.isNullable = "false";

                if (dp.columnName.ToUpper() == "ID" && isKeyAssigned == false)
                {
                    isKeyAssigned = true;
                    dp.isKey = true;

                }

                switch (jp.Value.Type)
                {
                    case JTokenType.Integer:
                        dp.dataType = DataType.Int32;
                        dp.dataLength = "22";
                        break;
                    case JTokenType.Date:
                        dp.dataType = DataType.DateTime;
                        dp.dataLength = "0";
                        break;
                    case JTokenType.String:
                        dp.dataType = DataType.String;
                        dp.dataLength = "1000";
                        break;
                    case JTokenType.Float:
                        dp.dataType = DataType.Double;
                        dp.dataLength = "50";
                        break;
                    case JTokenType.Boolean:
                        dp.dataType = DataType.Boolean;
                        dp.dataLength = "1";
                        break;
                    case JTokenType.Bytes:
                        dp.dataType = DataType.Byte;
                        dp.dataLength = "8";
                        break;
                    case JTokenType.Uri:
                        dp.dataType = DataType.Reference;
                        dp.dataLength = "1000";
                        break;
                    default:
                        dp.dataType = DataType.String;
                        dp.dataLength = "1000";
                        break;

                }

                dataPrpCollectionTemp.Add(dp);
                
            }


            if (isKeyAssigned == false)
            {
                foreach (var dp in dataPrpCollectionTemp)
                {
                    if (dp.columnName.ToUpper().EndsWith( "_ID"))
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
                        _dataObject.keyDelimeter =Constants.DELIMITER_CHAR;
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
                //  _logger.Error("Error in loading data dictionary : " + ex);
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
        /// it returns Parse jsonString into DataTable object
        /// </summary>
        /// <param name="jsonString">Json string</param>
        /// <returns></returns>
        private DataTable GetDataTableFrom(string jsonString)
        {
            DataTable dt = new DataTable();

            JObject o = JObject.Parse(jsonString);

            JArray items = (JArray)o["Items"];
  
            //Create Columns for dataTable
            JObject item = (JObject)items[0];
            foreach (var jp in item.Properties())
            {
                dt.Columns.Add(new DataColumn(jp.Name));
            }

            foreach(JObject jo in (JArray)o["Items"])
            {
                DataRow dr = dt.NewRow();
                foreach (var jp in jo.Properties())
                {
                    dr[jp.Name] = jp.Value; 
                }

                dt.Rows.Add(dr);
            }

            return dt;
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
         
        #endregion

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
                DataTable datatable = GetDataTable(objectType, identifiers);
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
            _dataFilter = filter;
            try
            {
                string tableName = GetTableName(objectType);
                string whereClause = string.Empty;

                if (filter != null)
                    whereClause = filter.ToSqlWhereClause(_dbDictionary, tableName, _whereClauseAlias);

                DataTable dataTable = GetDataTable(tableName, whereClause, start, limit);
                IList<IDataObject> dataObjects = ToDataObjects(dataTable, objectType);
                return dataObjects;
            }
            catch (Exception ex)
            {
                _logger.Error("Error get data table: " + ex);
                throw ex;
            }
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

        public override long GetCount(string tableName, string whereClause)
        {
            throw new NotImplementedException();
        }

        public override System.Data.DataTable GetDataTable(string tableName, string whereClause, long start, long limit)
        {
            try
            {
                string url = GetObjectUrl(tableName);

                url = url + @"?start="+ Convert.ToString(start) +@"&limit=" + Convert.ToString(limit);

                string jsonString = GetJsonResponseFrom(url);
                DataTable datatable = GetDataTableFrom(jsonString);
                return datatable;
            }
            catch (Exception ex)
            {
                string error = String.Format("Error getting DataTable from table {0}: {1}", tableName, ex);
                _logger.Error(error);
                throw new Exception(error);
            }         
        }

        public override System.Data.DataTable GetDataTable(string tableName, IList<string> identifiers)
        {
            try
            {
                string url = GetObjectUrl(tableName);

                if (identifiers != null)
                {
                    foreach (string id in identifiers)
                    {
                        //url = url +@"/Search?q=" + id;
                        url = url + @"/" + id;
                        break;
                    }
                }

                /*
                DatabaseDictionary _dbDictionary = GetDatabaseDictionary();
                DataObject objDef = _dbDictionary.dataObjects.Find(p => p.tableName == tableName);
                IList<string> keyCols = GetKeyColumns(objDef);
                */

                string jsonString = GetJsonResponseFrom(url);
                DataTable datatable = GetDataTableFrom(jsonString);
                return datatable;
            }
            catch(Exception ex)
            {
                string error = String.Format("Error data rows from table {0} with identifiers {1}: {2}", tableName, identifiers.ToString(), ex);
                _logger.Error(error);
                throw new Exception(error);
            }            

        }

        public override DatabaseDictionary GetDatabaseDictionary()
        {
            _dictionary = Utility.Read<DatabaseDictionary>(String.Format("{0}{1}DataBaseDictionary.{2}.{3}.xml", _baseDirectory, _xmlPath, _projectName, _applicationName));
            return _dictionary;
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

        public override Response PostDataTables(IList<System.Data.DataTable> dataTables)
        {
            throw new NotImplementedException();
        }

        public override Response RefreshDataTable(string tableName)
        {
           // throw new NotImplementedException();
            return new Response();
        }
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
