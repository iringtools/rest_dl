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

        private Dictionary<string, string> _configDictionary = null;


        public RestDataLayer(AdapterSettings settings)
            : base(settings)
        {
            _settings = settings;
            _xmlPath = _settings["xmlPath"];
            _projectName = _settings["projectName"];
            _applicationName = _settings["applicationName"];
            _baseDirectory = _settings["BaseDirectoryPath"];
            _configDictionary = LoadConfigrationDetails();
        }

        private Dictionary<string, string> LoadConfigrationDetails()
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
              // _logger.Error(ex.Message);
            }
            return dict;

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
                  

                   // Connectionstring = GetConnectionString();
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
            catch(Exception  ex)
            {
                string error = "Error in getting dictionary";
              //  _logger.Error(error);
                throw new Exception(error);
            }
        }

        private void FillDataProperties(string jsonString, List<DataProp> dataPrpCollection,string objectName)
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
                dp.dataLength = "1000";
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
                        break;
                    case JTokenType.Date:
                        dp.dataType = DataType.DateTime;
                        break;
                    case JTokenType.String:
                        dp.dataType = DataType.String;
                        break;
                    default:
                        dp.dataType = DataType.String;
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
                                  where obj.Key.StartsWith("Object_") == true
                                  select obj).ToList();

                foreach (var dic in objectList)
                {
                    string objectName = dic.Key.Split('_')[1];
                    string url = dic.Value;
                    string jsonString = GetJsonResponse(url);
                    FillDataProperties(jsonString, dataPrpCollection, objectName);
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
                        _dataObject.keyDelimeter = "_";
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

        private string GetXmlResponse(string url)
        {
            // string url = "https://api-staging.mypsn.com/svc2/v1/watchdog/users/pksingh/permissions";


            WebRequest request = WebRequest.Create(url);

           


            //request.Headers.Add("Authorization", "NbFk6TjUNUtejPnCWPwOvFSgh6ng");
            //request.Headers.Add("X-myPSN-AppKey", "3627c519d78e24772ed8375d4e878e2d");

            string authToken = _configDictionary["AuthToken"];
            string appKey = _configDictionary["AppKey"];

            //request.Headers.Add(HttpRequestHeader.Accept, "application/json") ;
            request.Headers[HttpRequestHeader.Accept] = "application/json";

            
            //request.Headers.Add("Accept", "application/json");
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

        private string GetJsonResponse(string url)
        {

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            
            string authToken = _configDictionary["AuthToken"];
            string appKey = _configDictionary["AppKey"];
            
            request.Accept =  "application/json" ;
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
            throw new NotImplementedException();
        }

        public override System.Data.DataTable GetDataTable(string tableName, IList<string> identifiers)
        {
            throw new NotImplementedException();
        }

        public override DatabaseDictionary GetDatabaseDictionary()
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
