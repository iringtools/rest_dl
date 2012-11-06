using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

using System.Xml.Linq;
using System.IO;
using System.Data;
using org.iringtools.library;
using log4net;
using org.iringtools.adapter;
using org.iringtools.utility;
using StaticDust.Configuration;




namespace RestDataLayer.Test
{
   [TestFixture]
   public class Tests
    {

        private static readonly ILog _logger = LogManager.GetLogger(typeof(Tests));
        private IDataLayer2 _dataLayer;
        private string _objectType;
        private string _modifiedProperty;
        private string _modifiedValue;
        private DataObject _objectDefinition;
        private DataFilter _filter;
        

       public Tests()
       {
           _objectType = "Function";

           string baseDir = Directory.GetCurrentDirectory();
           Directory.SetCurrentDirectory(baseDir.Substring(0, baseDir.LastIndexOf("\\bin")));

           AdapterSettings adapterSettings = new AdapterSettings();
           adapterSettings.AppendSettings(new AppSettingsReader("App.config"));

           FileInfo log4netConfig = new FileInfo("Log4net.config");
           log4net.Config.XmlConfigurator.Configure(log4netConfig);

           string twConfigFile = String.Format("{0}{1}.{2}.config",
             adapterSettings["AppDataPath"],
             adapterSettings["ProjectName"],
             adapterSettings["ApplicationName"]
           );

           AppSettingsReader twSettings = new AppSettingsReader(twConfigFile);
           adapterSettings.AppendSettings(twSettings);

           _dataLayer = new Bechtel.DataLayer.RestDataLayer(adapterSettings);

           _filter = Utility.Read<DataFilter>(adapterSettings["FilterPath"]);
           

           //_scenarios = Utility.Read<Scenarios>("Scenarios.xml");
           _objectType = adapterSettings["ObjectType"];
           //_modifiedProperty = adapterSettings["ModifiedProperty"];
           //_modifiedValue = adapterSettings["ModifiedValue"];
           //_objectDefinition = GetObjectDefinition(_objectType);
       }


       [Test]
       public void Test_Dictionary_Creation()
       {
         
           #region Test dictionary
           Console.WriteLine("\nTesting get dictionary ...");
           _logger.Info("\nTesting get dictionary ...");

           DataDictionary dictionary = _dataLayer.GetDictionary();
           Assert.Greater(dictionary.dataObjects.Count, 0);
           #endregion

          
       }

       [Test]
       public void Test_GetDataTable()
       {
         DataDictionary dictionary = _dataLayer.GetDictionary();
         IList<string> identifiers = new List<string>();
         identifiers.Add("1");

         IList<IDataObject> dataObject = _dataLayer.Get(_objectType, identifiers);
         
           Assert.AreEqual(dataObject.Count, 1);


       }

       [Test]
       public void Test_GetCount()
       {
           DataDictionary dictionary = _dataLayer.GetDictionary();

           long count = _dataLayer.GetCount(_objectType, null);

           Assert.Greater(count, 1);


       }

       [Test]
       public void Test_Get_With_Filter()
       {
         DataDictionary  dictionary = _dataLayer.GetDictionary();
         IList<IDataObject> dataObject = _dataLayer.Get(_objectType, _filter, 10, 0);
         Assert.AreEqual(dataObject.Count, 1);
           
       }

       [Test]
       public void TestCreate()
       {
           IList<IDataObject> dataObjects = _dataLayer.Create(_objectType, null);
           Assert.AreNotEqual(dataObjects, null);
       }
       //[Test]
       //public void Test_Get_Data_With_Paging()
       //{
       //  DataDictionary  dictionary = _dataLayer.GetDictionary();
       // IList<string> identifiers = new List<string>();
       //  identifiers.Add("1");

       //  IList<IDataObject> dataObject = _dataLayer.Get("Function", null, 10, 2);
       //  Assert.AreEqual(dataObject.Count, 10);

 
       //}

       //[Test]
       //public void Test_Get_Identifiers()
       //{

       //    IList<string> identifiers = _dataLayer.GetIdentifiers("Function", null);
       //    Assert.Greater(identifiers.Count, 0);

       //}




       [Test]
       public void TestGetWithIdentifiers()
       {
           IList<string> identifiers = _dataLayer.GetIdentifiers("Function", new DataFilter());
           IList<string> identifier = ((List<string>)identifiers).GetRange(1, 1);
           IList<IDataObject> dataObjects = _dataLayer.Get("Function", identifier);
           Assert.Greater(dataObjects.Count, 0);
       }

       //private string GetIdentifier(IDataObject dataObject)
       //{
       //    string[] identifierParts = new string[_objectDefinition.keyProperties.Count];

       //    int i = 0;
       //    foreach (KeyProperty keyProperty in _objectDefinition.keyProperties)
       //    {
       //        identifierParts[i] = dataObject.GetPropertyValue(keyProperty.keyPropertyName).ToString();
       //        i++;
       //    }

       //    return String.Join(_objectDefinition.keyDelimeter, identifierParts);
       //}

       //private void SetIdentifier(IDataObject dataObject, string identifier)
       //{
       //    IList<string> keyProperties = GetKeyProperties();

       //    if (keyProperties.Count == 1)
       //    {
       //        dataObject.SetPropertyValue(keyProperties[0], identifier);
       //    }
       //    else if (keyProperties.Count > 1)
       //    {
       //        StringBuilder identifierBuilder = new StringBuilder();

       //        foreach (string keyProperty in keyProperties)
       //        {
       //            dataObject.SetPropertyValue(keyProperty, identifier);

       //            if (identifierBuilder.Length > 0)
       //            {
       //                identifierBuilder.Append(_objectDefinition.keyDelimeter);
       //            }

       //            identifierBuilder.Append(identifier);
       //        }

       //        identifier = identifierBuilder.ToString();
       //    }
       //}

       //private IList<string> GetKeyProperties()
       //{
       //    IList<string> keyProperties = new List<string>();

       //    foreach (DataProperty dataProp in _objectDefinition.dataProperties)
       //    {
       //        foreach (KeyProperty keyProp in _objectDefinition.keyProperties)
       //        {
       //            if (dataProp.propertyName == keyProp.keyPropertyName)
       //            {
       //                keyProperties.Add(dataProp.propertyName);
       //            }
       //        }
       //    }
       //    return keyProperties;
       //}

       //private DataObject GetObjectDefinition(string objectType)
       //{
       //    DataDictionary dictionary = _dataLayer.GetDictionary();

       //    if (dictionary.dataObjects != null)
       //    {
       //        foreach (DataObject dataObject in dictionary.dataObjects)
       //        {
       //            if (dataObject.objectName.ToLower() == objectType.ToLower())
       //            {
       //                return dataObject;
       //            }
       //        }
       //    }
       //    return null;
       //}
   }
}
