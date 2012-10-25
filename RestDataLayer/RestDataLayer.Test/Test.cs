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
        private Scenarios _scenarios = null;
        private string _objectType;
        private string _modifiedProperty;
        private string _modifiedValue;
        private DataObject _objectDefinition;

       public Tests()
       {
           string baseDir = Directory.GetCurrentDirectory();
           Directory.SetCurrentDirectory(baseDir.Substring(0, baseDir.LastIndexOf("\\bin")));

           AdapterSettings adapterSettings = new AdapterSettings();
           adapterSettings.AppendSettings(new AppSettingsReader("App.config"));

           FileInfo log4netConfig = new FileInfo("Log4net.config");
           log4net.Config.XmlConfigurator.Configure(log4netConfig);

           
           _dataLayer = new Bechtel.DataLayer.RestDataLayer(adapterSettings);

           //_scenarios = Utility.Read<Scenarios>("Scenarios.xml");
           //_objectType = adapterSettings["ObjectType"];
           //_modifiedProperty = adapterSettings["ModifiedProperty"];
           //_modifiedValue = adapterSettings["ModifiedValue"];
           //_objectDefinition = GetObjectDefinition(_objectType);
       }


       [Test]
       public void Test_Dictionary_Creation()
       {
           Response response = null;
           int MAX_ITEMS = 25;

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

         IList<IDataObject> dataObject = _dataLayer.Get("Function", identifiers);
         
           Assert.AreEqual(dataObject.Count, 1);


       }



       //[Test]
       //public void TestGetWithIdentifiers()
       //{
       //    IList<string> identifiers = _dataLayer.GetIdentifiers(_objectType, new DataFilter());
       //    IList<string> identifier = ((List<string>)identifiers).GetRange(1, 1);
       //    IList<IDataObject> dataObjects = _dataLayer.Get(_objectType, identifier);
       //    Assert.Greater(dataObjects.Count, 0);
       //}

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
