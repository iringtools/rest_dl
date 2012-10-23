using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Oracle.DataAccess.Client;
using System.Xml.Linq;
using System.IO;
using System.Data;
using org.iringtools.library;
using log4net;
using org.iringtools.adapter;
using org.iringtools.utility;
using StaticDust.Configuration;


namespace BCSDataLayer.Test
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

           _dataLayer = new Bechtel.DataLayer.BCSDataLayer(adapterSettings);
           _scenarios = Utility.Read<Scenarios>("Scenarios.xml");
           _objectType = adapterSettings["ObjectType"];
           _modifiedProperty = adapterSettings["ModifiedProperty"];
           _modifiedValue = adapterSettings["ModifiedValue"];
           _objectDefinition = GetObjectDefinition(_objectType);
       }


       [Test]
       public void RunTest()
       {
           Response response = null;
           int MAX_ITEMS = 25;

           #region Test dictionary
           Console.WriteLine("\nTesting get dictionary ...");
           _logger.Info("\nTesting get dictionary ...");

           DataDictionary dictionary = _dataLayer.GetDictionary();
           Assert.Greater(dictionary.dataObjects.Count, 0);
           #endregion

           #region Test refresh dictionary
           Console.WriteLine("Testing refresh dictionary ...");
           _logger.Info("Testing refresh dictionary ...");

           response = _dataLayer.RefreshAll();
           Assert.AreEqual(response.Level, StatusLevel.Success);
           #endregion

           foreach (Scenario scenario in _scenarios)
           {
               Console.WriteLine(string.Format("\nExecuting scenario [{0}] ...", scenario.Name));
               _logger.Info(string.Format("\nExecuting scenario [{0}] ...", scenario.Name));

               string objectType = scenario.ObjectType;
               string padding = scenario.IdentifierPadding;
               Properties properties = scenario.Properties;
               DataFilter dataFilter = (scenario.DataFilter != null)
                 ? Utility.DeserializeDataContract<DataFilter>(scenario.DataFilter)
                 : new DataFilter();

               #region Test get count
               Console.WriteLine("Testing get count ...");
               _logger.Info("Testing get count ...");

               long count = _dataLayer.GetCount(objectType, dataFilter);
               Assert.Greater(count, 0);
               #endregion

               if (count > MAX_ITEMS) count = MAX_ITEMS;

               #region Test get page
               Console.WriteLine("Testing get page ...");
               _logger.Info("Testing get page ...");

               IList<IDataObject> dataObjects = _dataLayer.Get(objectType, dataFilter, (int)count, 0);
               Assert.Greater(dataObjects.Count, 0);
               #endregion

           }
       }

       [Test]
       public void TestPostWithUpdate()
       {
           IList<IDataObject> dataObjects = _dataLayer.Get(_objectType, new DataFilter(), 1, 0);
           string orgIdentifier = GetIdentifier(dataObjects[0]); 
           string orgPropValue = Convert.ToString(dataObjects[0].GetPropertyValue(_modifiedProperty)) ?? String.Empty;
           string newPropValue = _modifiedValue;

           // post data object with modified property
           dataObjects[0].SetPropertyValue(_modifiedProperty, Convert.ToString( newPropValue));
           Response response = _dataLayer.Post(dataObjects);
           Assert.AreEqual(response.Level, StatusLevel.Success);

           // verify post result
           dataObjects = _dataLayer.Get(_objectType, new List<string> { orgIdentifier });
           Assert.AreEqual(dataObjects[0].GetPropertyValue(_modifiedProperty), newPropValue);

           // reset property to its orginal value
           dataObjects[0].SetPropertyValue(_modifiedProperty, orgPropValue);
           response = _dataLayer.Post(dataObjects);
           Assert.AreEqual(response.Level, StatusLevel.Success);
       }

       [Test]
       public void TestPostWithAddAndDeleteByIdentifier()
       {
           //
           // create a new data object by getting an existing one and change its identifier
           //
           IList<IDataObject> dataObjects = _dataLayer.Get(_objectType, new DataFilter(), 1, 1);
           string orgIdentifier = GetIdentifier(dataObjects[0]);

           string newIdentifier = _modifiedValue;
           SetIdentifier(dataObjects[0], newIdentifier);

           // post the new data object
           Response response = _dataLayer.Post(dataObjects);
           Assert.AreEqual(response.Level, StatusLevel.Success);

           //
           // delete the new data object by its identifier
           //
           response = _dataLayer.Delete(_objectType, new List<string> { newIdentifier });
           Assert.AreEqual(response.Level, StatusLevel.Success);
       }

       [Test]
       public void TestPostWithAddAndDeleteByFilter()
       {
           //
           // create new data object by getting an existing one and change its identifier
           //
           IList<IDataObject> dataObjects = _dataLayer.Get(_objectType, new DataFilter(), 1, 1);
           string orgIdentifier = GetIdentifier(dataObjects[0]);
           string newIdentifier = _modifiedValue;
           SetIdentifier(dataObjects[0], newIdentifier);

           // post new data object
           Response response = _dataLayer.Post(dataObjects);
           Assert.AreEqual(response.Level, StatusLevel.Success);
           //
           // delete the new data object with a filter
           //
           DataFilter filter = new DataFilter();

           filter.Expressions.Add(
             new Expression()
             {
                 PropertyName = "Tag",
                 RelationalOperator = org.iringtools.library.RelationalOperator.EqualTo,
                 Values = new Values() { newIdentifier }
             }
           );

           response = _dataLayer.Delete(_objectType, filter);
           Assert.AreEqual(response.Level, StatusLevel.Success);
       }

       private string GetIdentifier(IDataObject dataObject)
       {
           string[] identifierParts = new string[_objectDefinition.keyProperties.Count];

           int i = 0;
           foreach (KeyProperty keyProperty in _objectDefinition.keyProperties)
           {
               identifierParts[i] = dataObject.GetPropertyValue(keyProperty.keyPropertyName).ToString();
               i++;
           }

           return String.Join(_objectDefinition.keyDelimeter, identifierParts);
       }

       private void SetIdentifier(IDataObject dataObject, string identifier)
       {
           IList<string> keyProperties = GetKeyProperties();

           if (keyProperties.Count == 1)
           {
               dataObject.SetPropertyValue(keyProperties[0], identifier);
           }
           else if (keyProperties.Count > 1)
           {
               StringBuilder identifierBuilder = new StringBuilder();

               foreach (string keyProperty in keyProperties)
               {
                   dataObject.SetPropertyValue(keyProperty, identifier);

                   if (identifierBuilder.Length > 0)
                   {
                       identifierBuilder.Append(_objectDefinition.keyDelimeter);
                   }

                   identifierBuilder.Append(identifier);
               }

               identifier = identifierBuilder.ToString();
           }
       }

       private IList<string> GetKeyProperties()
       {
           IList<string> keyProperties = new List<string>();

           foreach (DataProperty dataProp in _objectDefinition.dataProperties)
           {
               foreach (KeyProperty keyProp in _objectDefinition.keyProperties)
               {
                   if (dataProp.propertyName == keyProp.keyPropertyName)
                   {
                       keyProperties.Add(dataProp.propertyName);
                   }
               }
           }
           return keyProperties;
       }

       private DataObject GetObjectDefinition(string objectType)
       {
           DataDictionary dictionary = _dataLayer.GetDictionary();

           if (dictionary.dataObjects != null)
           {
               foreach (DataObject dataObject in dictionary.dataObjects)
               {
                   if (dataObject.objectName.ToLower() == objectType.ToLower())
                   {
                       return dataObject;
                   }
               }
           }
           return null;
       }
   }
}
