using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bechtel.DataLayer
{
    public interface IWebClient
    {
        string MakeGetRequest(string url);
        void MakePutRequest(string url, string objectString);
        void MakePostRequest(string url, string objectString);
<<<<<<< HEAD
//void MakePostRequest(string url, string objectString);

=======
        void MakeDeleteRequest(string url);
>>>>>>> Add, update and delete operations added
    }

    internal class IringWebClient : IWebClient
    {
        org.iringtools.utility.WebHttpClient client = null ;

        public IringWebClient()
        {
            client = new org.iringtools.utility.WebHttpClient("");
        }

        public IringWebClient(string baseUrl)
        {
            client = new org.iringtools.utility.WebHttpClient(baseUrl);
        }

        public IringWebClient(string baseUrl, string appKey, string accessToken)
            : this(baseUrl)
        {
            client.AppKey = appKey;
            client.AccessToken = accessToken;
            
            //client.ContentType = @"application/json";
            
        }

        public string MakeGetRequest(string url)
        {
            string response = client.GetMessage(url);
            return response;
        }

        public void MakePutRequest(string url, string jsonString)
        {

          
        }

        public void MakePostRequest(string url, string jsonString)
        {
          
        }


        public void MakeDeleteRequest(string url)
        {
           
        }
    }
}
