using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Bechtel.DataLayer
{
    public interface IWebClient
    {
        string MakeGetRequest(string url);
        void MakePutRequest(string url, string objectString);
        void MakePostRequest(string url, string objectString);
        void MakeDeleteRequest(string url);

    }

    internal class IringWebClient : IWebClient
    {
        org.iringtools.utility.WebHttpClient _client = null ;

        public IringWebClient()
        {
            _client = new org.iringtools.utility.WebHttpClient("");
        }

        public IringWebClient(string baseUrl)
        {
            _client = new org.iringtools.utility.WebHttpClient(baseUrl);
        }

        public IringWebClient(string baseUrl, string appKey, string accessToken)
            : this(baseUrl)
        {
            _client.AppKey = appKey;
            _client.AccessToken = accessToken;
            
            //client.ContentType = @"application/json";
            
        }

        public string MakeGetRequest(string url)
        {
            string response = _client.GetMessage(url);
            return response;
        }

        public void MakePutRequest(string url, string jsonString)
        {

            byte[] byteArray = Encoding.UTF8.GetBytes(jsonString);
            using (MemoryStream stream = new MemoryStream(byteArray)) 
            {
                _client.PutStream(url, stream);
            }
        }

        public void MakePostRequest(string url, string jsonString)
        {
            byte[] byteArray = Encoding.UTF8.GetBytes(jsonString);
            using (MemoryStream stream = new MemoryStream(byteArray))
            {
                _client.PostStream(url, stream);
            }
        }


        public void MakeDeleteRequest(string url)
        {
            throw new NotImplementedException();
        }
    }
}
