using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Http;

namespace Bechtel.DataLayer
{
    internal class WebClient : IWebClient
    {
        private HttpClient client = null;

        public WebClient()
        {
            client = new HttpClient();                
        }
        
        public WebClient(string baseUrl):this()
        {
            client.BaseAddress = new Uri(baseUrl);
            // Add an Accept header for JSON format.
            client.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));
        }

        public WebClient(string baseUrl,string appKey, string accessToken):this(baseUrl)
        {
            client.DefaultRequestHeaders.Add("Authorization", accessToken);
            client.DefaultRequestHeaders.Add("X-myPSN-AppKey", appKey);
        }

        public string MakeGetRequest(string url)
        {
            string response = client.GetStringAsync(url).Result;
            return response;
        }

        public void MakePutRequest(string url,string objectString)
        {
           StringContent sc = new StringContent(objectString);
           var rsponse = client.PutAsync(url, sc).Result.EnsureSuccessStatusCode();
           
        }

        public void MakePostRequest(string url, string objectString)
        {
            StringContent sc = new StringContent(objectString);
            var rsponse = client.PostAsync(url, sc).Result.EnsureSuccessStatusCode();
            
        }
    }
}
