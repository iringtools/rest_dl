using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Http;

namespace Bechtel.DataLayer
{
    internal class WebClient
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



    }
}
