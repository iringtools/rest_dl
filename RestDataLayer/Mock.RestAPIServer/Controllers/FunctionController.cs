using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Mock.RestAPIServer.Models;

namespace Mock.RestAPIServer.Controllers
{
    public class FunctionController : ApiController
    {
        // GET api/function
        public GenericObject<Function> Get()
        {
            GenericObject<Function> obj = new GenericObject<Function>() { total=4,limit=4,Items = Utility.GetFunctions()};
            return obj;
        }

       

        // POST api/function
        public void Post([FromBody]string value)
        {
        }

        // PUT api/function/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/function/5
        public void Delete(int id)
        {
        }
    }
}
