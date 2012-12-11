using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Mock.RestAPIServer.Models;

namespace Mock.RestAPIServer.Controllers
{
    public class ProjectController : ApiController
    {
        // GET api/project
        public GenericObject<Project> Get()
        {
            GenericObject<Project> obj = new GenericObject<Project>() { total = 4, limit = 4, Items = Utility.GetProjects() };
            return obj;

        }

        // GET api/project/5
        public string Get(int id)
        {
            return "value";
        }

        // POST api/project
        public void Post([FromBody]string value)
        {
        }

        // PUT api/project/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/project/5
        public void Delete(int id)
        {
        }
    }
}
