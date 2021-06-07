using MAT.OCS.RTA.Services;
using MAT.OCS.RTA.Services.AspNetCore.Controllers;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace RTA.Examples.DataAdapter.Service.Controllers
{
    [Route("rta/v2/data/{identity}")]
    [ApiController]
    [Authorize]
    public class DemoDataController : DataControllerV2
    {
        public DemoDataController(IEventStore eventStore, ISampleDataStore sampleDataStore) :
            base(eventStore, sampleDataStore)
        {
        }
    }
}