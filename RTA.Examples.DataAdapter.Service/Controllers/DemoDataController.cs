// <copyright file="DemoDataController.cs" company="McLaren Applied Ltd.">
// Copyright (c) McLaren Applied Ltd.</copyright>

using MAT.OCS.RTA.Services;
using MAT.OCS.RTA.Services.AspNetCore.Controllers.Services;
using Microsoft.AspNetCore.Mvc;

namespace RTA.Examples.DataAdapter.Service.Controllers
{
    [Route("rta/v2")]
    [ApiController]
    public class DemoDataController : BaseDataServiceControllerV2
    {
        public DemoDataController(IEventStore eventStore, ISampleDataStore sampleDataStore) :
            base(eventStore, sampleDataStore)
        {
        }
    }
}