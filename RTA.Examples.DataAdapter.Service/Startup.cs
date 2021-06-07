using Grpc.Core;
using Grpc.Net.Client;
using MAT.OCS.RTA.Services;
using MAT.OCS.RTA.Services.AspNetCore;
using MAT.OCS.RTA.Services.AspNetCore.Authorization;
using MAT.OCS.RTA.Toolkit.API.SchemaMappingService;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RTA.Examples.DataAdapter.Service.Controllers;

namespace RTA.Examples.DataAdapter.Service
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            // *** Add Controller and Services ***
            services.AddSingleton<ChannelBase>(sp => GrpcChannel.ForAddress("http://localhost:2682"));
            services.AddSingleton<SchemaMappingStore.SchemaMappingStoreClient>();
            services.AddTransient<DemoDataController>();
            services.AddTransient<IEventStore, DefaultEventStore>();
            services.AddTransient<ISampleDataStore, DemoSampleDataStore>();

            // *** RTA hooks ***
            services.AddRTAFormatters();
            services.AddRTAResponseCompression();
            services.RemoveRTAMvcApplicationPart();
            services.DisableAuthorization();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
                endpoints.MapGet("/",
                    async context =>
                    {
                        context.Response.ContentType = "text/plain";
                        await context.Response.WriteAsync("RTA.Demo.DataAdapter.Service");
                    });
            });
        }
    }
}
