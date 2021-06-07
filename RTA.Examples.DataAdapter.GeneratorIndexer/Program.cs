using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Grpc.Net.Client;
using MAT.OCS.Configuration;
using MAT.OCS.Configuration.Builder;
using MAT.OCS.RTA.Model;
using MAT.OCS.RTA.Toolkit.API.ConfigService;
using MAT.OCS.RTA.Toolkit.API.SchemaMappingService;
using MAT.OCS.RTA.Toolkit.API.SessionService;
using Microsoft.Data.Sqlite;

namespace RTA.Examples.DataAdapter.GeneratorIndexer
{
    internal class Program
    {
        private const string DataSource = "sqlite";

        private static readonly List<(string Table, string Field, long IntervalMillis)> Schema = new()
        {
            ("samples_100", "alpha", 10),
            ("samples_100", "beta", 10),
            ("samples_100", "gamma", 10),
            ("samples_50", "delta", 20),
            ("samples_50", "epsilon", 20),
            ("samples_50", "zeta", 20),
        };

        public static async Task Main(string[] args)
        {
            // make some sample data
            await DataGenerator.GenerateFilesAsync(8);

            using var sessionChannel = GrpcChannel.ForAddress("http://localhost:2652");
            using var configChannel = GrpcChannel.ForAddress("http://localhost:2662");
            using var schemaMappingChannel = GrpcChannel.ForAddress("http://localhost:2682");
            var sessionClient = new SessionStore.SessionStoreClient(sessionChannel);
            var configClient = new ConfigStore.ConfigStoreClient(configChannel);
            var schemaMappingClient = new SchemaMappingStore.SchemaMappingStoreClient(schemaMappingChannel);

            // configuration can be shared across sessions so just publish it once
            var configIdentifier = Guid.NewGuid().ToString();
            await configClient.PutConfigAsync(configIdentifier, MakeConfiguration());

            // schema mapping is also the same every time
            var schemaMapping = MakeSchemaMapping();

            Console.WriteLine("Indexing files");
            await foreach (var (path, timestamp, identifier, details, timeRange) in FindFilesAsync())
            {
                var file = Path.GetFileName(path);
                Console.WriteLine($"  {file}");

                // schema mapping will be used later by the data adapter service
                // and should be published before making the session visible
                await schemaMappingClient.PutSchemaMappingAsync(new PutSchemaMappingRequest
                {
                    DataSource = DataSource,
                    DataIdentity = path,
                    SchemaMapping = schemaMapping
                });

                var sessionRequest = new CreateOrUpdateSessionRequest
                {
                    Identity = file,
                    CreateIfNotExists = new()
                    {
                        Timestamp = timestamp,
                        Identifier = identifier
                    },
                    Updates =
                    {
                        new SessionUpdate
                        {
                            SetState = (int) SessionState.Closed
                        },
                        new SessionUpdate
                        {
                            SetType = "sqlite"
                        },
                        new SessionUpdate
                        {
                            // AddRange is an extension method
                            PutDetails = new SessionUpdate.Types.SessionDetailsList().AddRange(details)
                        },
                        new SessionUpdate
                        {
                            SetConfigBindings = new()
                            {
                                ConfigBindings =
                                {
                                    new ConfigBinding
                                    {
                                        ConfigIdentifier = configIdentifier
                                    }
                                }
                            }
                        },
                        new SessionUpdate
                        {
                            SetDataBindings = new()
                            {
                                DataBindings =
                                {
                                    new DataBinding
                                    {
                                        Key = new DataBindingKey
                                        {
                                            Source = DataSource,
                                            Identity = path
                                        }
                                    }
                                }
                            }
                        }
                    }
                };

                if (timeRange == null)
                {
                    sessionRequest.Updates.Add(new SessionUpdate
                    {
                        ClearTimeRange = new()
                    });
                }
                else
                {
                    sessionRequest.Updates.Add(new SessionUpdate
                    {
                        SetTimeRange = new()
                        {
                            StartTime = timeRange.StartTime,
                            EndTime = timeRange.EndTime
                        }
                    });
                }

                await sessionClient.CreateOrUpdateSessionAsync(sessionRequest);
            }
        }

        /// <summary>
        ///     Scans for *.sqlite files and reads the <c>properties</c> table.
        /// </summary>
        /// <returns>Metadata and session details.</returns>
        private static async IAsyncEnumerable<(string Path, string Timestamp, string Identifier, Dictionary<string, string> Details, SessionTimeRange? timeRange)> FindFilesAsync()
        {
            var paths = Directory.EnumerateFiles(Environment.CurrentDirectory, "*.sqlite");
            foreach (var path in paths)
            {
                var properties = new Dictionary<string, string>();

                await using var conn = new SqliteConnection($"Data Source={path}");
                conn.Open();

                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT key, value FROM properties";

                    await using var reader = await cmd.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        properties[reader.GetString(0)] = reader.GetString(1);
                    }
                }

                var timestamp = properties[Properties.Timestamp];
                var identifier = properties[Properties.Identifier];
                var details = new Dictionary<string, string>(properties.Where(kv => !kv.Key.StartsWith("$")));

                SessionTimeRange? timeRange = null;

                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT MIN(unix_time), MAX(unix_time) FROM samples_100";

                    await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow);
                    if (await reader.ReadAsync())
                    {
                        var minTimeMillis = reader.GetInt64(0);
                        var maxTimeMillis = reader.GetInt64(1);
                        timeRange = new SessionTimeRange
                        {
                            StartTime = minTimeMillis * 1_000_000L,
                            EndTime = maxTimeMillis * 1_000_000L
                        };
                    }
                }

                yield return (path, timestamp, identifier, details, timeRange);
            }
        }

        private static SchemaMapping MakeSchemaMapping()
        {
            return new SchemaMapping
            {
                FieldMappings =
                {
                    Schema.Select((row, i) => new FieldMapping
                    {
                        SourceField = row.Field,
                        TargetChannel = (uint) i,
                        Properties =
                        {
                            ["table"] = row.Table
                        }
                    })
                }
            };
        }

        private static Configuration MakeConfiguration()
        {
            return new ApplicationBuilder("demo")
            {

                Channels = Schema.Select((row, i) =>
                    new ChannelBuilder(
                        (uint) i,
                        row.IntervalMillis * 1_000_000L, // ms -> ns
                        DataType.Double64Bit,
                        ChannelDataSource.Periodic)).ToList(),

                ChildParameters = Schema.Select((row, i) =>
                    new ParameterBuilder(
                        $"{row.Field}:demo",
                        row.Field,
                        $"{row.Field} at {row.IntervalMillis}ms intervals")
                    {
                        ChannelIds = {(uint) i},
                        MinimumValue = -1500,
                        MaximumValue = +1500
                    }).ToList()

            }.BuildConfiguration();
        }

    }
}
