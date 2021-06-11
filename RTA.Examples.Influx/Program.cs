// <copyright file="Program.cs" company="McLaren Applied Ltd.">
// Copyright (c) McLaren Applied Ltd.</copyright>

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Grpc.Net.Client;
using MAT.OCS.Configuration;
using MAT.OCS.Configuration.Builder;
using MAT.OCS.RTA.Model;
using MAT.OCS.RTA.Toolkit.API.ConfigService;
using MAT.OCS.RTA.Toolkit.API.SchemaMappingService;
using MAT.OCS.RTA.Toolkit.API.SessionService;
using RTA.Examples.Util;

namespace RTA.Examples.Influx
{
    internal class Program
    {
        private static readonly string[] Fields = {"alpha", "beta", "gamma"};

        private const string DataBindingSource = "rta-influxdatasvc";
        private const string Database = "rtademo";
        private const string Measurement = "data";
        private const string SessionTag = "session";

        public static async Task Main(string[] args)
        {
            var influxUri = new Uri("http://localhost:8086");

            using var sessionChannel = GrpcChannel.ForAddress("http://localhost:2652");
            using var configChannel = GrpcChannel.ForAddress("http://localhost:2662");
            using var schemaMappingChannel = GrpcChannel.ForAddress("http://localhost:2682");
            var sessionClient = new SessionStore.SessionStoreClient(sessionChannel);
            var configClient = new ConfigStore.ConfigStoreClient(configChannel);
            var schemaMappingClient = new SchemaMappingStore.SchemaMappingStoreClient(schemaMappingChannel);

            var sessionIdentity = Guid.NewGuid().ToString();
            var timestamp = DateTimeOffset.Now;
            var startNanos = (timestamp.ToUniversalTime() - DateTimeOffset.UnixEpoch).Ticks * 100;
            var durationNanos = TimeSpan.FromMinutes(10).Ticks * 100;
            var intervalNanos = TimeSpan.FromMilliseconds(10).Ticks * 100;

            var sessionTag = sessionIdentity;
            var dataIdentity = FormatDataIdentity(sessionTag);

            await WriteDataAsync(
                influxUri, sessionTag, startNanos, durationNanos, intervalNanos);

            await WriteSchemaMappingAsync(schemaMappingClient, dataIdentity);

            var configIdentifier = await WriteConfigAsync(configClient);

            await WriteSessionAsync(
                sessionClient, sessionIdentity, dataIdentity,
                timestamp, startNanos, durationNanos, intervalNanos, configIdentifier);

            Console.WriteLine();
            Console.WriteLine(sessionIdentity);
        }

        private static string FormatDataIdentity(string sessionTag)
        {
            return $"{SessionTag}='{sessionTag}'";
        }

        private static async Task WriteDataAsync(
            Uri influxDbUri,
            string sessionTag,
            long startNanos,
            long durationNanos,
            long intervalNanos)
        {
            var writeUri = new UriBuilder(influxDbUri)
            {
                Path = "/write",
                Query = $"db={Database}"
            }.Uri;

            // note that longer sessions may need to be split into multiple requests
            var request = (HttpWebRequest) WebRequest.Create(writeUri);
            request.Method = "POST";
            request.SendChunked = true;

            await using (var stream = await request.GetRequestStreamAsync())
            await using (var writer = new StreamWriter(stream, new UTF8Encoding(false)) {NewLine = "\n"})
            {
                var lineBuffer = new StringBuilder();
                foreach (var (timestamp, values) in GenerateData(startNanos, durationNanos, intervalNanos))
                {
                    lineBuffer.Append(Measurement);
                    lineBuffer.Append(',');
                    lineBuffer.Append(SessionTag);
                    lineBuffer.Append('=');
                    lineBuffer.Append(sessionTag);
                    
                    lineBuffer.Append(' ');

                    for (var f = 0; f < Fields.Length; f++)
                    {
                        if (f > 0)
                        {
                            lineBuffer.Append(',');
                        }

                        lineBuffer.Append(Fields[f]);
                        lineBuffer.Append('=');
                        lineBuffer.Append(values[f]);
                    }

                    lineBuffer.Append(' ');
                    lineBuffer.Append(timestamp);

                    await writer.WriteLineAsync(lineBuffer);
                    lineBuffer.Clear();

                    if (((timestamp - startNanos) % 1_000_000_000L) == 0)
                    {
                        Console.Write(".");
                    }
                }
            }

            using var response = await request.GetResponseAsync();

            Console.WriteLine();
        }

        private static IEnumerable<(long Timestamp, double[] Values)> GenerateData(
            long startNanos,
            long durationNanos,
            long intervalNanos)
        {
            var rng = new Random();
            var signals = new SignalGenerator[Fields.Length];
            for (var f = 0; f < signals.Length; f++)
                signals[f] = new SignalGenerator(rng);

            for (long t = startNanos, endTime = startNanos + durationNanos; t < endTime; t += intervalNanos)
            {
                var values = new double[Fields.Length];
                for (var f = 0; f < values.Length; f++)
                {
                    values[f] = signals[f][t];
                }

                yield return (t, values);
            }
        }

        private static async Task WriteSchemaMappingAsync(
            SchemaMappingStore.SchemaMappingStoreClient schemaMappingClient,
            string dataIdentity)
        {
            var schemaMapping = new SchemaMapping
            {
                Properties =
                {
                    ["dialect"] = "influxql",
                    ["database"] = Database,
                    ["measurement"] = Measurement
                },
                FieldMappings =
                {
                    Fields.Select((field, f) => new FieldMapping
                    {
                        SourceField = field,
                        TargetChannel = (uint) f
                    })
                }
            };

            await schemaMappingClient.PutSchemaMappingAsync(new PutSchemaMappingRequest
            {
                DataSource = DataBindingSource,
                DataIdentity = dataIdentity,
                SchemaMapping = schemaMapping
            });
        }

        private static async Task<string> WriteConfigAsync(ConfigStore.ConfigStoreClient configClient)
        {
            var channels = new ChannelBuilder[Fields.Length];
            var parameters = new ParameterBuilder[Fields.Length];

            for (var f = 0; f < Fields.Length; f++)
            {
                channels[f] = new ChannelBuilder((uint) f, 0L, DataType.Double64Bit, ChannelDataSource.Timestamped);

                parameters[f] = new ParameterBuilder($"{Fields[f]}:demo", Fields[f], $"{Fields[f]} field")
                {
                    ChannelIds = {(uint) f},
                    MinimumValue = -1500,
                    MaximumValue = +1500
                };
            }

            var config = new ConfigurationBuilder
            {
                Applications =
                {
                    new ApplicationBuilder("demo")
                    {
                        ChildParameters = parameters,
                        Channels = channels
                    }
                }
            }.BuildConfiguration();

            var configIdentifier = Guid.NewGuid().ToString();
            await configClient.PutConfigAsync(configIdentifier, config);

            return configIdentifier;
        }

        private static async Task WriteSessionAsync(
            SessionStore.SessionStoreClient sessionClient,
            string sessionIdentity,
            string dataIdentity,
            DateTimeOffset timestamp,
            long startNanos,
            long durationNanos,
            long intervalNanos,
            string configIdentifier)
        {
            await sessionClient.CreateOrUpdateSessionAsync(new CreateOrUpdateSessionRequest
            {
                Identity = sessionIdentity,
                CreateIfNotExists = new CreateOrUpdateSessionRequest.Types.CreateIfNotExists
                {
                    Identifier = $"Influx Demo {timestamp:f}",
                    Timestamp = timestamp.ToString("O"),
                    State = (int) SessionState.Closed
                },
                Updates =
                {
                    new SessionUpdate
                    {
                        SetType = "influx"
                    },
                    new SessionUpdate
                    {
                        // inclusive timestamps
                        SetTimeRange = new()
                        {
                            StartTime = startNanos,
                            EndTime = startNanos + durationNanos - intervalNanos
                        }
                    },
                    new SessionUpdate
                    {
                        SetConfigBindings = new SessionUpdate.Types.ConfigBindingsList
                        {
                            ConfigBindings =
                            {
                                new ConfigBinding
                                {
                                    Identifier = configIdentifier
                                }
                            }
                        }
                    },
                    new SessionUpdate
                    {
                        SetDataBindings = new SessionUpdate.Types.DataBindingsList
                        {
                            DataBindings =
                            {
                                new DataBinding
                                {
                                    Key = new DataBindingKey
                                    {
                                        Source = DataBindingSource,
                                        Identity = dataIdentity
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
    }
}