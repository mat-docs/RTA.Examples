// <copyright file="Program.cs" company="McLaren Applied Ltd.">
// Copyright (c) McLaren Applied Ltd.</copyright>

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Net.Client;
using MAT.OCS.Configuration;
using MAT.OCS.Configuration.Builder;
using MAT.OCS.RTA.Model;
using MAT.OCS.RTA.Model.Data;
using MAT.OCS.RTA.Model.Net.Stream;
using MAT.OCS.RTA.StreamBuffer;
using MAT.OCS.RTA.Toolkit.API.ConfigService;
using MAT.OCS.RTA.Toolkit.API.SchemaMappingService;
using MAT.OCS.RTA.Toolkit.API.SessionService;
using RTA.Examples.Util;
using Session = MAT.OCS.RTA.Model.Session;

namespace RTA.Examples.InfluxLive
{
    internal class Program
    {
        private static readonly string[] Fields = {"alpha", "beta", "gamma"};

        private const string DataBindingSource = "rta-influxdatasvc";
        private const string Database = "rtademo";
        private const string Measurement = "data";
        private const string SessionTag = "session";

        private const int SamplesPerBurst = 10;
        private const string RedisStreamName = "rtademo";

        public static async Task Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, eventArgs) =>
            {
                cts.Cancel();
                eventArgs.Cancel = true;
            };

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
            var durationNanos = TimeSpan.FromMinutes(5).Ticks * 100;
            var intervalNanos = TimeSpan.FromMilliseconds(10).Ticks * 100;

            var sessionTag = sessionIdentity;
            var dataIdentity = FormatDataIdentity(sessionTag);

            // publish config as soon as it's known - but session can be published first
            // and bound to config later if config takes some time to resolve in your environment
            var configIdentifier = await WriteConfigAsync(configClient);

            // data can't be loaded from REST API until schema mapping is published
            await WriteSchemaMappingAsync(schemaMappingClient, dataIdentity);

            // publish session as early as possible so it can be browsed
            // but in Open state so client looks for live streaming data
            var session = await OpenSessionAsync(
                sessionClient, sessionIdentity, dataIdentity, timestamp, configIdentifier);

            Console.WriteLine(sessionIdentity);

            try
            {
                await WriteDataAsync(
                    influxUri, sessionClient, session, sessionTag,
                    startNanos, durationNanos, intervalNanos, cts.Token);
            }
            catch (Exception)
            {
                try
                {
                    // since the session is published before data is complete
                    // we should mark it as failed if we had to bail out
                    await sessionClient.CreateOrUpdateSessionAsync(new CreateOrUpdateSessionRequest
                    {
                        Identity = sessionIdentity,
                        Updates =
                        {
                            new SessionUpdate
                            {
                                SetState = (int) SessionState.Failed
                            }
                        }
                    });
                }
                catch (Exception fex)
                {
                    Console.Error.WriteLine("Failed to mark session as failed: " + fex);
                }

                throw;
            }

            Console.WriteLine();
        }

        private static string FormatDataIdentity(string sessionTag)
        {
            return $"{SessionTag}='{sessionTag}'";
        }

        private static async Task WriteDataAsync(
            Uri influxDbUri,
            SessionStore.SessionStoreClient sessionClient,
            Session session,
            string sessionTag,
            long startNanos,
            long durationNanos,
            long intervalNanos,
            CancellationToken ct)
        {
            using var redisBuffer = new RedisStreamBuffer("localhost", 0);
            var streamBuffer = await redisBuffer.InitStreamSessionAsync(session.Identity, RedisStreamName);

            await streamBuffer.WriteSessionJsonAsync(session);
            
            var sampleCount = 0;
            var timestampsBuffer = new long[SamplesPerBurst];
            var valuesBuffers = new double[Fields.Length][];
            for (var f = 0; f < Fields.Length; f++)
            {
                valuesBuffers[f] = new double[SamplesPerBurst];
            }

            var (request, writer) = await BeginInfluxRequestAsync(influxDbUri);

            var lastStreamWrite = Task.CompletedTask;
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

                    valuesBuffers[f][sampleCount] = values[f];
                }

                lineBuffer.Append(' ');
                lineBuffer.Append(timestamp);

                timestampsBuffer[sampleCount] = timestamp;

                await writer.WriteLineAsync(lineBuffer);
                lineBuffer.Clear();

                sampleCount++;

                // aim to update the stream at around 10Hz
                // and consider transfer efficiency by avoiding lots of tiny updates
                if (sampleCount == SamplesPerBurst)
                {
                    await lastStreamWrite;
                    lastStreamWrite = WriteStreamBurstAsync(
                        streamBuffer, startNanos, timestamp, timestampsBuffer, valuesBuffers, sampleCount);

                    sampleCount = 0;
                }

                if (((timestamp - startNanos) % 1_000_000_000L) == 0)
                {
                    // start a new InfluxDB write so the data becomes visible
                    await EndInfluxRequestAsync(request, writer);
                    (request, writer) = await BeginInfluxRequestAsync(influxDbUri);

                    // is not necessary to keep updating the streamed session JSON if the only change is time range
                    // but make sure time range on the JSON is accurate if it is republished to update other metadata

                    // session service should be updated fairly regularly (e.g. 1Hz - 0.1Hz)
                    // so users can see progress when browsing sessions
                    await sessionClient.CreateOrUpdateSessionAsync(new CreateOrUpdateSessionRequest
                    {
                        Identity = session.Identity,
                        Updates =
                        {
                            new SessionUpdate
                            {
                                ExtendTimeRange = new()
                                {
                                    StartTime = startNanos,
                                    EndTime = timestamp
                                }
                            }
                        }
                    });

                    Console.Write(".");
                }

                if (ct.IsCancellationRequested)
                {
                    Console.WriteLine("#");
                    break;
                }
            }

            // final burst of data
            await lastStreamWrite;
            if (sampleCount > 0)
            {
                await WriteStreamBurstAsync(
                    streamBuffer, startNanos, timestampsBuffer[sampleCount - 1], timestampsBuffer, valuesBuffers,
                    sampleCount);
            }

            // flush remaining InfluxDB data
            await EndInfluxRequestAsync(request, writer);

            // final update to session model to go into closed state (or truncated if cancelled)
            // so the client doesn't keep looking for new live data
            session.State = ct.IsCancellationRequested ? SessionState.Truncated : SessionState.Closed;
            session.TimeRange = new SessionTimeRange
            {
                StartTime = startNanos,
                EndTime = startNanos + durationNanos - intervalNanos
            };
            await streamBuffer.WriteSessionJsonAsync(session);

            static async Task<(HttpWebRequest Request, StreamWriter Writer)> BeginInfluxRequestAsync(Uri influxDbUri)
            {
                var writeUri = new UriBuilder(influxDbUri)
                {
                    Path = "/write",
                    Query = $"db={Database}"
                }.Uri;

                var request = (HttpWebRequest)WebRequest.Create(writeUri);
                request.Method = "POST";
                request.SendChunked = true;

                var stream = await request.GetRequestStreamAsync();
                var writer = new StreamWriter(stream, new UTF8Encoding(false)) {NewLine = "\n"};

                return (request, writer);
            }

            static async Task EndInfluxRequestAsync(HttpWebRequest request, StreamWriter writer)
            {
                await writer.FlushAsync();
                await writer.DisposeAsync();

                using var response = await request.GetResponseAsync();
            }

            static async Task WriteStreamBurstAsync(IStreamBufferSessionProducer streamBuffer,
                long startTime, long endTime, long[] timestampsBuffer, double[][] valuesBuffers, int sampleCount)
            {
                // stream data in bursts for transfer efficiency
                var burst = new StreamDataBurst();

                // bursts can contain an arbitrary mix of data types and channels
                for (var f = 0; f < valuesBuffers.Length; f++)
                {
                    var tData = new TimestampedData
                    {
                        ChannelId = (uint) f,
                        Buffer = ByteString.CopyFrom(MemoryMarshal.AsBytes(valuesBuffers[f].AsSpan(0, sampleCount)))
                    };
                    tData.SetTimestamps(timestampsBuffer);

                    burst.Data.Add(new StreamData
                    {
                        TimestampedData = tData
                    });
                }

                // keep the time range up to date so the client can scroll smoothly
                burst.Data.Add(new StreamData
                {
                    TimeRange = new StreamTimeRange
                    {
                        StartTime = startTime,
                        EndTime = endTime
                    }
                });

                await streamBuffer.WriteAsync(burst);
            }

            // mark session as closed only after data is fully flushed where feasible
            // to avoid clients seeing the session as complete when data is still not written
            await sessionClient.CreateOrUpdateSessionAsync(new CreateOrUpdateSessionRequest
            {
                Identity = session.Identity,
                Updates =
                {
                    new SessionUpdate
                    {
                        // inclusive timestamps
                        ExtendTimeRange = new()
                        {
                            StartTime = startNanos,
                            EndTime = startNanos + durationNanos - intervalNanos
                        }
                    },
                    new SessionUpdate
                    {
                        SetState = (int) session.State
                    },
                    new SessionUpdate
                    {
                        SetType = "influx"
                    }
                }
            });

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

            var realStartTimeMillis = Environment.TickCount64;
            for (long t = startNanos, endTime = startNanos + durationNanos; t < endTime; t += intervalNanos)
            {
                var values = new double[Fields.Length];
                for (var f = 0; f < values.Length; f++)
                {
                    values[f] = signals[f][t - startNanos];
                }

                yield return (t, values);

                var elapsedMillis = Environment.TickCount64 - realStartTimeMillis;
                var durationMillis = (t - startNanos) / 1_000_000L;
                if (durationMillis > elapsedMillis)
                {
                    Thread.Sleep(TimeSpan.FromMilliseconds(durationMillis - elapsedMillis));
                }
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

        private static async Task<Session> OpenSessionAsync(
            SessionStore.SessionStoreClient sessionClient,
            string sessionIdentity,
            string dataIdentity,
            DateTimeOffset timestamp,
            string configIdentifier)
        {
            var identifier = $"Influx Live Demo {timestamp:f}";

            await sessionClient.CreateOrUpdateSessionAsync(new CreateOrUpdateSessionRequest
            {
                Identity = sessionIdentity,
                CreateIfNotExists = new CreateOrUpdateSessionRequest.Types.CreateIfNotExists
                {
                    Identifier = identifier,
                    Timestamp = timestamp.ToString("O"),
                    State = (int) SessionState.Open
                },
                Updates =
                {
                    new SessionUpdate
                    {
                        SetType = "influx"
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

            return new Session(sessionIdentity, SessionState.Open, timestamp, identifier)
            {
                Type = "influx",
                ConfigBindings = new List<SessionConfigBinding>
                {
                    new SessionConfigBinding(configIdentifier, 0u)
                }
            };
        }
    }
}