// <copyright file="Program.cs" company="McLaren Applied Ltd.">
// Copyright (c) McLaren Applied Ltd.</copyright>

using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using MAT.OCS.Configuration;
using MAT.OCS.Configuration.Builder;
using MAT.OCS.RTA.Model.Data;
using MAT.OCS.RTA.Model.Net;
using MAT.OCS.RTA.Toolkit.API.ConfigService;
using MAT.OCS.RTA.Toolkit.API.DataService;
using MAT.OCS.RTA.Toolkit.API.SessionService;
using RTA.Examples.Util;

namespace RTA.Examples.Loader
{
    public class Program
    {
        private static readonly string[] Fields = {"alpha", "beta", "gamma"};

        public static async Task Main(string[] args)
        {
            using var channel = GrpcChannel.ForAddress("http://localhost:8082");
            var sessionClient = new SessionStore.SessionStoreClient(channel);
            var configClient = new ConfigStore.ConfigStoreClient(channel);
            var dataClient = new DataWriter.DataWriterClient(channel);

            var dataIdentity = Guid.NewGuid().ToString();
            var timestamp = DateTimeOffset.Now;
            var startNanos = (timestamp.ToUniversalTime() - DateTimeOffset.UnixEpoch).Ticks * 100;
            var durationNanos = TimeSpan.FromMinutes(10).Ticks * 100;
            var intervalNanos = TimeSpan.FromMilliseconds(10).Ticks * 100;

            await WriteDataAsync(
                dataClient, dataIdentity, startNanos, durationNanos, intervalNanos);

            var configIdentifier = await WriteConfigAsync(
                configClient, intervalNanos);

            await WriteSessionAsync(
                sessionClient, dataIdentity,
                timestamp, startNanos, durationNanos, intervalNanos, configIdentifier);

            Console.WriteLine(dataIdentity);
        }

        private static async Task WriteDataAsync(
            DataWriter.DataWriterClient dataClient,
            string dataIdentity,
            long startNanos,
            long durationNanos,
            long intervalNanos)
        {
            var rng = new Random();
            var signals = new SignalGenerator[Fields.Length];
            for (var f = 0; f < signals.Length; f++)
                signals[f] = new SignalGenerator(rng);

            const int burstLength = 100;
            var burstSamples = new short[burstLength];

            using var dataStream = dataClient.WriteDataStream();
            var requestStream = dataStream.RequestStream;
            var responseStream = dataStream.ResponseStream;

            await requestStream.WriteAsync(new WriteDataStreamMessage
            {
                DataIdentity = dataIdentity
            });

            for (var offset = 0L; offset < durationNanos; offset += burstLength * intervalNanos)
            {
                for (var f = 0; f < signals.Length; f++)
                {
                    var signal = signals[f];
                    for (var t = 0; t < burstLength; t++)
                    {
                        var timeOffsetNanos = offset + t * intervalNanos;
                        burstSamples[t] = (short) signal[timeOffsetNanos]; // short precision for this demo
                    }

                    var burst = new PeriodicData
                    {
                        ChannelId = (uint) f,
                        StartTimestamp = startNanos + offset,
                        Interval = intervalNanos,
                        Samples = burstLength,
                        Buffer = ByteString.CopyFrom(MemoryMarshal.AsBytes(burstSamples.AsSpan()))
                    };

                    await requestStream.WriteAsync(new WriteDataStreamMessage
                    {
                        PeriodicData = burst
                    });
                }

                if ((offset % 1_000_000_000L) == 0)
                {
                    Console.Write(".");
                }
            }

            // MUST do both these steps for the write to complete:
            // refer to https://docs.microsoft.com/en-us/aspnet/core/grpc/client?view=aspnetcore-5.0#bi-directional-streaming-call

            // 1. complete request stream
            await requestStream.CompleteAsync();

            // 2. drain response stream (can also be done in a background task, or interleaved)
            // (no responses expected since no flush tokens were sent)
            await foreach (var _ in responseStream.ReadAllAsync());

            Console.WriteLine();
        }

        private static async Task<string> WriteConfigAsync(
            ConfigStore.ConfigStoreClient configClient,
            long intervalNanos)
        {
            var channels = new ChannelBuilder[Fields.Length];
            var parameters = new ParameterBuilder[Fields.Length];

            for (var f = 0; f < Fields.Length; f++)
            {
                channels[f] = new ChannelBuilder((uint) f, intervalNanos, DataType.Signed16Bit,
                    ChannelDataSource.Periodic);

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

            // arbitrary, unique and can be reused as long as the config stays the same
            const string configIdentifier = "6D711DBC-5F0D-47D2-B127-8C9D20BCEC02";

            await configClient.PutConfigAsync(configIdentifier, config, MediaTypes.JsonConfig);

            return configIdentifier;
        }

        private static async Task WriteSessionAsync(
            SessionStore.SessionStoreClient sessionClient,
            string dataIdentity,
            DateTimeOffset timestamp,
            long startNanos,
            long durationNanos,
            long intervalNanos,
            string configIdentifier)
        {
            await sessionClient.CreateOrUpdateSessionAsync(new CreateOrUpdateSessionRequest
            {
                Identity = dataIdentity,
                CreateIfNotExists = new()
                {
                    Identifier = $"Data Service Demo {timestamp:f}",
                    Timestamp = timestamp.ToString("O")
                },
                Updates =
                {
                    new SessionUpdate
                    {
                        SetType = "rta"
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
                        SetConfigBindings = new()
                        {
                            ConfigBindings =
                            {
                                new ConfigBinding
                                {
                                    Identifier = configIdentifier
                                }
                            }
                        }
                    }
                }
            });
        }
    }
}