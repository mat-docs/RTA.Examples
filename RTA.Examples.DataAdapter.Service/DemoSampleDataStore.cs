// <copyright file="DemoSampleDataStore.cs" company="McLaren Applied Ltd.">
// Copyright (c) McLaren Applied Ltd.</copyright>

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using MAT.OCS.RTA.Model.Data;
using MAT.OCS.RTA.Services;
using MAT.OCS.RTA.Toolkit.API.SchemaMappingService;
using Microsoft.Data.Sqlite;

namespace RTA.Examples.DataAdapter.Service
{
    public class DemoSampleDataStore : DefaultSampleDataStore
    {
        private const string DataSource = "sqlite";

        private const int SamplesPerResult = 100;
        private const int ResultsPerChunk = 10;
        private const long ExpectedInterval = 10_000_000L;

        private static readonly ChunkTime ChunkTime =
            ChunkTime.OfDuration(SamplesPerResult * ResultsPerChunk * ExpectedInterval);

        private readonly SchemaMappingStore.SchemaMappingStoreClient schemaMappingClient;

        public DemoSampleDataStore(SchemaMappingStore.SchemaMappingStoreClient schemaMappingClient)
        {
            this.schemaMappingClient = schemaMappingClient;
        }

        public override async Task<ChunkedResult> GetPeriodicDataChunksAsync(
            string dataIdentity,
            long? startTime,
            long? endTime,
            ChannelRangeSet channels,
            RequestContext context,
            CancellationToken cancellationToken)
        {
            // schema mapping provides pre-filtered table and field names
            SchemaMapping sm;
            try
            {
                sm = await schemaMappingClient.QuerySchemaMappingAsync(
                    new QuerySchemaMappingRequest
                    {
                        DataSource = DataSource,
                        DataIdentity = dataIdentity,
                        SelectChannels = channels.ToString()
                    },
                    new CallOptions(cancellationToken: cancellationToken));
            }
            catch (RpcException ex)
            {
                if (ex.StatusCode == StatusCode.NotFound)
                    return ChunkedResult.Empty();

                throw;
            }

            return new ChunkedResult(ReadChunksAsync(dataIdentity, sm, startTime, endTime, cancellationToken));
        }

        private static async IAsyncEnumerable<Chunk> ReadChunksAsync(
            string path,
            SchemaMapping sm,
            long? startTime,
            long? endTime,
            [EnumeratorCancellation] CancellationToken ct)
        {
            await using var conn = new SqliteConnection($"Data Source={path}; Mode=ReadOnly");
            conn.Open();

            // query 100Hz chunks
            await foreach (var chunk in ReadChunksAsync(conn, "samples_100", sm, 10_000_000L, startTime, endTime, ct))
            {
                yield return chunk;
            }

            // query 50Hz chunks
            await foreach (var chunk in ReadChunksAsync(conn, "samples_50", sm, 20_000_000L, startTime, endTime, ct))
            {
                yield return chunk;
            }
        }


        private static async IAsyncEnumerable<Chunk> ReadChunksAsync(
            SqliteConnection conn,
            string table,
            SchemaMapping schemaMapping,
            long intervalNanos,
            long? startTime,
            long? endTime,
            [EnumeratorCancellation] CancellationToken ct)
        {
            var fields = schemaMapping.FieldMappings.Where(field => field.Properties["table"] == table).ToList();

            if (fields.Count == 0)
            {
                yield break;
            }

            // establish stable chunk boundaries by snapping to time intervals
            var (startChunkTime, endChunkTime) = ChunkTime.FromSessionTimeRange(startTime, endTime);

            // prepare chunking buffer to accumulate data
            var buffer = new ChunkingBuffer(fields, intervalNanos);
            var samples = new double[fields.Count];
            Chunk[] pendingFlush;

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = FormatQuery(table, fields, startChunkTime, endChunkTime);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                // read the database record
                var timestamp = reader.GetInt64(0) * 1_000_000L; // ms -> ns
                for (var i = 0; i < samples.Length; i++)
                {
                    samples[i] = reader.GetDouble(i + 1);
                }

                // append to chunking buffer
                pendingFlush = buffer.Append(timestamp, samples);

                // flush any chunks
                for (var i = 0; i < pendingFlush.Length; i++)
                {
                    yield return pendingFlush[i];
                }
            }

            // flush remaining data in chunks
            pendingFlush = buffer.Flush();
            for (var i = 0; i < pendingFlush.Length; i++)
            {
                yield return pendingFlush[i];
            }
        }

        /// <summary>
        ///     Format SQL Query, assuming that table and fields are sanitized (from Schema Mapping).
        /// </summary>
        /// <param name="table">Table name.</param>
        /// <param name="fields">Field mappings.</param>
        /// <param name="startChunkTime">Optional chunk start time (session nanos).</param>
        /// <param name="endChunkTime">Optional chunk end time (session nanos).</param>
        /// <returns>SQL.</returns>
        private static string FormatQuery(string table, IEnumerable<FieldMapping> fields, long? startChunkTime,
            long? endChunkTime)
        {
            var query = new StringBuilder();

            var filterConditions = new List<string>();

            if (startChunkTime != null)
            {
                filterConditions.Add($"(unix_time >= {startChunkTime / 1_000_000L})");
            }

            if (endChunkTime != null)
            {
                filterConditions.Add($"(unix_time <= {endChunkTime / 1_000_000L})");
            }

            query.Append("SELECT unix_time");

            foreach (var field in fields)
            {
                query.Append(", ");
                query.Append(field.SourceField);
            }

            query.Append(" FROM ").Append(table);

            if (filterConditions.Count > 0)
            {
                query.Append(" WHERE ");
                query.AppendJoin(" AND ", filterConditions);
            }

            query.Append(" ORDER BY unix_time");

            return query.ToString();
        }

        private sealed class ChunkingBuffer
        {
            private readonly IList<FieldMapping> fields;
            private readonly long intervalNanos;

            private readonly double[][] buffers;
            private readonly List<PeriodicData>[] results;

            private long unflushedStartTime = -1L;
            private long unflushedEndTime = -1L;
            private long bufferStartTime = -1L;

            private int sampleCount;
            private int resultCount;

            public ChunkingBuffer(IList<FieldMapping> fields, long intervalNanos)
            {
                this.fields = fields;
                this.intervalNanos = intervalNanos;

                buffers = new double[fields.Count][];
                for (var i = 0; i < buffers.Length; i++)
                {
                    buffers[i] = new double[SamplesPerResult];
                }

                results = new List<PeriodicData>[fields.Count];
                for (var i = 0; i < results.Length; i++)
                {
                    results[i] = new List<PeriodicData>(ResultsPerChunk);
                }
            }

            public Chunk[] Append(long timestamp, double[] samples)
            {
                Debug.Assert(samples.Length == fields.Count);

                var chunks = Array.Empty<Chunk>();

                // flush based on chunk duration
                if (ShouldFlush(timestamp))
                {
                    chunks = Flush();
                }

                if (sampleCount == SamplesPerResult)
                {
                    CollateSamples();
                }

                if (bufferStartTime == -1L)
                {
                    bufferStartTime = timestamp;
                }

                if (unflushedStartTime == -1L)
                {
                    unflushedStartTime = timestamp;
                }

                unflushedEndTime = timestamp;

                // accumulate data into buffers
                for (var i = 0; i < buffers.Length; i++)
                {
                    buffers[i][sampleCount] = samples[i];
                }

                sampleCount++;

                return chunks;
            }

            public Chunk[] Flush()
            {
                CollateSamples();
                return EncodeChunks();
            }

            private bool ShouldFlush(long timestamp)
            {
                return (sampleCount > 0 || resultCount > 0) &&
                       timestamp - unflushedStartTime >= ChunkTime.ChunkDuration;
            }

            private void CollateSamples()
            {
                if (sampleCount == 0)
                {
                    return;
                }

                for (var i = 0; i < buffers.Length; i++)
                {
                    var result = new PeriodicData
                    {
                        ChannelId = fields[i].TargetChannel,
                        Samples = sampleCount,
                        Interval = intervalNanos,
                        StartTimestamp = bufferStartTime,
                        Buffer = ByteString.CopyFrom(MemoryMarshal.AsBytes(buffers[i].AsSpan(0, sampleCount)))
                    };

                    results[i].Add(result);
                }

                bufferStartTime = -1L;
                sampleCount = 0;
                resultCount++;
            }

            private Chunk[] EncodeChunks()
            {
                if (resultCount == 0)
                {
                    return Array.Empty<Chunk>();
                }

                var chunks = new Chunk[fields.Count];
                for (var i = 0; i < chunks.Length; i++)
                {
                    // each Chunk contains a compressed PeriodicDataList
                    var data = new PeriodicDataList
                    {
                        PeriodicData =
                        {
                            results[i]
                        }
                    };

                    // one periodic channel per chunk
                    var chId = fields[i].TargetChannel;
                    var chunkData = ChunkData.EncodePooled(ChunkDataMemoryPool.Shared, data, new[] {chId});
                    var chunk = new Chunk(unflushedStartTime, unflushedEndTime, chunkData);
                    chunks[i] = chunk;

                    results[i].Clear();
                }

                resultCount = 0;
                unflushedStartTime = -1L;
                unflushedEndTime = -1L;
                return chunks;
            }
        }
    }
}