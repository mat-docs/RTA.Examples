using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using RTA.Examples.Util;

namespace RTA.Examples.DataAdapter.GeneratorIndexer
{
    internal static class DataGenerator
    {
        public static async Task GenerateFilesAsync(int nFiles)
        {
            Console.WriteLine($"Generating data ({nFiles} files)");
            for (var i = 0; i < nFiles; i++)
            {
                var file = $"{i:D3}.sqlite";
                var path = Path.GetFullPath(file);

                if (File.Exists(file))
                {
                    Console.WriteLine($"  {file} - exists");
                    continue;
                }

                Console.WriteLine($"  {file}");

                await using var conn = new SqliteConnection($"Data Source={file}");
                conn.Open();

                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"
CREATE TABLE [properties](
  [key] TEXT PRIMARY KEY NOT NULL, 
  [value] TEXT NOT NULL);

CREATE TABLE [samples_100](
  [unix_time] INTEGER(8) PRIMARY KEY ASC NOT NULL, 
  [alpha] DOUBLE NOT NULL, 
  [beta] DOUBLE NOT NULL, 
  [gamma] DOUBLE NOT NULL);

CREATE TABLE [samples_50](
  [unix_time] INTEGER(8) PRIMARY KEY ASC NOT NULL, 
  [delta] DOUBLE NOT NULL, 
  [epsilon] DOUBLE NOT NULL, 
  [zeta] DOUBLE NOT NULL);";

                    await cmd.ExecuteNonQueryAsync();
                }

                var rng = new Random();
                var timestamp = DateTimeOffset.Now;
                var startMillis = (timestamp.ToUniversalTime() - DateTimeOffset.UnixEpoch).Ticks * 100 / 1_000_000L;
                var durationMillis = (long) TimeSpan.FromMinutes(60).TotalMilliseconds;

                var properties = new Dictionary<string, string>
                {
                    [Properties.Timestamp] = timestamp.ToString("O"),
                    [Properties.Identifier] = $"SQLite {file} {timestamp:F}",
                    ["file"] = file
                };

                // set some metadata
                await using (var tx = await conn.BeginTransactionAsync())
                {
                    await using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "INSERT INTO properties (key, value) VALUES (@key, @value)";

                        var pKey = cmd.Parameters.Add("key", SqliteType.Text);
                        var pValue = cmd.Parameters.Add("value", SqliteType.Text);
                        await cmd.PrepareAsync();

                        foreach (var (k, v) in properties)
                        {
                            pKey.Value = k;
                            pValue.Value = v;
                            await cmd.ExecuteNonQueryAsync();
                        }
                    }

                    await tx.CommitAsync();
                }

                // make some 100Hz data
                await using (var tx = await conn.BeginTransactionAsync())
                {
                    await using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText =
                            "INSERT INTO samples_100(unix_time, alpha, beta, gamma) VALUES (@unix_time, @alpha, @beta, @gamma)";

                        var pUnixTime = cmd.Parameters.Add("unix_time", SqliteType.Integer);
                        var pAlpha = cmd.Parameters.Add("alpha", SqliteType.Real);
                        var pBeta = cmd.Parameters.Add("beta", SqliteType.Real);
                        var pGamma = cmd.Parameters.Add("gamma", SqliteType.Real);
                        await cmd.PrepareAsync();

                        foreach (var (t, alpha, beta, gamma) in GenerateData(rng, startMillis, durationMillis, 10))
                        {
                            pUnixTime.Value = t;
                            pAlpha.Value = alpha;
                            pBeta.Value = beta;
                            pGamma.Value = gamma;
                            await cmd.ExecuteNonQueryAsync();
                        }
                    }

                    await tx.CommitAsync();
                }

                // make some 50Hz data
                await using (var tx = await conn.BeginTransactionAsync())
                {
                    await using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText =
                            "INSERT INTO samples_50(unix_time, delta, epsilon, zeta) VALUES (@unix_time, @delta, @epsilon, @zeta)";

                        var pUnixTime = cmd.Parameters.Add("unix_time", SqliteType.Integer);
                        var pDelta = cmd.Parameters.Add("delta", SqliteType.Real);
                        var pEpsilon = cmd.Parameters.Add("epsilon", SqliteType.Real);
                        var pZeta = cmd.Parameters.Add("zeta", SqliteType.Real);
                        await cmd.PrepareAsync();

                        foreach (var (t, delta, epsilon, zeta) in GenerateData(rng, startMillis, durationMillis, 20))
                        {
                            pUnixTime.Value = t;
                            pDelta.Value = delta;
                            pEpsilon.Value = epsilon;
                            pZeta.Value = zeta;
                            await cmd.ExecuteNonQueryAsync();
                        }
                    }

                    await tx.CommitAsync();
                }
            }

            Console.WriteLine();
        }

        private static IEnumerable<(long Timestamp, double x, double y, double z)> GenerateData(
            Random rng,
            long startMillis,
            long durationMillis,
            long intervalMillis)
        {
            var signals = new SignalGenerator[3];
            for (var f = 0; f < signals.Length; f++)
                signals[f] = new SignalGenerator(rng);

            for (long t = startMillis, endTime = startMillis + durationMillis; t < endTime; t += intervalMillis)
            {
                var ns = t * 1_000_000L;
                yield return (t, signals[0][ns], signals[1][ns], signals[2][ns]);
            }
        }
    }
}