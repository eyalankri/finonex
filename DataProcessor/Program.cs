using System.Text.Json;
using Npgsql;

namespace DataProcessor
{
    public class Event
    {
        public required string UserId { get; set; }
        public required string Name { get; set; }
        public required int Value { get; set; }
    }

    class Program
    {
        private const string ConnectionString = "Host=localhost;Port=5432;Username=postgres;Password=Qwerty123!;Database=Finonex";

        public static async Task Main(string[] args)
        {
            // Task root folder
            string filePath = args.Length > 0
                ? Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", args[0]))
                : Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "event_log.jsonl"));

            if (!File.Exists(filePath))
            {
                Console.WriteLine($"File not found: {filePath}");
                return;
            }

            var revenueMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            int totalLines = 0;
            int skippedLines = 0;

            Console.WriteLine($"Reading events from: {filePath}");

            using var reader = new StreamReader(filePath);
            string? line;

            // Read line by line – efficient for large files.
            // Consider chunked DB updates if handling millions of unique users.
            while ((line = await reader.ReadLineAsync()) != null)
            {
                totalLines++;
                if (string.IsNullOrWhiteSpace(line)) continue;

                try
                {
                    var ev = JsonSerializer.Deserialize<Event>(line, new JsonSerializerOptions
                    {
                        PropertyNameCaseInsensitive = true
                    });

                    if (ev == null)
                    {
                        Console.WriteLine($"Skipped: Deserialization returned null. Line: {line}");
                        skippedLines++;
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(ev.UserId))
                    {
                        Console.WriteLine($"Skipped: Missing userId. Line: {line}");
                        skippedLines++;
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(ev.Name) ||
                        (ev.Name != "add_revenue" && ev.Name != "subtract_revenue"))
                    {
                        Console.WriteLine($"Skipped: Invalid event name '{ev.Name}'. Line: {line}");
                        skippedLines++;
                        continue;
                    }

                    if (ev.Value < 0)
                    {
                        Console.WriteLine($"Skipped: Negative value. Line: {line}");
                        skippedLines++;
                        continue;
                    }

                    int delta = ev.Name == "add_revenue" ? ev.Value : -ev.Value;

                    revenueMap[ev.UserId] = revenueMap.TryGetValue(ev.UserId, out int current)
                        ? current + delta
                        : delta;
                }
                catch (JsonException jsonEx)
                {
                    Console.WriteLine($"Skipped: JSON error: {jsonEx.Message}. Line: {line}");
                    skippedLines++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Skipped: Unexpected error: {ex.Message}. Line: {line}");
                    skippedLines++;
                }
            }

            Console.WriteLine("Updating database...");
            await UpdateDatabaseAsync(revenueMap);
            Console.WriteLine("Processing complete.");

            Console.WriteLine($"Total lines processed: {totalLines}");
            Console.WriteLine($"Total lines skipped:  {skippedLines}");
        }


        private static async Task UpdateDatabaseAsync(Dictionary<string, int> revenueMap)
        {
            await using var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync();

            foreach (var pair in revenueMap)
            {
                var userId = pair.Key;
                var delta = pair.Value;

                // Safe for concurrent processors:
                // PostgreSQL handles row-level locking internally during ON CONFLICT DO UPDATE.
                // This ensures atomic updates to the revenue field without requiring manual locking.

                var cmdText = @"
                    INSERT INTO users_revenue (user_id, revenue)
                    VALUES (@id, @val)
                    ON CONFLICT (user_id) DO UPDATE
                    SET revenue = users_revenue.revenue + @val;";

                await using var cmd = new NpgsqlCommand(cmdText, conn);
                cmd.Parameters.AddWithValue("id", userId);
                cmd.Parameters.AddWithValue("val", delta);

                await cmd.ExecuteNonQueryAsync();
                Console.WriteLine($"Updated {userId}: delta {delta}");
            }
        }
    }
}
