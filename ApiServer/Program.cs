using Npgsql;
using System.Text.Json;

namespace EtlApiServer
{
    public class Event
    {
        public required string UserId { get; set; }
        public required string Name { get; set; }
        public required int Value { get; set; }
    }

    public class Program
    {
        private const string ConnectionString = "Host=localhost;Port=5432;Username=postgres;Password=Qwerty123!;Database=Finonex";
        private const string FilePath = "../event_log.jsonl"; // Input log file

        // Reusable JSON options for deserialization
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNameCaseInsensitive = true
        };

        public static void Main(string[] args)
        {

            var builder = WebApplication.CreateBuilder(args);
            builder.Services.AddRouting();
            var app = builder.Build();

            // Helper method to validate event
            static bool IsValidEvent(Event ev, out string error)
            {
                if (ev == null)
                {
                    error = "Event is null.";
                    return false;
                }

                if (string.IsNullOrWhiteSpace(ev.UserId))
                {
                    error = "Missing or empty 'userId'.";
                    return false;
                }

                if (string.IsNullOrWhiteSpace(ev.Name) || (ev.Name != "add_revenue" && ev.Name != "subtract_revenue"))
                {
                    error = $"Invalid 'name': {ev.Name}. Must be 'add_revenue' or 'subtract_revenue'.";
                    return false;
                }

                if (ev.Value < 0)
                {
                    error = "'value' must be non-negative.";
                    return false;
                }

                error = string.Empty;
                return true;
            }


            // POST: Events are sent from Client. Append data to event_log.jsonl
            app.MapPost("/liveEvent", async context =>
            {
                if (!context.Request.Headers.TryGetValue("Authorization", out var authHeader) || authHeader != "secret")
                {
                    context.Response.StatusCode = 401;
                    await context.Response.WriteAsync("Unauthorized");
                    return;
                }

                using var reader = new StreamReader(context.Request.Body);
                var body = await reader.ReadToEndAsync();

                try
                {
                    var ev = JsonSerializer.Deserialize<Event>(body, JsonOptions);

                    if (ev == null)
                    {
                        context.Response.StatusCode = 400;
                        await context.Response.WriteAsync("Invalid event data: Event is null.");
                        return;
                    }

                    if (!IsValidEvent(ev, out string error))
                    {
                        context.Response.StatusCode = 400;
                        await context.Response.WriteAsync($"Invalid event data: {error}");
                        return;
                    }


                    await File.AppendAllTextAsync(FilePath, body + "\n");
                    context.Response.StatusCode = 200;
                    await context.Response.WriteAsync("Event saved.");
                }
                catch (JsonException jsonEx)
                {
                    context.Response.StatusCode = 400;
                    await context.Response.WriteAsync($"Malformed JSON: {jsonEx.Message}");
                }
                catch (Exception)
                {
                    context.Response.StatusCode = 500;
                    await context.Response.WriteAsync("An unexpected error occurred. Please try again later.");
                }
            });



            // GET specific user data
            app.MapGet("/userEvents/{userId}", async context =>
            {
                var userId = context.Request.RouteValues["userId"] as string;

               
                await using var conn = new NpgsqlConnection(ConnectionString);
                await conn.OpenAsync();

                await using var cmd = new NpgsqlCommand("SELECT revenue FROM users_revenue WHERE user_id = @id", conn);
                cmd.Parameters.AddWithValue("id", userId!);// Use 'userId!' to suppress nullable warning since this endpoint cannot be hit without it.


                var result = await cmd.ExecuteScalarAsync();

                if (result == null)
                {
                    context.Response.StatusCode = 404;
                    await context.Response.WriteAsync("User not found.");
                }
                else
                {
                    var response = JsonSerializer.Serialize(new { userId = userId, revenue = result });
                    context.Response.ContentType = "application/json";
                    await context.Response.WriteAsync(response);
                }
            });

            app.MapGet("/", async context =>
            {
                await context.Response.WriteAsync("Hello world");
            });

            app.Run("http://localhost:8000");
        }
    }
}
