using System.Text;

namespace EtlClient
{
    class Program
    {
        private static readonly string EventsFilePath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "events.jsonl"));

        private const string ServerUrl = "http://localhost:8000/liveEvent";
        private const string Secret = "secret";

        public static async Task Main(string[] args)
        {
            if (!File.Exists(EventsFilePath))
            {
                Console.WriteLine($"File not found: {EventsFilePath}");
                return;
            }

            using var client = new HttpClient();
            client.DefaultRequestHeaders.Add("Authorization", Secret);
                        
            var lines = await File.ReadAllLinesAsync(EventsFilePath); 

            foreach (var line in lines)
            {
                var content = new StringContent(line, Encoding.UTF8, "application/json");

                try
                {
                    var response = await client.PostAsync(ServerUrl, content); // send to server for event logging.
                   
                   // Optional: Log or display responseText:
                   // var responseText = await response.Content.ReadAsStringAsync();

                    Console.WriteLine($"Sent event: {line} - Status: {response.StatusCode}");

                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error sending event: {ex.Message}");
                }
            }

            Console.WriteLine("All events sent.");
        }

    }
}
