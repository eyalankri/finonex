# ETL Assignment – C# Implementation

This project implements a simple ETL (Extract, Transform, Load) pipeline using three separate C# applications:

1. **Client** (console application) – reads events from a file and sends them to the server.  
2. **Server** (Web API) – receives events and appends them to a local file.  
3. **DataProcessor** (console application) – reads the server’s event log file and updates a PostgreSQL database.

---

## 📁 Project Structure

```
1. events.jsonl             # Input file for the Client
2. event_log.jsonl          # Server-generated log file used by the DataProcessor
3. Client/                  # Client project
4. ApiServer/               # Server project
5. DataProcessor/           # Data processor project
```

---

## 🔁 ETL Flow Summary

This section outlines the full flow of how data moves through the system:

### 🔹 Client
- Reads the `events.jsonl` file (a JSON Lines file containing raw event data).
- Sends each line as a POST request to the server at `http://localhost:8000/liveEvent`.

### 🔹 Server (http://localhost:8000)
The server exposes two endpoints:
- **GET** `/userEvents/{userId}` – Returns the current revenue for the specified user from the PostgreSQL database.
- **POST** `/liveEvent` – Accepts event data from the Client and appends each event to `event_log.jsonl`.  
  > ⚠️ Requires the header: `Authorization: secret`

### 🔹 DataProcessor
- Reads `event_log.jsonl` by default, or another specified file if provided as an argument.
- Calculates total revenue per user.
- Updates the `users_revenue` table in the PostgreSQL database.

---

## ✅ How to Run

### 🔧 Prerequisites
- Update the PostgreSQL connection string in the `Program.cs` files of both the `Server` and `DataProcessor` projects to match your local database setup. 
- Ensure the `events.jsonl` file is located at the root of the project folder. Each line must be a valid JSON object in the following format:

```json
{ "userId": "user1", "name": "add_revenue", "value": 1000 }
{ "userId": "user2", "name": "add_revenue", "value": 500 }
{ "userId": "user2", "name": "subtract_revenue", "value": 5 }
```

> Valid values for `name` are `"add_revenue"` and `"subtract_revenue"`.

---

### ▶️ Run the ApiServer
Open a **command prompt** in the `ApiServer` directory and run:
`dotnet run`

This will launch the API server at `http://localhost:8000`.



### ▶️ Run the Client
In the `Client` directory, run:
`dotnet run`

Once the client finishes sending events, the server writes them to the `event_log.jsonl` file in the server-side project folder.

### ▶️ Run the DataProcessor

In the `DataProcessor` directory, run:
`dotnet run`

To use a specific input file instead:
`dotnet run custom_file.txt`

