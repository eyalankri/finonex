-- db.sql

-- Create the users_revenue table
CREATE TABLE IF NOT EXISTS users_revenue (
    user_id TEXT PRIMARY KEY,
    revenue INTEGER NOT NULL DEFAULT 0
);



