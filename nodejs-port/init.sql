-- Initial database setup for testing
-- Create a sample table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create a table without primary key to test REPLICA IDENTITY FULL
CREATE TABLE IF NOT EXISTS events (
    event_type VARCHAR(50),
    data JSONB,
    timestamp TIMESTAMP DEFAULT NOW()
);

-- Insert some sample data
INSERT INTO users (name, email) VALUES
    ('John Doe', 'john@example.com'),
    ('Jane Smith', 'jane@example.com'),
    ('Bob Wilson', 'bob@example.com');

INSERT INTO events (event_type, data) VALUES
    ('user_login', '{"user_id": 1, "ip": "192.168.1.1"}'),
    ('page_view', '{"page": "/dashboard", "user_id": 2}');