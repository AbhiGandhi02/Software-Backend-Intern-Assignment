DROP TABLE IF EXISTS netflix;

CREATE TABLE netflix (
    show_id VARCHAR(10) PRIMARY KEY,
    type VARCHAR(20),      
    title VARCHAR(255),
    director TEXT,
    cast_members TEXT,      
    country VARCHAR(255),
    date_added VARCHAR(50), 
    release_year INT,
    rating VARCHAR(20),
    duration VARCHAR(20),
    listed_in TEXT,
    description TEXT
);