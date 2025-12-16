DROP TABLE IF EXISTS titanic;

CREATE TABLE titanic (
    passenger_id INT PRIMARY KEY,
    survived INT,
    pclass INT,
    name VARCHAR(255),
    sex VARCHAR(10),
    age FLOAT,
    sibsp INT, -- Siblings/Spouses (New column in this dataset)
    parch INT, -- Parents/Children (New column)
    ticket VARCHAR(50),
    fare FLOAT,
    cabin VARCHAR(50),
    embarked VARCHAR(5)
);