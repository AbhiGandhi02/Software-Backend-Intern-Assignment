# 🚀 Scalable Backend ETL & Automation Pipeline

![Node.js](https://img.shields.io/badge/Node.js-v18-green) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-NeonDB-blue) ![Google Sheets API](https://img.shields.io/badge/Google_Sheets-Automation-green) ![Status](https://img.shields.io/badge/Status-Completed-brightgreen)

## 📌 Project Overview

This project is a comprehensive backend system designed to solve real-world data fragmentation problems. It features a robust **ETL (Extract, Transform, Load) pipeline** that ingests messy data from Google Sheets, CSVs, and JSON files into a normalized **PostgreSQL (NeonDB)** database.

It includes a custom **"Bridge Automation"** system that synchronizes data between Google Sheets and the database in real-time using **Google Apps Script** and **Node.js**, effectively bridging the gap between non-technical user interfaces and structured backend storage.

---

## 🏗️ Architecture

![System Architecture](./docs/architecture.png)

**The Workflow:**

1. **Ingest:** Data is entered into Google Sheets or provided via CSV/JSON.
2. **Validate:** Google Apps Script triggers on edit, validating data and marking rows as `⏳ Pending Sync`.
3. **Process:** Node.js ETL pipeline polls for pending rows, cleans data, and handles relational integrity.
4. **Load:** Data is inserted into **NeonDB (PostgreSQL)** using Stored Procedures.
5. **Feedback:** The system writes back `✅ Synced` or `❌ Error` to the Google Sheet.

---

## 🛠️ Tech Stack

* **Runtime:** Node.js
* **Database:** PostgreSQL (Serverless via NeonDB)
* **APIs:** Google Sheets API (v4), Google Apps Script
* **Libraries:** `pg` (node-postgres), `googleapis`, `csv-parser`, `dotenv`
* **Concepts:** 3NF Normalization, Idempotency, Indexing, ACID Transactions

---

## 📂 Project Structure

```bash
backend-assignment/
├── etl/
├── sql/
├── scripts/
├── students.csv           # Sample Local Data
├── students.json          # Sample Local Data
├── titanic.csv            # Titanic Dataset
├── netflix.csv            # Netflix Dataset
├── .gitignore             # Git Ignore Rules
├── package.json           # Dependencies
└── README.md              # Documentation
```

---

## ⚡ Key Features

### 1. Robust ETL Pipeline

* **Multi-Source Extraction:** Capable of reading from Google Sheets, local CSV, and JSON files seamlessly.
* **Data Cleaning:** Normalizes text, handles missing values (defaults), and parses dates.
* **Idempotency:** Uses `ON CONFLICT DO UPDATE/NOTHING` to allow safe re-runs without creating duplicates.

### 2. Google Sheets Automation (The "Bridge")

Solves the limitation of Apps Script's lack of Postgres support.

* **Frontend:** Apps Script validates email formats and provides visual feedback (Color coding).
* **Backend:** Node.js acts as the middleware worker, processing queue items and updating status.

### 3. Database Optimization

* **3NF Design:** Separated Students, Courses, Departments, and Enrollments tables.
* **Indexing:** Implemented B-Tree Index on `students(email)`.
* **Performance:** Reduced query cost from ~18.5 (Seq Scan) to ~8.0 (Index Scan).

### 4. Public Dataset Scalability

Proven adaptability by ingesting two external datasets:

* **Titanic:** 1,300+ rows (Numerical Analysis).
* **Netflix:** 8,800+ rows (Text/Content Analysis).

---

## 🚀 How to Run

### Prerequisites

* Node.js (v16 or higher) installed
* A [NeonDB](https://neon.tech) (PostgreSQL) account
* Google Cloud Console Project (with Sheets API enabled)
* Git installed

### 1. Installation

```bash
git clone https://github.com/AbhiGandhi02/Software-Backend-Intern-Assignment.git
cd backend-assignment
npm install
```

### 2. Environment Setup

Create a `.env` file in the root:

```env
DATABASE_URL="postgres://user:password@endpoint.neon.tech/neondb?sslmode=require"
```

**For Google Sheets Integration:**

1. Create a project in [Google Cloud Console](https://console.cloud.google.com/)
2. Enable Google Sheets API
3. Create a Service Account and download the JSON key
4. Rename it to `service-account.json` and place it in the root directory
5. Share your Google Sheet with the service account email

### 3. Database Setup

Run the SQL scripts in order:

```bash
# 1. Create the schema
node sql/schema.sql

# 2. Seed initial data
node sql/seed.sql

# 3. Create views
node sql/views.sql

# 4. Create stored procedures
node sql/procedures.sql

# 5. (Optional) Setup additional datasets
node sql/titanic.sql
node sql/netflix.sql
```

### 4. Run the Pipelines

**Run the Main University Sync:**

```bash
node etl/etl.js
```

**Run Public Dataset Tests:**

```bash
node etl/titanic_etl.js
node etl/netflix_etl.js
```

**Test Database Connection:**

```bash
node scripts/test-connection.js
```

---

## 📊 Analytics & Reporting

The project includes SQL scripts to generate business insights.

### Example 1: Average GPA by Course

```sql
SELECT 
    c.course_name, 
    ROUND(AVG(
        CASE 
            WHEN e.grade = 'A' THEN 4.0
            WHEN e.grade = 'B' THEN 3.0
            WHEN e.grade = 'C' THEN 2.0
            WHEN e.grade = 'D' THEN 1.0
            ELSE 0.0
        END
    ), 2) as avg_gpa
FROM courses c 
JOIN enrollments e ON c.course_id = e.course_id 
WHERE e.grade IS NOT NULL
GROUP BY c.course_name
ORDER BY avg_gpa DESC;
```

### Example 2: Students per Department

```sql
SELECT 
    d.department_name, 
    COUNT(DISTINCT s.student_id) as total_students
FROM departments d
LEFT JOIN courses c ON d.department_id = c.department_id
LEFT JOIN enrollments e ON c.course_id = e.course_id
LEFT JOIN students s ON e.student_id = s.student_id
GROUP BY d.department_name
ORDER BY total_students DESC;
```

### Example 3: Titanic Survival Rate by Class

```sql
SELECT 
    pclass as passenger_class,
    COUNT(*) as total_passengers,
    SUM(survived) as survived_count,
    ROUND((SUM(survived)::decimal / COUNT(*)) * 100, 1) as survival_rate
FROM titanic 
GROUP BY pclass
ORDER BY pclass;
```

### Example 4: Netflix Content Analysis

```sql
SELECT 
    type,
    COUNT(*) as total_titles,
    ROUND(AVG(release_year), 0) as avg_release_year
FROM netflix
GROUP BY type
ORDER BY total_titles DESC;
```

---

## 🧪 Testing

### Manual Testing Checklist

- [x] Database connection test
- [x] Schema creation and constraints
- [x] CSV file parsing and import
- [x] JSON file parsing and import
- [x] Google Sheets read/write operations
- [x] Idempotency verification (re-run without duplicates)
- [x] Stored procedure execution
- [x] Index performance comparison
- [x] Error handling for invalid data
- [x] Large dataset handling (8,800+ Netflix rows)

### Performance Metrics

| Operation | Rows Processed | Execution Time | Notes |
|-----------|---------------|----------------|-------|
| Titanic ETL | 1,309 | ~3-5 seconds | Numerical data |
| Netflix ETL | 8,807 | ~15-20 seconds | Text-heavy data |
| Sheet Sync | Variable | ~2-3 sec/row | Network dependent |

---

## 🔐 Security & Best Practices

* **Environment Variables:** All sensitive credentials stored in `.env` (gitignored)
* **Service Account:** Google API keys kept in `service-account.json` (gitignored)
* **SQL Injection Prevention:** Uses parameterized queries (`$1, $2, etc.`)
* **Error Handling:** Try-catch blocks with detailed logging
* **Data Validation:** Email format checking, type coercion, null handling

---

## 📝 Database Schema

### Core Tables

**Students**
```sql
student_id (PK) | first_name | last_name | email (UNIQUE) | enrollment_date
```

**Departments**
```sql
department_id (PK) | department_name | department_code
```

**Courses**
```sql
course_id (PK) | course_name | department_id (FK) | credits
```

**Enrollments**
```sql
enrollment_id (PK) | student_id (FK) | course_id (FK) | grade
```

### Additional Datasets

**Titanic**
```sql
passenger_id (PK) | survived | pclass | name | sex | age | sibsp | parch | ticket | fare | cabin | embarked
```

**Netflix**
```sql
show_id (PK) | type | title | director | cast_members | country | date_added | release_year | rating | duration | listed_in | description
```

---

## 🎯 Project Highlights

### Technical Achievements

✅ **3NF Normalization** - Eliminated data redundancy  
✅ **Stored Procedures** - Automated complex business logic  
✅ **Indexing Strategy** - 53% query performance improvement  
✅ **Idempotent Design** - Safe for repeated execution  
✅ **Multi-Source ETL** - Sheets, CSV, JSON support  
✅ **Real-time Sync** - Bi-directional Google Sheets integration  
✅ **Scalability Proof** - Tested with 8,800+ row dataset  

### Problem Solving

**Challenge:** Google Apps Script cannot directly connect to PostgreSQL.  
**Solution:** Created a two-tier architecture where Apps Script handles validation/UI, and Node.js handles database operations.

**Challenge:** Google Sheets API rate limits and quotas.  
**Solution:** Implemented batch processing and status tracking to minimize API calls.

**Challenge:** Data inconsistency from multiple sources.  
**Solution:** Robust transformation layer with defaults, type coercion, and normalization.

---

## 👨‍💻 Author

**Abhi Gandhi**  
Backend Engineering Intern Applicant

---

## 🙏 Acknowledgments

* **NeonDB** - Serverless PostgreSQL platform
* **Google Cloud** - Sheets API and service infrastructure
* **Kaggle** - Public datasets (Titanic, Netflix)
* **PostgreSQL Community** - Excellent documentation


---

**Built with ❤️ for scalable data engineering**
