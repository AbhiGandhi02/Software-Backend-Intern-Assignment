-- schema.sql
-- 1. Clean up existing tables (for development only)
DROP TABLE IF EXISTS enrollments;
DROP TABLE IF EXISTS courses;
DROP TABLE IF EXISTS students;
DROP TABLE IF EXISTS departments;

-- 2. Create Departments Table
CREATE TABLE departments (
    department_id SERIAL PRIMARY KEY,
    department_name VARCHAR(100) UNIQUE NOT NULL,
    department_code VARCHAR(10)
);

-- 3. Create Students Table
CREATE TABLE students (
    student_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    enrollment_year INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Create Courses Table
CREATE TABLE courses (
    course_id SERIAL PRIMARY KEY,
    course_name VARCHAR(100) NOT NULL,
    credits INT CHECK (credits > 0),
    department_id INT REFERENCES departments(department_id) ON DELETE SET NULL
);

-- 5. Create Enrollments Table (The Join Table)
CREATE TABLE enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    student_id INT REFERENCES students(student_id) ON DELETE CASCADE,
    course_id INT REFERENCES courses(course_id) ON DELETE CASCADE,
    grade VARCHAR(5),
    enrollment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_enrollment UNIQUE (student_id, course_id)
);

-- 6. Add Indexes for Performance (Task 5 Requirement Preview)
CREATE INDEX idx_students_email ON students(email);
CREATE INDEX idx_courses_dept ON courses(department_id);