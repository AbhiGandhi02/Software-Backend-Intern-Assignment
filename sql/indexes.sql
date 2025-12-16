-- Advanced indexing strategy for improved query performance

CREATE INDEX idx_students_email ON students(email);
CREATE INDEX idx_courses_dept ON courses(department_id);

-- 1. Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_enrollments_student_course 
ON enrollments(student_id, course_id);

CREATE INDEX IF NOT EXISTS idx_enrollments_course_grade 
ON enrollments(course_id, grade) 
WHERE grade IS NOT NULL;

-- 2. Partial indexes for filtering
CREATE INDEX IF NOT EXISTS idx_students_recent 
ON students(enrollment_year, created_at) 
WHERE enrollment_year >= EXTRACT(YEAR FROM CURRENT_DATE) - 1;

-- 3. Netflix full-text search indexes
CREATE INDEX IF NOT EXISTS idx_netflix_title_gin 
ON netflix USING gin(to_tsvector('english', title));

CREATE INDEX IF NOT EXISTS idx_netflix_description_gin 
ON netflix USING gin(to_tsvector('english', description));

CREATE INDEX IF NOT EXISTS idx_netflix_type_year 
ON netflix(type, release_year);

CREATE INDEX IF NOT EXISTS idx_netflix_rating 
ON netflix(rating) 
WHERE rating IS NOT NULL;

-- 4. Titanic analytical indexes
CREATE INDEX IF NOT EXISTS idx_titanic_survival 
ON titanic(survived, pclass);

CREATE INDEX IF NOT EXISTS idx_titanic_age 
ON titanic(age) 
WHERE age IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_titanic_fare 
ON titanic(fare) 
WHERE fare > 0;

CREATE INDEX IF NOT EXISTS idx_titanic_sex_class 
ON titanic(sex, pclass, survived);

-- 5. Performance improvement indexes
CREATE INDEX IF NOT EXISTS idx_courses_name 
ON courses(course_name);

CREATE INDEX IF NOT EXISTS idx_departments_code 
ON departments(department_code);

-- 6. Index for common joins
CREATE INDEX IF NOT EXISTS idx_enrollments_date 
ON enrollments(enrollment_date DESC);

-- Analyze tables to update statistics after creating indexes
ANALYZE students;
ANALYZE courses;
ANALYZE departments;
ANALYZE enrollments;
ANALYZE netflix;
ANALYZE titanic;

-- Display index information
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;
