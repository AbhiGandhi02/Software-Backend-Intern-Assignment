-- 1. Aggregation: Count of Students per Department
-- Shows which departments are busiest
SELECT d.department_name, COUNT(s.student_id) as total_students
FROM departments d
LEFT JOIN courses c ON d.department_id = c.department_id
LEFT JOIN enrollments e ON c.course_id = e.course_id
LEFT JOIN students s ON e.student_id = s.student_id
GROUP BY d.department_name
ORDER BY total_students DESC;

-- 2. Complex Join: Average Grade Points per Course
-- Converts letter grades (A, B) to numbers (4.0, 3.0) for calculation
SELECT 
    c.course_name,
    COUNT(e.student_id) as enrolled_count,
    ROUND(AVG(CASE 
        WHEN e.grade = 'A' THEN 4.0
        WHEN e.grade = 'A-' THEN 3.7
        WHEN e.grade = 'B+' THEN 3.3
        WHEN e.grade = 'B' THEN 3.0
        WHEN e.grade = 'C' THEN 2.0
        ELSE 0 
    END), 2) as average_gpa
FROM courses c
JOIN enrollments e ON c.course_id = e.course_id
GROUP BY c.course_name;

-- 3. Filter: Find Students with Low Credits (At Risk)
SELECT first_name, last_name, email
FROM students
WHERE student_id IN (
    SELECT student_id 
    FROM enrollments 
    GROUP BY student_id 
    HAVING COUNT(course_id) < 2
);