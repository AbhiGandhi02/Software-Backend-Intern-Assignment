-- View: Student Transcripts
-- Combines Student, Course, and Grade info into one clean table
CREATE OR REPLACE VIEW student_transcripts AS
SELECT 
    s.student_id,
    s.first_name || ' ' || s.last_name as full_name,
    s.email,
    c.course_name,
    d.department_name,
    e.grade,
    e.enrollment_date
FROM students s
JOIN enrollments e ON s.student_id = e.student_id
JOIN courses c ON e.course_id = c.course_id
JOIN departments d ON c.department_id = d.department_id;