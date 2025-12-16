-- Procedure: Auto-Register Student
-- Input: Name, Email, Course Name
-- Logic: Creates student if new, finds course, enrolls them.
CREATE OR REPLACE PROCEDURE register_student(
    p_first_name VARCHAR, 
    p_last_name VARCHAR, 
    p_email VARCHAR,
    p_course_name VARCHAR
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_student_id INT;
    v_course_id INT;
BEGIN
    -- 1. Get or Create Student
    INSERT INTO students (first_name, last_name, email)
    VALUES (p_first_name, p_last_name, p_email)
    ON CONFLICT (email) DO UPDATE SET email = p_email -- No-op to ensure we can get ID
    RETURNING student_id INTO v_student_id;

    -- If student existed, the RETURNING clause might be skipped, so fetch ID manually
    IF v_student_id IS NULL THEN
        SELECT student_id INTO v_student_id FROM students WHERE email = p_email;
    END IF;

    -- 2. Get Course ID from Name
    SELECT course_id INTO v_course_id FROM courses WHERE course_name = p_course_name;

    -- 3. Enroll (only if course exists)
    IF v_course_id IS NOT NULL THEN
        INSERT INTO enrollments (student_id, course_id, grade) 
        VALUES (v_student_id, v_course_id, NULL)
        ON CONFLICT DO NOTHING;
    END IF;
END;
$$;