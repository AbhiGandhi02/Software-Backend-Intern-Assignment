-- Insert Departments
INSERT INTO departments (department_name, department_code) VALUES 
('Computer Science', 'CS'),
('Mathematics', 'MATH'),
('Physics', 'PHY');

-- Insert Students
INSERT INTO students (first_name, last_name, email, phone, enrollment_year) VALUES 
('Alice', 'Wonder', 'alice@university.edu', '555-1234', 1),
('Bob', 'Builder', 'bob@university.edu', '555-5678', 2);

-- Insert Courses
INSERT INTO courses (course_name, credits, department_id) VALUES 
('Intro to CS', 4, 1),
('Calculus I', 3, 2),
('Physics 101', 4, 3);

-- Insert Enrollments
INSERT INTO enrollments (student_id, course_id, grade) VALUES 
(1, 1, 'A'), -- Alice in CS
(1, 2, 'B'), -- Alice in Math
(2, 3, 'A'); -- Bob in Physics