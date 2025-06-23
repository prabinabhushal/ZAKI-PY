
select * from school;
select * from student;
select * from teacher;

-- 1. Write a query to count the number of students in each school and display the school name and the student count, ordered by the count in descending order.
SELECT DISTINCT school_name,
COUNT(student_id) OVER (PARTITION BY school_name) AS st_count
FROM student st
JOIN school sc ON sc.school_id = st.school_id
ORDER BY st_count DESC;


-- 2.Calculate the average salary of teachers in each department across all schools, displaying only departments with an average salary above $55,000.

with cte_teacher as (
SELECT DISTINCT department,
round(AVG(salary) OVER (PARTITION BY department),2)AS avg_salary
FROM teacher
)
select *
from cte_teacher
WHERE avg_salary > 55000;


-- 3.Rank teachers within each school based on their salary (highest salary gets rank 1), displaying the teacher’s name, school name, salary, and rank.

with cte_teacher as (
select salary,school_id,
UPPER (first_name || ' ' || last_name) AS teacher_name,
DENSE_RANK() OVER (partition by school_id ORDER BY salary DESC) as rank 
from teacher
)
select teacher_name,school_name,salary, rank 
from cte_teacher t
join school s on  t.school_id = s.school_id;

-- 4.Create a function to calculate a student’s seniority (years since enrollment) and use it to list students with their seniority.

CREATE OR REPLACE FUNCTION functions.calculate_seniority(enroll_date DATE)
RETURNS varchar AS $$
BEGIN
    RETURN DATE_PART('year', AGE(CURRENT_DATE, enroll_date)) || 'year';
END;
$$ LANGUAGE plpgsql;

SELECT student_id,first_name || ' ' || last_name AS full_name,enrollment_date,
functions.calculate_seniority(enrollment_date) AS seniority_years
FROM student;

-- 5.Rank students within each school based on their grade (highest grade gets rank 1), and display the school name, student name (full_name), grade, and rank.

with cte_student as (
select grade,school_id,
UPPER (first_name || ' ' || last_name) AS student_name,
dense_rank() over (partition by school_id order by grade desc) as rank 
from student
)
select school_name,student_name,grade, rank 
from cte_student st
join school s on  st.school_id = s.school_id;


-- 6.Calculate the cumulative salary of teachers within each school, ordered by hire date (earliest hired first), and display the school name,
-- teacher name, hire date, salary, and cumulative salary.

with cte_teacher as (
select salary,school_id,hire_date,
UPPER (first_name || ' ' || last_name) AS teacher_name,
SUM(salary) over (partition by school_id) as cumulative_salary 
from teacher
)
select school_name,teacher_name,hire_date,salary,cumulative_salary
from cte_teacher t
join school s on  t.school_id = s.school_id
order by hire_date;


-- 7.For each student, display their school name, teacher’s name, student’s name, and the number of students assigned to their teacher. 
-- Additionally, rank teachers within each school based on the number of students they have (highest student count gets rank 1).

with student_cte as (
select school_name,teacher_id,
UPPER (first_name || ' ' || last_name) AS student_name,
count(student_id) over (partition by teacher_id) as assigned_student
from student s
join school sc on s.school_id = sc.school_id
)
select school_name,student_name,assigned_student,
UPPER (first_name || ' ' || last_name) AS teacher_name
from student_cte st
join teacher t on st.teacher_id = t.teacher_id


