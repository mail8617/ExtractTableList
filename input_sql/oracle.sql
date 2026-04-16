/* [Oracle] 
   1. 복잡한 블록 주석 제외 테스트
   2. 들여쓰기와 줄바꿈이 불규칙한 형태 파싱 테스트
*/
WITH emp_hierarchy AS (
    SELECT emp_id, manager_id, NVL(salary, 0) as salary
    FROM a.hr.employees
    WHERE department_id = 10
)
SELECT 
    e.emp_id, 
    d.department_name 
FROM 
    
    emp_hierarchy e
    
INNER JOIN 
    hr.departments d 
    ON e.manager_id = d.manager_id
    
/* 퇴사자 테이블 조인 보류
LEFT OUTER JOIN hr.retired_emps re 
    ON e.emp_id = re.emp_id 
*/
WHERE ROWNUM <= 50;