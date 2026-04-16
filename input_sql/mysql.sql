-- [MySQL] 백틱(`)으로 감싸진 식별자 테스트
WITH `recent_users` AS (
    SELECT `user_id`, `login_date`
    FROM `db_user`.`login_history`
    WHERE `login_date` > '2026-01-01'
)
SELECT 
    u.`username`, 
    ru.`login_date`
FROM `db_user`.`users` u
JOIN `recent_users` ru 
  ON u.`user_id` = ru.`user_id`
-- JOIN `db_user`.`banned_users` bu ON u.user_id = bu.user_id
WHERE u.`is_active` = 1;