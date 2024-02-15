/* Student, Zongyu Wu -- CS5200-05, Spring 2024 */
/* Q1 */
SELECT
  count(*)
FROM
  Worker
WHERE
  DEPARTMENT = "HR"
  and SALARY > 250000;

/* Q2 */
SELECT
  w.LAST_NAME,
  t.WORKER_TITLE,
  w.DEPARTMENT
FROM
  Worker w
  JOIN Title t ON w.WORKER_ID = t.WORKER_REF_ID
WHERE
  w.SALARY < (
    SELECT
      SUM(SALARY) * 1.0 / COUNT(WORKER_ID)
    FROM
      Worker
  );

/* Q3 */
select
  DEPARTMENT,
  SUM(SALARY) * 1.0 / COUNT(*) as AvgSal,
  COUNT(*) as Num
from
  Worker
group by
  DEPARTMENT;

/* Q4 */
select
  w.FIRST_NAME,
  w.LAST_NAME,
  t.WORKER_TITLE,
  ROUND(
    (
      w.SALARY + (
        CASE
          when w.WORKER_ID in (
            select
              WORKER_REF_ID
            from
              Bonus
          ) then b.BONUS_AMOUNT
          ELSE 0
        END
      )
    ) * 1.0 / 12
  ) as MonthlyComp
from
  Worker w
  left JOIN (
    select
      WORKER_REF_ID,
      sum(BONUS_AMOUNT) as BONUS_AMOUNT
    from
      Bonus
    group by
      WORKER_REF_ID
  ) as b on w.WORKER_ID = b.WORKER_REF_ID
  left JOIN Title t on w.WORKER_ID = t.WORKER_REF_ID;

/* Q5 */
select
  UPPER(w.FIRST_NAME),
  UPPER(w.LAST_NAME)
from
  Worker w
where
  w.WORKER_ID not in (
    select
      WORKER_REF_ID
    from
      Bonus
  );

/* Q6 */
select
  w.FIRST_NAME,
  w.LAST_NAME
from
  Worker w
  join Title t on w.WORKER_ID = t.WORKER_REF_ID
where
  t.WORKER_TITLE LIKE "%Manager%";