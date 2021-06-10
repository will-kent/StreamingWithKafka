SET 'auto.offset.reset' = 'earliest';

CREATE TABLE T_AUTHOR_AVERAGE_SUBMISSION_SCORE
AS
SELECT AUTHOR_FULLNAME AS KEY1
	,AS_VALUE(AUTHOR_FULLNAME) AS AUTHOR_FULLNAME
	,AVG(SCORE) AS AVG_AUTHOR_SCORE
FROM  SUBMISSIONS
WINDOW TUMBLING(SIZE 5 MINUTE)
GROUP BY AUTHOR_FULLNAME
HAVING COUNT(AUTHOR_FULLNAME) = 1
EMIT CHANGES
;