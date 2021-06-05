SET 'auto.offset.reset' = 'earliest';

CREATE TABLE T_SUBMISSION_AUTHOR_COMMENTS
AS
SELECT LINK_AUTHOR
	,COUNT(*) AS TOTAL_COMMENTS
FROM  COMMENTS
WINDOW TUMBLING(SIZE 24 HOUR)
GROUP BY LINK_AUTHOR
EMIT CHANGES
;