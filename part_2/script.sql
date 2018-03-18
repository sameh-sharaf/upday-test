-- session_events table structure
CREATE TABLE session_events (
	id              VARCHAR(50) UNIQUE NOT NULL,
	user_id         VARCHAR(50),
	session_id      VARCHAR(50),
	event_name      VARCHAR(100),
	event_duration  INT,
	at              TIMESTAMP
);

-- screen_flows table structure
CREATE TABLE screen_flows (
	session_id      VARCHAR(50),
  at              TIMESTAMP,
	name_1          VARCHAR(100),
  duration_1      INT,
  event_count_1   INT,
  name_2          VARCHAR(100),
  duration_2      INT,
  event_count_2   INT,
  name_3          VARCHAR(100),
	duration_3      INT,
	event_count_3   INT
); 

-- Aggregate event records and insert into screen_flows
INSERT INTO screen_flows
WITH cte_1 AS (
	SELECT
		session_id,
		event_name,
		MIN(at) AS min_at,
		SUM (event_duration) AS event_duration,
		COUNT(*) AS event_count
	FROM
		session_events
	GROUP BY
		session_id,
		event_name
),

cte_2 AS (
	SELECT
		a.*,
		row_number() OVER (PARTITION BY session_id ORDER BY MIN_at) AS event_ORDER,
		MIN(MIN_at) OVER (PARTITION BY session_id) AS first_event_at
	FROM cte_1 a
),

cte_3 AS (
SELECT
	DISTINCT session_id,
	first_event_at AS at,
	(SELECT event_name FROM cte_2 a WHERE a.session_id = b.session_id AND event_ORDER = 1) AS name_1,
	(SELECT event_duration FROM cte_2 a WHERE a.session_id = b.session_id AND event_ORDER = 1) AS duration_1,
	(SELECT event_COUNT FROM cte_2 a WHERE a.session_id = b.session_id AND event_ORDER = 1) AS event_COUNT_1,

	(SELECT event_name FROM cte_2 a WHERE a.session_id = b.session_id AND event_ORDER = 2) AS name_2,
	(SELECT event_duration FROM cte_2 a WHERE a.session_id = b.session_id AND event_ORDER = 2) AS duration_2,
	(SELECT event_COUNT FROM cte_2 a WHERE a.session_id = b.session_id AND event_ORDER = 2) AS event_COUNT_2,

	(SELECT event_name FROM cte_2 a WHERE a.session_id = b.session_id AND event_ORDER = 3) AS name_3,
	(SELECT event_duration FROM cte_2 a WHERE a.session_id = b.session_id AND event_ORDER = 3) AS duration_3,
	(SELECT event_COUNT FROM cte_2 a WHERE a.session_id = b.session_id AND event_ORDER = 3) AS event_COUNT_3
FROM
	cte_2 b
ORDER BY
	session_id
)

SELECT *
FROM cte_3
ORDER BY at
;
