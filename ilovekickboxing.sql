WITH meeting_events AS (SELECT
            ID,
            EVENTTYPE,
            STUDIOID,
            CATEGORY,
            SUBCATEGORY,
            STAFFID,
            STAFFNAME,
            CASE WHEN extract(year from CREATEDATE) < 2020
            THEN date('2020-04-06')
            ELSE CREATEDATE END AS CREATEDATE,
            DURATIONINSECONDS,
            SCHEDULEDSTARTTIME,
            SCHEDULEDENDTIME,
            NUMBEROFATTENDEES
            FROM "MBO_VIRTUAL_PREP"."MEETING_EVENTS_V1"
            UNION
          SELECT
            MEETING_MEETINGID,
            'meeting:ended' as EVENTTYPE,
            MEETING_SUBSCRIBERID AS STUDIOID,
            NULL AS CATEGORY, NULL AS SUBCATEGORY,
            MEETING_STAFFID, NULL AS STAFFNAME,
            MEETING_STARTTIME AS CREATEDATE,
            TIMESTAMPDIFF('SECOND', MEETING_ENDTIME, MEETING_STARTTIME) AS DURATIONINSECONDS,
            MEETING_STARTTIME AS SCHEDULEDSTARTTIME,
            MEETING_ENDTIME AS SCHEDULEDENDTIME,
            NUMBEROFATTENDEES
            FROM "MBO_VIRTUAL_PREP"."MEETINGS"
            )
SELECT
	meeting_events.STUDIOID, mbaccountnumber,
	TO_CHAR(date_trunc('week', meeting_events."CREATEDATE") , 'YYYY-MM-DD HH24:MI:SS') AS "week_start_date",
	TO_CHAR(meeting_events."CREATEDATE", 'YYYY-MM-DD HH24:MI:SS') AS "stream_date",
	COALESCE(SUM((meeting_events."NUMBEROFATTENDEES") ) + 1, 0) AS "meeting_events.class_attendee_count"
FROM meeting_events
left join
(select studioid, mbaccountnumber,
 row_number() over (partition by studioid order by mbaccountnumber) as rn1
  from "MINDBODY"."MBO_CLIENT_PREP"."LOCATION") loc
on meeting_events.studioid = loc.studioid::text and rn1=1

WHERE
      ((meeting_events."CREATEDATE" < (DATEADD('day', 1, CURRENT_DATE()))))
  AND ((meeting_events."EVENTTYPE"  IN ('meeting:ended', 'MeetingEnded')))
  AND (((date_trunc('week', meeting_events."CREATEDATE")) >= (DATEADD('day', -42, DATE_TRUNC('week', CURRENT_DATE())))))
  --AND (((date_trunc('week', meeting_events."CREATEDATE")) < (DATEADD('day', -6, CURRENT_DATE()))))
and meeting_events.studioid is not null
GROUP BY meeting_events.STUDIOID, mbaccountnumber, date_trunc('week', meeting_events."CREATEDATE"), meeting_events."CREATEDATE"
ORDER BY 3 desc, 4 desc, 2
