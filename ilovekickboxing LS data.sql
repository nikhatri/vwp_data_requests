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
	--account.name as logo_name,
	meeting_events.STUDIOID, mbaccountnumber, logo.logo_name,
	TO_CHAR(date_trunc('week', meeting_events."CREATEDATE") , 'YYYY-MM-DD HH24:MI:SS') AS "week_start_date",
	TO_CHAR(meeting_events."CREATEDATE", 'YYYY-MM-DD HH24:MI:SS') AS "stream_date",
	COALESCE(SUM((meeting_events."NUMBEROFATTENDEES") ) + 1, 0) AS "meeting_events.class_attendee_count"
FROM meeting_events
left join
(select studioid, mbaccountnumber,
 row_number() over (partition by studioid order by mbaccountnumber) as rn1
  from "MINDBODY"."MBO_CLIENT_PREP"."LOCATION") loc
on meeting_events.studioid = loc.studioid::text and rn1=1
left join (select distinct mb_studio_id, logo_name from "MBANALYSIS"."TESTDSSCHEMA"."DIM_CUSTOMER_MINDBODY") logo
on (try_cast(meeting_events.STUDIOID as integer) = logo.mb_studio_id and logo.logo_name = 'iLoveKickboxing')

WHERE
      ((meeting_events."CREATEDATE" < (DATEADD('day', 1, CURRENT_DATE()))))
  AND ((meeting_events."EVENTTYPE"  IN ('meeting:ended', 'MeetingEnded')))
  --AND (((date_trunc('week', meeting_events."CREATEDATE")) >= (DATEADD('day', -42, DATE_TRUNC('week', CURRENT_DATE())))))
  --AND (((date_trunc('week', meeting_events."CREATEDATE")) < (DATEADD('day', -6, CURRENT_DATE()))))
and meeting_events.studioid is not null
and try_cast(meeting_events.studioid as integer) in (14293,422938,469799,561573,578326,581224,631517,643573,656601,664848,866825,992538,495267,975054,662051)
--and try_cast(meeting_events.studioid as integer) in (561573, 974313, 581224, 484718, 514339, 580710, 422938, 578326, 822608, 805097, 768658, 549604, 442735, 747026, 550995, 817455, 862461, 803667, 631517, 690616, 901313, 618284, 912017, 564327, 765846, 975054, 640755, 866825, 698966, 656601, 805037, 851977, 544648, 612594, 457944, 951980, 753017, 643573, 662051, 687316, 453682, 425024, 752056, 556234, 664848, 842707, 655857, 904158, 924116, 562552, 485714, 972914, 786069, 992538, 652658, 621598, 794357, 495267, 14293 , 885420, 975018, 469799, 670073, 443146, 692381, 765106, 796644, 960718, 922328, 796299, 881811, 714430, 448036, 538113, 750299, 890866, 495984, 701308, 989598)
GROUP BY --account.name,
meeting_events.STUDIOID, mbaccountnumber, logo.logo_name, date_trunc('week', meeting_events."CREATEDATE"), meeting_events."CREATEDATE"
ORDER BY 3 desc, 4 desc, 2
