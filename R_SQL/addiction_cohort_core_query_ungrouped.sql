
SELECT

	te.BrcId, te.accepted_date AS entry_addiction, ev.Start_Date AS entry_date, te.discharge_date AS exit_addiction,
	ev.Location_Name, ev.event_outcome_id, te.cag, te.referral_admin_status_id, te.location_udf_8, ev.site_code

	FROM

	(
	
		(SELECT 

		referral_Admin_status_id, brcid, cag, accepted_date, 

		discharge_date = CASE	

		WHEN discharge_Date is null or discharge_Date = '01-jan-1900' then getdate() else discharge_date end

		, site_code, location_udf_6, location_udf_8, cn_doc_id

		FROM team_episode te

		WHERE

		(referral_admin_status_id = 'discharged' or

		referral_admin_status_id = 'accepted')

		and

		accepted_date >= '01-jan-2007'

		and

		(CAG = 'addictions' or location_udf_8 = 'addictions') 

		) 

	)te

	join

	event ev

	on ev.brcid = te.brcid and ev.site_code = te.site_code

	where

	(event_outcome_id = 'attended' or

	event_outcome_id like '%was seen%')

	and Event_Type_Of_Contact_ID like '%face%'

	and ev.start_date between te.accepted_date and discharge_date

	and ev.Location_Name like 'add%' 
