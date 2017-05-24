select 
	te.BrcId, MIN(te.accepted_date) as entry_addiction, MIN(ev.Start_Date) as entry_date, MIN(te.discharge_date) as exit_addiction
	from
	(
		(select 
		referral_Admin_status_id,brcid, cag, accepted_date, 
		discharge_date = case	
		when discharge_Date is null or discharge_Date = '01-jan-1900' then getdate() else discharge_date end
		, site_code, location_udf_6, location_udf_8, cn_doc_id
		from team_episode te
		where
		(referral_admin_status_id = 'discharged' or
		referral_admin_status_id = 'accepted')
		and
		accepted_date >= '01-jan-2007'
		and
		(CAG = 'addictions' or location_udf_8 = 'addictions') 
		) 
		union all
		(
		select 
		current_ward_stay_status_id,brcid, cag, actual_start_date accepted_date, 
		discharge_date = case
		when actual_end_date is null or actual_End_date = '01-jan-1900' then getdate() else actual_end_date end
		, site_code, location_udf_6, location_udf_8, cn_doc_id
		from ward_stay ws
		where
		(current_ward_stay_status_id = 'closed' or
		current_ward_stay_status_id = 'Bed Occupied'or
		current_ward_stay_status_id = 'delay'or
		current_ward_stay_status_id = 'absent')
		and
		actual_start_date >= '01-jan-2007'
		and
		Location_Name like 'add%' 
		)
	)te
	join
	event ev
	on ev.brcid = te.brcid and ev.site_code = te.site_code
	where
	(event_outcome_id = 'attended' or
	event_outcome_id like '%was seen%' or
	event_outcome_id like '%inpatient%')
	and Event_Type_Of_Contact_ID like '%face%'
	and ev.start_date between te.accepted_date and discharge_date
	and ev.Location_Name like 'add%' 
	group by te.BrcId
	
