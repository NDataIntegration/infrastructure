
getSqlSetupTemp <- function(startDate, endDate){
	sql <- paste("
		create TEMPORARY table person_demo as
		select
			d.household_id,
			a.PERSON_ID,
			AGE,
			GENDER_CODE		
		from person_dimension a 
		inner join 
		(
			select
				b.household_id,
				a.person_id,
				max(a.person_key) as person_key,
				sum(b.person_intab_flag) as num_intab_days
			from person_dimension a
			
			inner join person_installed_fact b 
				on a.person_key = b.person_key 
				and a.sample_type_key = b.sample_type_key
				
			inner join ny_date_dimension c 
				on c.date_key = b.date_key
				
			where b.sample_type_key = 100 
				and b.person_installed_flag = 1
				and a.long_term_visitor_flag = 'No' 
				and a.short_term_visitor_flag= 'No'
				and c.nmr_date between '%s' and '%s'
				
			group by b.household_id, a.person_id
		) d	
			on a.person_key = d.person_key
			
		where d.num_intab_days > 0
		order by household_id, person_id;
		
		create temp table program_minutes as
		select		
			did.interface_date as date,
			ppvf.TIME_KEY,
			ppvf.date_key,
			ppvf.TIME_ZONE_KEY,
			ppvf.STATION_KEY,
			ppvf.HOUSEHOLD_ID,
			ppvf.HOUSEHOLD_KEY,
			ppvf.PERSON_KEY,
			pd.person_id,
			td.program_long_name as program,
			td.episode_long_name as episode,
			td.episode_number,
			td.telecast_key,
			td.Program_Distributor_Name,
			td.TELECAST_REPORT_DURATION,
			td.TELECAST_PROP_REPEAT_FLAG as repeat_flag,
			td.telecast_nonproprietary_repeat_flag as np_repeat_flag,
			sd.distributor_type_code, 
			sd.distributor_type_desc,
			pif.Daily_Person_Weight,
			CASE 
				WHEN ppvf.PLAY_DELAY_KEY = 1 THEN 1 
				ELSE 0 
			END as live_minutes,
			
			CASE 
				WHEN pdd.PLAY_DELAY_SAME_DAY_FLAG = 'Yes' THEN 1 
				ELSE 0 
			END as sd_minutes,
			
			CASE 
				WHEN ppvf.PLAY_DELAY_KEY > 1 and ppvf.PLAY_DELAY_KEY < 10080 THEN 1 
				ELSE 0 
			END as sevend_minutes			 
			
		from  
			PROGRAM_PERSON_TUNER_VIEWING_FACT ppvf
			
			inner join STATION_DIMENSION sd
				on ppvf.STATION_KEY = sd.STATION_KEY
				
			inner join NY_DATE_DIMENSION ndd
				on ppvf.Person_Viewing_Weight_Date_Key = ndd.Date_Key
				
			inner join PLAY_DELAY_DIMENSION pdd
				on ppvf.PLAY_DELAY_KEY = pdd.PLAY_DELAY_KEY
			
			inner join Telecast_Dimension td
				on td.Telecast_key = ppvf.Telecast_key
				AND td.Sample_type_key = 100
				AND td.Telecast_Report_Duration >= 5
				AND td.Program_Cutback_flag = 1		
				AND td.Valid_data_flag = 1
				
			inner join Person_Installed_Fact pif
				ON pif.Date_Key = ndd.Date_Key
				AND pif.Concat_Household_person_id = ppvf.Concat_household_person_id
				AND pif.sample_type_key = 100
				
			inner join Person_Dimension pd
				ON pd.Person_key = pif.Person_key 
				and pd.short_term_Visitor_Flag = 'No'
				and pd.long_term_visitor_flag = 'No' 
				and pd.Building_Block_Break <> 'N/A'
				
			inner join Telecast_Date_Xref_Cplx tdxc
				ON tdxc.Telecast_Key = ppvf.Telecast_key
				and tdxc.sample_type_key = 100
			
			inner join Date_Interface_Dimension did
				ON  did.Date_interface_key = tdxc.date_interface_key
				AND did.interface_date between '%s' and '%s'
				  
			inner join broadcast_date_dimension bdd
				on bdd.broadcast_date_key = ppvf.broadcast_date_key
				AND bdd.broadcast_date between '%s' and '%s'
			
		WHERE 
			ppvf.VALID_DATA_FLAG = 1
			and ppvf.SIMULTANEOUS_VIEWING_FLAG = 0
			and ppvf.report_type_key = 100
			and (td.program_long_name = 'MODERN FAMILY' or td.program_long_name = 'CASTLE' or td.program_long_name = 'WALKING DEAD')
			-- and ppvf.time_zone_key = 1
		;
			
		create temp table viewed as
		select program, household_id, person_id, case when count(TELECAST_REPORT_DURATION) > 0 then 1 else 0 end as viewed
		from program_minutes
		where repeat_flag = 'No'
			--and TIME_ZONE_KEY = 1
		group by program, household_id, person_id
		order by program, household_id, person_id;
	")
	
	return(sprintf(sql, startDate, endDate, startDate, endDate, startDate, endDate))
}
# ===========================================================================================================

getSqlGetViewed <- function(){
	sql <- paste("
		select a.program, a.household_id, a.person_id, case when b.viewed is null then 0 else b.viewed end as viewed
		from 
		(
				select program, household_id, person_id
				from person_demo 
				cross join 
				(
						select distinct program
						from viewed
				) a
		) a
		left join viewed b
			on a.household_id = b.household_id
			and a.person_id = b.person_id
			and a.program = b.program
	")
}