
--set hive.execution.engine=mr;


----------------------------------------------------------
--This section is a work around for missing records needed
--that get dropped in Checkplease


drop table if exists premier_extracts.wk_OR_SUPPLY;

create table premier_extracts.wk_OR_SUPPLY as
SELECT DISTINCT
  ol.LOG_ID,
  CAST(ol.SURGERY_DATE AS DATE) as SURGERY_DT,
--Added the code for location name fix as per Aaron on 01/25/2019
  CASE WHEN A_clarity_loc_orlog.LOC_NAME='HH OR HSH' THEN 'OR HSH' ELSE A_clarity_loc_orlog.LOC_NAME END AS LOC_NAME,
  A_zc_or_service_orlog.NAME as SERVICE_LINE,
  A_or_proc_orlog_allproc.or_proc_id as PROC_ID,
  A_or_proc_orlog_allproc.PROC_NAME,
  A_clarity_ser_orlog_surg.PROV_ID as SURGERY_PROV_ID,
  A_clarity_ser_orlog_surg.PROV_NAME AS SURGERY_PROV_NAME,
  D_OR_SUPPLY.SUPPLIES_USED*D_OR_SUPPLY.COST_PER_UNIT_OT as COST_OF_SUPPLIES,
  D_OR_SUPPLY.SUPPLIES_USED,

  ( D_OR_SUPPLY.SUPPLIES_WASTED ) * ( D_OR_SUPPLY.COST_PER_UNIT_OT )as WASTAGE_COST,
  D_OR_SUPPLY.PICK_LIST_ID,
  D_OR_SUPPLY.SUPPLY_NAME,
  D_OR_SUPPLY.COST_PER_UNIT_OT,
  zorw.NAME as REASON_FOR_WASTAGE,
  D_OR_SUPPLY.BIN_LOCATION,
  D_OR_SUPPLY.SUPPLIES_WASTED,
  D_OR_SUPPLY.SUPPLY_ID,
  osm.MAN_CTLG_NUM,
  zom.NAME AS MANUFACTURER_NAME,
  ol.ROOM_ID,
  A_clarity_ser_or_room.PROV_NAME AS ROOM_NAME,
  zors.NAME AS OR_STATUS,
    coalesce(
datediff(A_or_log_case_times_patleaveor.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_orlog_casetime_pat_leave_asu.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_or_log_case_times_dep_proc.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_orlog_casetimes_endopstpxarr.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_orlog_casetime_pat_arrrec.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_log_casetime_strt_perioppost.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_or_log_casetime_pt_arr_pacu1.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_or_log_casetime_pt_arr_pacu2.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_orlog_arrive_endo_peds_post.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_or_log_case_times_patenteror.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_orlog_casetime_pat_enter_asu.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_or_log_case_times_ent_proc.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
0) - coalesce(
datediff(A_or_log_case_times_patenteror.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_orlog_casetime_pat_enter_asu.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_or_log_case_times_ent_proc.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_or_log_case_times_patleaveor.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_orlog_casetime_pat_leave_asu.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_or_log_case_times_dep_proc.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_orlog_casetimes_endopstpxarr.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_orlog_casetime_pat_arrrec.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_log_casetime_strt_perioppost.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_or_log_casetime_pt_arr_pacu1.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_or_log_casetime_pt_arr_pacu2.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
datediff(A_orlog_arrive_endo_peds_post.TRACKING_TIME_IN, CAST('2000-01-01 00:00:00' AS timestamp ) ) ,
0) AS TIME_IN_MINS,
 D_OR_SUPPLY.LAWSON_SUPPLY_ID
FROM
  current_epic_prod.OR_PROC  A_or_proc_orlog_allproc
   JOIN current_epic_prod.OR_LOG_ALL_PROC olap
  ON (olap.OR_PROC_ID=A_or_proc_orlog_allproc.OR_PROC_ID)
   JOIN current_epic_prod.OR_LOG ol ON (ol.LOG_ID=olap.LOG_ID)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_ALL_SURG olas ON (ol.LOG_ID=olas.LOG_ID)
   LEFT OUTER JOIN current_epic_prod.CLARITY_SER  A_clarity_ser_orlog_surg ON (olas.SURG_ID=A_clarity_ser_orlog_surg.PROV_ID)
   LEFT OUTER JOIN current_epic_prod.ZC_OR_STATUS zors ON (ol.STATUS_C=zors.STATUS_C)
   LEFT OUTER JOIN current_epic_prod.ZC_OR_SERVICE  A_zc_or_service_orlog ON (A_zc_or_service_orlog.SERVICE_C=ol.SERVICE_C)
   LEFT OUTER JOIN current_epic_prod.CLARITY_LOC  A_clarity_loc_orlog ON (ol.LOC_ID=A_clarity_loc_orlog.LOC_ID)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_log_casetime_strt_perioppost ON (ol.LOG_ID=A_log_casetime_strt_perioppost.LOG_ID and A_log_casetime_strt_perioppost.TRACKING_EVENT_C IN (325,874))
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_or_log_casetime_pt_arr_pacu1 ON (ol.LOG_ID=A_or_log_casetime_pt_arr_pacu1.LOG_ID and A_or_log_casetime_pt_arr_pacu1.TRACKING_EVENT_C = 355)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_or_log_casetime_pt_arr_pacu2 ON (ol.LOG_ID=A_or_log_casetime_pt_arr_pacu2.LOG_ID and A_or_log_casetime_pt_arr_pacu2.TRACKING_EVENT_C = 365)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_or_log_case_times_patenteror ON (ol.LOG_ID=A_or_log_case_times_patenteror.LOG_ID and A_or_log_case_times_patenteror.TRACKING_EVENT_C = 210)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_or_log_case_times_patleaveor ON (ol.LOG_ID=A_or_log_case_times_patleaveor.LOG_ID and A_or_log_case_times_patleaveor.TRACKING_EVENT_C = 215)
   LEFT OUTER JOIN current_epic_prod.CLARITY_SER  A_clarity_ser_or_room ON (ol.ROOM_ID=A_clarity_ser_or_room.PROV_ID)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_orlog_casetimes_endopstpxarr ON (ol.LOG_ID=A_orlog_casetimes_endopstpxarr.LOG_ID
and A_orlog_casetimes_endopstpxarr.TRACKING_EVENT_C = 173)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_or_log_case_times_ent_proc ON (ol.LOG_ID=A_or_log_case_times_ent_proc.LOG_ID
and A_or_log_case_times_ent_proc.TRACKING_EVENT_C = 430)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_or_log_case_times_dep_proc ON (ol.LOG_ID=A_or_log_case_times_dep_proc.LOG_ID
and A_or_log_case_times_dep_proc.TRACKING_EVENT_C = 435)
   LEFT OUTER JOIN (
  SELECT * from check_please.WK_OR_LOG_SUPPLY_ITEM
  )  D_OR_SUPPLY ON (ol.LOG_ID=D_OR_SUPPLY.LOG_ID)
   LEFT OUTER JOIN current_epic_prod.OR_SPLY_MANFACTR osm ON (D_OR_SUPPLY.SUPPLY_ID=osm.ITEM_ID)
   LEFT OUTER JOIN current_epic_prod.ZC_OR_MANUFACTURER zom ON (osm.MANUFACTURER_C=zom.MANUFACTURER_C)
   LEFT OUTER JOIN current_epic_prod.ZC_OR_RSN_WASTED  zorw ON (D_OR_SUPPLY.RSN_SUP_WASTED_C=zorw.RSN_SUP_WASTED_C)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_orlog_casetime_pat_enter_asu ON (ol.LOG_ID=A_orlog_casetime_pat_enter_asu.LOG_ID AND A_orlog_casetime_pat_enter_asu.TRACKING_EVENT_C = 503)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_orlog_casetime_pat_leave_asu ON (ol.LOG_ID=A_orlog_casetime_pat_leave_asu.LOG_ID AND A_orlog_casetime_pat_leave_asu.TRACKING_EVENT_C = 504)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_orlog_casetime_pat_arrrec ON (ol.LOG_ID=A_orlog_casetime_pat_arrrec.LOG_ID AND A_orlog_casetime_pat_arrrec.TRACKING_EVENT_C = 530)
   LEFT OUTER JOIN current_epic_prod.OR_LOG_CASE_TIMES  A_orlog_arrive_endo_peds_post ON (ol.LOG_ID=A_orlog_arrive_endo_peds_post.LOG_ID  AND A_orlog_arrive_endo_peds_post.TRACKING_EVENT_C = 774)

WHERE
  (
   CAST(ol.SURGERY_DATE AS DATE)  BETWEEN  date_sub(current_date, 60) and current_date -- To extract two months
   AND
   OL.STATUS_C = 2 -- Posted surgeries only
   --AND
--Added Holy Spirit and Jersey Shore on 09/17/2018 as per Karafinski, Adam request
   --A_clarity_loc_orlog.LOC_NAME  IN  ( 'OR GSACH','OR GSWB','OR GBH','OR GWV','OR GLH','OR GCMC','OR GMC','OR SWB','OR OSSC','OR OSW','OR HSH','HH OR HSH','OR GJSH'  )
  )
  and D_OR_SUPPLY.SUPPLIES_USED IS NOT NULL and D_OR_SUPPLY.SUPPLIES_USED != 0 ;

ANALYZE TABLE premier_extracts.wk_OR_SUPPLY COMPUTE STATISTICS;  
ANALYZE TABLE premier_extracts.wk_OR_SUPPLY COMPUTE STATISTICS FOR COLUMNS;  
  
--------------------------------------------------------------------

 

drop table if exists premier_extracts.wk_or_sup_combined;

create table
premier_extracts.wk_or_sup_combined as
select
t.src_priority as src_priority,
t.log_id as log_id,
t.surgery_dt as surgery_dt,
t.loc_name as loc_name,
t.service_line as service_line,
t.proc_id as proc_id,
t.proc_name as proc_name,
t.surgery_prov_id as surgery_prov_id,
t.surgery_prov_name as surgery_prov_name,
t.implant_action_nm as implant_action_nm,
sum(t.cost_of_supplies) as cost_of_supplies,
sum(t.supplies_used) as supplies_used,
sum(t.wastage_cost) as wastage_cost,
t.supply_name as supply_name,
t.cost_per_unit_ot as cost_per_unit_ot,
t.reason_for_wastage as reason_for_wastage,
t.bin_location as bin_location,
sum(t.supplies_wasted) as supplies_wasted,
t.supply_id as supply_id,
t.man_ctlg_num as man_ctlg_num,
t.manufacturer_name as manufacturer_name,
t.room_id as room_id,
t.room_name as room_name,
t.or_status as or_status,
t.time_in_mins as time_in_mins,
t.model_number as model_number,
t.lot_number as lot_number,
t.implant_name as implant_name,
t.implant_rsn_wstd_c as implant_rsn_wstd_c,
t.serial_number as serial_number,
t.lawson_supply_id as lawson_supply_id
from
(
SELECT
'1' as src_priority,
log_id,
surgery_dt,
loc_name,
service_line,
proc_id,
proc_name,
surgery_prov_id,
       surgery_prov_name,
     'Supply' as         implaNt_action_nm,
       cost_of_supplies,
       supplies_used,
       wastage_cost,
       --pick_list_id, Removed this column since a same item can exist under multiple picklists
       supply_name,
       cost_per_unit_ot,
       reason_for_wastage,
       bin_location,
       supplies_wasted,
       supply_id,
       man_ctlg_num,
       manufacturer_name,
       room_id,
       room_name,
       or_status,
       time_in_mins,
       CAST(NULL AS STRING) AS  model_number,
       CAST(NULL AS STRING) AS  lot_number,
       CAST(NULL AS STRING) AS  implant_name,
       CAST(NULL AS STRING) AS  implant_rsn_wstd_c,
       CAST(NULL AS STRING) AS  serial_number,
       lawson_supply_id
FROM premier_extracts.wk_or_supply
union all
SELECT
'2' as src_priority,
log_id,
       surgery_dt,
       loc_name,
       service_line,
       proc_id,
       proc_name,
       prov_id AS SURGERY_PROV_ID,
       prov_name AS SURGERY_PROV_NAME,
       implaNt_action_nm,
       cost_of_supplies,
       supplies_used,
       wastage_cost,
       --pick_list_id,
       supply_name,
       cost_per_unit_ot,
       reason_for_wastage,
       bin_location,
       supplies_wasted,
       supply_id,
       man_ctlg_num,
       manufacturer_name,
       room_id,
       room_name,
       or_status,
       time_in_mins,
       CAST(NULL AS STRING) AS model_number,
       CAST(NULL AS STRING) AS lot_number,
       CAST(NULL AS STRING) AS implant_name,
       CAST(NULL AS STRING) AS implant_rsn_wstd_c,
       CAST(NULL AS STRING) AS serial_number,
       lawson_supply_id
FROM check_please.wk_or_implants
) t
group by
t.src_priority,
t.log_id,
t.surgery_dt,
t.loc_name,
t.service_line,
t.proc_id,
t.proc_name,
t.surgery_prov_id,
t.surgery_prov_name,
t.implant_action_nm,
t.supply_name,
t.cost_per_unit_ot,
t.reason_for_wastage,
t.bin_location,
t.supply_id,
t.man_ctlg_num,
t.manufacturer_name,
t.room_id,
t.room_name,
t.or_status,
t.time_in_mins,
t.model_number,
t.lot_number,
t.implant_name,
t.implant_rsn_wstd_c,
t.serial_number,
t.lawson_supply_id
;
 
ANALYZE TABLE premier_extracts.wk_or_sup_combined COMPUTE STATISTICS;  
ANALYZE TABLE premier_extracts.wk_or_sup_combined COMPUTE STATISTICS FOR COLUMNS;   
  
---------------------------------------------------------------------

drop table if exists premier_extracts.or_supply;

create table premier_extracts.or_supply as select * from premier_extracts.wk_or_sup_combined;

ANALYZE TABLE premier_extracts.or_supply COMPUTE STATISTICS;  
ANALYZE TABLE premier_extracts.or_supply COMPUTE STATISTICS FOR COLUMNS;

-----------------------------------------------------------


drop table if exists premier_extracts.or_supply_proc_view;

create table premier_extracts.or_supply_proc_view as
SELECT distinct src_priority,
       log_id,
	   proc_name,
       implant_action_nm,
       supplies_used,
       supply_name,
       cost_per_unit_ot,
       supplies_wasted,
       supply_id,
       man_ctlg_num,
       manufacturer_name
FROM premier_extracts.or_supply
where substr(cast(add_months(FROM_UNIXTIME( UNIX_TIMESTAMP() ),-1) as string),1,7)  = substr(cast(surgery_dt as string),1,7)
;

ANALYZE TABLE premier_extracts.or_supply_proc_view COMPUTE STATISTICS;  
ANALYZE TABLE premier_extracts.or_supply_proc_view COMPUTE STATISTICS FOR COLUMNS;


--End of special section for missing records
----------------------------------------------------------

--Staging table 
DROP TABLE IF EXISTS premier_extracts.premier_supply;
CREATE TABLE premier_extracts.premier_supply as 
SELECT distinct
      case when os.prim_locn = 'HSH' then 'HOLY SPIRIT HOSPITAL OF THE SISTERS OF CHRISTIAN'
           when os.prim_locn = 'GBH' then 'GEISINGER BLOOMSBURG HOSPITAL'
           when os.prim_locn in ('GLH','GECL') then 'GEISINGER LEWISTOWN HOSPITAL'
           when os.prim_locn = 'GCMC' then 'GEISINGER COMMUNITY MEDICAL CENTER'
           when os.prim_locn in ('GWV','GSWB') then 'GEISINGER WYOMING VALLEY MEDICAL CENTER'
           else 'GEISINGER MEDICAL CENTER'        end as facility 
      ,os.loc_name as department           
      ,'Epic' as sourcename
      ,case when os.prim_locn = 'HSH' then '653362'
           when os.prim_locn = 'GBH' then '709177'
           when os.prim_locn in ('GLH','GECL') then '730455'
           when os.prim_locn = 'GCMC' then 'PA0030'
           when os.prim_locn in ('GWV','GSWB') then 'PA2003'           
           else 'PA0024'        end as entity       
      ,os.pat_mrn_id AS mrn
      ,coalesce(cast(pe.hsp_account_id as string),cast(os.hosp_enc_csn_id as string)) as acct_nbr
      ,os.log_id as surgical_log_id
      ,case when ol.pat_type_c = '6' then 'Outpatient' ELSE 'Inpatient' end patient_type
      ,cast(os.surgery_date as date) surgery_date
      ,regexp_replace(csi.prov_name,',',' ') as SurgeonName
      --USED TO CONTROL 1 SET OF IMPLANTS WHEN THERE ARE MULTIPLE PROCEDURES ON AN ENCOUNTER
      ,regexp_replace(os.proc_display_name,',',' ') as  primary_surg_proc_desc
      ,sply.supply_id as mmisitemnumber 
      ,regexp_replace(cast(sply.man_ctlg_num as string),',',' ') as manufact_ctlg_nbr
      ,regexp_replace(sply.manufacturer_name,',',' ') as manufacturer
      --
      ,orsply.reusable_yn --NEEDED TO EXCLUDE REUSABLES 
      --
      ,regexp_replace(sply.supply_name,',',' ') as item_desc
      ,sply.cost_per_unit_ot as each_price
      ,regexp_replace(cmts.comments,',',' ') as additional_item_desc
      ,sply.supplies_used as qty      
      ,sply.supplies_wasted as qty_wasted
      ---
      ,case when sply.implant_action_nm = 'Implanted' then 'True' else 'False' end as Implant
      ,tlin.tracking_time_in as wheels_in_time
      ,tlout.tracking_time_in as wheels_out_time
      ,regexp_replace(zc.abbr,'RA','') as asa
      ,substr(cast(add_months(FROM_UNIXTIME( UNIX_TIMESTAMP() ),-1) as string),1,7) as year_month
      ,current_date as load_date
FROM or_anes_db.or_summary os
left join current_epic_prod.pat_enc pe on cast(pe.pat_enc_csn_id as string) = cast(os.hosp_enc_csn_id as string) 
join current_epic_prod.or_log ol on ol.pat_id = os.pat_id
                               and ol.log_id = os.log_id
left join or_anes_db.or_case_staff_info csi on csi.log_id = os.log_id
                                           and csi.role = 'Primary Surgeon'
left join premier_extracts.or_supply_proc_view sply on sply.log_id = os.log_id
join or_anes_db.or_timeline tstrt on tstrt.log_id = os.log_id
                                 and tstrt.tracking_event = 'Procedure(s) Start'
                                 and tstrt.tracking_time_in is not null
join or_anes_db.or_timeline tend on tend.log_id = os.log_id
                                and tend.tracking_event = 'Procedure(s) Stop'                                     
                                and tend.tracking_time_in is not null
left join or_anes_db.or_timeline tlin on tlin.log_id = os.log_id
                                     and tlin.tracking_event = 'Patient Enters Or'
left join or_anes_db.or_timeline tlout on tlout.log_id = os.log_id
                                      and tlout.tracking_event = 'Patient Leaves Or'                                      
left join current_epic_prod.or_sply_comments cmts on cmts.item_id = sply.supply_id 
                                                 and cmts.line = '1'
left join current_epic_prod.or_sply orsply on orsply.supply_id = sply.supply_id                                      
left join current_epic_prod.zc_or_asa_rating zc on zc.asa_rating_c = ol.asa_rating_c
where substr(cast(add_months(FROM_UNIXTIME( UNIX_TIMESTAMP() ),-1) as string),1,7)  = substr(cast(os.surgery_date as string),1,7)
  and (coalesce(orsply.reusable_yn,'Y') = 'Y' or sply.implant_action_nm = 'Implanted'); 
  

 
ANALYZE TABLE premier_extracts.premier_supply COMPUTE STATISTICS;  
ANALYZE TABLE premier_extracts.premier_supply COMPUTE STATISTICS FOR COLUMNS;  


----------------------------------------------------------------------------------------------------------------
--needed to pull first procedure only
DROP TABLE IF EXISTS premier_extracts.tmp_proc;
CREATE TABLE premier_extracts.tmp_proc as
select log_id
      ,regexp_replace(proc_name,',',' ') as proc_name
      ,RANK() over (PARTITION by log_id order by log_id, proc_name ) as log_rank
from premier_extracts.or_supply_proc_view
where log_id in (select surgical_log_id from premier_extracts.premier_supply)
group by log_id, proc_name;

ANALYZE TABLE premier_extracts.tmp_proc COMPUTE STATISTICS;  
ANALYZE TABLE premier_extracts.tmp_proc COMPUTE STATISTICS FOR COLUMNS; 


-----------------------------------------------------------------

------Table to get min/max procedure start/end times
DROP TABLE IF EXISTS premier_extracts.tmp_times;
CREATE TABLE premier_extracts.tmp_times as
select vw.* from 
(
select log_id, min(tracking_time_in) tracktime, 'procstart' time_type
from or_anes_db.or_timeline
where log_id in (select surgical_log_id from premier_extracts.premier_supply)
  and tracking_time_in is not NULL
  and tracking_event = 'Procedure(s) Start'
group by log_id
UNION 
select log_id, max(tracking_time_in), 'procend' time_type
from or_anes_db.or_timeline
where log_id in (select surgical_log_id from premier_extracts.premier_supply)
  and tracking_time_in is not NULL
  and tracking_event = 'Procedure(s) Stop'
group by log_id
)vw;


---------------------------------------------------------------------------------------
--Temp table to keep pertinent encounters only
--Staging table 
DROP TABLE IF EXISTS premier_extracts.tmp_supply;
CREATE TABLE premier_extracts.tmp_supply as
select surgical_log_id, count(*)
from premier_extracts.premier_supply
where (manufacturer is not null 
   or manufact_ctlg_nbr is not null 
   or item_desc is not NULL
   or each_price is not NULL
   or qty is not NULL
   or implant = 'True')
Group by surgical_log_id
having count(*) > 1;


----------------------------------------------------------------------------------------
--Creation of partitioned table by Facility & year-month

DROP TABLE IF EXISTS premier_extracts.premier_supply_part_all;
CREATE TABLE premier_extracts.premier_supply_part_all
(FacilityName string
,Department string
,SourceName string
,EntityCodeSA string
,EntityCodeQA string
,MedicalRecordNumber string
,AccountNumber string
,SurgicalLogID string
,PatientType string
,SurgeryDate string
,SurgeonName string
,SurgProcDesc string
,SurgProcLongDesc string
,MMISItemNumber string
,ManufacturerCatalogNumber string
,Manufacturer string
,DistributorCatalogNumber string
,Distributor string
,ItemDesc string
,EachPrice string
,AdditionalItemDesc string
,Qty string
,QtyWasted string
,Implant string
,ProcedureStartTime string
,ProcedureEndTime string
,WheelsInTime string
,WheelsOutTime string
,ASA string
,PONumber string
,InvoiceNumber string);



insert overwrite table premier_extracts.premier_supply_part_all
select distinct
 facility as FacilityName
,department as Department
,sourcename as SourceName
,entity as EntityCodeSA
,entity as EntityCodeQA
,mrn as MedicalRecordNumber
,acct_nbr as AccountNumber
,pe.surgical_log_id as SurgicalLogID
,patient_type as PatientType
,surgery_date as SurgeryDate
,surgeonname as SurgeonName
,tp.proc_name as SurgProcDesc
,cast(null as string) SurgProcLongDesc
,mmisitemnumber as MMISItemNumber
,manufact_ctlg_nbr as ManufacturerCatalogNumber
,manufacturer as Manufacturer
,cast(null as string) as DistributorCatalogNumber
,cast(null as string) as Distributor
,item_desc as ItemDesc
,each_price as EachPrice 
,additional_item_desc as AdditionalItemDesc
,qty as Qty
,qty_wasted as QtyWasted
,implant as Implant
,concat( substr(cast(ttin.tracktime as string),1,10) , 'T', substr(cast(ttin.tracktime as string),12,8), '-04:00') ProcedureStartTime
,concat( substr(cast(ttout.tracktime as string),1,10) , 'T', substr(cast(ttout.tracktime as string),12,8), '-04:00') ProcedureEndTime
,concat( substr(cast(wheels_in_time as string),1,10) , 'T', substr(cast(wheels_in_time as string),12,8), '-04:00') WheelsInTime
,concat( substr(cast(wheels_out_time as string),1,10) , 'T', substr(cast(wheels_out_time as string),12,8), '-04:00') WheelsOutTime
,asa as ASA
,cast(null as string) as PONumber
,cast(null as string) as InvoiceNumber
--
from premier_extracts.premier_supply pe
join premier_extracts.tmp_proc tp on tp.log_id = pe.surgical_log_id
                and tp.log_rank = 1
left join premier_extracts.tmp_times ttin on ttin.log_id = pe.surgical_log_id
                         and ttin.time_type = 'procstart'
left join premier_extracts.tmp_times ttout on ttout.log_id = pe.surgical_log_id
                         and ttout.time_type = 'procend'                         
where pe.surgical_log_id in (select surgical_log_id from premier_extracts.tmp_supply); 


analyze table premier_extracts.premier_supply_part_all  compute statistics;
analyze table premier_extracts.premier_supply_part_all  compute statistics for columns;

              

-----------------------------------------------------------------------

DROP TABLE IF EXISTS premier_extracts.year_mo_extract;
CREATE TABLE premier_extracts.year_mo_extract as
select substr(cast(add_months(FROM_UNIXTIME( UNIX_TIMESTAMP() ),-1) as string),1,7) year_mo;

------------------------------------------------------------------------
--Create Header record

drop table if exists premier_extracts.premier_SLA_header;
create table premier_extracts.premier_SLA_header as 
select * 
from premier_extracts.premier_supply_part_all
limit 0;

insert into table premier_extracts.premier_SLA_header
values ('FacilityName','Department','SourceName','EntityCodeSA'
,'EntityCodeQA','MedicalRecordNumber','AccountNumber','SurgicalLogID'
,'PatientType','SurgeryDate','SurgeonName','SurgProcDesc'
,'SurgProcLongDesc','MMISItemNumber','ManufacturerCatalogNumber'
,'Manufacturer','DistributorCatalogNumber','Distributor'
,'ItemDesc','EachPrice','AdditionalItemDesc','Qty','QtyWasted'
,'Implant','ProcedureStartTime','ProcedureEndTime','WheelsInTime'
,'WheelsOutTime','ASA','PONumber','InvoiceNumber');


-------------------------------------------------------------------------
--Create monthly files
--Facility                                     entity code SA
--HOLY SPIRIT HOSPITAL OF THE SISTERS OF CHRISTIAN	653362
--GEISINGER BLOOMSBURG HOSPITAL						709177
--GEISINGER LEWISTOWN HOSPITAL						730455
--GEISINGER MEDICAL CENTER							PA0024
--GEISINGER COMMUNITY MEDICAL CENTER				PA0030
--GEISINGER WYOMING VALLEY MEDICAL CENTER			PA2003

--Holy Spirit (653362)
DROP TABLE IF EXISTS premier_extracts.epic_653362_${hivevar:vardate};
CREATE TABLE premier_extracts.epic_653362_${hivevar:vardate} as
select * 
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = '653362';


--BLOOMSBURG HOSPITAL (709177)
DROP TABLE IF EXISTS premier_extracts.epic_709177_${hivevar:vardate};
CREATE TABLE premier_extracts.epic_709177_${hivevar:vardate} as
select * 
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = '709177';


--LEWISTOWN HOSPITAL (730455)
DROP TABLE IF EXISTS premier_extracts.epic_730455_${hivevar:vardate};
CREATE TABLE premier_extracts.epic_730455_${hivevar:vardate} as
select * 
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = '730455';


--GEISINGER MEDICAL CENTER (PA0024)
DROP TABLE IF EXISTS premier_extracts.epic_PA0024_${hivevar:vardate};
CREATE TABLE premier_extracts.epic_PA0024_${hivevar:vardate} as
select * 
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = 'PA0024';


--GEISINGER COMMUNITY MEDICAL CENTER (PA0030)
DROP TABLE IF EXISTS premier_extracts.epic_PA0030_${hivevar:vardate};
CREATE TABLE premier_extracts.epic_PA0030_${hivevar:vardate} as
select * 
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = 'PA0030';


--GEISINGER WYOMING VALLEY MEDICAL CENTER (PA2003)
DROP TABLE IF EXISTS premier_extracts.epic_PA2003_${hivevar:vardate};
CREATE TABLE premier_extracts.epic_PA2003_${hivevar:vardate} as
select *  
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = 'PA2003';

------------------------------------------------------------------