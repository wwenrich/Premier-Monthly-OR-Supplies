
set hive.execution.engine=mr; 

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
left join current_epic_prod.pat_enc pe on pe.pat_enc_csn_id = os.hosp_enc_csn_id  
join current_epic_prod.or_log ol on ol.pat_id = os.pat_id
                               and ol.log_id = os.log_id
left join or_anes_db.or_case_staff_info csi on csi.log_id = os.log_id
                                           and csi.role = 'Primary Surgeon'
left join check_please.or_supply_proc_view sply on sply.log_id = os.log_id
join or_anes_db.or_timeline tstrt on tstrt.log_id = os.log_id
                                 and tstrt.tracking_event = 'Procedure(s) Start'
                                 and tstrt.tracking_time_in is not null
join or_anes_db.or_timeline tend on tend.log_id = os.log_id
                                and tend.tracking_event = 'Procedure(s) Stop'                                     
                                and tend.tracking_time_in is not null
join or_anes_db.or_timeline tlin on tlin.log_id = os.log_id
                                     and tlin.tracking_event = 'Patient Enters Or'
join or_anes_db.or_timeline tlout on tlout.log_id = os.log_id
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
DROP TABLE IF EXISTS tmp_proc;
CREATE TEMPORARY TABLE tmp_proc as
select log_id
      ,regexp_replace(proc_name,',',' ') as proc_name
      ,RANK() over (PARTITION by log_id order by log_id, proc_name ) as log_rank
from check_please.or_supply_proc_view
where log_id in (select surgical_log_id from premier_extracts.premier_supply)
group by log_id, proc_name;

ANALYZE TABLE tmp_proc COMPUTE STATISTICS;  
ANALYZE TABLE tmp_proc COMPUTE STATISTICS FOR COLUMNS; 


-----------------------------------------------------------------

------Table to get min/max procedure start/end times
DROP TABLE IF EXISTS tmp_times;
CREATE TEMPORARY TABLE tmp_times as
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
DROP TABLE IF EXISTS tmp_supply;
CREATE TEMPORARY TABLE tmp_supply as
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



insert into premier_extracts.premier_supply_part_all
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
join tmp_proc tp on tp.log_id = pe.surgical_log_id
                and tp.log_rank = 1
left join tmp_times ttin on ttin.log_id = pe.surgical_log_id
                         and ttin.time_type = 'procstart'
left join tmp_times ttout on ttout.log_id = pe.surgical_log_id
                         and ttout.time_type = 'procend'                         
where pe.surgical_log_id in (select surgical_log_id from tmp_supply); 


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
DROP TABLE IF EXISTS premier_extracts.epic_653362;
CREATE TABLE premier_extracts.epic_653362 as
select * 
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = '653362';


--BLOOMSBURG HOSPITAL (709177)
DROP TABLE IF EXISTS premier_extracts.epic_709177;
CREATE TABLE premier_extracts.epic_709177 as
select * 
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = '709177';


--LEWISTOWN HOSPITAL (730455)
DROP TABLE IF EXISTS premier_extracts.epic_730455;
CREATE TABLE premier_extracts.epic_730455 as
select * 
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = '730455';


--GEISINGER MEDICAL CENTER (PA0024)
DROP TABLE IF EXISTS premier_extracts.epic_PA0024;
CREATE TABLE premier_extracts.epic_PA0024 as
select * 
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = 'PA0024';


--GEISINGER COMMUNITY MEDICAL CENTER (PA0030)
DROP TABLE IF EXISTS premier_extracts.epic_PA0030;
CREATE TABLE premier_extracts.epic_PA0030 as
select * 
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = 'PA0030';


--GEISINGER WYOMING VALLEY MEDICAL CENTER (PA2003)
DROP TABLE IF EXISTS premier_extracts.epic_PA2003;
CREATE TABLE premier_extracts.epic_PA2003 as
select *  
from premier_extracts.premier_supply_part_all pa 
where pa.entitycodesa = 'PA2003';

------------------------------------------------------------------
