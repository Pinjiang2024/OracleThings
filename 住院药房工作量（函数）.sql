create or replace function CalWorkLoad(in searchType int, in startDate date, in endDate date)
returns table(workLoad numeric, grdrugPerson text, grDrugTime text)
as $$
declare
result text;
t_startYear text;
t_endYear text;
--searchType: 0, statistics by month; 1, by year.
begin 
if searchType = 0 then 
return query with detail as (select count(*) as "No_Count" , z."Grdrug_Drugvr" , z."Grdrug_Time" from (
select gsd."Order_Id" || '-' || gsd."Rcp_No" as "Grdrug_Order" , gsd."Pati_Id" , gsd."Pati_Name" , dsg."Takedept_Id" , dsg."Takedept_Name" , dsg."Drug_Id" , dsg."Drug_Name" 
, dsg."Grdrug_Drugvr_Id" , dsg."Grdrug_Drugvr" , substring(cast(dsg."Grdrug_Time" as text), 1, 10) as "Grdrug_Time" 
from "DeptSummaryGrdrug" dsg 
join "GrdrugSumDetail" gsd on dsg."Grdrug_Id" = gsd."Grdrug_Id" 
where dsg."Grdrug_Time" >= startDate and dsg."Grdrug_Time" <= endDate
and dsg."Grdrug_Dispsr" <> '系统管理员'
and dsg."Storehouse_Id" = '74' --住院药房
) z
group by z."Grdrug_Time" , z."Grdrug_Drugvr"
order by z."Grdrug_Time" desc, z."Grdrug_Drugvr"
)
, sumDetail as (
select sum("No_Count") as "No_Count", d."Grdrug_Drugvr", substring(d."Grdrug_Time", 1, 7) as "Grdrug_Time" from detail d
group by substring(d."Grdrug_Time", 1, 7), d."Grdrug_Drugvr"
)
, sumAllDepartment as (
select sum("No_Count") as "No_Count", '全住院药房已发处方统计' as "Grdrug_Drugvr", substring(sd."Grdrug_Time", 1, 7) as "Grdrug_Time" from sumDetail sd
group by substring(sd."Grdrug_Time", 1, 7)
)
select * from detail
union all
select * from sumDetail
union all 
select * from sumAllDepartment
order by "Grdrug_Drugvr", "Grdrug_Time"
;
elseif searchType = 1 then
t_startYear := to_char(startDate, 'YYYY') || '-01-01 00:00:00';
t_endYear := to_char(endDate, 'YYYY') || '-01-01 00:00:00';
return query with detail as (select count(*) as "No_Count" , z."Grdrug_Drugvr" , z."Grdrug_Time" from (
select gsd."Order_Id" || '-' || gsd."Rcp_No" as "Grdrug_Order" , gsd."Pati_Id" , gsd."Pati_Name" , dsg."Takedept_Id" , dsg."Takedept_Name" , dsg."Drug_Id" , dsg."Drug_Name" 
, dsg."Grdrug_Drugvr_Id" , dsg."Grdrug_Drugvr" , substring(cast(dsg."Grdrug_Time" as text), 1, 4) as "Grdrug_Time" 
from "DeptSummaryGrdrug" dsg 
join "GrdrugSumDetail" gsd on dsg."Grdrug_Id" = gsd."Grdrug_Id" 
where dsg."Grdrug_Time" >= t_startYear::date and dsg."Grdrug_Time" <= t_endYear::date
and dsg."Grdrug_Dispsr" <> '系统管理员'
and dsg."Storehouse_Id" = '74' --住院药房
) z
group by z."Grdrug_Time" , z."Grdrug_Drugvr"
order by z."Grdrug_Time" desc, z."Grdrug_Drugvr"
)
, detail_total as (
select sum("No_Count") as "No_Count", '住院药房汇总' as "Grdrug_Drugvr", "Grdrug_Time" from detail d
group by "Grdrug_Time"
)
select "No_Count"::numeric as workLoad, "Grdrug_Drugvr"::text as grdrugPerson, "Grdrug_Time"::text as grdrugTime from detail d;
--union all
--select "No_Count"::numeric as workLoad, "Grdrug_Drugvr"::text as grdrugPerson, "Grdrug_Time"::text as grdrugTime from detail_total dt;
end if;
return;
end;
$$
language plpgsql;


select * from zldrstore.public.CalWorkLoad(1::int, '2024-03-01 00:00:00'::date, '2025-04-01 00:00:00'::date);


drop function CalWorkLoad;