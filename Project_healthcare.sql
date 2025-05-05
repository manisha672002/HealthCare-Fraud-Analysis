create database healthcare;
use healthcare;

-- loading all tables

create table Trainbeneficiary(BeneID varchar(15),DOB date,DOD varchar(15),Gender int,Race int,RenalDiseaseIndicator varchar(10),State int,County
int,NoOfMonths_PartACov int,NoOfMonths_PartBCov int,ChronicCond_Alzheimer int,ChronicCond_Heartfailure int,ChronicCond_KidneyDisease int,
ChronicCond_Cancer int,ChronicCond_ObstrPulmonary int,ChronicCond_Depression int,ChronicCond_Diabetes int,ChronicCond_IschemicHeart int,
ChronicCond_Osteoporasis int,ChronicCond_rheumatoidarthritis int,ChronicCond_stroke int,IPAnnualReimbursementAmt int,IPAnnualDeductibleAmt
int,OPAnnualReimbursementAmt int,OPAnnualDeductibleAmt int);
load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Train_Beneficiarydata.csv' into table Trainbeneficiary fields terminated by ',' 
ignore 1 rows;
select * from Trainbeneficiary;

create table Traininpatient(BeneID varchar(15),ClaimID varchar(15),ClaimStartDt date,ClaimEndDt date,Provider varchar(15),InscClaimAmtReimbursed
int,AttendingPhysician varchar(15),OperatingPhysician varchar(15),OtherPhysician varchar(15),AdmissionDt date,ClmAdmitDiagnosisCode
varchar(15),DeductibleAmtPaid varchar(15),DischargeDt date,DiagnosisGroupCode varchar(15),ClmDiagnosisCode_1 varchar(15),ClmDiagnosisCode_2
varchar(15),ClmDiagnosisCode_3 varchar(15),ClmDiagnosisCode_4 varchar(15),ClmDiagnosisCode_5 varchar(15),ClmDiagnosisCode_6 varchar(15),ClmDiagnosisCode_7
varchar(15),ClmDiagnosisCode_8 varchar(15),ClmDiagnosisCode_9 varchar(15),ClmDiagnosisCode_10 varchar(15),ClmProcedureCode_1
varchar(15),ClmProcedureCode_2 varchar(15),ClmProcedureCode_3 varchar(15),ClmProcedureCode_4 varchar(15),ClmProcedureCode_5
varchar(15),ClmProcedureCode_6 varchar(15));
load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Train_Inpatientdata.csv' into table Traininpatient fields terminated by ',' 
ignore 1 rows;
select * from Traininpatient;
drop table Traininpatient;

create table TrainOutpatient(BeneID varchar(15),ClaimID varchar(15),ClaimStartDt date,ClaimEndDt date,Provider varchar(15),
InscClaimAmtReimbursed int,AttendingPhysician varchar(15),OperatingPhysician varchar(15),OtherPhysician varchar(15),
ClmDiagnosisCode_1 varchar(15),ClmDiagnosisCode_2 varchar(15),ClmDiagnosisCode_3 varchar(15),ClmDiagnosisCode_4 varchar(15),
ClmDiagnosisCode_5 varchar(15),ClmDiagnosisCode_6 varchar(15),ClmDiagnosisCode_7 varchar(15),ClmDiagnosisCode_8 varchar(15),ClmDiagnosisCode_9 varchar(15),ClmDiagnosisCode_10
varchar(15),ClmProcedureCode_1 varchar(15),ClmProcedureCode_2 varchar(15),ClmProcedureCode_3 varchar(15),ClmProcedureCode_4 varchar(15),ClmProcedureCode_5 varchar(15),ClmProcedureCode_6 varchar(15),DeductibleAmtPaid
int,ClmAdmitDiagnosisCode varchar(15));
load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Train_Outpatientdata.csv' into table TrainOutpatient fields terminated by ',' 
ignore 1 rows;
select * from TrainOutpatient;

create table Train(Provider varchar(15),PotentialFraud varchar(15));
load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Train.csv' into table Train fields terminated by ',' 
ignore 1 rows;
select * from Train;

-- null values evaluation from each tables 
select b.BeneID, b.DOD,i.ClaimID,i.AdmissionDt from TrainBeneficiary b
join Traininpatient i on 
b.BeneID=i.BeneID
where i.AdmissionDt>b.DOD;

-- Removing col DOD from TrainBeneficiary
alter table TrainBeneficiary drop column DOD;

-- --------------- FRAUD DETECTION IN IPD-------------------------
update Traininpatient set ClmProcedureCode_1=null where ClmProcedureCode_1='NA';
SET  SQL_SAFE_UPDATES=0;

-- providers claims with missing procedure code
select Provider,count(*) as claim_count,sum(ClmProcedureCode_1 is null) as missing_codes,
sum(InscClaimAmtReimbursed) as Total_claim_amt from Traininpatient
group by Provider having missing_codes>5
order by Total_claim_amt desc;

-- Providers with no. of fraud claims
select i.Provider,count(*) as claim_count,
sum(case when TRIM(BOTH '"' from t.PotentialFraud)='Yes' then 1 else 0 end) as fraud_claims
from Traininpatient i join Train t on i.Provider=t.Provider
group by i.Provider
order by fraud_claims desc;

-- No of providers doing fraud ,total number of providers 
select count(*) AS no_of_fraud_providers from Train where PotentialFraud='"Yes"';
select distinct count(Provider) as total_providers from Train;

select * from Train;
select * from Traininpatient;

-- DiagnosisGroupCodes most involved in Fraud cases
select DiagnosisGroupCode,count(*) as fraud_count
from Traininpatient i join Train t on i.Provider=t.Provider
where t.PotentialFraud='"Yes"'
group by DiagnosisGroupCode
order by fraud_count desc;

-- Top 10 Providers with Fraud claims 
select i.Provider,count(*) as total_claims,
sum(case when
t.PotentialFraud='"Yes"' then 1 else 0 end) as fraud_providers,
round(sum(case when t.PotentialFraud='"Yes"' then 1 else 0 end) /count(*),2) as fraud_rate
from Traininpatient i join Train t on i.Provider=t.Provider
group by i.Provider
order by fraud_providers desc
limit 10;

-- sum of amt reimbursed from ipd fraud claims 
select i.Provider,i.BeneID,sum(InscClaimAmtReimbursed) as TotalFraudClaimAmt
from Traininpatient i join Train t on i.Provider=t.Provider
where PotentialFraud='"Yes"'
group by i.Provider,i.BeneID
order by TotalFraudClaimAmt desc;

-- no of fraud_claims according to year and month
select year(ClaimStartDt) as year,
month(ClaimStartDt) as month,count(*) as fraud_claims
from Traininpatient i join Train t on i.Provider=t.Provider
where t.PotentialFraud='"Yes"'
group by year,month
order by year,month;

-- Avg Stay of Patient in Hospital for Fraud and non Fraud claims
Select t.PotentialFraud,avg(datediff(DischargeDt,AdmissionDt)) as Average_Stay
From TrainInpatient i join Train t on i.Provider=t.Provider
group by PotentialFraud;

-- ----------------- FRAUD DETECTION IN OPD -----------------------------------------------
-- top 10 providers with high outpatient fraud claims
select o.Provider,count(*) as Fraud_outpatient_claims
from Trainoutpatient o join Train t on o.Provider=t.Provider
where PotentialFraud='"Yes"'
group by o.Provider
order by Fraud_outpatient_claims desc
limit 10;

-- date wise no of fraud opd claims by providers
select year(ClaimStartDt) as year,
month(ClaimStartDt) as month,count(*) as fraud_opd_claims
from Trainoutpatient o join Train t on o.Provider=t.Provider
where PotentialFraud='"Yes"'
group by year,month
order by year,month;

-- sum of amt reimbursed from opd fraud claims 
select o.Provider,o.BeneID,sum(InscClaimAmtReimbursed) as TotalFraudClaimAmt
from Trainoutpatient o join Train t on o.Provider=t.Provider
where PotentialFraud='"Yes"'
group by o.Provider,o.BeneID
order by TotalFraudClaimAmt desc;

-- -----------------FRAUD DETECTION THROUGH BENEFICIARY DETAILS----------------------------
-- analysis of Beneficiaries data
select * from TrainBeneficiary;
alter table Trainbeneficiary
add column age int;
alter table Trainbeneficiary
modify column age int after DOB;
update Trainbeneficiary set age=timestampdiff(year,DOB,curdate());

set session wait_timeout=600;

-- age group most affected by fraud
select case
when b.age between 40 and 70 then '40-70'
when b.age between 71 and 100 then '71-100'
when b.age between 101 and 130 then '101-130'
else  'unknown'
end as age_group,count(distinct case when t.PotentialFraud='"Yes"' and ip.BeneID is not null then ip.ClaimID end) as IPD_Fraud_Claims,
count(distinct case when t.PotentialFraud='"Yes"' and op.BeneID is not null then op.ClaimID end) as OPD_Fraud_Claims
from Trainbeneficiary b left join Traininpatient ip on b.BeneID=ip.BeneID
left join Trainoutpatient op on b.BeneID=op.BeneID
left join Train t on ip.Provider=t.Provider or op.Provider=t.Provider
group by age_group
order by age_group;


-- no of fraud claims by race 
select b.Race,count(*) as fraud_claims 
from Trainbeneficiary b left join Traininpatient ip on b.BeneID=ip.BeneID
left join Trainoutpatient op on b.BeneID=op.BeneID
left join Train t on ip.Provider=t.Provider or op.Provider=t.Provider
where t.PotentialFraud='"Yes"'
group by b.Race
order by fraud_claims desc
limit 5;

-- no of fraud claims by gender 
select b.Gender,count(*) as fraud_claims 
from Trainbeneficiary b left join Traininpatient ip on b.BeneID=ip.BeneID
left join Trainoutpatient op on b.BeneID=op.BeneID
left join Train t on ip.Provider=t.Provider or op.Provider=t.Provider
where t.PotentialFraud='"Yes"'
group by b.Gender
order by fraud_claims desc
limit 5;


-- -------------COMPARISON BETWEEN OPD AND IPD FRAUD --------------------------------
select * from Trainbeneficiary;
select  * from Traininpatient;
select * from Trainoutpatient;

-- no of fraud claims in ipd vs opd
select 'inpatient' as Type,count(*) as Fraud_claims,sum(InscClaimAmtReimbursed) as fraud_claim_amt
from Traininpatient i join Train t on i.Provider=t.Provider
where t.PotentialFraud='"Yes"'
union all
select 'outpatient',count(*),sum(InscClaimAmtReimbursed) 
from Trainoutpatient o join Train t on o.Provider=t.Provider
where t.PotentialFraud='"Yes"'; 

-- debugging issue
show variables like 'wait_timeout';
show variables like 'max_allowed_packet';












