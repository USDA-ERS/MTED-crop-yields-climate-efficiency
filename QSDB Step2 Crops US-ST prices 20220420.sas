/* Start timer */
%let _timer_start = %sysfunc(datetime());

DM "CLEAR LOG ; CLEAR OUTPUT ; ";
options source2 ls=80 ps=60 cpucount=8 fullstimer formchar="|----|+|---+=|-/\<>*";
/*************************************************************/
/************** NASS complete database **** Saleem, Shaik  ***/
Title 'NASS QSBD Saleem SHAIK - March, Oct 2021';
/*************************************************************/
/*
Saleem Shaik, Ph.D
Branch Chief
Agricultural Policy and Models Branch
ERS, USDA
E:Saleem.Shaik@usda.gov
*/



/*************************************************************/
/*************************************************************/
LIBNAME NASS2 'C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\dataforanalysis\NASS';
LIBNAME NASS3 'C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\dataforanalysis\SCP\domestic';

LIBNAME CLIM "C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\dataforanalysis\Climate";
LIBNAME CLIM1 "C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\analysis\climate";
/*************************************************************/
/*************************************************************/



/*
LIBNAME NASS1 "\\usda.net\ers\MTEDCOMMON\APM\STAFF\SShaik\US\data";
LIBNAME NASS2 "\\usda.net\ers\MTEDCOMMON\APM\STAFF\SShaik\US\dataforanalysis\NASS";
LIBNAME NASS3 "\\usda.net\ers\MTEDCOMMON\APM\STAFF\SShaik\US\dataforanalysis\SCP";

LIBNAME NASS1 'C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\dataforanalysis\NASS';
LIBNAME NASS2 'C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\dataforanalysis\SCP';
LIBNAME CLIM "C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\dataforanalysis\Climate";
LIBNAME CLIM1 "C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\analysis\climate";
*/



/*************************************************************/
%let yrmonday = 20220428.;
%let yrmonday1 = 20220428;
/*************************************************************/



/*************************************************************/
/*************************************************************/
data PRICE REVENUE PRODUCTION COUNTY_PRODUCTION check;
	set NASS2.step1_QS_Crops; 
	if STATISTICCAT_DESC IN ('PRODUCTION') AND 
		UNIT_DESC IN ('$', '$ / TON', '$, CHERRY BASIS', 
		'$, ON TREE EQUIV', '$, PHD EQUIV') 
		then STATISTICCAT_DESC = 'PRODUCTION in $';

	if AGG_LEVEL_DESC IN ('STATE', 'NATIONAL') AND
		STATISTICCAT_DESC IN ('PRICE RECEIVED') AND
		UTIL_PRACTICE_DESC IN ('ALL UTILIZATION PRACTICES',
		'GRAIN', 'SUGAR') then output PRICE; 

	if AGG_LEVEL_DESC IN ('STATE', 'NATIONAL') AND
		STATISTICCAT_DESC IN ('PRODUCTION in $') AND
		UTIL_PRACTICE_DESC IN ('ALL UTILIZATION PRACTICES',
		'GRAIN', 'SUGAR') AND 
		FREQ_DESC IN ('ANNUAL') then output REVENUE; 

	if AGG_LEVEL_DESC IN ('STATE', 'NATIONAL') AND
		STATISTICCAT_DESC IN ('PRODUCTION') AND
		UTIL_PRACTICE_DESC IN ('ALL UTILIZATION PRACTICES',
		'GRAIN', 'SUGAR') AND 
		FREQ_DESC IN ('ANNUAL') then output PRODUCTION; 

	if AGG_LEVEL_DESC IN ('COUNTY') AND
		STATISTICCAT_DESC IN ('PRODUCTION') AND
		UTIL_PRACTICE_DESC IN ('ALL UTILIZATION PRACTICES',
		'GRAIN', 'SUGAR') then output COUNTY_PRODUCTION;

	if AGG_LEVEL_DESC IN ('NATIONAL') AND
		COMMODITY_DESC IN ('CANOLA') then output check;
run;
/*************************************************************/
/*************************************************************/


proc freq data=PRICE;
table AGG_LEVEL_DESC
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC UNIT_DESC 
		STATE_FIPS_CODE STATE_NAME 
		YEAR FREQ_DESC REFERENCE_PERIOD_DESC ;
run;


  
/*************************************************************/
* Create STATE PRICE dataset;
/*************************************************************/
data PRICES_1(keep=AGG_LEVEL_DESC
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC UNIT_DESC 
		STATE_FIPS_CODE STATE_NAME 
		YEAR FREQ_DESC REFERENCE_PERIOD_DESC VALUE VALUE1);
	set PRICE;
	WHERE AGG_LEVEL_DESC IN ('STATE', 'NATIONAL') AND
			STATISTICCAT_DESC IN ('PRICE RECEIVED') AND 
			/*FREQ_DESC IN ('ANNUAL', 'MONTHLY') AND
			*/REFERENCE_PERIOD_DESC IN ('MARKETING YEAR', 'YEAR',
					'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
					'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC') AND
			UNIT_DESC NOTIN ('PCT', 'PCT BY COLOR', 'PCT BY GRADE',
					'PCT BY TYPE', 'PCT OF PARITY',
					'PCT OF PRODUCTION', 'PCT OF TOTAL STOCKS');
*	PRICES = value1;
*	PRICES_unit=UNIT_DESC;
run;
data PRICES_Annual PRICES_month;
	set PRICES_1;
	if REFERENCE_PERIOD_DESC = 'MARKETING YEAR' then 
		REFERENCE_PERIOD_DESC = 'MARKETING YEAR PRICES';
	if REFERENCE_PERIOD_DESC = 'YEAR' then 
		REFERENCE_PERIOD_DESC = 'CALENDER YEAR PRICES';

	if REFERENCE_PERIOD_DESC IN ('MARKETING YEAR PRICES', 
		'CALENDER YEAR PRICES') then output PRICES_Annual; 
	else if REFERENCE_PERIOD_DESC IN ('JAN', 'FEB', 'MAR', 
		'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 
		'OCT', 'NOV', 'DEC') then output PRICES_month;
run;
proc sort data=PRICES_Annual 
			DUPout=DUP_PRICES_1_annual NODUPKEY 
			OUT=PRICES_Annual_1;
	where FREQ_DESC in ('ANNUAL');
	by AGG_LEVEL_DESC
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC UNIT_DESC 
		STATE_FIPS_CODE STATE_NAME 
		YEAR 
		REFERENCE_PERIOD_DESC;
run;
proc transpose data=PRICES_Annual_1 out=PRICES_Annual_2(drop=_NAME_);
	where FREQ_DESC in ('ANNUAL');
	id REFERENCE_PERIOD_DESC;
	idlabel REFERENCE_PERIOD_DESC;
	var VALUE1;
	by AGG_LEVEL_DESC
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC UNIT_DESC 
		STATE_FIPS_CODE STATE_NAME 
		YEAR; 
run;
proc sort data=PRICES_Month 
		DUPout=DUP_PRICES_Month_1 NODUPKEY 
		OUT=PRICES_Month_1;
	where FREQ_DESC notin ('ANNUAL');
	by AGG_LEVEL_DESC
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC UNIT_DESC 
		STATE_FIPS_CODE STATE_NAME 
		YEAR
		REFERENCE_PERIOD_DESC;
run;
proc transpose data=PRICES_Month_1 out=PRICES_Month_2(drop=_NAME_) prefix=Price_;
	where FREQ_DESC NOTIN ('ANNUAL');
	id REFERENCE_PERIOD_DESC;
	idlabel REFERENCE_PERIOD_DESC;
	var VALUE1;
	by AGG_LEVEL_DESC
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC UNIT_DESC 
		STATE_FIPS_CODE STATE_NAME 
		YEAR;
run;
data PRICES_3;
	retain Merge_price;
	merge PRICES_Annual_2(in=a) PRICES_Month_2(in=b);
	by AGG_LEVEL_DESC
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC UNIT_DESC 
		STATE_FIPS_CODE STATE_NAME 
		YEAR;
	if a and b then Merge_price='Annual and Monthly'; else
	if a then Merge_price='Annual'; else
	if b then Merge_price='Monthly'; 
run;
proc freq data=PRICES_3; table Merge_price; run;
data PRICES_4;
	set PRICES_3;
	JD_PRICES_N= N(of Price_JAN, Price_FEB, Price_MAR,
						Price_APR, Price_MAY, Price_JUN,
						Price_JUL, Price_AUG, Price_SEP,
						Price_OCT, Price_NOV, Price_DEC);
	if JD_PRICES_N>3 then 
	JD_PRICES= mean(of Price_JAN, Price_FEB, Price_MAR,
						Price_APR, Price_MAY, Price_JUN,
						Price_JUL, Price_AUG, Price_SEP,
						Price_OCT, Price_NOV, Price_DEC);
	proc sort; 
	by AGG_LEVEL_DESC COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC UNIT_DESC
		STATE_FIPS_CODE STATE_NAME YEAR;
run;
data PRICES_5(Keep=Merge_price 
		AGG_LEVEL_DESC 
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit
		STATE_FIPS_CODE STATE_NAME YEAR
		MARKETING_YEAR_PRICES MARKETING_YEAR_PRICES1 
		MARKETING_YEAR_PRICES2 CALENDER_YEAR_PRICES 
		CALENDER_YEAR_PRICES1 JD_PRICES JD_PRICES_N
		Price_JAN Price_FEB Price_MAR 
		Price_APR Price_MAY Price_JUN 
		Price_JUL Price_AUG Price_SEP 
		Price_OCT Price_NOV Price_DEC);
	retain Merge_price 
		AGG_LEVEL_DESC 
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit
		STATE_FIPS_CODE STATE_NAME YEAR
		MARKETING_YEAR_PRICES MARKETING_YEAR_PRICES1 
		MARKETING_YEAR_PRICES2 CALENDER_YEAR_PRICES 
		CALENDER_YEAR_PRICES1 JD_PRICES JD_PRICES_N
		Price_JAN Price_FEB Price_MAR 
		Price_APR Price_MAY Price_JUN 
		Price_JUL Price_AUG Price_SEP 
		Price_OCT Price_NOV Price_DEC; 
	set PRICES_4;
	if CALENDER_YEAR_PRICES = . then 
	CALENDER_YEAR_PRICES1=JD_PRICES; else
	CALENDER_YEAR_PRICES1=CALENDER_YEAR_PRICES;
	if MARKETING_YEAR_PRICES = . then 
	MARKETING_YEAR_PRICES1=CALENDER_YEAR_PRICES; else
	MARKETING_YEAR_PRICES1=MARKETING_YEAR_PRICES;
	if MARKETING_YEAR_PRICES1 = . then 
	MARKETING_YEAR_PRICES2=JD_PRICES; else
	MARKETING_YEAR_PRICES2=MARKETING_YEAR_PRICES1;
	PRICES_unit=UNIT_DESC;
	proc sort; 
	by AGG_LEVEL_DESC 
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit
		STATE_FIPS_CODE STATE_NAME YEAR;
run;
/***********************************************/
/***********************************************/
/*This step will utilize more recent years with non-missing data, such as 1975,*/
/*to compute a value for an earlier year with missing data, such as 1974.*/
/* Notes:
prev_Agriculture_New is a fiction variable and not a real variable
_601 is the variable for which data is generated for earlier years
_551 is the variable used to generate the rate of changes for _601
*/
data PRICES_5a(drop=MARKETING_YEAR_PRICES1 
		MARKETING_YEAR_PRICES2); 
	set PRICES_5;
	SP='_' ;
  	Var_set = trim(left(AGG_LEVEL_DESC)) || sp || trim(left(COMMODITY_GROUP_code)) || sp || 
			trim(left(COMMODITY_GROUP)) || sp || trim(left(SOURCE_DESC)) || sp || 
			trim(left(SECTOR_DESC)) || sp || trim(left(GROUP_DESC)) || sp || 
			trim(left(COMMODITY_DESC)) || sp || trim(left(CLASS_DESC)) || sp || 
			trim(left(PRODN_PRACTICE_DESC)) || sp || trim(left( UTIL_PRACTICE_DESC)) || sp || 
			trim(left(STATISTICCAT_DESC)) || sp || trim(left(PRICES_unit)) || sp || 
			trim(left(STATE_FIPS_CODE)) || sp || trim(left(STATE_NAME)) ;
	proc sort; 
*	by AGG_LEVEL_DESC 
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit
		STATE_FIPS_CODE STATE_NAME descending YEAR;
	by Var_set descending year; 
run;
data PRICES_5b;
	set PRICES_5a;
	by Var_set;
	retain prev_MYP_New;
	lag_CALENDER_YEAR_PRICES1=lag(CALENDER_YEAR_PRICES1);
	if first.Var_set then prev_MYP_New = .;
	if MARKETING_YEAR_PRICES ne . then MARKETING_YEAR_PRICES_1 = MARKETING_YEAR_PRICES; 
	else if prev_MYP_New ne . then 
	MARKETING_YEAR_PRICES_1=prev_MYP_New*(CALENDER_YEAR_PRICES1/lag_CALENDER_YEAR_PRICES1);
	prev_MYP_New = MARKETING_YEAR_PRICES_1;
	drop prev:;
	format MARKETING_YEAR_PRICES_1 12.5;
run;
proc sort data=PRICES_5b; by Var_set year; run;
data PRICES_5c;
	set PRICES_5b;
	by Var_set;
	retain prev_MYP_New;
	lag_MARKETING_YEAR_PRICES_1=lag(MARKETING_YEAR_PRICES_1);
	if first.Var_set then prev_MYP_New = .;
	if MARKETING_YEAR_PRICES_1 ne . then MARKETING_YEAR_PRICES_2 = MARKETING_YEAR_PRICES_1; 
	else if prev_MYP_New ne . then 
	MARKETING_YEAR_PRICES_2=prev_MYP_New*(MARKETING_YEAR_PRICES_1/lag_MARKETING_YEAR_PRICES_1);
	prev_MYP_New = MARKETING_YEAR_PRICES_2;
	drop prev:;
	format MARKETING_YEAR_PRICES_2 12.5;
run;
data PRICES_6(Keep=Merge_price 
		AGG_LEVEL_DESC 
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit
		STATE_FIPS_CODE STATE_NAME YEAR
		MARKETING_YEAR_PRICES MARKETING_YEAR_PRICES_1 MARKETING_YEAR_PRICES_2
		MARKETING_YEAR_PRICES1 MARKETING_YEAR_PRICES2 
		CALENDER_YEAR_PRICES CALENDER_YEAR_PRICES1 JD_PRICES);
	set PRICES_5c;
	proc sort; 
	by AGG_LEVEL_DESC 
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit
		STATE_FIPS_CODE STATE_NAME YEAR;
run;
data PRICES_7(Keep=Merge_price 
		AGG_LEVEL_DESC 
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit
		STATE_FIPS_CODE STATE_NAME YEAR
		MARKETING_YEAR_PRICES MARKETING_YEAR_PRICES_1 MARKETING_YEAR_PRICES_2 
		MARKETING_YEAR_PRICES_3 MARKETING_YEAR_PRICES1 MARKETING_YEAR_PRICES2 
		CALENDER_YEAR_PRICES CALENDER_YEAR_PRICES1 
		JD_PRICES);
	set PRICES_6;
	if MARKETING_YEAR_PRICES_2 = . then 
	MARKETING_YEAR_PRICES_3=CALENDER_YEAR_PRICES1; else
	MARKETING_YEAR_PRICES_3=MARKETING_YEAR_PRICES_2;
run;
data PRICES_8(Keep=Merge_price 
		AGG_LEVEL_DESC 
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit
		STATE_FIPS_CODE STATE_NAME YEAR
		MARKETING_YEAR_PRICES MARKETING_YEAR_PRICES_3 
		CALENDER_YEAR_PRICES JD_PRICES);
	set PRICES_7;
	proc sort; 
	by AGG_LEVEL_DESC 
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATE_FIPS_CODE STATE_NAME YEAR;
run;
/*
proc sort data=PRICES_8 
			DUPout=DUP_PRICES_8 NODUPKEY 
			OUT=PRICES_8_1;
	by STATE_FIPS_CODE STATE_NAME
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		YEAR AGG_LEVEL_DESC;
run;
proc transpose data=PRICES_8_1 out=PRICES_8_2(drop=_NAME_);
	id AGG_LEVEL_DESC;
	idlabel AGG_LEVEL_DESC;
	var MARKETING_YEAR_PRICES_3 ;
	by STATE_FIPS_CODE STATE_NAME
		COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		YEAR;
run;
*/
/*************************************************************/
/*************************************************************/
/*************************************************************/
/*************************************************************/






/*************************************************************/
/*************************************************************/
/*************************************************************/
/*************************************************************/
data PRICES_8_st PRICES_8_us;
	set PRICES_8;
	if AGG_LEVEL_DESC IN ('STATE') then output PRICES_8_st; else
	if AGG_LEVEL_DESC IN ('NATIONAL') then output PRICES_8_us;
run;
data PRICES_8_us_1(keep=COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit YEAR
		MARKETING_YEAR_PRICES_US MARKETING_YEAR_PRICES_3_US 
		CALENDER_YEAR_PRICES_US JD_PRICES_US);
	set PRICES_8_us;
	MARKETING_YEAR_PRICES_US=MARKETING_YEAR_PRICES; 
	MARKETING_YEAR_PRICES_3_US=MARKETING_YEAR_PRICES_3; 
	CALENDER_YEAR_PRICES_US=CALENDER_YEAR_PRICES; 
	JD_PRICES_US=JD_PRICES;
	proc sort;
	by COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit YEAR;
run;
data PRICES_8_st_1(Keep=COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit
		STATE_FIPS_CODE STATE_NAME YEAR
		MARKETING_YEAR_PRICES_ST MARKETING_YEAR_PRICES_3_ST 
		CALENDER_YEAR_PRICES_ST JD_PRICES_ST);
	set PRICES_8_st;
	MARKETING_YEAR_PRICES_ST=MARKETING_YEAR_PRICES; 
	MARKETING_YEAR_PRICES_3_ST=MARKETING_YEAR_PRICES_3; 
	CALENDER_YEAR_PRICES_ST=CALENDER_YEAR_PRICES; 
	JD_PRICES_ST=JD_PRICES;
	proc sort;
	by COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit YEAR;
run;
data PRICES_9;
	retain Merge_price1;
	merge PRICES_8_us_1(in=a) PRICES_8_st_1(in=b);
	by COMMODITY_GROUP_code COMMODITY_GROUP 
		SOURCE_DESC SECTOR_DESC GROUP_DESC COMMODITY_DESC 
		CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC 
		STATISTICCAT_DESC PRICES_unit YEAR;
	if a and b then Merge_price1='State and National'; else
	if a then Merge_price1='National'; else
	if b then Merge_price1='State'; 
run;
/*************************************************************/
/*************************************************************/
/*************************************************************/
/*************************************************************/







/********************************************************/
/********************************************************/
data NASS2.Step1_PRICES_US; set PRICES_8_us_1; run;
data NASS2.Step1_PRICES_ST; set PRICES_8_st_1; run;
/********************************************************/
/********************************************************/




/* Stop timer */
data _null_;
  dur = datetime() - &_timer_start;
  put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-';
run;








