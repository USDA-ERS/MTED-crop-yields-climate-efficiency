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
/*************************************************************
LIBNAME NASS1 "\\usda.net\ers\MTEDCOMMON\APM\STAFF\SShaik\US\data\NASS";
LIBNAME NASS2 "\\usda.net\ers\MTEDCOMMON\APM\STAFF\SShaik\US\dataforanalysis\NASS";
LIBNAME NASS3 "\\usda.net\ers\MTEDCOMMON\APM\STAFF\SShaik\US\dataforanalysis\SCP";
*************************************************************/
/*************************************************************/



/*************************************************************/
/*************************************************************/
LIBNAME NASS2 'C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\dataforanalysis\NASS';
LIBNAME NASS3 'C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\dataforanalysis\SCP\domestic';

LIBNAME CLIM "C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\dataforanalysis\Climate";
LIBNAME CLIM1 "C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\analysis\climate";
/*************************************************************/
/*************************************************************/




/*************************************************************/
%let yrmonday = 20220312.;
%let yrmonday1 = 20220312;
/*************************************************************/


/*************************************************************/
/*************************************************************
filename reglink temp;
proc http
 url="https://www.ers.usda.gov/media/9592/reglink.xls"
 method="GET"
 out=reglink;
run;

proc import file=reglink
 out=reglink replace
 dbms=xls;
 NAMEROW=3;
 STARTROW=4;
run;
data reglink_1(keep=fips ERS_resource_region_code);
	set reglink;
	ERS_resource_region_code=ERS_resource_region;
run;
data reglink_2;
	length ERS_resource_region $250;
	set reglink_1;
	if ERS_resource_region_code=1 then ERS_resource_region='Heartland'; else
	if ERS_resource_region_code=2 then ERS_resource_region='Northern Crescent'; else
	if ERS_resource_region_code=3 then ERS_resource_region='Northern Great Plains'; else
	if ERS_resource_region_code=4 then ERS_resource_region='Prairie Gateway'; else
	if ERS_resource_region_code=5 then ERS_resource_region='Eastern Uplands'; else
	if ERS_resource_region_code=6 then ERS_resource_region='Southern Seaboard'; else
	if ERS_resource_region_code=7 then ERS_resource_region='Fruitful Rim'; else
	if ERS_resource_region_code=8 then ERS_resource_region='Basin and Range'; else
	if ERS_resource_region_code=9 then ERS_resource_region='Mississippi Portal';
	STATE_FIPS_CODE=round((fips/1000),1);
	if 24449 < fips <24999 then STATE_FIPS_CODE=24;
	if 29449 < fips <29999 then STATE_FIPS_CODE=29;
	if 32449 < fips <32999 then STATE_FIPS_CODE=32;
	if 48449 < fips <48999 then STATE_FIPS_CODE=48;
	if 51449 < fips <51999 then STATE_FIPS_CODE=51;
run;
data reglink_3;
	retain fips ERS_resource_region_code;
	set reglink_2;
	COUNTY_CODE=roundz((fips-(STATE_FIPS_CODE*1000)),1);
	cfips=fips;
	proc sort;
	by STATE_CODE COUNTY_CODE;
run;
*************************************************************/
/*************************************************************/
/*proc freq data=reglink_3; table cfips; run;*/



/*************************************************************/
/*************************************************************/
/*************************************************************/
/***********************************************
**************/
*filename CRzip ZIP "\\usda.net\ers\MTEDCOMMON\APM\STAFF\SShaik\US\data\NASS\qs.crops_&yrmonday.txt.gz" GZIP;
filename CRzip ZIP "\\usda.net\ers\MTEDCOMMON\APM\STAFF\SShaik\US\data\NASS\qs.crops_&yrmonday.txt.gz" GZIP;
data QS_CROPS(where=(SOURCE_DESC IN ('SURVEY')));
	%let _EFIERR_ = 0; 
	infile CRzip delimiter='09'x MISSOVER DSD lrecl=32767 firstobs=2 ;
/*
ftp://ftp.nass.usda.gov/quickstats/
data WORK.QS_CROPS    ;
	%let _EFIERR_ = 0;
	infile "C:\Users\Saleem.Shaik\OneDrive - USDA\Saleem.Shaik\papers\data\NASS\qs.crops_&yrmonday.txt"
	delimiter='09'x MISSOVER DSD lrecl=32767 firstobs=2 ;
*/
       informat SOURCE_DESC $250. ;
       informat SECTOR_DESC $250. ;
       informat GROUP_DESC $250. ;
       informat COMMODITY_DESC $250. ;
       informat CLASS_DESC $250. ;
       informat PRODN_PRACTICE_DESC $250. ;
       informat UTIL_PRACTICE_DESC $250. ;
       informat STATISTICCAT_DESC $250. ;
       informat UNIT_DESC $250. ;
       informat SHORT_DESC $450. ;
       informat DOMAIN_DESC $250. ;
       informat DOMAINCAT_DESC $250. ;
       informat AGG_LEVEL_DESC $250. ;
       informat STATE_ANSI best32. ;
       informat STATE_FIPS_CODE best32. ;
       informat STATE_ALPHA $250. ;
       informat STATE_NAME $250. ;
       informat ASD_CODE best32. ;
       informat ASD_DESC $250. ;
       informat COUNTY_ANSI best32. ;
       informat COUNTY_CODE best32. ;
       informat COUNTY_NAME $250. ;
       informat REGION_DESC $250. ;
       informat ZIP_5 $250. ;
       informat WATERSHED_CODE best32. ;
       informat WATERSHED_DESC $250. ;
       informat CONGR_DISTRICT_CODE $250. ;
       informat COUNTRY_CODE best32. ;
       informat COUNTRY_NAME $250. ;
       informat LOCATION_DESC $250. ;
       informat YEAR best32. ;
       informat FREQ_DESC $250. ;
       informat BEGIN_CODE best32. ;
       informat END_CODE best32. ;
       informat REFERENCE_PERIOD_DESC $250. ;
       informat WEEK_ENDING $250. ;
       informat LOAD_TIME anydtdtm40. ;
       informat VALUE $250. ;
       informat CV__ $4. ;
       format SOURCE_DESC $250. ;
       format SECTOR_DESC $250. ;
       format GROUP_DESC $250. ;
       format COMMODITY_DESC $250. ;
       format CLASS_DESC $250. ;
       format PRODN_PRACTICE_DESC $250. ;
       format UTIL_PRACTICE_DESC $250. ;
       format STATISTICCAT_DESC $250. ;
       format UNIT_DESC $250. ;
       format SHORT_DESC $450. ;
       format DOMAIN_DESC $250. ;
       format DOMAINCAT_DESC $250. ;
       format AGG_LEVEL_DESC $250. ;
       format STATE_ANSI best12. ;
       format STATE_FIPS_CODE best12. ;
       format STATE_ALPHA $250. ;
       format STATE_NAME $250. ;
       format ASD_CODE best12. ;
       format ASD_DESC $250. ;
       format COUNTY_ANSI best12. ;
       format COUNTY_CODE best12. ;
       format COUNTY_NAME $250. ;
       format REGION_DESC $250. ;
       format ZIP_5 $250. ;
       format WATERSHED_CODE best12. ;
       format WATERSHED_DESC $250. ;
       format CONGR_DISTRICT_CODE $250. ;
       format COUNTRY_CODE best12. ;
       format COUNTRY_NAME $250. ;
       format LOCATION_DESC $250. ;
       format YEAR best12. ;
       format FREQ_DESC $250. ;
       format BEGIN_CODE best12. ;
       format END_CODE best12. ;
       format REFERENCE_PERIOD_DESC $250. ;
       format WEEK_ENDING $250. ;
       format LOAD_TIME datetime. ;
       format VALUE $250. ;
       format CV__ $4. ;
    input
                SOURCE_DESC  $
                SECTOR_DESC  $
                GROUP_DESC  $
                COMMODITY_DESC  $
                CLASS_DESC  $
                PRODN_PRACTICE_DESC  $
                UTIL_PRACTICE_DESC  $
                STATISTICCAT_DESC  $
                UNIT_DESC  $
                SHORT_DESC  $
                DOMAIN_DESC  $
                DOMAINCAT_DESC  $
                AGG_LEVEL_DESC  $
                STATE_ANSI
                STATE_FIPS_CODE
                STATE_ALPHA  $
                STATE_NAME  $
                ASD_CODE
                ASD_DESC  $
                COUNTY_ANSI
                COUNTY_CODE
                COUNTY_NAME  $
                REGION_DESC  $
                ZIP_5  $
                WATERSHED_CODE
                WATERSHED_DESC  $
                CONGR_DISTRICT_CODE  $
                COUNTRY_CODE
                COUNTRY_NAME  $
                LOCATION_DESC  $
                YEAR
                FREQ_DESC  $
                BEGIN_CODE
                END_CODE
                REFERENCE_PERIOD_DESC  $
                WEEK_ENDING  $
                LOAD_TIME
                VALUE  $
                CV__  $
    ;
    if _ERROR_ then call symputx('_EFIERR_',1);
run;
Title 'Flag 1 & 2 - Limit to SURVEY data and Limit to Value-Numericals';
data QS_CROPS_0(drop=CV__) QS_CROPS_miss; set QS_CROPS; 
	where STATISTICCAT_DESC IN ('AREA HARVESTED', 'AREA PLANTED',
								'PRODUCTION', 'YIELD', 
								'PRICE RECEIVED', 'STOCKS') AND
		AGG_LEVEL_DESC IN ('NATIONAL', 'STATE', 'AGRICULTURAL DISTRICT', 
								'COUNTY') AND
		PRODN_PRACTICE_DESC IN ('ALL PRODUCTION PRACTICES', 'IRRIGATED',
							'NON-IRRIGATED', 'NON-IRRIGATED, CONTINUOUS CROP',
							'NON-IRRIGATED, FOLLOWING SUMMER FALLOW');

	if COUNTY_NAME='ST. JOSEPH' then COUNTY_NAME='ST JOSEPH';
	if COUNTY_NAME='ST. LOUIS' then COUNTY_NAME='ST LOUIS';
	if COUNTY_NAME='ST. JOHN THE BAPTIST' then COUNTY_NAME='ST JOHN THE BAPTIST';
	if COUNTY_NAME='ST. JOHNS' then COUNTY_NAME='ST JOHNS';
	if COUNTY_NAME='ST. LUCIE' then COUNTY_NAME='ST LUCIE';
	if COUNTY_NAME='STE. GENEVIEVE' then COUNTY_NAME='STE GENEVIEVE';

	if AGG_LEVEL_DESC='NATIONAL' then AGG_LEVEL_DESC_code=1; else  
	if AGG_LEVEL_DESC='STATE' then AGG_LEVEL_DESC_code=2; else 
	if AGG_LEVEL_DESC='AGRICULTURAL DISTRICT' then AGG_LEVEL_DESC_code=3; else
	if AGG_LEVEL_DESC='COUNTY' then AGG_LEVEL_DESC_code=4;  

	if STATE_NAME='US TOTAL' then STATE_ANSI=99;

	if VALUE IN ('(NA)', 'NA', 'N/A', '(N/A)', '(S)', '(X)',
				'(Y)', '(Z)', '(D)', 'D', '(DU)', '(O)', '(-)',
				'(.)', '(1)', '1-Apr',
'10-Apr',
'11-Apr',
'12-Apr',
'13-Apr',
'14-Apr',
'15-Apr',
'16-Apr',
'17-Apr',
'2-Apr',
'20-Apr',
'21-Apr',
'22-Apr',
'23-Apr',
'24-Apr',
'25-Apr',
'26-Apr',
'28-Apr',
'3-Apr',
'30-Apr',
'4-Apr',
'5-Apr',
'6-Apr',
'7-Apr',
'8-Apr',
'9-Apr',
'1-Feb'
'10-Feb',
'11-Feb',
'12-Feb',
'14-Feb',
'15-Feb',
'16-Feb',
'17-Feb',
'18-Feb',
'19-Feb',
'2-Feb',
'20-Feb',
'21-Feb',
'22-Feb',
'23-Feb',
'24-Feb',
'25-Feb',
'26-Feb',
'27-Feb',
'28-Feb',
'Feb-29',
'3-Feb',
'4-Feb',
'6-Feb',
'7-Feb',
'9-Feb',
'1-Jan',
'10-Jan',
'11-Jan',
'12-Jan',
'13-Jan',
'15-Jan',
'16-Jan',
'18-Jan',
'19-Jan',
'2-Jan',
'20-Jan',
'21-Jan',
'22-Jan',
'23-Jan',
'25-Jan',
'26-Jan',
'27-Jan',
'28-Jan',
'4-Jan',
'5-Jan',
'7-Jan',
'8-Jan',
'9-Jan',
'1-Mar',
'10-Mar',
'12-Mar',
'13-Mar',
'14-Mar',
'15-Mar',
'16-Mar',
'17-Mar',
'19-Mar',
'2-Mar',
'20-Mar',
'21-Mar',
'22-Mar',
'24-Mar',
'26-Mar',
'27-Mar',
'28-Mar',
'29-Mar',
'30-Mar',
'31-Mar',
'4-Mar',
'6-Mar',
'7-Mar',
'8-Mar',
'1-May',
'10-May',
'13-May',
'14-May',
'2-May',
'20-May',
'26-May',
'29-May',
'3-May',
'4-May',
'5-May') then output QS_CROPS_miss; else output QS_CROPS_0;
format VALUE;
run;
/*
proc freq data=QS_CROPS_miss;
*table (UNIT_DESC)*STATISTICCAT_DESC/norow nocol nocum nopercent out=freq_crops ;
table COMMODITY_DESC*STATISTICCAT_DESC/norow nocol nocum nopercent out=freq_crops ;
run;
*/
Title 'Flag 3 & 4 - Limit to specific STATISTICCAT_DESC';
data QS_CROPS_1 QS_CROPS_1_negative QS_CROPS_1_missing;
	length COMMODITY_GROUP $250;
	set QS_CROPS_0;
	where COMMODITY_DESC NOTIN ('BEDDING PLANT TOTALS','BERRY TOTALS','CITRUS TOTALS',
							'CROP TOTALS','FIELD CROP & VEGETABLE TOTALS',
							'FIELD CROP TOTALS','FLORICULTURE TOTALS',
							'FOOD CROP TOTALS','FRUIT & TREE NUT TOTALS',
							'FRUIT TOTALS','GRASSES & LEGUMES TOTALS','HORTICULTURE TOTALS',
							'NON-CITRUS FRUIT & TREE NUTS TOTALS','NON-CITRUS TOTALS',
							'NURSERY TOTALS','TREE NUT TOTALS','VEGETABLE TOTALS') AND
		DOMAIN_DESC IN ('TOTAL') AND
		REFERENCE_PERIOD_DESC IN ('MARKETING YEAR','YEAR',
								'JAN','FEB','MAR','APR','MAY','JUN', 
								'JUL','AUG','SEP','OCT','NOV','DEC',
								'FIRST OF DEC','FIRST OF JUN',
								'FIRST OF MAR','FIRST OF SEP') and
		UNIT_DESC NOTIN ('PCT', 'PCT BY COLOR', 'PCT BY GRADE',
					'PCT BY TYPE', 'PCT OF PARITY',
					'PCT OF PRODUCTION', 'PCT OF TOTAL STOCKS');

		if STATISTICCAT_DESC IN ('PRODUCTION') AND 
			UNIT_DESC IN ('$', '$ / TON', '$, CHERRY BASIS', 
			'$, ON TREE EQUIV', '$, PHD EQUIV') 
			then STATISTICCAT_DESC = 'PRODUCTION in $';
										
/**********************************/
if COMMODITY_DESC IN ('RICE','RYE','WHEAT') then do;
	COMMODITY_GROUP='FOOD GRAINS'; 
	COMMODITY_GROUP_CODE=1;
end; 

else if COMMODITY_DESC IN ('BARLEY','CORN','HAY','MILLET','OATS',
						'PROSO MILLET','SORGHUM') then do; 
	COMMODITY_GROUP='FEED CROPS';
	COMMODITY_GROUP_CODE=2;
end; 

else if COMMODITY_DESC IN ('COTTON','COTTON LINT','COTTON LINT, LONG STAPLE',
						'COTTON LINT, UPLAND','COTTONSEED') then do; 
	COMMODITY_GROUP='FIBER'; 
	COMMODITY_GROUP_CODE=3;
end; 

else if COMMODITY_DESC IN ('TOBACCO') then do;
	COMMODITY_GROUP='TOBACCO';
	COMMODITY_GROUP_CODE=4; 
end; 

else if COMMODITY_DESC IN ('CAMELINA','CANOLA','FLAXSEED','MISCELLANEOUS OIL CROPS',
						'MUSTARD','MUSTARD SEED','PEANUTS','RAPESEED','SAFFLOWER',
						'SOYBEANS','SUNFLOWER') then do ;
	COMMODITY_GROUP='OIL CROPS';
	COMMODITY_GROUP_CODE=5; 
end; 

else if COMMODITY_DESC IN ('BEANS','BEANS, GREEN LIMA','BEANS, GREEN LIMA, PROCESSING',
						'BEANS, SNAP','BEANS, SNAP, FRESH','BEANS, SNAP, PROCESSING',
						'DRY BEANS','LEGUMES') then do;
	COMMODITY_GROUP='VEGETABLES AND MELONS(DRY PULSES-BEANS)';
 	COMMODITY_GROUP_CODE=6;
end; 

else if COMMODITY_DESC IN ('CHICKPEAS','DRY PEAS','DRY PEAS, AUSTRIAN WINTER',
						'DRY PEAS, EDIBLE','DRY PEAS, WRINKLED SEED',
						'LENTILS','PEAS') then do ;
	COMMODITY_GROUP='VEGETABLES AND MELONS(DRY PULSES-PEAS)'; 
	COMMODITY_GROUP_CODE=6;
end; 

else if COMMODITY_DESC IN ('POTATOES','POTATOES, FALL','POTATOES, SPRING',
						'POTATOES, SUMMER','SWEET POTATOES','TARO') then do;
	COMMODITY_GROUP='VEGETABLES AND MELONS(POTATOES)'; 
	COMMODITY_GROUP_CODE=7;
end; 

else if COMMODITY_DESC IN ('ARTICHOKES','ASPARAGUS','BEETS','BROCCOLI','BRUSSELS SPROUTS',
						'CABBAGE','CABBAGE, FRESH','CANTALOUPS','CARROTS','CARROTS, FRESH',
						'CARROTS, PROCESSING','CAULIFLOWER','CELERY','CORN, SWEET',
						'CORN, SWEET, FRESH','CORN, SWEET, PROCESSING','CUCUMBERS',
						'CUCUMBERS, FRESH','CUCUMBERS, PROCESSING','EGGPLANT','ESCAROLE & ENDIVE',
						'GARLIC','GREENS','GINGER ROOT','HONEYDEWS','LETTUCE','LETTUCE, HEAD',
					  	'LETTUCE, LEAF','LETTUCE, ROMAINE','MELONS','OKRA','ONIONS',
						'ONIONS, SPRING','ONIONS, STORAGE','ONIONS, SUMMER, NONSTORAGE',
						'PEAS, GREEN','PEAS, GREEN, PROCESSING','PEPPERS','PEPPERS, CHILE',
                      	'PEPPERS, BELL','PUMPKINS','RADISHES','SPINACH','SPINACH, FRESH',
						'SPINACH, PROCESSING','SQUASH','SWEET CORN','TOMATOES',
						'TOMATOES, FRESH','TOMATOES, PROCESSING','WATERMELON') then do;
	COMMODITY_GROUP='VEGETABLES AND MELONS(SPECIALTY)';
	COMMODITY_GROUP_CODE=8; 
end; 

else if COMMODITY_DESC IN ('APPLES','APRICOTS','AVOCADOS','BANANAS','BLACKBERRIES',
						'BLACKBERRY GROUP','BOYSENBERRIES','BLUEBERRIES','CANEBERRIES',
						'CHERRIES','CHERRIES, SWEET','CHERRIES, TART','COFFEE','CRANBERRIES',
                      	'DATES','FIGS','GOOSEBERRIES','GRAPEFRUIT','GRAPES','GUAVAS',
						'KIWIFRUIT','LEMONS','LIMES','LOGANBERRIES','NECTARINES','OLIVES',
						'ORANGES','PAPAYAS','PEACHES','PEARS','PINEAPPLES','PLUMS',
                      	'PLUMS AND PRUNES','PRUNES','RASPBERRIES','STRAWBERRIES',
						'TANGELOS','TANGERINES') then do;
	COMMODITY_GROUP='FRUITS AND NUTS(FRUITS)';
	COMMODITY_GROUP_CODE=9; 
end; 

else if COMMODITY_DESC IN ('ALMONDS','HAZELNUTS','MACADAMIA NUTS','MACADAMIAS','PECANS',
						'PISTACHIOS','WALNUTS') then do;
	COMMODITY_GROUP='FRUITS AND NUTS(NUTS)';
	COMMODITY_GROUP_CODE=10; 
end; 

else if COMMODITY_DESC IN ('MAPLE PRODUCTS','SUGAR BEETS','SUGARBEETS','SUGARCANE',
						'SUGARCANE FOR SUGAR AND SEED') then do;
	COMMODITY_GROUP='SUGARS';
	COMMODITY_GROUP_CODE=11; 
end; else

if COMMODITY_DESC IN ('FLORICULTURE','HOPS','MINT','MISCELLANEOUS CROPS','MUSHROOMS',
						'PEPPERMINT OIL','SPEARMINT OIL') then do;
	COMMODITY_GROUP='ALL OTHER CROPS';
	COMMODITY_GROUP_CODE=12; 
end; 

else if COMMODITY_DESC IN ('BEDDING PLANT TOTALS','BERRY TOTALS','CITRUS TOTALS',
							'CROP TOTALS','FIELD CROP & VEGETABLE TOTALS',
							'FIELD CROP TOTALS','FLORICULTURE TOTALS',
							'FOOD CROP TOTALS','FRUIT & TREE NUT TOTALS',
							'FRUIT TOTALS','GRASSES & LEGUMES TOTALS','HORTICULTURE TOTALS',
							'NON-CITRUS FRUIT & TREE NUTS TOTALS','NON-CITRUS TOTALS',
							'NURSERY TOTALS','TREE NUT TOTALS','VEGETABLE TOTALS') then do;
	COMMODITY_GROUP='ONLY TOTALS';
	COMMODITY_GROUP_CODE=13; 
end; 
else do;
	COMMODITY_GROUP = GROUP_DESC;
	COMMODITY_GROUP_CODE=14;
end ;
/**********************************/

	if 1900 <= year <= 1909 then Decade_code  = 1;
	else if 1910 <= year <= 1919 then Decade_code  = 2;
	else if 1920 <= year <= 1929 then Decade_code  = 3;
	else if 1930 <= year <= 1939 then Decade_code  = 4;
	else if 1940 <= year <= 1949 then Decade_code  = 5;
	else if 1950 <= year <= 1959 then Decade_code  = 6;
	else if 1960 <= year <= 1969 then Decade_code  = 7;
	else if 1970 <= year <= 1979 then Decade_code  = 8;
	else if 1980 <= year <= 1989 then Decade_code  = 9;
	else if 1990 <= year <= 1999 then Decade_code  = 10;
	else if 2000 <= year <= 2009 then Decade_code = 11;
	else if 2010 <= year <= 2020 then Decade_code  = 12;

	if year < 1933 then fbdum_code = 1;
	else if 1933 <= year <= 1947 then fbdum_code = 2;
	else if 1948 <= year <= 1953 then fbdum_code = 3;
	else if 1954 <= year <= 1955 then fbdum_code = 4;
	else if 1956 <= year <= 1964 then fbdum_code = 5;
	else if 1965 <= year <= 1969 then fbdum_code = 6;
	else if 1970 <= year <= 1972 then fbdum_code = 7;
	else if 1973 <= year <= 1976 then fbdum_code = 8;
	else if 1977 <= year <= 1980 then fbdum_code = 9;
	else if 1981 <= year <= 1984 then fbdum_code = 10;
	else if 1985 <= year <= 1989 then fbdum_code = 11;
	else if 1990 <= year <= 1995 then fbdum_code = 12;
	else if 1996 <= year <= 2001 then fbdum_code = 13;
	else if 2002 <= year <= 2007 then fbdum_code = 14;
	else if 2008 <= year <= 2013 then fbdum_code = 15;
	else if 2014 <= year <= 2018 then fbdum_code = 16;
	else if 2019 <= year <= 2022 then fbdum_code = 17;

	VALUE1 = input(VALUE,comma30.0);
	if VALUE1>=0 then output QS_CROPS_1; else
	if VALUE1='.' then output QS_CROPS_1_missing; else
	output QS_CROPS_1_negative;
	format VALUE VALUE1;
run;
proc means data=QS_CROPS_1 mean noprint;
class COMMODITY_DESC CLASS_DESC PRODN_PRACTICE_DESC UTIL_PRACTICE_DESC AGG_LEVEL_DESC STATISTICCAT_DESC;
types () COMMODITY_DESC*CLASS_DESC*PRODN_PRACTICE_DESC*UTIL_PRACTICE_DESC*AGG_LEVEL_DESC*STATISTICCAT_DESC;
var VALUE1;
output out=sumstat_CROPS;
run;
data sumstat_CROPS_a; set sumstat_CROPS; where _STAT_='MEAN';run;
/*
proc contents short data=QS_CROPS_1 varnum; run;
COMMODITY_GROUP COMMODITY_GROUP_CODE 
SOURCE_DESC SECTOR_DESC GROUP_DESC 
COMMODITY_DESC CLASS_DESC PRODN_PRACTICE_DESC 
UTIL_PRACTICE_DESC STATISTICCAT_DESC UNIT_DESC 
SHORT_DESC DOMAIN_DESC DOMAINCAT_DESC 
AGG_LEVEL_DESC AGG_LEVEL_DESC_code 
STATE_ANSI STATE_FIPS_CODE STATE_ALPHA STATE_NAME 
ASD_CODE ASD_DESC COUNTY_ANSI COUNTY_CODE COUNTY_NAME 
REGION_DESC ZIP_5 WATERSHED_CODE WATERSHED_DESC 
CONGR_DISTRICT_CODE COUNTRY_CODE COUNTRY_NAME 
LOCATION_DESC 
YEAR Decade_code fbdum_code 
FREQ_DESC BEGIN_CODE END_CODE 
REFERENCE_PERIOD_DESC WEEK_ENDING LOAD_TIME 
VALUE VALUE1 
proc freq data=QS_CROPS_1;
table COMMODITY_GROUP COMMODITY_GROUP_CODE 
SOURCE_DESC SECTOR_DESC GROUP_DESC 
COMMODITY_DESC CLASS_DESC PRODN_PRACTICE_DESC 
UTIL_PRACTICE_DESC STATISTICCAT_DESC UNIT_DESC 
AGG_LEVEL_DESC AGG_LEVEL_DESC_code 
FREQ_DESC REFERENCE_PERIOD_DESC /norow nocol nocum nopercent ;
*table COMMODITY_DESC*STATISTICCAT_DESC/norow nocol nocum nopercent ;
run;
proc freq data=QS_CROPS_1_negative;
*table STATISTICCAT_DESC*AGG_LEVEL_DESC/norow nocol nocum nopercent ;
table COMMODITY_DESC*STATISTICCAT_DESC/norow nocol nocum nopercent ;
run;
proc freq data=QS_CROPS_1;
*where STATISTICCAT_DESC IN ('PRICE RECEIVED');
where UNIT_DESC IN ('PCT', 'PCT BY COLOR', 'PCT BY GRADE',
					'PCT BY TYPE', 'PCT OF PARITY',
					'PCT OF PRODUCTION', 'PCT OF TOTAL STOCKS');
*table COMMODITY_DESC*(FREQ_DESC REFERENCE_PERIOD_DESC)/norow nocol nocum nopercent ;
table COMMODITY_DESC*(FREQ_DESC REFERENCE_PERIOD_DESC UNIT_DESC)/norow nocol nocum nopercent ;
run;
*/
data NASS2.Step1_QS_CROPS(drop=DOMAIN_DESC DOMAINCAT_DESC 
				REGION_DESC ZIP_5 WATERSHED_CODE WATERSHED_DESC 
				CONGR_DISTRICT_CODE COUNTRY_CODE COUNTRY_NAME 
				BEGIN_CODE END_CODE 
				WEEK_ENDING LOAD_TIME);
	set QS_CROPS_1;
*	merge QS_CROPS_1(in=a) reglink_3(in=b);
*	by STATE_FIPS_CODE COUNTY_CODE;
*	if a;
run;
/*************************************************************/
/*************************************************************/


/* Stop timer */
data _null_;
  dur = datetime() - &_timer_start;
  put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-';
run;








