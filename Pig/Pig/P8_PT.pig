h1b = load '/user/hive/warehouse/h1b_final' using PigStorage('\t') as (s_no:int, case_status:chararray,employer_name:chararray,soc_name:chararray,job_tittle:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray,worksite:chararray,longitude:double,latitude:double);



h1b2 = filter h1b by $1=='CERTIFIED';


h1b3 = filter h1b2 by $5 == 'N';

h1b_a = foreach h1b3 generate $4,$7,$6,$5;

h1b_2011 = filter h1b_a by $1=='2011';

h1b_2012 = filter h1b_a by $1=='2012';

h1b_2013 = filter h1b_a by $1=='2013';

h1b_2014 = filter h1b_a by $1=='2014';

h1b_2015 = filter h1b_a by $1=='2015';

h1b_2016 = filter h1b_a by $1=='2016'; 


h1b_groupa = group h1b_2011 by ($0,$1,$3);

h1b_groupb = group h1b_2012 by ($0,$1,$3);

h1b_groupc = group h1b_2013 by ($0,$1,$3);

h1b_groupd = group h1b_2014 by ($0,$1,$3);

h1b_groupe = group h1b_2015 by ($0,$1,$3);

h1b_groupf = group h1b_2016 by ($0,$1,$3);


h1b_counta = foreach h1b_groupa generate group,  ROUND_TO(AVG(h1b_2011.$2),2);
h1b_countb = foreach h1b_groupb generate group,  ROUND_TO(AVG(h1b_2012.$2),2);
h1b_countc = foreach h1b_groupc generate group,  ROUND_TO(AVG(h1b_2013.$2),2);
h1b_countd = foreach h1b_groupd generate group,  ROUND_TO(AVG(h1b_2014.$2),2);
h1b_counte = foreach h1b_groupe generate group,  ROUND_TO(AVG(h1b_2015.$2),2);
h1b_countf = foreach h1b_groupf generate group,  ROUND_TO(AVG(h1b_2016.$2),2);

h1b_all = UNION h1b_counta,h1b_countb,h1b_countc,h1b_countd,h1b_counte,h1b_countf;
h1b_order = order h1b_all by $0 desc;
dump h1b_order;

---union of those

--h1b_overall = union h1b_all,h1b_overall;
--dump h1b_overall;

 







