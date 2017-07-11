h1b = load '/user/hive/warehouse/h1b_final' using PigStorage() as (s_no:int,case_status:chararray,emp_name:chararray,soc_name:chararray,job_title:chararray,full_time:chararray,prevailing_wage:int,year:chararray,worksite:chararray,longitude:int,latitude:int);

h1b_a = foreach h1b generate emp_name,case_status;
h1b_b = filter h1b_a by case_status == 'CERTIFIED-WITHDRAWN';
h1b_c= filter h1b_a by case_status == 'CERTIFIED';
h1b_d = group h1b_a by emp_name;
h1b_e = group h1b_c by emp_name;
h1b_f = group h1b_b by emp_name;
h1b_g = foreach h1b_d generate group as Emp_name, COUNT(h1b_a) as cnt;
h1b_h = foreach h1b_e generate group as Emp_name, COUNT(h1b_c) as cnt;
h1b_i = foreach h1b_f generate group as Emp_name, COUNT(h1b_b) as cnt;
h1b_j = join h1b_g by $0,h1b_h by $0,h1b_i by $0;
h1b_k = foreach h1b_j generate $0,$1,($3+$5);
h1b_l = foreach h1b_k generate $0,$1,((float)$2*100/(float)$1)as h1b_l;
h1b_m = filter h1b_l by $1>1000 and $2>70.0;
h1b_order = order h1b_m by $2 desc;
dump h1b_order;

