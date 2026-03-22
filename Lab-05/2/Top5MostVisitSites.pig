users = LOAD '/user/cloudera/users.csv' USING PigStorage(',') AS (user: chararray, age: int);

users18_25 = FILTER users BY age >= 18 AND age <= 25;

pages = LOAD '/user/cloudera/pages.csv' USING PigStorage(',') AS (user: chararray, page: chararray);

users18_25Pages = JOIN users18_25 BY user, pages BY user;

accessPages = FOREACH users18_25Pages GENERATE pages::page AS page;

groupPages = GROUP accessPages BY page;

counts = FOREACH groupPages GENERATE group AS page, COUNT(accessPages) AS cnt;

sorted = ORDER counts BY cnt DESC;

top5 = LIMIT sorted 5;

-- DUMP top5;
STORE top5 INTO '/user/cloudera/output/top5' USING PigStorage(',');
