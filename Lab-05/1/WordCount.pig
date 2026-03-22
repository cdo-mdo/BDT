lines = LOAD '/user/cloudera/InputForWC.txt' USING TextLoader() AS (line: chararray);

clean = FOREACH lines GENERATE REPLACE(REPLACE(line, '\\r', ''), '\\t', ' ') AS line;

words = FOREACH clean GENERATE FLATTEN(TOKENIZE(line)) AS word;

groups = GROUP words BY word;

counts = FOREACH groups GENERATE group AS word, COUNT(words) AS cnt;

sorted = ORDER counts BY word;

-- DUMP sorted;
STORE sorted INTO '/user/cloudera/output/WordCount' USING PigStorage(',');

