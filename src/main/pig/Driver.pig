DEFINE EXTRACT_XML_LOG_EVENT com.hortonworks.pso.udf.pig.logs.microsoft.EXTRACT_XML_LOG_EVENT;

ROWS = LOAD '$input' USING PigStorage('\u0001') as (row:chararray);
out = foreach ROWS generate EXTRACT_XML_LOG_EVENT(row);
STORE out into '$output' using PigStorage('\u0001');
