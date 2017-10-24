CREATE  OR REPLACE  FUNCTION <schema_name>.f_formatNumber
( arg1 integer )
RETURNS varchar(15) IMMUTABLE
AS $$
#
# Format the number with commas
#Change log
#	Who		When		        What
#	dtk		19Jul2016	    Created
#
return "{:12,d}".format(arg1)
$$ LANGUAGE plpythonu;