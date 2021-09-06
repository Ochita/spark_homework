import os
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, IntegerType
import datetime


def _get_year(data):
    if isinstance(data, str) or isinstance(data, unicode):
        try:
            dt = datetime.datetime.strptime(data, '%Y-%m-%d')
        except ValueError:
            pass
        else:
            return str(dt.year)
    elif isinstance(data, datetime.date) or isinstance(data, datetime.datetime):
        return str(data.year)
    return "Unknown"


def _get_array(num):
    if isinstance(num, str):
        try:
            num = int(num)
        except ValueError:
            pass
    if isinstance(num, int):
        return [x for x in range(num)]
    return []


get_year_udf = udf(_get_year, StringType())
get_array_udf = udf(_get_array, ArrayType(IntegerType()))
