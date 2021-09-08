import os
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, IntegerType
import datetime
import geohash


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


def _get_geohash(lat, lon):
    try:
        lat = float(lat)
        lon = float(lon)
    except ValueError:
        pass
    else:
        return geohash.encode(lat, lon, precision=4)


get_year_udf = udf(_get_year, StringType())
get_array_udf = udf(_get_array, ArrayType(IntegerType()))
get_geohash_udf = udf(_get_geohash, StringType())
