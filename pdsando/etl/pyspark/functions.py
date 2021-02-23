import hashlib
import pyspark.sql as ps
import pyspark.sql.types as pst
import pyspark.sql.functions as psf

convertUDF = psf.udf(lambda x: hashlib.md5(str(x).encode("utf-8")).hexdigest())


def generate_hash(*col_list, **kwargs):
    null_val = kwargs.get("null_val", "")

    return convertUDF(
        psf.concat_ws(
            ",",
            *[
                psf.trim(
                    psf.coalesce(psf.col(c).cast(pst.StringType()), psf.lit(null_val))
                )
                for c in col_list
            ]
        )
    )


def fill_na(df, fill_val, *col_list):
    temp = df
    for c in col_list:
        temp = temp.withColumn(c, psf.coalesce(psf.col(c), psf.lit(fill_val)))
    return temp