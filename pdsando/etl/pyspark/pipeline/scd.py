import pyspark.sql as ps
import pyspark.sql.types as pst
import pyspark.sql.window as psw
import pyspark.sql.functions as psf

import pdsando.etl.pyspark.functions as pds

from pdsando.etl.constants import (
    SCD_CHANGED_INTERMEDIATE,
    SCD_CHANGED_NEW,
    SCD_CHANGED_OLD,
    SCD_DELETED,
    SCD_NEW,
    SCD_UNCHANGED,
    ROW_TYPE_EXISTING,
    ROW_TYPE_INCOMING,
    NULL_VAL_REP,
    COL_SCD_CODE,
    COL_KEY,
    COL_VALUE,
    COL_ROW_TYPE,
    COL_LAST_VALUE,
    COL_NEXT_VALUE,
)


def code_to_name(code):
    if code == SCD_CHANGED_INTERMEDIATE:
        return "changed-intermediate"
    if code == SCD_CHANGED_NEW:
        return "changed-new"
    if code == SCD_CHANGED_OLD:
        return "changed-old"
    if code == SCD_DELETED:
        return "deleted"
    if code == SCD_NEW:
        return "new"
    if code == SCD_UNCHANGED:
        return "unchanged"
    return None


class SCD:
    def __init__(
        self,
        old_df,
        key_columns=None,
        value_columns=None,
        scd_type=2,
        full_snapshot=False,
        nulls_are_blanks=False,
        debug=False,
        **kwargs,
    ):
        self._old_df = old_df
        self._key_columns = key_columns if key_columns else []
        self._value_columns = (
            value_columns
            if value_columns
            else [c for c in list(old_df.columns) if c not in self._key_columns]
        )
        self._scd_type = scd_type
        self._full_snapshot = full_snapshot
        self._nulls_are_blanks = nulls_are_blanks
        self._debug = debug

        intersect = set(self._key_columns).intersection(set(self._value_columns))
        if intersect:
            raise ValueError(
                f'Provided key_columns and value_columns cannot have any columns in common. Found: ({", ".join(intersect)})'
            )
        if self._scd_type < 0 or self._scd_type > 2:
            raise ValueError(
                "Invalid SCD Type. Valid values are between 0 and 2 (inclusive)."
            )
        if not self._key_columns and self._scd_type != 0:
            raise ValueError("Keys may only be exempted for SCD type 0")

    def apply(self, df, verbose=False):
        if verbose:
            print("Executing SCD processing")
            print("  SCD Type {}".format(self._scd_type))
            print("  Incoming Data is Full Snapshot: {}".format(self._full_snapshot))
            print("  Convert Nulls to Blanks: {}".format(self._nulls_are_blanks))
            print("  Key Columns: {}".format(self._key_columns))
            print("  Value Columns: {}".format(self._value_columns))

        scd_func = type_0
        if self._scd_type == 1:
            scd_func = type_1
        elif self._scd_type == 2:
            scd_func = type_2

        null_val = "" if self._nulls_are_blanks else NULL_VAL_REP

        if self._key_columns:
            # Incoming DF: Coalesce null vals to string rep, then generate key hash
            inc_df = df.withColumn(
                COL_KEY, pds.generate_hash(*self._key_columns, null_val=null_val)
            )

            # Existing DF: Coalesce null vals to string rep, then generate key hash
            old_df = self._old_df.withColumn(
                COL_KEY, pds.generate_hash(*self._key_columns, null_val=null_val)
            )
        else:
            inc_df = df.withColumn(COL_KEY, psf.lit(None))
            old_df = self._old_df.withColumn(COL_KEY, psf.lit(None))

        inc_df = inc_df.withColumn(
            COL_VALUE, pds.generate_hash(*self._value_columns, null_val=null_val)
        ).withColumn(COL_ROW_TYPE, psf.lit(ROW_TYPE_INCOMING))

        old_df = old_df.withColumn(
            COL_VALUE, pds.generate_hash(*self._value_columns, null_val=null_val)
        ).withColumn(COL_ROW_TYPE, psf.lit(ROW_TYPE_EXISTING))

        union_df = (
            old_df.union(inc_df)
            .withColumn(
                COL_LAST_VALUE,
                psf.lag(COL_VALUE).over(
                    psw.Window.partitionBy(COL_KEY).orderBy(COL_ROW_TYPE)
                ),
            )
            .withColumn(
                COL_NEXT_VALUE,
                psf.lead(COL_VALUE).over(
                    psw.Window.partitionBy(COL_KEY).orderBy(COL_ROW_TYPE)
                ),
            )
        )

        processed_df = scd_func(union_df)

        return processed_df.drop(
            COL_KEY, COL_VALUE, COL_ROW_TYPE, COL_LAST_VALUE, COL_NEXT_VALUE
        ).filter(psf.col(COL_SCD_CODE).isNotNull())

        # if self._debug:
        #    union_df[COL_SCD_CODE] = union_df[COL_SCD_CODE].apply(code_to_name)
        # else:
        #    union_df.drop(
        #        [COL_KEY, COL_VALUE, COL_ROW_TYPE, COL_LAST_VALUE, COL_NEXT_VALUE],
        #        axis=1,
        #        inplace=True,
        #    )
        # return union_df[union_df._scd_code_.notna()]


# Type 0: Once set, values are immutable.
#  If processing full snapshot, any existing key not found in the incoming data will be deleted.
#  If processing incremental data, any existing key not found in the incoming data will be considered unchanged.
def type_0(df, full_snapshot=False):
    if full_snapshot:
        return df.withColumn(
            COL_SCD_CODE,
            psf.when(
                psf.col(COL_ROW_TYPE)
                == ROW_TYPE_INCOMING & psf.col(COL_LAST_VALUE).isNull(),
                psf.lit(SCD_NEW),
            )
            .when(
                psf.col(COL_ROW_TYPE)
                == ROW_TYPE_EXISTING & psf.col(COL_NEXT_VALUE).isNotNull(),
                psf.lit(SCD_UNCHANGED),
            )
            .otherwise(psf.lit(None)),
        )
    else:
        return df.withColumn(
            COL_SCD_CODE,
            psf.when(
                psf.col(COL_ROW_TYPE)
                == ROW_TYPE_INCOMING & psf.col(COL_LAST_VALUE).isNull(),
                psf.lit(SCD_NEW),
            )
            .when(
                psf.col(COL_ROW_TYPE) == ROW_TYPE_EXISTING,
                psf.lit(SCD_UNCHANGED),
            )
            .otherwise(psf.lit(None)),
        )


# Type 1: Overwrite values of existing keys.
#  If processing full snapshot, any existing key not found in the incoming data will be deleted.
#  If processing incremental data, any existing key not found in the incoming data will be considered unchanged.
def type_1(df, full_snapshot=False):
    if full_snapshot:
        return df.withColumn(
            COL_SCD_CODE,
            psf.when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_INCOMING)
                & psf.col(COL_LAST_VALUE).isNull(),
                psf.lit(SCD_NEW),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_INCOMING)
                & psf.col(COL_LAST_VALUE).isNotNull()
                & (psf.col(COL_VALUE) != psf.col(COL_LAST_VALUE)),
                psf.lit(SCD_CHANGED_NEW),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_INCOMING)
                & psf.col(COL_LAST_VALUE).isNotNull()
                & (psf.col(COL_VALUE) == psf.col(COL_LAST_VALUE)),
                psf.lit(SCD_UNCHANGED),
            )
            .otherwise(psf.lit(None)),
        )
    else:
        return df.withColumn(
            COL_SCD_CODE,
            psf.when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_INCOMING)
                & psf.col(COL_LAST_VALUE).isNull(),
                psf.lit(SCD_NEW),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_INCOMING)
                & psf.col(COL_LAST_VALUE).isNotNull(),
                psf.lit(SCD_CHANGED_NEW),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_EXISTING)
                & psf.col(COL_NEXT_VALUE).isNull(),
                psf.lit(SCD_UNCHANGED),
            )
            .otherwise(psf.lit(None)),
        )


# Type 2: Track all changes and keep history.
#   If processing full snapshot, any existing key not found in the incoming data will be considered a delete event.
#   If processing incremental data, any existing key not found in the incoming data will be considered unchanged.
def type_2(df, full_snapshot=False):
    if full_snapshot:
        return df.withColumn(
            COL_SCD_CODE,
            psf.when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_INCOMING)
                & psf.col(COL_LAST_VALUE).isNull(),
                psf.lit(SCD_NEW),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_EXISTING)
                & psf.col(COL_NEXT_VALUE).isNull(),
                psf.lit(SCD_DELETED),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_EXISTING)
                & psf.col(COL_NEXT_VALUE).isNotNull()
                & (psf.col(COL_VALUE) != psf.col(COL_NEXT_VALUE)),
                psf.lit(SCD_CHANGED_OLD),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_INCOMING)
                & psf.col(COL_LAST_VALUE).isNotNull()
                & (psf.col(COL_VALUE) != psf.col(COL_LAST_VALUE)),
                psf.lit(SCD_CHANGED_NEW),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_EXISTING)
                & psf.col(COL_NEXT_VALUE).isNotNull()
                & (psf.col(COL_VALUE) == psf.col(COL_NEXT_VALUE)),
                psf.lit(SCD_UNCHANGED),
            )
            .otherwise(psf.lit(None)),
        )
    else:
        return df.withColumn(
            COL_SCD_CODE,
            psf.when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_INCOMING)
                & psf.col(COL_LAST_VALUE).isNull(),
                psf.lit(SCD_NEW),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_EXISTING)
                & psf.col(COL_NEXT_VALUE).isNull(),
                psf.lit(SCD_UNCHANGED),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_EXISTING)
                & psf.col(COL_NEXT_VALUE).isNotNull()
                & (psf.col(COL_VALUE) != psf.col(COL_NEXT_VALUE)),
                psf.lit(SCD_CHANGED_OLD),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_INCOMING)
                & psf.col(COL_LAST_VALUE).isNotNull()
                & (psf.col(COL_VALUE) != psf.col(COL_LAST_VALUE)),
                psf.lit(SCD_CHANGED_NEW),
            )
            .when(
                (psf.col(COL_ROW_TYPE) == ROW_TYPE_EXISTING)
                & psf.col(COL_NEXT_VALUE).isNotNull()
                & (psf.col(COL_VALUE) == psf.col(COL_NEXT_VALUE)),
                psf.lit(SCD_UNCHANGED),
            )
            .otherwise(psf.lit(None)),
        )
