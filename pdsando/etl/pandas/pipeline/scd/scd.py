import hashlib
from pdsando.core.wrappers import PipelineStage
import pdsando.etl.pandas.pipeline.scd.templates as templates
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


def hash_string(value):
    return hashlib.md5(str(value).encode("utf-8")).hexdigest()


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


class SCD(PipelineStage):
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
        super().__init__(exmsg="SCD failure", desc="ETL Stage")

    def _prec(self, df):
        intersect = set(self._key_columns).intersection(set(self._value_columns))
        if intersect:
            print(
                f'Provided key_columns and value_columns cannot have any columns in common. Found: ({", ".join(intersect)})'
            )
            return False
        if self._scd_type < 0 or self._scd_type > 2:
            print("Invalid SCD Type. Valid values are between 0 and 2 (inclusive).")
            return False
        if not self._key_columns and self._scd_type != 0:
            print("Keys may only be exempted for SCD type 0")
            return False
        return True

    def _transform(self, df, verbose):
        if verbose:
            print("Executing SCD processing")
            print("  SCD Type {}".format(self._scd_type))
            print("  Incoming Data is Full Snapshot: {}".format(self._full_snapshot))
            print("  Convert Nulls to Blanks: {}".format(self._nulls_are_blanks))
            print("  Key Columns: {}".format(self._key_columns))
            print("  Value Columns: {}".format(self._value_columns))

        ret_df = df
        old_copy = self._old_df.copy()

        scd_func = getattr(templates, "type_{}".format(self._scd_type))
        null_val = "" if self._nulls_are_blanks else NULL_VAL_REP

        if self._key_columns:
            # Incoming DF: Coalesce null vals to string rep, then generate key hash
            key_df = ret_df[self._key_columns].fillna(null_val)
            ret_df[COL_KEY] = key_df.astype(str).sum(1).apply(hash_string)
            # Existing DF: Coalesce null vals to string rep, then generate key hash
            key_df = old_copy[self._key_columns].fillna(null_val)
            old_copy[COL_KEY] = key_df.astype(str).sum(1).apply(hash_string)
        else:
            ret_df[COL_KEY] = None
            old_copy[COL_KEY] = None

        val_df = ret_df[self._value_columns].fillna(null_val)
        ret_df[COL_VALUE] = val_df.astype(str).sum(1).apply(hash_string)
        ret_df[COL_ROW_TYPE] = ROW_TYPE_INCOMING

        val_df = old_copy[self._value_columns].fillna(null_val)
        old_copy[COL_VALUE] = val_df.astype(str).sum(1).apply(hash_string)
        old_copy[COL_ROW_TYPE] = ROW_TYPE_EXISTING

        union_df = old_copy.append(ret_df).reset_index(drop=True)
        grouped = union_df.groupby(COL_KEY)[COL_VALUE]
        union_df[COL_LAST_VALUE] = grouped.shift(1)
        union_df[COL_NEXT_VALUE] = grouped.shift(-1)
        union_df[COL_SCD_CODE] = union_df.apply(
            lambda row: scd_func(row, self._full_snapshot), axis=1
        )

        if self._debug:
            union_df[COL_SCD_CODE] = union_df[COL_SCD_CODE].apply(code_to_name)
        else:
            union_df.drop(
                [COL_KEY, COL_VALUE, COL_ROW_TYPE, COL_LAST_VALUE, COL_NEXT_VALUE],
                axis=1,
                inplace=True,
            )

        return union_df[union_df._scd_code_.notna()]
