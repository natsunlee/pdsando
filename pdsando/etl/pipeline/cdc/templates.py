import pandas as pd
import numpy as np
from pdsando.etl.pipeline.cdc.constants import (
    CDC_CHANGED_INTERMEDIATE,
    CDC_CHANGED_NEW,
    CDC_CHANGED_OLD,
    CDC_DELETED,
    CDC_NEW,
    CDC_UNCHANGED,
    ROW_TYPE_EXISTING,
    ROW_TYPE_INCOMING,
    COL_CDC_CODE,
    COL_KEY,
    COL_VALUE,
    COL_ROW_TYPE,
    COL_LAST_VALUE,
    COL_NEXT_VALUE,
)

# Type 0: Once set, values are immutable.
#  If processing full snapshot, any existing key not found in the incoming data will be deleted.
#  If processing incremental data, any existing key not found in the incoming data will be considered unchanged.
def type_0(row, full_snapshot=False):
    if full_snapshot:
        if (row[COL_ROW_TYPE] == ROW_TYPE_INCOMING) and pd.isnull(row[COL_LAST_VALUE]):
            return CDC_NEW
        if (row[COL_ROW_TYPE] == ROW_TYPE_EXISTING) and pd.notnull(row[COL_NEXT_VALUE]):
            return CDC_UNCHANGED
    else:
        if (row[COL_ROW_TYPE] == ROW_TYPE_INCOMING) and pd.isnull(row[COL_LAST_VALUE]):
            return CDC_NEW
        if row[COL_ROW_TYPE] == ROW_TYPE_EXISTING:
            return CDC_UNCHANGED
    return None


# Type 1: Overwrite values of existing keys.
#  If processing full snapshot, any existing key not found in the incoming data will be deleted.
#  If processing incremental data, any existing key not found in the incoming data will be considered unchanged.
def type_1(row, full_snapshot=False):
    if full_snapshot:
        if (row[COL_ROW_TYPE] == ROW_TYPE_INCOMING) and pd.isnull(row[COL_LAST_VALUE]):
            return CDC_NEW
        if (
            (row[COL_ROW_TYPE] == ROW_TYPE_INCOMING)
            and pd.notnull(row[COL_LAST_VALUE])
            and (row[COL_VALUE] != row[COL_LAST_VALUE])
        ):
            return CDC_CHANGED_NEW
        if (
            (row[COL_ROW_TYPE] == ROW_TYPE_INCOMING)
            and pd.notnull(row[COL_LAST_VALUE])
            and (row[COL_VALUE] == row[COL_LAST_VALUE])
        ):
            return CDC_UNCHANGED
    else:
        if (row[COL_ROW_TYPE] == ROW_TYPE_INCOMING) and pd.isnull(row[COL_LAST_VALUE]):
            return CDC_NEW
        if (row[COL_ROW_TYPE] == ROW_TYPE_INCOMING) and pd.notnull(row[COL_LAST_VALUE]):
            return CDC_CHANGED_NEW
        if (row[COL_ROW_TYPE] == ROW_TYPE_EXISTING) and pd.isnull(row[COL_NEXT_VALUE]):
            return CDC_UNCHANGED
    return None


# Type 2: Track all changes and keep history.
#   If processing full snapshot, any existing key not found in the incoming data will be considered a delete event.
#   If processing incremental data, any existing key not found in the incoming data will be considered unchanged.
def type_2(row, full_snapshot=False):
    if full_snapshot:
        if (row[COL_ROW_TYPE] == ROW_TYPE_INCOMING) and pd.isnull(row[COL_LAST_VALUE]):
            return CDC_NEW
        if (row[COL_ROW_TYPE] == ROW_TYPE_EXISTING) and pd.isnull(row[COL_NEXT_VALUE]):
            return CDC_DELETED
        if (
            (row[COL_ROW_TYPE] == ROW_TYPE_EXISTING)
            and pd.notnull(row[COL_NEXT_VALUE])
            and (row[COL_VALUE] != row[COL_NEXT_VALUE])
        ):
            return CDC_CHANGED_OLD
        if (
            (row[COL_ROW_TYPE] == ROW_TYPE_INCOMING)
            and pd.notnull(row[COL_LAST_VALUE])
            and (row[COL_VALUE] != row[COL_LAST_VALUE])
        ):
            return CDC_CHANGED_NEW
        if (
            (row[COL_ROW_TYPE] == ROW_TYPE_EXISTING)
            and pd.notnull(row[COL_NEXT_VALUE])
            and (row[COL_VALUE] == row[COL_NEXT_VALUE])
        ):
            return CDC_UNCHANGED
    else:
        if (row[COL_ROW_TYPE] == ROW_TYPE_INCOMING) and pd.isnull(row[COL_LAST_VALUE]):
            return CDC_NEW
        if (row[COL_ROW_TYPE] == ROW_TYPE_EXISTING) and pd.isnull(row[COL_NEXT_VALUE]):
            return CDC_UNCHANGED
        if (
            (row[COL_ROW_TYPE] == ROW_TYPE_EXISTING)
            and pd.notnull(row[COL_NEXT_VALUE])
            and (row[COL_VALUE] != row[COL_NEXT_VALUE])
        ):
            return CDC_CHANGED_OLD
        if (
            (row[COL_ROW_TYPE] == ROW_TYPE_INCOMING)
            and pd.notnull(row[COL_LAST_VALUE])
            and (row[COL_VALUE] != row[COL_LAST_VALUE])
        ):
            return CDC_CHANGED_NEW
        if (
            (row[COL_ROW_TYPE] == ROW_TYPE_EXISTING)
            and pd.notnull(row[COL_NEXT_VALUE])
            and (row[COL_VALUE] == row[COL_NEXT_VALUE])
        ):
            return CDC_UNCHANGED
    return None
