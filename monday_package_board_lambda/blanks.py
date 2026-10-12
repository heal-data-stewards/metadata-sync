"""
Shared Monday-upload failsafe: every empty cell becomes '-', except the key
column. Monday only overwrites a cell when the import gives it some value,
so a genuinely empty cell would leave whatever was there before instead of
clearing it.
"""

import pandas as pd


def fill_blanks(df: pd.DataFrame, key_column: str = "HDPID") -> pd.DataFrame:
    out = df.copy()
    value_columns = [c for c in out.columns if c != key_column]
    out[value_columns] = out[value_columns].fillna("-")
    out[value_columns] = out[value_columns].replace("", "-")
    return out
