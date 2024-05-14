"""
Utility classes, methods, and variables.
"""

from pprint import pprint
import sys
import typing

import snowflake.snowpark as snowpark  # type: ignore


def create_data_checkpoint(
    df: typing.Union[snowpark.table.Table, snowpark.dataframe.DataFrame],
    df_name: str,
    file_name: str,
) -> None:
    """
    Creates a data checkpoint to resume execution in the LocalStack Snowflake
    emulator. Generates an EXPLAIN ANALYZE file, saves the SnowPark dataframe
    schema, and snapshots the data of the entire dataframe and saves it in a
    Parquet file.

    :param df: Actual Snowpark DataFrame or Table.
    :param df_name: Variable name that the dataframe is assigned to.
    :param file_name: The name of the file e.g. `01_feature_engineering` that
        should be used to save.
    """
    # Write the Snowflake SQL `EXPLAIN ANALYZE` query plan to a text file in
    # order to compare against the PostgreSQL transpiled SQL by temporarily
    # redirecting the stdout (`.explain()` returns no text, just prints to
    # stdout).

    # Open a file where you want to redirect the output
    with open(f"tempdata/{file_name}.{df_name}.explain.txt", "w") as f:
        # Save the original stdout so that we can revert stdout back to its
        # original state later.
        original_stdout = sys.stdout
        try:
            # Set stdout to the file object.
            sys.stdout = f
            # Line of code that writes to stdout.
            df.explain()
        finally:
            # Reset stdout back to its original setting.
            sys.stdout = original_stdout

    # Write the Snowflake Snowpark dataframe schema, since schema details are
    # lost when writing data over to a Parquet file and cannot be inferred with
    # 100% confidence from a Pandas dataframe.

    # Open a file where you want to redirect the output
    with open(f"tempdata/{file_name}.{df_name}.schema.txt", "w") as f:
        # Save the original stdout so that we can revert stdout back to its
        # original state later.
        original_stdout = sys.stdout
        try:
            # Set stdout to the file object.
            sys.stdout = f
            # Line of code that writes to stdout.
            print(df.schema)
            print("\n\n")
            pprint(df.schema.fields)
        finally:
            # Reset stdout back to its original setting.
            sys.stdout = original_stdout

    # Write the data over to a Parquet file in order to provide data checkpoint
    # for resuming execution for the Snowflake emulator.
    df.to_pandas().to_parquet(f"tempdata/{file_name}.{df_name}.parquet")
