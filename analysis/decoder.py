# %%
from os import getenv

import pandas as pd
from dotenv import load_dotenv
from snowflake.snowpark import Session

load_dotenv("../.env")
# %%
connection_parameters = {
    "USER": getenv("SF_USERNAME"),
    "PASSWORD": getenv("SF_PASSWORD"),
    "DATABASE": getenv("SF_DATABASE"),
    "SCHEMA": getenv("SF_SCHEMA"),
    "WAREHOUSE": getenv("SF_WAREHOUSE"),
    "ROLE": getenv("SF_ROLE"),
    "QUERY_TAG": getenv("SF_QUERY_TAG"),
    "ACCOUNT": getenv("SF_ACCOUNT"),
    "REGION": getenv("SF_REGION"),
}

new_session = Session.builder.configs(connection_parameters).create()

# %%
df_table = new_session.table("streamline.decode_logs")
# %%
df_block_number = df_table.select("BLOCK_NUMBER")

# %%
df_grp = df_block_number.group_by(["BLOCK_NUMBER"]).count()
# %%
df = df_grp.collect()
# %%
df = pd.DataFrame(df)
# %%
df.sort_values("BLOCK_NUMBER", inplace=True)
# %%
df["BIN"] = df["CUM_COUNT"] // 10000000
results = df.groupby(["BIN"]).agg({"BLOCK_NUMBER": [min, max]})
# %%
from shutil import copy2
from pathlib import Path

# %%


def create_model(row):
    template = Path("./streamline__decode_logs_history_start_stop.sql")
    pad_length = 9
    copy2(
        template,
        Path("../models/silver/streamline/decoder/")
        / template.name.replace("start", str(row[0]).zfill(pad_length)).replace(
            "stop", str(row[1]).zfill(pad_length)
        ),
    )


# %%
results["BLOCK_NUMBER"].apply(create_model, axis=1)
# %%
