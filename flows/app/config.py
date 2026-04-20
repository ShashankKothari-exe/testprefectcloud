import os
import string
from dataclasses import dataclass

from dotenv import dotenv_values

env_config = dotenv_values()
os_config = os.environ
config = env_config | os_config


CONCURRENT_EXECUTION_COUNT = int(config.get("CCT_CONCURRENT_EXECUTION_COUNT", 6))

# Azure: same container and Prefect Secret block as ``flows/etl_flow.py`` in this repo.
AZURE_STORAGE_ACCOUNT = (config.get("CCT_AZURE_STORAGE_ACCOUNT") or "").strip() or "jmdtestingkyg"
AZURE_BLOB_CONTAINER = (config.get("CCT_AZURE_BLOB_CONTAINER") or "").strip() or "ptest"
AZURE_PT_SECRET_BLOCK = (config.get("CCT_AZURE_SECRET_BLOCK_NAME") or "").strip() or "azure-ptest"


# Silver files for HS codes
@dataclass
class SilverFileNamesHS:
    hs_codes: string.Template
    duty_rates: string.Template
    regulation_data: string.Template
    hs_tree_desc: string.Template


SILVER_FILES = SilverFileNamesHS(
    hs_codes=string.Template(
        "regulatory_source/${country_name}/HS_Code_Description.json"
    ),
    duty_rates=string.Template("regulatory_source/${country_name}/HS_DutyRates.json"),
    regulation_data=string.Template(
        "regulatory_source/${country_name}/HS_RegulationData.json"
    ),
    hs_tree_desc=string.Template(
        "regulatory_source/${country_name}/HS_Code_Description_Tree.csv"
    ),
)
