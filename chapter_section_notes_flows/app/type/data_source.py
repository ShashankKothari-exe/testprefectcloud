from dataclasses import dataclass
from typing import Literal


@dataclass
class ABSCreds:
    account_name: str
    sas_token: str


@dataclass
class DataSource:
    """
    It is a directory inside a blob store or local file system where files are read from or written to.
    """

    type: Literal["AZURE_BLOB_STORAGE", "LOCAL_FILE_SYSTEM", "BITBUCKET"]
    path: str  # Its the basepath of the data source, inside the blob store, not the final path of a file
    creds: ABSCreds | None = None
