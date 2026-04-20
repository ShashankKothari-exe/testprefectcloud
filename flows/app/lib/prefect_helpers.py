"""Minimal Prefect helpers for chapter/section notes flows (Secret blocks only)."""

from prefect.blocks.system import Secret


def load_secret(blockname: str):
    res = Secret.load(blockname)
    return res.get()
