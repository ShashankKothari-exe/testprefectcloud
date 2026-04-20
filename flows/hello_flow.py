"""A trivial Prefect flow used as a smoke test for the deployment pipeline.

Run locally:
    python -m flows.hello_flow
"""

from __future__ import annotations

from prefect import flow, get_run_logger, task


@task
def say_hello(name: str) -> str:
    logger = get_run_logger()
    greeting = f"Hello, {name}!"
    logger.info(greeting)
    return greeting


@task
def add(a: int, b: int) -> int:
    logger = get_run_logger()
    result = a + b
    logger.info("add(%s, %s) = %s", a, b, result)
    return result


@flow(name="hello-flow")
def hello_flow(name: str = "world", a: int = 2, b: int = 3) -> dict[str, int | str]:
    greeting = say_hello(name)
    total = add(a, b)
    return {"greeting": greeting, "total": total}


if __name__ == "__main__":
    hello_flow()
