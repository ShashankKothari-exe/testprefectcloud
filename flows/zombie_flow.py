from prefect import flow
from prefect import task
import time


@task
def extract():
    pass

@task
def transform():
    pass

@task
def load():
    pass

@task
def wait_for_1_minute():
    time.sleep(60)

@flow(name="zombie-flow")
def zombie_flow():
    while True: # infinite loop 
        extract()
        transform()
        load()
        wait_for_1_minute()