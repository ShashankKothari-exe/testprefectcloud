from prefect import flow
from prefect import task


@task
def extract():
    pass

@task
def transform():
    pass

@task
def load():
    pass

@flow(name="zombie-flow")
def zombie_flow():
    while True: # infinite loop 
        extract()
        transform()
        load()