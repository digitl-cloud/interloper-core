import datetime as dt

import interloper as il
from interloper_assets import Adup

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    def on_event(event: il.Event) -> None:
        print(event)

    il.subscribe(on_event)

    partition = il.TimePartition(dt.date(2024, 1, 1))
    io = il.FileIO("data/")
    source = Adup(io=io)
    result = il.DAG(source).materialize(partition_or_window=partition)
    print(result)
