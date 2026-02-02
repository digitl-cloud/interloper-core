"""Example script demonstrating serial runner."""

import interloper as il

io = il.FileIO("data")


@il.asset(io=io)
def a() -> None:
    print("A")


@il.asset(io=io)
def b(a: str) -> None:
    print("B")


@il.asset(io=io)
def c(a: str) -> None:
    print("C")


@il.asset(io=io)
def d(a: str) -> None:
    print("D")


@il.asset(io=io)
def e(b: str, c: str, d: str) -> None:
    print("E")


dag = il.DAG(a, b, c, d, e)

#    ↗ b ↘
#  a → c → e
#    ↘ d ↗


if __name__ == "__main__":

    def on_event(event: il.Event) -> None:
        print(event.to_json())

    with il.SerialRunner(on_event=on_event) as runner:
        result = runner.run(dag=dag)
    print(result)
