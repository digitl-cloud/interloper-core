"""Example script demonstrating multi-thread runner."""

import interloper as il

destination = il.FileDestination(base_path="data")


@il.asset(destination=destination)
def a(context: il.ExecutionContext) -> None:
    context.logger.info("Hello from A")


@il.asset(destination=destination)
def b(context: il.ExecutionContext) -> None:
    context.logger.info("Hello from B")


@il.asset(destination=destination)
def c(context: il.ExecutionContext) -> None:
    context.logger.info("Hello from C")


@il.asset(destination=destination)
def d(context: il.ExecutionContext) -> None:
    context.logger.info("Hello from D")


@il.asset(destination=destination)
def e(context: il.ExecutionContext) -> None:
    context.logger.info("Hello from E")


dag = il.DAG(a, b, c, d, e)
#    ↗ b ↘
#  a → c → e
#    ↘ d ↗


if __name__ == "__main__":
    with il.MultiThreadRunner(on_event=print) as runner:
        result = runner.run(dag=dag)
    print(result)
