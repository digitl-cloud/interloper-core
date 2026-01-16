import interloper as il

io = il.FileIO("data/")
partitioning = il.TimePartitionConfig(column="date")


@il.asset(io=io, partitioning=partitioning)
def a(context: il.ExecutionContext) -> list[dict]:
    return [{"date": context.partition_date, "v": 1}]


@il.asset(io=io, partitioning=partitioning)
def b(context: il.ExecutionContext) -> list[dict]:
    return [{"date": context.partition_date, "v": 2}]


@il.asset(io=io, partitioning=partitioning)
def c(context: il.ExecutionContext, a: list[dict]) -> list[dict]:
    return [{"date": context.partition_date, "v": int(a[0]["v"] + 1)}]


@il.asset(io=io, partitioning=partitioning)
def d(context: il.ExecutionContext, a: list[dict]) -> list[dict]:
    return [{"date": context.partition_date, "v": int(a[0]["v"] * 3)}]


@il.asset(io=io, partitioning=partitioning)
def e(context: il.ExecutionContext, b: list[dict], c: list[dict]) -> list[dict]:
    return [{"date": context.partition_date, "v": int(b[0]["v"] + c[0]["v"])}]


@il.asset(io=io, partitioning=partitioning)
def f(context: il.ExecutionContext, d: list[dict]) -> list[dict]:
    return [{"date": context.partition_date, "v": int(d[0]["v"] - 1)}]


@il.asset(io=io, partitioning=partitioning)
def g(context: il.ExecutionContext, e: list[dict], f: list[dict]) -> list[dict]:
    return [{"date": context.partition_date, "v": int(e[0]["v"] + f[0]["v"])}]


assets = [a, b, c, d, e, f, g]
dag = il.DAG(*assets)
