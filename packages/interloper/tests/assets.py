"""Assets for testing.

These assets are used to test the multiprocessing runner. They are defined at module level so they can be
loaded from a spec.
"""

import interloper as il


@il.asset
def asset_a(context: il.ExecutionContext) -> str:  # noqa: D103
    return "a_ran"


@il.asset
def asset_b(context: il.ExecutionContext) -> str:  # noqa: D103
    return "b_ran"


@il.asset
def asset_c(context: il.ExecutionContext, a: str) -> str:  # noqa: D103
    return f"c_ran_with_{a}"


@il.asset
def asset_a_fails(context: il.ExecutionContext) -> str:  # noqa: D103
    raise RuntimeError("boom")


@il.asset
def asset_b_success(context: il.ExecutionContext) -> str:  # noqa: D103
    return "b_success_ran"
