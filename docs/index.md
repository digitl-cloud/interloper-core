---
hide:
    - navigation
    - toc
---

<div class="title center" markdown>
# Interloper
## The ultra-portable data asset framework
</div>

Interloper is an open-source Python data asset framework that makes defining, configuring and materializing data assets effortless.
It combines the flexibility of a very lightweight Python library with powerful execution features inspired by modern orchestrators.

---

## Core concepts

- **Everything is an asset** -- In Interloper, an asset is a first-class entity. It **produces data**, which is then **materialized independently**. The framework provides a simple, structured way to define assets without unnecessary complexity.
- **Flexible IO-based materialization** -- Asset materialization is **driven by IO configuration**, completely separate from how the data is produced. This allows for clean, flexible execution.
- **Framework-agnostic data outputs** -- Interloper **does not enforce metadata dependencies** on destinations. Your data is written and mutated in a **deterministic, transparent way**, ensuring full control over your pipelines.


## Features

- **Asset & source definition** -- Define structured, reusable data assets using decorators.
- **Multi-IO materialization** -- Materialize data to multiple destinations with ease.
- **Schema & normalizer** -- Normalize, validate and reconcile data structures automatically.
- **Upstream asset dependencies** -- Build logical relationships between assets with automatic dependency resolution.
- **Data validation** -- Ensure data integrity before and during materialization.
- **Partitioning & backfilling** -- Efficiently process and reprocess historical data.
- **Multiple runners** -- Execute assets serially, multi-threaded, multi-process, in Docker, or on Kubernetes.
- **Event system** -- Subscribe to lifecycle events for monitoring and observability.
- **CLI** -- Run and backfill DAGs from the command line.

---

<div class="center" markdown>
# IO Integrations
Interloper ships with built-in IO backends and additional packages for external destinations.
</div>

<div class="grid cards center" markdown>
- :material-file-outline: **FileIO**
    Pickle-based local filesystem storage. Ships with the core library.
- :material-memory: **MemoryIO**
    In-memory storage for testing and development. Ships with the core library.
- :simple-postgresql: **PostgreSQL**
    SQL-based materialization via `interloper-sql`.
- :simple-mysql: **MySQL**
    SQL-based materialization via `interloper-sql`.
- :simple-sqlite: **SQLite**
    SQL-based materialization via `interloper-sql`.
- :material-cloud: **BigQuery**
    Google BigQuery materialization via `interloper-google-cloud`.
</div>

---
<div class="center" markdown>
# Asset Library

Alongside Interloper, we maintain a **pre-built collection of assets** that pull data from well-known platforms -- ranging from **social media to digital marketing and beyond**. These ready-to-use assets help you **bootstrap your data stack instantly** without reinventing the wheel.

Install via `pip install interloper-assets`.
</div>

<div class="grid cards center" markdown>
- :material-advertisements: **Adservice**
- :material-advertisements: **Adup**
- :fontawesome-brands-amazon: **Amazon Ads**
- :fontawesome-brands-amazon: **Amazon Selling Partner**
- :material-link-variant: **Awin**
- :fontawesome-brands-microsoft: **Bing Ads**
- :fontawesome-brands-google: **Campaign Manager 360**
- :material-target: **Criteo**
- :fontawesome-brands-google: **Display Video 360**
- :fontawesome-brands-facebook: **Facebook Ads**
- :fontawesome-brands-facebook: **Facebook Insights**
- :fontawesome-brands-google: **Google Ads**
- :fontawesome-brands-instagram: **Instagram Insights**
- :fontawesome-brands-linkedin: **LinkedIn Ads**
- :fontawesome-brands-linkedin: **LinkedIn Organic**
- :fontawesome-brands-pinterest: **Pinterest Ads**
- :fontawesome-brands-google: **Search Ads 360**
- :fontawesome-brands-google: **Search Console**
- :fontawesome-brands-snapchat: **Snapchat Ads**
- :material-advertisements: **Teads**
- :material-advertisements: **The Trade Desk**
- :fontawesome-brands-tiktok: **TikTok Ads**
</div>
