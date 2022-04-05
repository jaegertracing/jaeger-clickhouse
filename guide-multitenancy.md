# Multi-tenant deployment

It may be desirable to share a common ClickHouse instance across multiple Jaeger instances.
There are two ways of doing this, depending on whether spanning the tenants across separate databases is preferable.

## Shared database/tables (1.11+, EXPERIMENTAL)

> This support is still under active development and should not be used for production yet.
> This notice will be removed once the new support has had some time to soak.

If you wish to reuse the same ClickHouse database/tables across all tenants, you can specify `tenant: "<name>"` in each Jaeger instance.

When a tenant name is specified, the ClickHouse tables will be initialized with a `tenant` column, and all reads/writes for a given Jaeger instance will be against the configured tenant name.

1. Create a shared database:
    ```sql
    CREATE DATABASE shared ENGINE=Atomic
    ```
2. Configure the per-tenant jaeger-clickhouse clients to specify tenant names. This is the value used in the `tenant` column:
    ```sql
    database: shared
    tenant: tenant_1
    ```
    ```sql
    database: shared
    tenant: tenant_2
    ```

Multitenant mode must be enabled when the deployment is first created and cannot be toggled later (except perhaps manually).
Multitenant/singletenant instances must not be mixed within the same database - the two modes are mutually exclusive of each other.

## Separate databases

If you wish to keep instances fully separate, you can configure one ClickHouse database per tenant.
This may be useful when different per-database configuration across tenants is desirable.

1. Create a database for each tenant:
    ```sql
    CREATE DATABASE tenant_1 ENGINE=Atomic;
    CREATE DATABASE tenant_2 ENGINE=Atomic;
    ```
2. Configure the per-tenant jaeger-clickhouse plugins matching databases:
    ```yaml
    database: tenant_1
    ```
    ```yaml
    database: tenant_2
    ```
