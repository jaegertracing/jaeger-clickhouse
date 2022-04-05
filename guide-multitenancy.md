# Multi-tenant deployment

It may be desirable to share a common ClickHouse instance across multiple Jaeger instances.
There are two ways of doing this, depending on whether spanning the tenants across separate databases is preferable.

## Shared database/tables

If you wish to reuse the same ClickHouse database/tables across all tenants, you can specify a different `tenant: "<name>"` in each jaeger-clickhouse instance config.

When a non-empty `tenant` is specified, all tables will be created with a `tenant` column, and all reads/writes for a given Jaeger instance will be applied against the configured tenant name for that instance.

1. Create a shared database:
    ```sql
    CREATE DATABASE shared ENGINE=Atomic
    ```
2. Configure the per-tenant jaeger-clickhouse clients to specify tenant names:
    ```yaml
    database: shared
    tenant: tenant_1
    ```
    ```yaml
    database: shared
    tenant: tenant_2
    ```

Multitenant mode must be enabled when the deployment is first created and cannot be toggled later, except perhaps by manually adding/removing the `tenant` column from all tables.
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

## Mixing methods in the same ClickHouse instance

Each of the methods applies on a per-database basis. The methods require different schemas and must not be mixed in a single database, but it is possible to have different databases using different methods in the same ClickHouse instance.

For example, there could be a `shared` database where multiple tenants are sharing the same tables:

    ```sql
    CREATE DATABASE shared ENGINE=Atomic
    ```
    ```yaml
    database: shared
    tenant: tenant_1
    ```
    ```yaml
    database: shared
    tenant: tenant_2
    ```

Then there could be separate `isolated_x` databases for tenants that should be provided with their own dedicated tables, enabling e.g. better ACL isolation:

    ```sql
    CREATE DATABASE isolated_1 ENGINE=Atomic
    CREATE DATABASE isolated_2 ENGINE=Atomic
    ```
    ```yaml
    database: isolated_1
    ```
    ```yaml
    database: isolated_2
    ```


