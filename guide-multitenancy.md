# Multi-tenant deployment

Multi-tenant deployment requires using a separate databases for each tenant.

Create a database:

```sql
CREATE DATABASE tenant_1;
```

Then configure plugin to use a specific database:

```yaml
database: tenant_1
```
