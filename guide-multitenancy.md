# Multi-tenant deployment

Multi-tenant deployment requires using a database per tenant. Each tenant will talk
to a separate Jaeger query and collector (or all-in-one).

Create a database:

```sql
CREATE DATABASE tenant_1 ENGINE=Atomic;
```

Then configure the plugin to use tenant's database:

```yaml
database: tenant_1
```
