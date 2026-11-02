# Query plan format

ColumnScope accepts a strict, versioned JSON document rather than SQL. Version 1
contains an input schema and one recursive logical plan. Unknown fields and
unknown enum values are errors.

```json
{
  "format_version": 1,
  "input_schema": {
    "fields": [
      { "name": "id", "data_type": "int64", "nullable": false },
      { "name": "metric", "data_type": "float64", "nullable": true }
    ]
  },
  "plan": { "type": "scan" }
}
```

Supported scalar types are `int64`, `float64`, `boolean`, and `utf8`. Field
names must be nonempty and unique. There are no implicit numeric or string
coercions.

## Logical nodes

A scan is the leaf:

```json
{ "type": "scan" }
```

Filters require a Boolean expression:

```json
{
  "type": "filter",
  "input": { "type": "scan" },
  "predicate": {
    "type": "lt",
    "left": { "type": "column", "name": "metric" },
    "right": { "type": "literal", "literal": { "type": "float64", "value": 10.0 } }
  }
}
```

Projection expressions have explicit, unique output aliases:

```json
{
  "type": "project",
  "input": { "type": "scan" },
  "expressions": [
    { "alias": "event_id", "expr": { "type": "column", "name": "id" } }
  ]
}
```

Aggregation accepts zero to two group keys. At least one group key or aggregate
must be present. Group keys are limited to Int64, Utf8, and Boolean.

```json
{
  "type": "aggregate",
  "input": { "type": "scan" },
  "group_by": [
    { "alias": "region", "expr": { "type": "column", "name": "region" } }
  ],
  "aggregates": [
    { "function": "count_star", "alias": "rows" },
    { "function": "count", "alias": "present", "expr": { "type": "column", "name": "metric" } },
    { "function": "sum", "alias": "total", "expr": { "type": "column", "name": "metric" } },
    { "function": "min", "alias": "minimum", "expr": { "type": "column", "name": "metric" } },
    { "function": "max", "alias": "maximum", "expr": { "type": "column", "name": "metric" } },
    { "function": "avg", "alias": "average", "expr": { "type": "column", "name": "metric" } }
  ]
}
```

COUNT outputs non-null Int64. Numeric SUM/MIN/MAX preserve the input type and
are nullable. AVG accepts Int64 or Float64 and outputs nullable Float64.

## Expressions

Columns use `{ "type": "column", "name": "id" }`. Literals preserve a type
even when null:

```json
{ "type": "literal", "literal": { "type": "int64", "value": null } }
```

Binary expressions use `left` and `right`:

| `type` | Contract |
|---|---|
| `eq`, `not_eq` | identical scalar operand types |
| `lt`, `lt_eq`, `gt`, `gt_eq` | identical Int64, Float64, or Utf8 operands |
| `and`, `or` | Boolean operands |

Unary forms use `expr`: `not` requires Boolean; `is_null` and `is_not_null`
accept any supported type and always return a non-null Boolean. Comparison and
Boolean output is nullable when an input is nullable, matching three-valued
logic. Non-finite Float64 literals are rejected.

Complete checked-in examples are available in
[`fixtures/plans/`](../fixtures/plans/).
