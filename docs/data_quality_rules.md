# Data Quality Rules

## Purpose

This document records the project's validation rules. It separates checks known to be present in the Silver data-quality notebook from additional checks that are useful to implement. Keep this document aligned with the actual exported notebook code.

## Silver layer — null checks known to be implemented

The described Silver DQ notebook loops over the tables and required columns below, counts null values, and reports PASS when the null count is zero; otherwise it reports FAIL.

| Table | Required fields checked for nulls |
|---|---|
| `workspace.silver.customers` | `customer_id`, `customer_name`, `country` |
| `workspace.silver.products` | `product_id`, `product_name`, `category` |
| `workspace.silver.orders` | `order_id`, `customer_id`, `order_date`, `status` |
| `workspace.silver.order_items` | `order_item_id`, `order_id`, `product_id`, `quantity` |

**Expected result:** every listed required field has zero null values.

A null check alone does not prove that a dataset is fully valid. It does not test uniqueness, valid ranges, data types, allowed status values, or relationships between tables.

## Gold layer — verify against the notebook

The target Gold table is `workspace.gold.customer_order_summary`. The Gold DQ notebook should be the source of truth for which checks are implemented. Record the checks you actually run in the table below before publishing.

| Rule | Expected result | Status |
|---|---|---|
| Gold table exists and can be read | Query completes successfully | Confirm in notebook |
| `customer_id` is not null | Zero null customer IDs | Confirm in notebook |
| One row per intended customer grouping | No duplicate grouping keys | Confirm in notebook |
| `total_orders`, `completed_orders`, `cancelled_orders`, `total_quantity_ordered` are non-negative | No negative values | Confirm in notebook |
| `completed_orders` does not exceed `total_orders` | No violations | Confirm in notebook |
| `cancelled_orders` does not exceed `total_orders` | No violations | Confirm in notebook |

Replace “Confirm in notebook” with “Implemented” only for rules the Gold DQ code actually checks. Remove rules that are not relevant to your implementation.

## Recommended future checks (do not claim as implemented unless code exists)

### Silver uniqueness
- `customers.customer_id` should be unique.
- `products.product_id` should be unique.
- `orders.order_id` should be unique.
- `order_items.order_item_id` should be unique.

### Valid numeric values
- `order_items.quantity` should be greater than zero.
- Product `price`, if present in the table, should not be negative.

### Valid statuses and dates
- `orders.status` should belong to a documented list of allowed statuses.
- `order_date` should parse as a valid date and should use the expected data type.

### Referential integrity
- Every order's `customer_id` should match a customer, if orphan orders are not allowed.
- Every order item's `order_id` should match an order.
- Every order item's `product_id` should match a product.

These checks should report the number of violating rows and make it clear whether the pipeline should fail, warn, or continue.

## Reporting conventions

- **PASS:** the implemented rule was checked and no violations were found.
- **FAIL:** the implemented rule was checked and one or more violations were found.
- **Not implemented:** the rule is a recommendation only and is not currently checked by code.

Do not report a PASS for a rule that was not run. If you update notebook logic, update this document and capture a fresh screenshot of the result.
