exclude :test_nulls_order_is_dumped, "NULLS FIRST/LAST are not included in the dump because they don't exist in CockroachDB. See https://www.cockroachlabs.com/docs/v19.2/null-handling.html#nulls-and-sorting."
exclude :test_non_default_order_with_nulls_is_dumped, "NULLS FIRST/LAST are not included in the dump because they don't exist in CockroachDB. See https://www.cockroachlabs.com/docs/v19.2/null-handling.html#nulls-and-sorting."
