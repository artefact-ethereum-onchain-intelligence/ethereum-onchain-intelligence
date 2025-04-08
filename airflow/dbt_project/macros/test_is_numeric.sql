{% test is_numeric(model, column_name) %}

  /*
  Tests that all non-null values in a column can be cast to a numeric type.

  Args:
      model: The model to test
      column_name: The name of the column to test

  Returns:
      query: A query that returns all rows where the column cannot be cast to numeric
  */

  select *
  from {{ model }}
  where {{ column_name }} is not null
    and safe_cast({{ column_name }} as numeric) is null

{% endtest %}
