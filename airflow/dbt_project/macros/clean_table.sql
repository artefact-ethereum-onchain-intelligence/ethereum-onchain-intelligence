{% macro clean_table(table_name) %}
/*
This macro cleans and validates transaction data from a specified source table.
It ensures data quality by:
1. Ensuring critical columns are present and non-empty (`blockNumber`, `timeStamp`, `nonce`, `transactionIndex`, `from`, `to`, `value`)
2. Validating 'from' and 'to' address formats (Ethereum standard: 0x followed by 40 hex chars)
3. Validating and casting 'value' to NUMERIC
4. Filtering out invalid records based on the above checks
5. Keeping all original columns (except 'value' which is cast)
6. Creating a unique transaction identifier

Parameters:
    table_name: The name of the source table to clean
*/
    SELECT
        -- Select all original columns except 'value' which is cast
        source_data.* EXCEPT(value),
        -- Cast value to NUMERIC
        SAFE_CAST(source_data.value AS NUMERIC) as value
        -- Create a unique identifier by combining table name, block number, and transaction index
        -- CONCAT(
        --     '{{ table_name }}_',
        --     CAST(source_data.blockNumber AS STRING),
        --     '_',
        --     CAST(source_data.transactionIndex AS STRING)
        -- ) as unique_transaction_id
    FROM {{ source('ethereum_sources', table_name) }} AS source_data
    WHERE
        -- Check 1: Critical columns non-empty/non-null
        source_data.blockNumber IS NOT NULL AND CAST(source_data.blockNumber AS STRING) != ''
        AND source_data.timeStamp IS NOT NULL AND CAST(source_data.timeStamp AS STRING) != ''
        AND source_data.nonce IS NOT NULL AND CAST(source_data.nonce AS STRING) != ''
        AND source_data.transactionIndex IS NOT NULL AND CAST(source_data.transactionIndex AS STRING) != ''
        AND source_data.`from` IS NOT NULL AND source_data.`from` != ''
        AND source_data.`to` IS NOT NULL AND source_data.`to` != ''
        AND source_data.value IS NOT NULL AND source_data.value != ''

        -- Check 2: Valid address formats (Ethereum standard)
        AND REGEXP_CONTAINS(source_data.`from`, r'^0x[0-9a-fA-F]{40}$')
        AND REGEXP_CONTAINS(source_data.`to`, r'^0x[0-9a-fA-F]{40}$')

        -- Check 3: Valid value (can be cast to numeric)
        AND SAFE_CAST(source_data.value AS NUMERIC) IS NOT NULL
{% endmacro %}
