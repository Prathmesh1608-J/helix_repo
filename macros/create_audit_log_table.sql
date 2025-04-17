{% macro create_audit_log_table() %}

CREATE TABLE IF NOT EXISTS {{ model.database }}.{{ model.schema }}.AUDIT_LOG
(
    QUERY_TAG STRING,
    INVOCATION_ID STRING,
    EXECUTION_TIMESTAMP TIMESTAMP,
    MODEL_NAME STRING,
    ROWS_INSERTED INT,
    ROWS_UPDATED INT,
    ROWS_DELETED INT
);

{% endmacro %}