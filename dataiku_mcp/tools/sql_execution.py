"""
SQL execution tools for Dataiku MCP integration.

Execute read-only SQL queries through DSS connections.
"""

import json
import re
from typing import Dict, Any, List, Optional
from dataiku_mcp.client import get_client


# Keywords that indicate non-SELECT (write/DDL/DCL) operations
BLOCKED_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "TRUNCATE", "MERGE", "EXEC", "EXECUTE", "GRANT", "REVOKE",
    "CALL", "BULK", "OPENROWSET", "OPENQUERY",
}


def _is_read_only(query: str) -> bool:
    """Check if a SQL query is read-only by examining the first keyword."""
    stripped = query.strip()
    # Remove leading comments
    while stripped.startswith("--"):
        stripped = stripped.split("\n", 1)[-1].strip()
    while stripped.startswith("/*"):
        end = stripped.find("*/")
        if end == -1:
            break
        stripped = stripped[end + 2:].strip()

    # Get first word
    first_word = re.split(r'\s+', stripped, maxsplit=1)[0].upper().rstrip(";")

    return first_word not in BLOCKED_KEYWORDS


def execute_sql_query(
    query: str,
    connection: str,
    database: Optional[str] = None,
    query_type: str = "sql",
    max_rows: int = 10000
) -> Dict[str, Any]:
    """
    Execute a read-only SQL query through a DSS connection.

    Only SELECT queries are allowed. DDL/DML statements (INSERT, UPDATE,
    DELETE, DROP, etc.) are blocked for safety.

    Args:
        query: SQL query to execute (SELECT only)
        connection: DSS connection name to execute against
        database: Optional database name (overrides connection default)
        query_type: Query type - 'sql', 'hive', or 'impala'
        max_rows: Maximum rows to return (default 10000, hard cap 50000)

    Returns:
        Dict containing query results with schema and rows
    """
    try:
        # Safety: block non-SELECT queries
        if not _is_read_only(query):
            return {
                "status": "error",
                "message": "Only SELECT queries are allowed. DDL/DML statements "
                           "(INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, etc.) are blocked."
            }

        # Validate query type
        valid_types = ["sql", "hive", "impala"]
        if query_type not in valid_types:
            return {
                "status": "error",
                "message": f"Invalid query_type. Must be one of: {valid_types}"
            }

        # Hard cap on rows
        max_rows = min(max_rows, 50000)

        client = get_client()

        # Build query params
        query_params = {
            "query": query,
            "connection": connection,
            "type": query_type,
        }
        if database:
            query_params["database"] = database

        # Execute
        result = client.sql_query(**query_params)

        # Get schema
        schema = result.get_schema()
        columns = [col.get("name", f"col_{i}") for i, col in enumerate(schema)]
        column_types = [col.get("type", "unknown") for col in schema]

        # Collect rows
        rows = []
        for row in result.iter_rows():
            if len(rows) >= max_rows:
                break
            # Convert row to dict keyed by column name
            row_dict = {}
            for i, value in enumerate(row):
                col_name = columns[i] if i < len(columns) else f"col_{i}"
                row_dict[col_name] = value
            rows.append(row_dict)

        truncated = len(rows) >= max_rows

        return {
            "status": "ok",
            "connection": connection,
            "database": database,
            "query_type": query_type,
            "schema": [
                {"name": columns[i], "type": column_types[i]}
                for i in range(len(columns))
            ],
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
            "max_rows": max_rows
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to execute SQL query: {str(e)}"
        }


def list_sql_connections() -> Dict[str, Any]:
    """
    List DSS connections that support SQL execution.

    Returns:
        Dict containing list of SQL-capable connections
    """
    try:
        client = get_client()
        all_connections = client.list_connections()

        sql_types = {
            "PostgreSQL", "MySQL", "SQLServer", "Oracle", "Snowflake",
            "BigQuery", "Redshift", "Teradata", "Vertica", "Synapse",
            "JDBC", "Hive", "Impala", "SparkSQL", "Athena", "Databricks",
            "SingleStore", "MariaDB", "SAP HANA", "Greenplum",
        }

        sql_connections = []
        for conn in all_connections:
            conn_type = conn.get("type", "")
            if conn_type in sql_types or "sql" in conn_type.lower() or "jdbc" in conn_type.lower():
                sql_connections.append({
                    "name": conn.get("name"),
                    "type": conn_type,
                    "usable": conn.get("usable", False),
                    "allow_write": conn.get("allowWrite", False),
                    "description": conn.get("description", ""),
                })

        return {
            "status": "ok",
            "connections": sql_connections,
            "connection_count": len(sql_connections),
            "total_connections": len(all_connections)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list SQL connections: {str(e)}"
        }
