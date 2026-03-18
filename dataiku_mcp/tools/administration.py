"""
Administration tools for Dataiku MCP integration.

Instance-level operations: info, settings, variables, usage, logs, and audit.
"""

import json
from typing import Dict, Any, List, Optional
from dataiku_mcp.client import get_client


SENSITIVE_KEYS = {"password", "secret", "key", "token", "credential", "apikey", "api_key"}


def _mask_sensitive(data: Any, depth: int = 0) -> Any:
    """Recursively mask sensitive values in dicts/lists."""
    if depth > 10:
        return data
    if isinstance(data, dict):
        masked = {}
        for k, v in data.items():
            if any(s in k.lower() for s in SENSITIVE_KEYS):
                masked[k] = "***HIDDEN***"
            else:
                masked[k] = _mask_sensitive(v, depth + 1)
        return masked
    elif isinstance(data, list):
        return [_mask_sensitive(item, depth + 1) for item in data[:100]]
    return data


def get_instance_info() -> Dict[str, Any]:
    """
    Get DSS instance information.

    Returns:
        Dict containing DSS version, node type, and license info
    """
    try:
        client = get_client()
        info = client.get_instance_info()
        raw = info._data if hasattr(info, '_data') else {}

        return {
            "status": "ok",
            "instance_info": {
                "dss_version": raw.get("dssVersion", "unknown"),
                "node_type": raw.get("nodeType", "unknown"),
                "node_id": raw.get("nodeId", "unknown"),
                "api_version": raw.get("apiVersion", "unknown"),
                "license_status": raw.get("licenseStatus", {}),
                "java_version": raw.get("javaVersion", "unknown"),
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get instance info: {str(e)}"
        }


def get_general_settings_summary() -> Dict[str, Any]:
    """
    Get non-sensitive general DSS settings.

    Sensitive values (passwords, keys, tokens) are masked.

    Returns:
        Dict containing general settings summary
    """
    try:
        client = get_client()
        settings = client.get_general_settings()
        raw = settings.get_raw()

        masked = _mask_sensitive(raw)

        # Extract key categories
        summary = {
            "auth_mode": masked.get("authMode", "unknown"),
            "ldap_settings": masked.get("ldapSettings", {}),
            "sso_settings": masked.get("ssoSettings", {}),
            "theme": masked.get("theme", {}),
            "impersonation": masked.get("impersonation", {}),
            "limits": masked.get("limits", {}),
            "audit": masked.get("audit", {}),
            "containerized_execution": masked.get("containerizedExecution", {}),
        }

        return {
            "status": "ok",
            "settings_summary": summary
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get general settings: {str(e)}"
        }


def get_global_variables() -> Dict[str, Any]:
    """
    Get global DSS variables.

    Sensitive values (passwords, keys, tokens) are masked.

    Returns:
        Dict containing global variables
    """
    try:
        client = get_client()
        variables = client.get_global_variables()

        masked = _mask_sensitive(variables)

        return {
            "status": "ok",
            "variables": masked,
            "variable_count": len(variables) if isinstance(variables, dict) else 0
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get global variables: {str(e)}"
        }


def get_global_usage_summary() -> Dict[str, Any]:
    """
    Get DSS instance usage summary.

    Returns:
        Dict containing project, user, connection, and dataset counts
    """
    try:
        client = get_client()
        usage = client.get_global_usage_summary()

        usage_data = {}
        if hasattr(usage, '_data'):
            usage_data = usage._data
        elif isinstance(usage, dict):
            usage_data = usage
        else:
            # Try common attributes
            for attr in ['projects_count', 'total_datasets_count', 'total_recipes_count',
                         'total_scenarios_count', 'users_count']:
                val = getattr(usage, attr, None)
                if val is not None:
                    usage_data[attr] = val

        return {
            "status": "ok",
            "usage_summary": usage_data
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get usage summary: {str(e)}"
        }


def list_dss_logs(
    max_logs: int = 50
) -> Dict[str, Any]:
    """
    List available DSS log files.

    Args:
        max_logs: Maximum number of log files to return (default 50)

    Returns:
        Dict containing list of available log file names
    """
    try:
        client = get_client()
        logs = client.list_logs()

        # Limit results
        log_names = logs[:max_logs] if isinstance(logs, list) else []

        return {
            "status": "ok",
            "logs": log_names,
            "log_count": len(log_names),
            "total_available": len(logs) if isinstance(logs, list) else 0,
            "truncated": len(logs) > max_logs if isinstance(logs, list) else False
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list logs: {str(e)}"
        }


def get_dss_log(
    log_name: str,
    max_lines: int = 500
) -> Dict[str, Any]:
    """
    Get content of a specific DSS log file.

    Returns the tail of the log, capped at max_lines.

    Args:
        log_name: Name of the log file to retrieve
        max_lines: Maximum number of lines to return (default 500, hard cap 2000)

    Returns:
        Dict containing log content
    """
    try:
        client = get_client()

        # Hard cap
        max_lines = min(max_lines, 2000)

        log_content = client.get_log(log_name)

        if not log_content:
            return {
                "status": "ok",
                "log_name": log_name,
                "content": "",
                "line_count": 0,
                "truncated": False
            }

        lines = log_content.split('\n')
        total_lines = len(lines)
        truncated = total_lines > max_lines

        # Take tail
        if truncated:
            lines = lines[-max_lines:]

        return {
            "status": "ok",
            "log_name": log_name,
            "content": '\n'.join(lines),
            "line_count": len(lines),
            "total_lines": total_lines,
            "truncated": truncated
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get log '{log_name}': {str(e)}"
        }


def log_custom_audit(
    audit_type: str,
    details: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Write a custom audit log entry.

    Args:
        audit_type: Type/category of the audit event
        details: Dict of event details

    Returns:
        Dict containing audit log result
    """
    try:
        client = get_client()
        client.log_custom_audit(audit_type, **details)

        return {
            "status": "ok",
            "audit_type": audit_type,
            "message": f"Audit event '{audit_type}' logged successfully"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to log audit event: {str(e)}"
        }
