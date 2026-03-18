"""
Data quality tools for Dataiku MCP integration.

Manage and execute data quality rules on datasets.
"""

from typing import Dict, Any, List, Optional
from dataiku_mcp.client import get_client, get_project, get_project_for_write


def list_data_quality_rules(
    project_key: str,
    dataset_name: str
) -> Dict[str, Any]:
    """
    List all data quality rules for a dataset.

    Args:
        project_key: The project key
        dataset_name: Name of the dataset

    Returns:
        Dict containing list of data quality rules
    """
    try:
        project = get_project(project_key)
        dataset = project.get_dataset(dataset_name)
        ruleset = dataset.get_data_quality_rules()
        rules = ruleset.list_rules()

        rule_list = []
        for rule in rules:
            rule_info = {
                "id": rule.get("id"),
                "name": rule.get("name", "unnamed"),
                "type": rule.get("type", "unknown"),
                "column": rule.get("column", ""),
                "enabled": rule.get("enabled", True),
                "params": rule.get("params", {}),
            }
            rule_list.append(rule_info)

        return {
            "status": "ok",
            "project_key": project_key,
            "dataset_name": dataset_name,
            "rules": rule_list,
            "rule_count": len(rule_list)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list data quality rules for '{dataset_name}': {str(e)}"
        }


def get_data_quality_status(
    project_key: str,
    dataset_name: str
) -> Dict[str, Any]:
    """
    Get the current pass/fail status of data quality rules.

    Args:
        project_key: The project key
        dataset_name: Name of the dataset

    Returns:
        Dict containing rule statuses
    """
    try:
        project = get_project(project_key)
        dataset = project.get_dataset(dataset_name)
        ruleset = dataset.get_data_quality_rules()
        status = ruleset.get_status()

        return {
            "status": "ok",
            "project_key": project_key,
            "dataset_name": dataset_name,
            "quality_status": status if isinstance(status, dict) else {"raw": str(status)}
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get data quality status for '{dataset_name}': {str(e)}"
        }


def get_data_quality_results(
    project_key: str,
    dataset_name: str
) -> Dict[str, Any]:
    """
    Get the last computed data quality rule results.

    Args:
        project_key: The project key
        dataset_name: Name of the dataset

    Returns:
        Dict containing last computed rule results
    """
    try:
        project = get_project(project_key)
        dataset = project.get_dataset(dataset_name)
        ruleset = dataset.get_data_quality_rules()
        results = ruleset.get_last_rules_results()

        return {
            "status": "ok",
            "project_key": project_key,
            "dataset_name": dataset_name,
            "results": results if isinstance(results, (dict, list)) else {"raw": str(results)}
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get data quality results for '{dataset_name}': {str(e)}"
        }


def compute_data_quality_rules(
    project_key: str,
    dataset_name: str
) -> Dict[str, Any]:
    """
    Trigger computation of data quality rules for a dataset.

    This may take time depending on dataset size and rule complexity.

    Args:
        project_key: The project key
        dataset_name: Name of the dataset

    Returns:
        Dict containing computation result
    """
    try:
        project = get_project_for_write(project_key)
        dataset = project.get_dataset(dataset_name)
        ruleset = dataset.get_data_quality_rules()

        result = ruleset.compute_rules()

        return {
            "status": "ok",
            "project_key": project_key,
            "dataset_name": dataset_name,
            "compute_result": result if isinstance(result, dict) else {"raw": str(result)},
            "message": f"Data quality rules computed for '{dataset_name}'"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to compute data quality rules for '{dataset_name}': {str(e)}"
        }


def create_data_quality_rule(
    project_key: str,
    dataset_name: str,
    rule_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Create a new data quality rule on a dataset.

    Args:
        project_key: The project key
        dataset_name: Name of the dataset
        rule_config: Rule configuration dict containing:
            - name: Rule name
            - type: Rule type (e.g., 'python', 'sql', 'non_empty', 'unique', etc.)
            - column: Column name (for column-level rules)
            - params: Rule-specific parameters

    Returns:
        Dict containing created rule info
    """
    try:
        project = get_project_for_write(project_key)
        dataset = project.get_dataset(dataset_name)
        ruleset = dataset.get_data_quality_rules()

        rule = ruleset.create_rule(rule_config)

        rule_info = {}
        if isinstance(rule, dict):
            rule_info = rule
        elif hasattr(rule, 'get_raw'):
            rule_info = rule.get_raw()
        else:
            rule_info = {"raw": str(rule)}

        return {
            "status": "ok",
            "project_key": project_key,
            "dataset_name": dataset_name,
            "created_rule": rule_info,
            "message": f"Data quality rule created on '{dataset_name}'"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create data quality rule on '{dataset_name}': {str(e)}"
        }


def delete_data_quality_rule(
    project_key: str,
    dataset_name: str,
    rule_id: str
) -> Dict[str, Any]:
    """
    Delete a data quality rule from a dataset.

    Args:
        project_key: The project key
        dataset_name: Name of the dataset
        rule_id: ID of the rule to delete

    Returns:
        Dict containing deletion result
    """
    try:
        project = get_project_for_write(project_key)
        dataset = project.get_dataset(dataset_name)
        ruleset = dataset.get_data_quality_rules()

        # Find and delete the rule
        rules = ruleset.list_rules()
        found = False
        for rule in rules:
            if rule.get("id") == rule_id:
                found = True
                break

        if not found:
            return {
                "status": "error",
                "message": f"Rule '{rule_id}' not found on dataset '{dataset_name}'"
            }

        # dataikuapi DSSDataQualityRule has a delete() method
        rule_obj = ruleset.get_rule(rule_id) if hasattr(ruleset, 'get_rule') else None
        if rule_obj and hasattr(rule_obj, 'delete'):
            rule_obj.delete()
        else:
            return {
                "status": "error",
                "message": f"Rule deletion not supported for this DSS version. "
                           f"Delete rules via the DSS UI."
            }

        return {
            "status": "ok",
            "project_key": project_key,
            "dataset_name": dataset_name,
            "deleted_rule_id": rule_id,
            "message": f"Rule '{rule_id}' deleted from '{dataset_name}'"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to delete data quality rule '{rule_id}': {str(e)}"
        }
