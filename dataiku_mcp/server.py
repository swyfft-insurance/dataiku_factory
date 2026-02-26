"""
MCP Server for Dataiku DSS integration.
"""

import json
import logging
from typing import Any

from mcp.server.fastmcp import FastMCP

from dataiku_mcp.client import get_project, list_projects
from dataiku_mcp.tools import (
    advanced_scenarios,
    code_development,
    datasets,
    environment_config,
    monitoring_debug,
    productivity,
    project_exploration,
    recipes,
    scenarios,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create MCP server
mcp = FastMCP("Dataiku DSS MCP Server")

# Server description
mcp.description = """
MCP server for Dataiku DSS. Manages recipes, datasets, scenarios, and
project flows. Project keys are uppercase identifiers (e.g. 'DATAWAREHOUSE').
"""


# Register Recipe Tools
@mcp.tool()
def create_recipe(
    project_key: str,
    recipe_type: str,
    recipe_name: str,
    inputs: list[str],
    outputs: list[dict[str, Any]],
    code: str | None = None,
) -> dict[str, Any]:
    """
    Create a new recipe in a Dataiku project.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        recipe_type: Recipe type ('sql', 'python',
            'sync', 'join', 'group')
        recipe_name: Name for the new recipe
        inputs: List of input dataset names
        outputs: List of output dataset configs
            [{"name": "...", "appendMode": false}]
        code: Optional SQL or Python code for the recipe

    Returns:
        Dict with recipe creation status and name
    """
    return recipes.create_recipe(
        project_key, recipe_type, recipe_name,
        inputs, outputs, code,
    )

@mcp.tool()
def update_recipe(
    project_key: str,
    recipe_name: str,
    **kwargs: Any
) -> dict[str, Any]:
    """
    Update an existing recipe's settings or code.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        recipe_name: Name of the recipe to update
        **kwargs: Recipe settings to update (e.g. code, inputs, outputs)

    Returns:
        Dict with update status
    """
    return recipes.update_recipe(
        project_key, recipe_name, **kwargs
    )

@mcp.tool()
def delete_recipe(
    project_key: str,
    recipe_name: str
) -> dict[str, Any]:
    """
    Delete a recipe from a project.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        recipe_name: Name of the recipe to delete

    Returns:
        Dict with deletion status
    """
    return recipes.delete_recipe(
        project_key, recipe_name
    )

@mcp.tool()
def run_recipe(
    project_key: str,
    recipe_name: str,
    build_mode: str | None = None
) -> dict[str, Any]:
    """
    Run a recipe to build its output datasets.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        recipe_name: Name of the recipe to run
        build_mode: Build mode ('RECURSIVE_BUILD',
            'NON_RECURSIVE_FORCED_BUILD', etc.)

    Returns:
        Dict with job ID and run status
    """
    return recipes.run_recipe(
        project_key, recipe_name, build_mode
    )

# Register Dataset Tools
@mcp.tool()
def create_dataset(
    project_key: str,
    dataset_name: str,
    dataset_type: str,
    params: dict[str, Any]
) -> dict[str, Any]:
    """
    Create a new dataset in a project.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        dataset_name: Name for the new dataset
        dataset_type: Type of dataset (e.g. 'SQL', 'Filesystem', 'S3')
        params: Dataset configuration (connection, table, path, etc.)

    Returns:
        Dict with creation status and dataset name
    """
    return datasets.create_dataset(
        project_key, dataset_name,
        dataset_type, params,
    )

@mcp.tool()
def update_dataset(
    project_key: str,
    dataset_name: str,
    **kwargs: Any
) -> dict[str, Any]:
    """
    Update dataset settings.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        dataset_name: Name of the dataset to update
        **kwargs: Dataset settings to update (connection, schema, params, etc.)

    Returns:
        Dict with update status
    """
    return datasets.update_dataset(
        project_key, dataset_name, **kwargs
    )

@mcp.tool()
def delete_dataset(
    project_key: str,
    dataset_name: str,
    drop_data: bool = False
) -> dict[str, Any]:
    """
    Delete a dataset from a project.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        dataset_name: Name of the dataset to delete
        drop_data: Whether to also drop the underlying SQL table or files

    Returns:
        Dict with deletion status
    """
    return datasets.delete_dataset(
        project_key, dataset_name, drop_data
    )

@mcp.tool()
def build_dataset(
    project_key: str,
    dataset_name: str,
    mode: str | None = None,
    partition: str | None = None
) -> dict[str, Any]:
    """
    Build a dataset by running its upstream recipe.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        dataset_name: Name of the dataset to build
        mode: Build mode ('RECURSIVE_BUILD',
            'NON_RECURSIVE_FORCED_BUILD', etc.)
        partition: Partition spec for partitioned datasets

    Returns:
        Dict with job ID and build status
    """
    return datasets.build_dataset(
        project_key, dataset_name, mode, partition
    )

@mcp.tool()
def inspect_dataset_schema(
    project_key: str,
    dataset_name: str
) -> dict[str, Any]:
    """
    Get column names, types, and meanings for a dataset.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        dataset_name: Name of the dataset

    Returns:
        Dict with columns list (name, type, meaning) and total column count
    """
    return datasets.inspect_dataset_schema(
        project_key, dataset_name
    )

@mcp.tool()
def check_dataset_metrics(
    project_key: str,
    dataset_name: str
) -> dict[str, Any]:
    """
    Get latest computed metrics for a dataset (row count, file size, etc.).

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        dataset_name: Name of the dataset

    Returns:
        Dict with metric values and last compute timestamp
    """
    return datasets.check_dataset_metrics(
        project_key, dataset_name
    )

@mcp.tool()
def get_dataset_post_write_statements(
    project_key: str,
    dataset_name: str
) -> dict[str, Any]:
    """
    Get post-write SQL statements for a dataset.

    Post-write statements execute AFTER a recipe writes data but BEFORE
    downstream recipes read it. Often contains index creation, column adds,
    or business logic transforms not visible in recipe code.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        dataset_name: Name of the dataset

    Returns:
        Dict with SQL statements list and dataset info
    """
    return datasets.get_dataset_post_write_statements(
        project_key, dataset_name
    )

# Register Scenario Tools
@mcp.tool()
def create_scenario(
    project_key: str,
    scenario_name: str,
    scenario_type: str,
    definition: dict[str, Any] | None = None
) -> dict[str, Any]:
    """
    Create a new scenario (workflow automation) in a project.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        scenario_name: Name for the new scenario
        scenario_type: Type of scenario ('step_based' or 'custom_python')
        definition: Optional scenario definition with steps and settings

    Returns:
        Dict with scenario ID and creation status
    """
    return scenarios.create_scenario(
        project_key, scenario_name,
        scenario_type, definition,
    )

@mcp.tool()
def update_scenario(
    project_key: str,
    scenario_id: str,
    **kwargs: Any
) -> dict[str, Any]:
    """
    Update scenario settings.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        scenario_id: Scenario identifier (e.g. 'REBUILD_DW')
        **kwargs: Scenario settings to update

    Returns:
        Dict with update status
    """
    return scenarios.update_scenario(
        project_key, scenario_id, **kwargs
    )

@mcp.tool()
def delete_scenario(
    project_key: str,
    scenario_id: str
) -> dict[str, Any]:
    """
    Delete a scenario from a project.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        scenario_id: Scenario identifier to delete

    Returns:
        Dict with deletion status
    """
    return scenarios.delete_scenario(
        project_key, scenario_id
    )

@mcp.tool()
def add_scenario_trigger(
    project_key: str,
    scenario_id: str,
    trigger_type: str,
    **params: Any
) -> dict[str, Any]:
    """
    Add a trigger to a scenario (time-based, dataset change, etc.).

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        scenario_id: Scenario identifier
        trigger_type: Trigger type ('temporal',
            'dataset_modified', 'sql_query', etc.)
        **params: Trigger-specific parameters (cron, dataset name, etc.)

    Returns:
        Dict with trigger addition status
    """
    return scenarios.add_scenario_trigger(
        project_key, scenario_id,
        trigger_type, **params,
    )

@mcp.tool()
def remove_scenario_trigger(
    project_key: str,
    scenario_id: str,
    trigger_idx: int
) -> dict[str, Any]:
    """
    Remove a trigger from a scenario by index.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        scenario_id: Scenario identifier
        trigger_idx: Zero-based index of the trigger to remove

    Returns:
        Dict with trigger removal status
    """
    return scenarios.remove_scenario_trigger(
        project_key, scenario_id, trigger_idx
    )

@mcp.tool()
def run_scenario(
    project_key: str,
    scenario_id: str
) -> dict[str, Any]:
    """
    Run a scenario manually (triggers all steps in sequence).

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        scenario_id: Scenario identifier to run

    Returns:
        Dict with run ID and initial status
    """
    return scenarios.run_scenario(
        project_key, scenario_id
    )

@mcp.tool()
def get_scenario_info(
    project_key: str,
    scenario_id: str
) -> dict[str, Any]:
    """
    Get scenario metadata, triggers, and last run status.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        scenario_id: Scenario identifier

    Returns:
        Dict with scenario name, type, triggers list, and last run info
    """
    return scenarios.get_scenario_info(
        project_key, scenario_id
    )

@mcp.tool()
def list_scenarios(
    project_key: str
) -> dict[str, Any]:
    """
    List all scenarios in a project with their IDs and last run status.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')

    Returns:
        Dict with scenarios list (id, name, type, last run status)
    """
    return scenarios.list_scenarios(project_key)

# Register Advanced Scenario Tools
@mcp.tool()
def get_scenario_logs(
    project_key: str,
    scenario_id: str,
    run_id: str | None = None
) -> dict[str, Any]:
    """
    Get detailed run logs for a scenario (useful for debugging failures).

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        scenario_id: Scenario identifier
        run_id: Specific run ID (defaults to most recent run)

    Returns:
        Dict with log text, step outcomes, run status, and timing
    """
    return advanced_scenarios.get_scenario_logs(
        project_key, scenario_id, run_id
    )

@mcp.tool()
def get_scenario_steps(
    project_key: str,
    scenario_id: str
) -> dict[str, Any]:
    """
    Get the ordered list of steps in a scenario with their configurations.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        scenario_id: Scenario identifier

    Returns:
        Dict with steps list (type, name, params, condition) in execution order
    """
    return advanced_scenarios.get_scenario_steps(
        project_key, scenario_id
    )

@mcp.tool()
def clone_scenario(
    project_key: str,
    source_scenario_id: str,
    new_scenario_name: str,
    modifications: dict[str, Any] | None = None
) -> dict[str, Any]:
    """
    Clone an existing scenario with optional modifications to steps/triggers.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        source_scenario_id: Source scenario ID to clone from
        new_scenario_name: Name for the cloned scenario
        modifications: Optional dict of settings to override in the clone

    Returns:
        Dict with new scenario ID and clone status
    """
    return advanced_scenarios.clone_scenario(
        project_key, source_scenario_id,
        new_scenario_name, modifications,
    )

# Register Code Development Tools
@mcp.tool()
def get_recipe_code(
    project_key: str,
    recipe_name: str
) -> dict[str, Any]:
    """
    Extract the Python or SQL source code from a recipe.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        recipe_name: Name of the recipe (e.g. 'compute_DimPolicy')

    Returns:
        Dict with code string, recipe type, inputs/outputs, and language
    """
    return code_development.get_recipe_code(
        project_key, recipe_name
    )

@mcp.tool()
def validate_recipe_syntax(
    project_key: str,
    recipe_name: str,
    code: str | None = None
) -> dict[str, Any]:
    """
    Validate Python/SQL syntax without executing the recipe.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        recipe_name: Name of the recipe to validate
        code: Code to validate (uses recipe's current code if omitted)

    Returns:
        Dict with valid (bool), errors list, and warnings list
    """
    return code_development.validate_recipe_syntax(
        project_key, recipe_name, code
    )

@mcp.tool()
def test_recipe_dry_run(
    project_key: str,
    recipe_name: str,
    sample_rows: int = 100
) -> dict[str, Any]:
    """
    Test recipe logic on a sample without writing to the output dataset.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        recipe_name: Name of the recipe to test
        sample_rows: Number of input rows to sample (default 100)

    Returns:
        Dict with sample output rows, schema, and any errors
    """
    return code_development.test_recipe_dry_run(
        project_key, recipe_name, sample_rows
    )

# Register Project Exploration Tools
@mcp.tool()
def get_project_flow(
    project_key: str
) -> dict[str, Any]:
    """
    Get the complete data pipeline DAG for a project.

    Args:
        project_key: Dataiku project key, uppercase
            (e.g. 'DATAWAREHOUSE')

    Returns:
        Dict with datasets, recipes, and edges
        (input->recipe->output connections)
    """
    return project_exploration.get_project_flow(
        project_key
    )

@mcp.tool()
def search_project_objects(
    project_key: str,
    search_term: str,
    object_types: list[str] | None = None
) -> dict[str, Any]:
    """
    Search for datasets, recipes, or scenarios by name pattern.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        search_term: Search pattern (substring match,
            e.g. 'DimPolicy', 'premium')
        object_types: Filter to specific types
            (['DATASET', 'RECIPE', 'SCENARIO']).
            All if omitted.

    Returns:
        Dict with matching objects grouped by type, each with name and metadata
    """
    return project_exploration.search_project_objects(
        project_key, search_term, object_types
    )

@mcp.tool()
def get_dataset_sample(
    project_key: str,
    dataset_name: str,
    rows: int = 100,
    columns: list[str] | None = None
) -> dict[str, Any]:
    """
    Get sample rows from a dataset for inspection.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        dataset_name: Name of the dataset to sample
        rows: Number of rows to return (default 100)
        columns: Specific column names to include (all columns if omitted)

    Returns:
        Dict with column headers, row data, total row count, and schema
    """
    return project_exploration.get_dataset_sample(
        project_key, dataset_name, rows, columns
    )

# Register Environment Configuration Tools
@mcp.tool()
def get_code_environments(
    project_key: str | None = None
) -> dict[str, Any]:
    """
    List available Python/R code environments and their installed packages.

    Args:
        project_key: Dataiku project key to filter by
            (all environments if omitted)

    Returns:
        Dict with environments list (name, language, version, packages)
    """
    return environment_config.get_code_environments(
        project_key
    )

@mcp.tool()
def get_project_variables(
    project_key: str
) -> dict[str, Any]:
    """
    Get project-level variables (used in recipes, scenarios, and SQL as ${var}).

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')

    Returns:
        Dict with standard and local variable key-value pairs
    """
    return environment_config.get_project_variables(
        project_key
    )

@mcp.tool()
def get_connections(
    project_key: str | None = None
) -> dict[str, Any]:
    """
    List available data connections (SQL databases, file stores, cloud storage).

    Args:
        project_key: Dataiku project key to filter by
            (all connections if omitted)

    Returns:
        Dict with connections list (name, type, database/host, credentials info)
    """
    return environment_config.get_connections(
        project_key
    )

# Register Monitoring and Debug Tools
@mcp.tool()
def get_recent_runs(
    project_key: str,
    limit: int = 50,
    status_filter: str | None = None
) -> dict[str, Any]:
    """
    Get recent job/scenario run history with status and timing.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        limit: Max number of runs to return (default 50)
        status_filter: Filter by status ('DONE', 'FAILED', 'RUNNING', etc.)

    Returns:
        Dict with runs list (id, type, status, start/end time, initiator)
    """
    return monitoring_debug.get_recent_runs(
        project_key, limit, status_filter
    )

@mcp.tool()
def get_job_details(
    project_key: str,
    job_id: str
) -> dict[str, Any]:
    """
    Get detailed execution info for a specific job (timing, logs, outputs).

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        job_id: Job identifier (from run_recipe,
            build_dataset, or get_recent_runs)

    Returns:
        Dict with job status, activities, logs, start/end time, and outputs
    """
    return monitoring_debug.get_job_details(
        project_key, job_id
    )

@mcp.tool()
def cancel_running_jobs(
    project_key: str,
    job_ids: list[str]
) -> dict[str, Any]:
    """
    Cancel one or more running jobs.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        job_ids: List of job IDs to cancel

    Returns:
        Dict with per-job cancellation status (success/failure)
    """
    return monitoring_debug.cancel_running_jobs(
        project_key, job_ids
    )

# Register Productivity Tools
@mcp.tool()
def duplicate_project_structure(
    source_project_key: str,
    target_project_key: str,
    include_data: bool = False
) -> dict[str, Any]:
    """
    Copy project structure (recipes, datasets, scenarios) to another project.

    Args:
        source_project_key: Source project key, uppercase (e.g. 'DATAWAREHOUSE')
        target_project_key: Target project key, uppercase
        include_data: Whether to also copy dataset
            contents (default: structure only)

    Returns:
        Dict with copied object counts and any errors
    """
    return productivity.duplicate_project_structure(
        source_project_key,
        target_project_key,
        include_data,
    )

@mcp.tool()
def export_project_config(
    project_key: str,
    format: str = "json"
) -> dict[str, Any]:
    """
    Export full project configuration (datasets, recipes, scenarios, variables).

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        format: Export format ('json' or 'yaml')

    Returns:
        Dict with complete project configuration in the requested format
    """
    return productivity.export_project_config(
        project_key, format
    )

@mcp.tool()
def batch_update_objects(
    project_key: str,
    object_type: str,
    pattern: str,
    updates: dict[str, Any]
) -> dict[str, Any]:
    """
    Apply the same update to multiple objects matching a name pattern.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        object_type: Type of objects ('DATASET', 'RECIPE', 'SCENARIO')
        pattern: Name pattern to match (substring match)
        updates: Dict of settings to apply to all matched objects

    Returns:
        Dict with matched count, updated count, and per-object status
    """
    return productivity.batch_update_objects(
        project_key, object_type, pattern, updates
    )

# Add resource for listing projects
@mcp.resource("projects://")
def list_available_projects() -> str:
    """
    List all available Dataiku projects.

    Returns:
        JSON string of available projects
    """
    projects = list_projects()
    return json.dumps({"projects": projects})

# Add resource for project info
@mcp.resource("project://{project_key}")
def get_project_info(project_key: str) -> str:
    """
    Get information about a specific project.

    Args:
        project_key: The project key

    Returns:
        JSON string of project information
    """
    try:
        project = get_project(project_key)
        project_info = {
            "key": project_key,
            "name": project.get_metadata()["name"],
            "description": project.get_metadata().get(
                "description", ""
            ),
        }
        return json.dumps(project_info)
    except Exception as e:
        return json.dumps({"error": str(e)})

def create_server():
    """Create and configure the MCP server."""
    return mcp
