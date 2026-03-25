"""
MCP Server for Dataiku DSS integration.
"""

import json
import logging
from typing import Any

from mcp.server.fastmcp import FastMCP

from dataiku_mcp.client import get_project, list_projects
from dataiku_mcp.tools import (
    administration,
    advanced_scenarios,
    code_development,
    data_quality,
    datasets,
    deployment,
    environment_config,
    managed_folders,
    monitoring_debug,
    productivity,
    project_exploration,
    recipes,
    scenarios,
    sql_execution,
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

@mcp.tool()
def compute_schema_updates(
    project_key: str,
    recipe_name: str
) -> dict[str, Any]:
    """
    Compute and apply schema updates for a recipe's output datasets.

    Call this after creating a recipe before running it, so the output
    dataset schema matches what the recipe will produce.

    Args:
        project_key: The project key
        recipe_name: Name of the recipe

    Returns:
        Dict with schema update results
    """
    return recipes.compute_schema_updates(
        project_key, recipe_name
    )

@mcp.tool()
def replace_recipe_input(
    project_key: str,
    recipe_name: str,
    current_input: str,
    new_input: str
) -> dict[str, Any]:
    """
    Replace one input dataset reference with another in a recipe.

    Input refs use "PROJECTKEY.DatasetName" for cross-project datasets,
    or just "DatasetName" for local datasets.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'DATAWAREHOUSE')
        recipe_name: Name of the recipe to update
        current_input: Current input ref to replace (e.g. 'DATAWAREHOUSE.FactLoss')
        new_input: New input ref (e.g. 'DWREFERENCE.FactLoss')

    Returns:
        Dict with before/after inputs
    """
    return recipes.replace_recipe_input(
        project_key, recipe_name, current_input, new_input
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
def create_flow_zone(
    project_key: str,
    zone_name: str,
    color: str = "#2ab1ac"
) -> dict[str, Any]:
    """
    Create a new zone in the project flow.

    Args:
        project_key: The project key
        zone_name: Display name for the zone
        color: Zone color as hex string (default teal)

    Returns:
        Dict containing created zone info
    """
    return project_exploration.create_flow_zone(
        project_key, zone_name, color
    )

@mcp.tool()
def add_dataset_reference(
    project_key: str,
    source_project_key: str,
    dataset_name: str
) -> dict[str, Any]:
    """
    Add a reference to a dataset from another project.

    The source dataset must be shared/exposed from its project.

    Args:
        project_key: Target project key
        source_project_key: Source project key where the dataset lives
        dataset_name: Name of the dataset to reference

    Returns:
        Dict containing reference creation result
    """
    return project_exploration.add_dataset_reference(
        project_key, source_project_key, dataset_name
    )

@mcp.tool()
def switch_dataset_source(
    project_key: str,
    dataset_name: str,
    old_source_project: str,
    new_source_project: str
) -> dict[str, Any]:
    """
    Switch a foreign dataset reference from one source project to another
    across ALL recipes in the flow.

    Auto-exposes from the new source and replaces all recipe inputs.
    Call remove_dataset_reference after to clean up the old node.

    Args:
        project_key: Dataiku project key, uppercase (e.g. 'TOPA2023EANDSNOTIONAL')
        dataset_name: Dataset name (e.g. 'DimCompanyLine')
        old_source_project: Current source (e.g. 'DATAWAREHOUSE')
        new_source_project: New source (e.g. 'DWREFERENCE')

    Returns:
        Dict with replacement status
    """
    return project_exploration.switch_dataset_source(
        project_key, dataset_name, old_source_project, new_source_project
    )

@mcp.tool()
def remove_dataset_reference(
    project_key: str,
    source_project_key: str,
    dataset_name: str
) -> dict[str, Any]:
    """
    Remove a foreign dataset reference by un-exposing it from the source project.

    Removes the target project from the source's exposed object rules.
    Use after replacing recipe inputs to clean up old cross-project references.

    Args:
        project_key: Target project key (the consumer that no longer needs the reference)
        source_project_key: Source project key where the dataset lives
        dataset_name: Name of the dataset to un-expose

    Returns:
        Dict with removal status
    """
    return project_exploration.remove_dataset_reference(
        project_key, source_project_key, dataset_name
    )

@mcp.tool()
def move_to_zone(
    project_key: str,
    zone_id: str,
    items: list[dict[str, str]]
) -> dict[str, Any]:
    """
    Move datasets, recipes, or managed folders into a flow zone.

    Args:
        project_key: The project key
        zone_id: ID of the target zone (from create_flow_zone)
        items: List of items to move, each with 'type' and 'name'.
               type: 'dataset', 'recipe', or 'managed_folder'.

    Returns:
        Dict containing move results
    """
    return project_exploration.move_to_zone(
        project_key, zone_id, items
    )

@mcp.tool()
def propagate_schema(
    project_key: str,
    dataset_name: str
) -> dict[str, Any]:
    """
    Propagate schema changes from a dataset through downstream recipes.

    Args:
        project_key: The project key
        dataset_name: Name of the dataset to propagate from

    Returns:
        Dict containing propagation results
    """
    return project_exploration.propagate_schema(
        project_key, dataset_name
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
def create_project(
    project_key: str,
    name: str,
    description: str = ""
) -> dict[str, Any]:
    """
    Create a new empty Dataiku project.

    Args:
        project_key: Unique project key (uppercase, no spaces)
        name: Display name for the project
        description: Optional project description

    Returns:
        Dict containing project creation result
    """
    return productivity.create_project(
        project_key, name, description
    )

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

@mcp.tool()
def set_project_variables(
    project_key: str,
    standard: dict[str, Any] | None = None,
    local: dict[str, Any] | None = None,
    merge: bool = True
) -> dict[str, Any]:
    """
    Set project-level variables.

    Args:
        project_key: The project key
        standard: Dict of standard (global) variables to set
        local: Dict of local (custom) variables to set
        merge: If True, merge with existing. If False, replace entirely.

    Returns:
        Dict containing update result
    """
    return environment_config.set_project_variables(project_key, standard, local, merge)

# Register Managed Folder Tools
@mcp.tool()
def list_managed_folders(project_key: str) -> dict[str, Any]:
    """
    List all managed folders in a project.

    Args:
        project_key: The project key

    Returns:
        Dict containing list of managed folders
    """
    return managed_folders.list_managed_folders(project_key)

@mcp.tool()
def get_managed_folder_contents(project_key: str, folder_id: str, path: str = "/") -> dict[str, Any]:
    """
    List files and subdirectories in a managed folder.

    Args:
        project_key: The project key
        folder_id: ID of the managed folder
        path: Path within the folder (default "/")

    Returns:
        Dict containing file listing
    """
    return managed_folders.get_managed_folder_contents(project_key, folder_id, path)

@mcp.tool()
def get_managed_folder_info(project_key: str, folder_id: str) -> dict[str, Any]:
    """
    Get settings and metadata for a managed folder.

    Args:
        project_key: The project key
        folder_id: ID of the managed folder

    Returns:
        Dict containing folder settings
    """
    return managed_folders.get_managed_folder_info(project_key, folder_id)

@mcp.tool()
def upload_file_to_folder(project_key: str, folder_id: str, path: str, content: str, is_base64: bool = False) -> dict[str, Any]:
    """
    Upload content to a file in a managed folder.

    Args:
        project_key: The project key
        folder_id: ID of the managed folder
        path: Target file path within the folder
        content: File content as text, or base64-encoded for binary
        is_base64: If True, content is base64-encoded binary data

    Returns:
        Dict containing upload result
    """
    return managed_folders.upload_file_to_folder(project_key, folder_id, path, content, is_base64)

@mcp.tool()
def download_file_from_folder(project_key: str, folder_id: str, path: str, max_size_bytes: int = 1048576) -> dict[str, Any]:
    """
    Download a file from a managed folder.

    Returns content as text if UTF-8 decodable, otherwise as base64.
    Default max size is 1MB.

    Args:
        project_key: The project key
        folder_id: ID of the managed folder
        path: File path within the folder
        max_size_bytes: Maximum file size to download (default 1MB)

    Returns:
        Dict containing file content
    """
    return managed_folders.download_file_from_folder(project_key, folder_id, path, max_size_bytes)

@mcp.tool()
def delete_file_from_folder(project_key: str, folder_id: str, path: str) -> dict[str, Any]:
    """
    Delete a file from a managed folder.

    Args:
        project_key: The project key
        folder_id: ID of the managed folder
        path: File path to delete

    Returns:
        Dict containing deletion result
    """
    return managed_folders.delete_file_from_folder(project_key, folder_id, path)

# Register Deployment Tools
@mcp.tool()
def list_api_deployer_services() -> dict[str, Any]:
    """List all services in the API Deployer."""
    return deployment.list_api_deployer_services()

@mcp.tool()
def list_api_deployer_deployments(service_id: str | None = None) -> dict[str, Any]:
    """List deployments in the API Deployer."""
    return deployment.list_api_deployer_deployments(service_id)

@mcp.tool()
def list_api_deployer_infras() -> dict[str, Any]:
    """List infrastructures in the API Deployer."""
    return deployment.list_api_deployer_infras()

@mcp.tool()
def get_api_deployment_status(deployment_id: str) -> dict[str, Any]:
    """Get status of a specific API deployment."""
    return deployment.get_api_deployment_status(deployment_id)

@mcp.tool()
def list_project_deployer_projects() -> dict[str, Any]:
    """List all projects in the Project Deployer."""
    return deployment.list_project_deployer_projects()

@mcp.tool()
def list_project_deployer_deployments(published_project_key: str | None = None) -> dict[str, Any]:
    """List deployments in the Project Deployer."""
    return deployment.list_project_deployer_deployments(published_project_key)

@mcp.tool()
def list_project_deployer_infras() -> dict[str, Any]:
    """List infrastructures in the Project Deployer."""
    return deployment.list_project_deployer_infras()

@mcp.tool()
def get_project_deployment_status(deployment_id: str) -> dict[str, Any]:
    """Get status of a specific project deployment."""
    return deployment.get_project_deployment_status(deployment_id)

# Register Data Quality Tools
@mcp.tool()
def list_data_quality_rules(project_key: str, dataset_name: str) -> dict[str, Any]:
    """List all data quality rules for a dataset."""
    return data_quality.list_data_quality_rules(project_key, dataset_name)

@mcp.tool()
def get_data_quality_status(project_key: str, dataset_name: str) -> dict[str, Any]:
    """Get the current pass/fail status of data quality rules."""
    return data_quality.get_data_quality_status(project_key, dataset_name)

@mcp.tool()
def get_data_quality_results(project_key: str, dataset_name: str) -> dict[str, Any]:
    """Get the last computed data quality rule results."""
    return data_quality.get_data_quality_results(project_key, dataset_name)

@mcp.tool()
def compute_data_quality_rules(project_key: str, dataset_name: str) -> dict[str, Any]:
    """Trigger computation of data quality rules for a dataset."""
    return data_quality.compute_data_quality_rules(project_key, dataset_name)

@mcp.tool()
def create_data_quality_rule(project_key: str, dataset_name: str, rule_config: dict[str, Any]) -> dict[str, Any]:
    """Create a new data quality rule on a dataset."""
    return data_quality.create_data_quality_rule(project_key, dataset_name, rule_config)

@mcp.tool()
def delete_data_quality_rule(project_key: str, dataset_name: str, rule_id: str) -> dict[str, Any]:
    """Delete a data quality rule from a dataset."""
    return data_quality.delete_data_quality_rule(project_key, dataset_name, rule_id)

# Register SQL Execution Tools
@mcp.tool()
def execute_sql_query(query: str, connection: str, database: str | None = None, query_type: str = "sql", max_rows: int = 10000) -> dict[str, Any]:
    """
    Execute a read-only SQL query through a DSS connection.

    Only SELECT queries are allowed. DDL/DML statements are blocked.

    Args:
        query: SQL query to execute (SELECT only)
        connection: DSS connection name
        database: Optional database name
        query_type: Query type - 'sql', 'hive', or 'impala'
        max_rows: Maximum rows to return (default 10000, hard cap 50000)

    Returns:
        Dict containing schema and result rows
    """
    return sql_execution.execute_sql_query(query, connection, database, query_type, max_rows)

@mcp.tool()
def list_sql_connections() -> dict[str, Any]:
    """List DSS connections that support SQL execution."""
    return sql_execution.list_sql_connections()

# Register Administration Tools
@mcp.tool()
def get_instance_info() -> dict[str, Any]:
    """Get DSS instance information: version, node type, license."""
    return administration.get_instance_info()

@mcp.tool()
def get_general_settings_summary() -> dict[str, Any]:
    """Get non-sensitive general DSS settings. Sensitive values are masked."""
    return administration.get_general_settings_summary()

@mcp.tool()
def get_global_variables() -> dict[str, Any]:
    """Get global DSS variables. Sensitive values are masked."""
    return administration.get_global_variables()

@mcp.tool()
def get_global_usage_summary() -> dict[str, Any]:
    """Get DSS instance usage summary: project, user, dataset counts."""
    return administration.get_global_usage_summary()

@mcp.tool()
def list_dss_logs(max_logs: int = 50) -> dict[str, Any]:
    """List available DSS log files."""
    return administration.list_dss_logs(max_logs)

@mcp.tool()
def get_dss_log(log_name: str, max_lines: int = 500) -> dict[str, Any]:
    """Get content of a specific DSS log file (tail)."""
    return administration.get_dss_log(log_name, max_lines)

@mcp.tool()
def log_custom_audit(audit_type: str, details: dict[str, Any]) -> dict[str, Any]:
    """Write a custom audit log entry."""
    return administration.log_custom_audit(audit_type, details)


def create_server():
    """Create and configure the MCP server."""
    return mcp
