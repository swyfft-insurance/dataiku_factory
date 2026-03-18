"""
Project exploration tools for Dataiku MCP integration.
"""

import re
from typing import Any

from dataiku_mcp.client import get_client, get_project, get_project_for_write


def get_project_flow(
    project_key: str
) -> dict[str, Any]:
    """
    Get complete data flow/pipeline structure.

    Args:
        project_key: The project key

    Returns:
        Dict containing flow structure and dependencies
    """
    try:
        project = get_project(project_key)

        # Get all project objects
        datasets = project.list_datasets()
        recipes = project.list_recipes()

        # Build flow structure
        flow_nodes = []
        flow_edges = []

        # Add datasets as nodes
        for dataset in datasets:
            dataset_info = {
                "id": dataset["name"],
                "name": dataset["name"],
                "type": "dataset",
                "dataset_type": dataset["type"],
                "tags": dataset.get("tags", [])
            }
            flow_nodes.append(dataset_info)

        # Add recipes as nodes and create edges
        for recipe in recipes:
            recipe_info = {
                "id": recipe["name"],
                "name": recipe["name"],
                "type": "recipe",
                "recipe_type": recipe["type"],
                "tags": recipe.get("tags", [])
            }
            flow_nodes.append(recipe_info)

            # Get recipe details for inputs/outputs
            try:
                recipe_obj = project.get_recipe(
                    recipe["name"]
                )
                recipe_def = recipe_obj.get_definition()

                # Create edges from inputs to recipe
                for input_def in recipe_def.get(
                    "inputs", []
                ):
                    input_ref = input_def["ref"]
                    flow_edges.append({
                        "from": input_ref,
                        "to": recipe["name"],
                        "type": "input"
                    })

                # Create edges from recipe to outputs
                for output_def in recipe_def.get(
                    "outputs", []
                ):
                    output_ref = output_def["ref"]
                    flow_edges.append({
                        "from": recipe["name"],
                        "to": output_ref,
                        "type": "output"
                    })

            except Exception:
                # Skip recipes that can't be accessed
                continue

        # Calculate dependencies
        _empty_deps = {
            "depends_on": [],
            "used_by": [],
        }
        dependencies = {}
        for edge in flow_edges:
            if edge["type"] == "input":
                # Recipe depends on dataset
                recipe_name = edge["to"]
                dataset_name = edge["from"]

                if recipe_name not in dependencies:
                    dependencies[recipe_name] = {
                        "depends_on": [],
                        "used_by": [],
                    }
                dependencies[recipe_name][
                    "depends_on"
                ].append(dataset_name)

                if dataset_name not in dependencies:
                    dependencies[dataset_name] = {
                        "depends_on": [],
                        "used_by": [],
                    }
                dependencies[dataset_name][
                    "used_by"
                ].append(recipe_name)

            elif edge["type"] == "output":
                # Dataset depends on recipe
                recipe_name = edge["from"]
                dataset_name = edge["to"]

                if dataset_name not in dependencies:
                    dependencies[dataset_name] = {
                        "depends_on": [],
                        "used_by": [],
                    }
                dependencies[dataset_name][
                    "depends_on"
                ].append(recipe_name)

                if recipe_name not in dependencies:
                    dependencies[recipe_name] = {
                        "depends_on": [],
                        "used_by": [],
                    }
                dependencies[recipe_name][
                    "used_by"
                ].append(dataset_name)

        # Find root nodes (no dependencies) and
        # leaf nodes (no dependents)
        root_nodes = []
        leaf_nodes = []

        for node_id, deps in dependencies.items():
            if not deps["depends_on"]:
                root_nodes.append(node_id)
            if not deps["used_by"]:
                leaf_nodes.append(node_id)

        # Calculate flow statistics
        flow_stats = {
            "total_nodes": len(flow_nodes),
            "total_edges": len(flow_edges),
            "datasets": len(datasets),
            "recipes": len(recipes),
            "root_nodes": len(root_nodes),
            "leaf_nodes": len(leaf_nodes)
        }

        return {
            "status": "ok",
            "project_key": project_key,
            "flow": {
                "nodes": flow_nodes,
                "edges": flow_edges
            },
            "dependencies": dependencies,
            "root_nodes": root_nodes,
            "leaf_nodes": leaf_nodes,
            "flow_stats": flow_stats
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                "Failed to get project flow: "
                f"{str(e)}"
            ),
        }


def search_project_objects(
    project_key: str,
    search_term: str,
    object_types: list[str] | None = None
) -> dict[str, Any]:
    """
    Search for datasets, recipes, scenarios.

    Args:
        project_key: The project key
        search_term: Search pattern (supports regex)
        object_types: Object types to search,
            e.g. ["datasets", "recipes", "scenarios"]

    Returns:
        Dict containing search results
    """
    try:
        project = get_project(project_key)

        if object_types is None:
            object_types = [
                "datasets",
                "recipes",
                "scenarios",
            ]

        # Compile regex pattern
        try:
            pattern = re.compile(
                search_term, re.IGNORECASE
            )
        except re.error:
            # If regex fails, use simple string matching
            pattern = None

        search_results = {}

        # Search datasets
        if "datasets" in object_types:
            datasets = project.list_datasets()
            matching_datasets = []

            for dataset in datasets:
                name = dataset["name"]
                description = dataset.get(
                    "description", ""
                )
                tags = dataset.get("tags", [])

                # Check if matches
                matches = False
                if pattern:
                    matches = (
                        pattern.search(name)
                        or pattern.search(description)
                        or any(
                            pattern.search(tag)
                            for tag in tags
                        )
                    )
                else:
                    term = search_term.lower()
                    matches = (
                        term in name.lower()
                        or term in description.lower()
                        or any(
                            term in tag.lower()
                            for tag in tags
                        )
                    )

                if matches:
                    term = search_term.lower()
                    match_type = (
                        "name"
                        if term in name.lower()
                        else "metadata"
                    )
                    matching_datasets.append({
                        "name": name,
                        "type": dataset["type"],
                        "description": description,
                        "tags": tags,
                        "match_type": match_type,
                    })

            search_results["datasets"] = (
                matching_datasets
            )

        # Search recipes
        if "recipes" in object_types:
            recipes = project.list_recipes()
            matching_recipes = []

            for recipe in recipes:
                name = recipe["name"]
                description = recipe.get(
                    "description", ""
                )
                tags = recipe.get("tags", [])

                # Check if matches
                matches = False
                if pattern:
                    matches = (
                        pattern.search(name)
                        or pattern.search(description)
                        or any(
                            pattern.search(tag)
                            for tag in tags
                        )
                    )
                else:
                    term = search_term.lower()
                    matches = (
                        term in name.lower()
                        or term in description.lower()
                        or any(
                            term in tag.lower()
                            for tag in tags
                        )
                    )

                if matches:
                    term = search_term.lower()
                    match_type = (
                        "name"
                        if term in name.lower()
                        else "metadata"
                    )
                    matching_recipes.append({
                        "name": name,
                        "type": recipe["type"],
                        "description": description,
                        "tags": tags,
                        "match_type": match_type,
                    })

            search_results["recipes"] = (
                matching_recipes
            )

        # Search scenarios
        if "scenarios" in object_types:
            scenarios = project.list_scenarios()
            matching_scenarios = []

            for scenario in scenarios:
                name = scenario["name"]
                description = scenario.get(
                    "description", ""
                )
                tags = scenario.get("tags", [])

                # Check if matches
                matches = False
                if pattern:
                    matches = (
                        pattern.search(name)
                        or pattern.search(description)
                        or any(
                            pattern.search(tag)
                            for tag in tags
                        )
                    )
                else:
                    term = search_term.lower()
                    matches = (
                        term in name.lower()
                        or term in description.lower()
                        or any(
                            term in tag.lower()
                            for tag in tags
                        )
                    )

                if matches:
                    term = search_term.lower()
                    match_type = (
                        "name"
                        if term in name.lower()
                        else "metadata"
                    )
                    matching_scenarios.append({
                        "name": name,
                        "id": scenario["id"],
                        "type": scenario["type"],
                        "description": description,
                        "tags": tags,
                        "active": scenario.get(
                            "active", False
                        ),
                        "match_type": match_type,
                    })

            search_results["scenarios"] = (
                matching_scenarios
            )

        # Calculate search statistics
        total_matches = sum(
            len(results)
            for results in search_results.values()
        )
        search_stats = {
            "search_term": search_term,
            "object_types_searched": object_types,
            "total_matches": total_matches,
            "matches_by_type": {
                obj_type: len(results)
                for obj_type, results
                in search_results.items()
            },
        }

        return {
            "status": "ok",
            "project_key": project_key,
            "search_stats": search_stats,
            "results": search_results
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                "Failed to search project objects: "
                f"{str(e)}"
            ),
        }


def get_dataset_sample(
    project_key: str,
    dataset_name: str,
    rows: int = 100,
    columns: list[str] | None = None
) -> dict[str, Any]:
    """
    Get sample data from datasets.

    Args:
        project_key: The project key
        dataset_name: Name of the dataset
        rows: Number of sample rows
        columns: Specific columns to include (optional)

    Returns:
        Dict containing sample data and schema
    """
    try:
        project = get_project(project_key)
        dataset = project.get_dataset(dataset_name)

        # Get dataset schema
        schema = dataset.get_schema()
        schema_columns = schema.get("columns", [])

        # Filter columns if specified
        if columns:
            # Validate requested columns exist
            available_columns = [
                col["name"] for col in schema_columns
            ]
            invalid_columns = [
                col
                for col in columns
                if col not in available_columns
            ]

            if invalid_columns:
                return {
                    "status": "error",
                    "message": (
                        "Invalid columns: "
                        f"{invalid_columns}. "
                        "Available columns: "
                        f"{available_columns}"
                    ),
                }

            # Filter schema to requested columns
            filtered_schema_columns = [
                col
                for col in schema_columns
                if col["name"] in columns
            ]
        else:
            filtered_schema_columns = schema_columns
            columns = [
                col["name"]
                for col in schema_columns
            ]

        # Get sample data
        try:
            if columns and len(columns) < len(
                schema_columns
            ):
                # If specific columns requested,
                # get dataframe and select columns
                df = dataset.get_dataframe(
                    limit=rows
                )
                df = df[columns]
                sample_data = df.to_dict('records')
            else:
                # Get full sample
                df = dataset.get_dataframe(
                    limit=rows
                )
                sample_data = df.to_dict('records')

            actual_rows = len(sample_data)

        except Exception:
            # Fallback to row iterator if df fails
            try:
                sample_data = []
                for i, row in enumerate(
                    dataset.iter_rows()
                ):
                    if i >= rows:
                        break

                    # Filter to requested columns
                    if columns:
                        filtered_row = {
                            col: row.get(col)
                            for col in columns
                        }
                        sample_data.append(
                            filtered_row
                        )
                    else:
                        sample_data.append(row)

                actual_rows = len(sample_data)

            except Exception as e2:
                return {
                    "status": "error",
                    "message": (
                        "Failed to read sample"
                        f" data: {str(e2)}"
                    ),
                }

        # Calculate sample statistics
        col_names = columns if columns else [
            col["name"] for col in schema_columns
        ]
        sample_stats = {
            "requested_rows": rows,
            "actual_rows": actual_rows,
            "requested_columns": (
                len(columns)
                if columns
                else len(schema_columns)
            ),
            "total_columns": len(schema_columns),
            "column_names": col_names,
        }

        # Generate column statistics
        column_stats = []
        for col in filtered_schema_columns:
            col_name = col["name"]
            col_type = col["type"]

            col_stat = {
                "name": col_name,
                "type": col_type,
                "meaning": col.get("meaning", ""),
                "description": col.get(
                    "description", ""
                ),
            }

            # Calculate basic statistics
            if sample_data:
                values = [
                    row.get(col_name)
                    for row in sample_data
                ]
                non_null_values = [
                    v for v in values
                    if v is not None
                ]

                null_count = (
                    len(values) - len(non_null_values)
                )
                col_stat["null_count"] = null_count
                col_stat["null_percentage"] = (
                    null_count / len(values) * 100
                    if values
                    else 0
                )

                if non_null_values:
                    numeric_types = [
                        "int",
                        "bigint",
                        "float",
                        "double",
                    ]
                    if col_type in numeric_types:
                        # Numeric statistics
                        try:
                            numeric_values = [
                                float(v)
                                for v
                                in non_null_values
                                if v is not None
                            ]
                            if numeric_values:
                                col_stat["min"] = (
                                    min(numeric_values)
                                )
                                col_stat["max"] = (
                                    max(numeric_values)
                                )
                                col_stat["mean"] = (
                                    sum(numeric_values)
                                    / len(numeric_values)
                                )
                        except Exception:
                            pass

                    elif col_type == "string":
                        # String statistics
                        string_values = [
                            str(v)
                            for v in non_null_values
                        ]
                        if string_values:
                            col_stat[
                                "unique_count"
                            ] = len(set(string_values))
                            total_len = sum(
                                len(s)
                                for s in string_values
                            )
                            col_stat[
                                "avg_length"
                            ] = (
                                total_len
                                / len(string_values)
                            )
                            col_stat[
                                "max_length"
                            ] = max(
                                len(s)
                                for s in string_values
                            )
                            col_stat[
                                "min_length"
                            ] = min(
                                len(s)
                                for s in string_values
                            )

                            # Most common values
                            from collections import (
                                Counter,
                            )
                            value_counts = Counter(
                                string_values
                            )
                            col_stat[
                                "most_common"
                            ] = (
                                value_counts
                                .most_common(5)
                            )

            column_stats.append(col_stat)

        # Get dataset metadata
        dataset_settings = dataset.get_settings()
        raw = dataset_settings.get_raw()
        connection = (
            raw
            .get("params", {})
            .get("connection", "unknown")
        )
        dataset_info = {
            "name": dataset_name,
            "type": raw["type"],
            "format": raw.get(
                "formatType", "unknown"
            ),
            "connection": connection,
        }

        return {
            "status": "ok",
            "project_key": project_key,
            "dataset_info": dataset_info,
            "sample_stats": sample_stats,
            "schema": {
                "columns": filtered_schema_columns,
                "column_count": len(
                    filtered_schema_columns
                ),
            },
            "column_stats": column_stats,
            "sample_data": sample_data
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                "Failed to get dataset sample: "
                f"{str(e)}"
            ),
        }


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
    try:
        project = get_project_for_write(project_key)
        flow = project.get_flow()
        zone = flow.create_zone(zone_name, color=color)

        return {
            "status": "ok",
            "project_key": project_key,
            "zone_id": zone.id,
            "zone_name": zone_name,
            "color": color,
            "message": f"Zone '{zone_name}' created successfully"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to create flow zone: {str(e)}"
        }


def add_dataset_reference(
    project_key: str,
    source_project_key: str,
    dataset_name: str
) -> dict[str, Any]:
    """
    Add a reference to a dataset from another project.

    Exposes the dataset from the source project (via add_exposed_object)
    and returns the reference notation for use in recipe inputs. The
    foreign dataset appears in the flow automatically when wired as input.

    Exposing does NOT require "Claude Write" tag — it's a sharing action.

    Args:
        project_key: Target project key
        source_project_key: Source project key where the dataset lives
        dataset_name: Name of the dataset to reference

    Returns:
        Dict containing reference info for use in recipes
    """
    try:
        client = get_client()

        # Verify source dataset exists
        source_project = client.get_project(source_project_key)
        source_dataset = source_project.get_dataset(dataset_name)
        source_raw = source_dataset.get_settings().get_raw()

        ds_type = source_raw.get("type", "unknown")
        params = source_raw.get("params", {})
        connection = params.get("connection", "")
        table = params.get("table", "")
        schema_name = params.get("schema", "")

        ref = f"{source_project_key}.{dataset_name}"

        # Expose dataset from source project (no Claude Write tag needed)
        exposed = False
        expose_error = None
        try:
            source_settings = source_project.get_settings()
            source_settings.add_exposed_object("DATASET", dataset_name, project_key)
            source_settings.save()
            exposed = True
        except Exception as ee:
            expose_error = str(ee)

        # Get column count
        col_count = 0
        try:
            ds_schema = source_dataset.get_schema()
            col_count = len(ds_schema.get("columns", []))
        except Exception:
            pass

        return {
            "status": "ok",
            "project_key": project_key,
            "source_project_key": source_project_key,
            "dataset_name": dataset_name,
            "reference": ref,
            "exposed": exposed,
            "expose_error": expose_error,
            "dataset_type": ds_type,
            "connection": connection,
            "table": f"{schema_name}.{table}" if schema_name else table,
            "column_count": col_count,
            "message": (
                f"Dataset '{dataset_name}' exposed from {source_project_key} to {project_key}. "
                f"Use create_recipe with input '{dataset_name}' and project_key='{source_project_key}' "
                f"in the with_input call (the builder handles the cross-project reference)."
            )
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to add dataset reference: {str(e)}"
        }


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
               type must be 'dataset', 'recipe', or 'managed_folder'.

    Returns:
        Dict containing move results
    """
    try:
        project = get_project_for_write(project_key)
        flow = project.get_flow()
        zone = flow.get_zone(zone_id)

        moved = []
        errors = []

        for item in items:
            item_type = item.get("type", "").lower()
            item_name = item.get("name", "")
            try:
                if item_type == "dataset":
                    obj = project.get_dataset(item_name)
                elif item_type == "recipe":
                    obj = project.get_recipe(item_name)
                elif item_type == "managed_folder":
                    obj = project.get_managed_folder(item_name)
                else:
                    errors.append({"name": item_name, "error": f"Unknown type '{item_type}'"})
                    continue
                zone.add_item(obj)
                moved.append({"type": item_type, "name": item_name})
            except Exception as e:
                errors.append({"name": item_name, "error": str(e)})

        return {
            "status": "ok" if not errors else "partial",
            "project_key": project_key,
            "zone_id": zone_id,
            "moved": moved,
            "errors": errors,
            "message": f"Moved {len(moved)} items to zone. {len(errors)} errors."
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to move items to zone: {str(e)}"
        }


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
    try:
        project = get_project_for_write(project_key)
        flow = project.get_flow()

        propagation = flow.new_schema_propagation(dataset_name)
        propagation.set_auto_rebuild(True)
        future = propagation.start()
        result = future.wait_for_result()

        return {
            "status": "ok",
            "project_key": project_key,
            "dataset_name": dataset_name,
            "result": result if isinstance(result, dict) else str(result),
            "message": f"Schema propagation completed from '{dataset_name}'"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to propagate schema from '{dataset_name}': {str(e)}"
        }
