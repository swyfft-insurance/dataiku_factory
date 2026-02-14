"""
Environment and configuration tools for Dataiku MCP integration.
"""

from typing import Any

from dataiku_mcp.client import get_client, get_project


def get_code_environments(
    project_key: str | None = None
) -> dict[str, Any]:
    """
    List available Python/R environments.

    Args:
        project_key: Project identifier (optional)

    Returns:
        Dict containing code environments information
    """
    try:
        client = get_client()

        # Get global code environments
        global_environments = []
        try:
            env_list = client.list_code_envs()
            for env in env_list:
                env_info = {
                    "name": env["envName"],
                    "language": env["envLang"],
                    "type": env["deploymentMode"],
                    "owner": env.get(
                        "owner", "unknown"
                    ),
                    "usable": env.get(
                        "usable", False
                    ),
                    "description": env.get(
                        "description", ""
                    )
                }

                # Get detailed environment info
                try:
                    env_details = (
                        client.get_code_env(
                            env["envLang"],
                            env["envName"]
                        )
                    )
                    env_settings = (
                        env_details.get_settings()
                    )

                    raw = env_settings.get_raw()
                    env_info[
                        "python_interpreter"
                    ] = raw.get(
                        "pythonInterpreter",
                        "unknown"
                    )
                    env_info[
                        "conda_environment"
                    ] = raw.get(
                        "condaEnvironment", ""
                    )
                    env_info["desc"] = raw.get(
                        "desc", ""
                    )

                    # Get package information
                    try:
                        packages = (
                            env_details.list_packages()
                        )
                        env_info[
                            "package_count"
                        ] = len(packages)
                        # First 10 packages
                        env_info[
                            "sample_packages"
                        ] = packages[:10]
                    except Exception:
                        env_info[
                            "package_count"
                        ] = 0
                        env_info[
                            "sample_packages"
                        ] = []

                except Exception as e:
                    env_info["error"] = (
                        "Could not get detailed"
                        f" info: {str(e)}"
                    )

                global_environments.append(
                    env_info
                )

        except Exception as e:
            global_environments = []
            (
                "Could not list global"
                f" environments: {str(e)}"
            )

        result = {
            "status": "ok",
            "global_environments": (
                global_environments
            ),
            "global_environment_count": len(
                global_environments
            )
        }

        # Get project-specific environment settings
        # if project_key provided
        if project_key:
            try:
                project = get_project(project_key)
                project_settings = (
                    project.get_settings()
                )

                # Get code env settings
                code_env_settings = (
                    project_settings
                    .get_code_env_settings()
                )

                py_settings = (
                    code_env_settings
                    .get("python", {})
                )
                r_settings = (
                    code_env_settings
                    .get("r", {})
                )

                project_env_info = {
                    "project_key": project_key,
                    "default_python_env": (
                        py_settings.get(
                            "defaultEnv",
                            "INHERIT"
                        )
                    ),
                    "default_r_env": (
                        r_settings.get(
                            "defaultEnv",
                            "INHERIT"
                        )
                    ),
                    "use_builtin_python": (
                        py_settings.get(
                            "mode", "INHERIT"
                        ) == "INHERIT"
                    ),
                    "use_builtin_r": (
                        r_settings.get(
                            "mode", "INHERIT"
                        ) == "INHERIT"
                    )
                }

                # Get environment overrides
                # for specific objects
                env_overrides = (
                    code_env_settings
                    .get("envOverrides", {})
                )
                project_env_info[
                    "environment_overrides"
                ] = env_overrides
                project_env_info[
                    "override_count"
                ] = len(env_overrides)

                result[
                    "project_environment_info"
                ] = project_env_info

            except Exception as e:
                result[
                    "project_environment_error"
                ] = (
                    "Could not get project"
                    " environment settings:"
                    f" {str(e)}"
                )

        return result

    except Exception as e:
        return {
            "status": "error",
            "message": (
                "Failed to get code"
                f" environments: {str(e)}"
            )
        }


def get_project_variables(
    project_key: str
) -> dict[str, Any]:
    """
    Get project-level variables and configuration.

    Args:
        project_key: The project key

    Returns:
        Dict containing project variables and metadata
    """
    try:
        project = get_project(project_key)

        # Get project variables
        variables = project.get_variables()

        # Separate standard and custom variables
        standard_variables = variables.get(
            "standard", {}
        )
        custom_variables = variables.get(
            "local", {}
        )

        # Get project metadata
        metadata = project.get_metadata()

        # Get project settings (requires admin)
        settings = None
        try:
            settings = project.get_settings()
        except Exception:
            pass  # Settings not available

        # Extract key project information
        project_info = {
            "key": project_key,
            "name": metadata.get(
                "name", project_key
            ),
            "description": metadata.get(
                "description", ""
            ),
            "short_description": metadata.get(
                "shortDesc", ""
            ),
            "tags": metadata.get("tags", []),
            "owner": metadata.get(
                "owner", "unknown"
            ),
            "creation_date": metadata.get(
                "creationDate", ""
            ),
            "last_modified": (
                metadata
                .get("versionTag", {})
                .get("lastModified", "")
            )
        }

        # Get custom fields
        custom_fields = metadata.get(
            "customFields", {}
        )

        # Process variables to hide sensitive info
        sensitive_keys = [
            "password", "secret", "key", "token"
        ]

        processed_standard = {}
        for key, value in standard_variables.items():
            if key.lower() in sensitive_keys:
                processed_standard[key] = {
                    "type": type(value).__name__,
                    "value": "***HIDDEN***"
                }
            else:
                processed_standard[key] = {
                    "type": type(value).__name__,
                    "value": value
                }

        processed_custom = {}
        for key, value in custom_variables.items():
            if key.lower() in sensitive_keys:
                processed_custom[key] = {
                    "type": type(value).__name__,
                    "value": "***HIDDEN***"
                }
            else:
                processed_custom[key] = {
                    "type": type(value).__name__,
                    "value": value
                }

        # Get project permissions (if available)
        project_permissions = {}
        try:
            # May not be available in all versions
            permissions = (
                project.get_permissions()
            )
            project_permissions = permissions
        except Exception:
            project_permissions = {
                "error": "Permissions not available"
            }

        # Get project settings summary
        if settings is not None:
            raw = settings.get_raw()
            settings_summary = {
                "bundle_export_options": raw.get(
                    "bundleExportOptions", {}
                ),
                "git_reference": raw.get(
                    "gitReference", {}
                ),
                "flow_display_settings": raw.get(
                    "flowDisplaySettings", {}
                ),
                "notebook_exports": raw.get(
                    "notebookExports", {}
                )
            }
        else:
            settings_summary = {
                "error": (
                    "Settings not available"
                    " (requires admin"
                    " permission)"
                )
            }

        # Calculate statistics
        variable_stats = {
            "standard_variable_count": len(
                standard_variables
            ),
            "custom_variable_count": len(
                custom_variables
            ),
            "total_variables": (
                len(standard_variables)
                + len(custom_variables)
            ),
            "custom_fields_count": len(
                custom_fields
            ),
            "tag_count": len(
                metadata.get("tags", [])
            )
        }

        return {
            "status": "ok",
            "project_info": project_info,
            "variables": {
                "standard": processed_standard,
                "custom": processed_custom
            },
            "variable_stats": variable_stats,
            "metadata": {
                "custom_fields": custom_fields,
                "tags": metadata.get(
                    "tags", []
                ),
                "checklists": metadata.get(
                    "checklists", []
                )
            },
            "settings_summary": settings_summary,
            "permissions": project_permissions
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                "Failed to get project"
                f" variables: {str(e)}"
            )
        }


def get_connections(
    project_key: str | None = None
) -> dict[str, Any]:
    """
    List available data connections.

    Args:
        project_key: Project identifier (optional)

    Returns:
        Dict containing connection information
    """
    try:
        client = get_client()

        # Get global connections
        global_connections = []
        try:
            connections = client.list_connections()
            for conn in connections:
                conn_info = {
                    "name": conn["name"],
                    "type": conn["type"],
                    "usable": conn.get(
                        "usable", False
                    ),
                    "allow_write": conn.get(
                        "allowWrite", False
                    ),
                    "allow_managed_datasets": (
                        conn.get(
                            "allowManagedDatasets",
                            False
                        )
                    ),
                    "description": conn.get(
                        "description", ""
                    )
                }

                # Get detailed connection info
                # (without sensitive parameters)
                try:
                    conn_details = (
                        client.get_connection(
                            conn["name"]
                        )
                    )
                    conn_settings = (
                        conn_details.get_settings()
                    )

                    # Get non-sensitive parameters
                    params = (
                        conn_settings
                        .get_raw()
                        .get("params", {})
                    )
                    safe_params = {}

                    # Filter sensitive parameters
                    sensitive = [
                        "password",
                        "secret",
                        "key",
                        "token",
                        "credentials"
                    ]
                    for key, value in params.items():
                        if any(
                            s in key.lower()
                            for s in sensitive
                        ):
                            safe_params[key] = (
                                "***HIDDEN***"
                            )
                        else:
                            safe_params[key] = (
                                value
                            )

                    conn_info[
                        "parameters"
                    ] = safe_params
                    conn_info["description"] = (
                        conn_settings
                        .get_raw()
                        .get("description", "")
                    )

                except Exception as e:
                    conn_info["error"] = (
                        "Could not get detailed"
                        f" info: {str(e)}"
                    )

                global_connections.append(
                    conn_info
                )

        except Exception as e:
            global_connections = []
            (
                "Could not list global"
                f" connections: {str(e)}"
            )

        result = {
            "status": "ok",
            "global_connections": (
                global_connections
            ),
            "global_connection_count": len(
                global_connections
            )
        }

        # Get project-specific connection usage
        # if project_key provided
        if project_key:
            try:
                project = get_project(
                    project_key
                )

                # Get datasets to see which
                # connections are used
                datasets = (
                    project.list_datasets()
                )
                connection_usage = {}

                for dataset in datasets:
                    try:
                        dataset_obj = (
                            project.get_dataset(
                                dataset["name"]
                            )
                        )
                        dataset_settings = (
                            dataset_obj
                            .get_settings()
                        )
                        dataset_params = (
                            dataset_settings
                            .get_raw()
                            .get("params", {})
                        )

                        conn_name = (
                            dataset_params
                            .get(
                                "connection",
                                "default"
                            )
                        )
                        if conn_name not in (
                            connection_usage
                        ):
                            connection_usage[
                                conn_name
                            ] = {
                                "datasets": [],
                                "count": 0
                            }

                        connection_usage[
                            conn_name
                        ]["datasets"].append({
                            "name": (
                                dataset["name"]
                            ),
                            "type": (
                                dataset["type"]
                            )
                        })
                        connection_usage[
                            conn_name
                        ]["count"] += 1

                    except Exception:
                        continue

                # Get recipes to see which
                # connections are used
                recipes = (
                    project.list_recipes()
                )
                for recipe in recipes:
                    try:
                        recipe_obj = (
                            project.get_recipe(
                                recipe["name"]
                            )
                        )
                        recipe_def = (
                            recipe_obj
                            .get_definition()
                        )

                        # Check inputs/outputs
                        # for connections
                        inputs = recipe_def.get(
                            "inputs", []
                        )
                        for input_def in inputs:
                            input_name = (
                                input_def["ref"]
                            )
                            try:
                                in_ds = (
                                    project
                                    .get_dataset(
                                        input_name
                                    )
                                )
                                in_settings = (
                                    in_ds
                                    .get_settings()
                                )
                                in_params = (
                                    in_settings
                                    .get_raw()
                                    .get(
                                        "params",
                                        {}
                                    )
                                )
                                conn_name = (
                                    in_params
                                    .get(
                                        "connection",
                                        "default"
                                    )
                                )

                                cu = (
                                    connection_usage
                                )
                                if conn_name in cu:
                                    if (
                                        "used_by_recipes"  # noqa: E501
                                        not in
                                        cu[conn_name]
                                    ):
                                        cu[
                                            conn_name
                                        ][
                                            "used_by_recipes"  # noqa: E501
                                        ] = []
                                    rbr = cu[
                                        conn_name
                                    ][
                                        "used_by_recipes"  # noqa: E501
                                    ]
                                    rn = recipe[
                                        "name"
                                    ]
                                    if (
                                        rn
                                        not in rbr
                                    ):
                                        rbr.append(
                                            rn
                                        )
                            except Exception:
                                continue

                        outputs = recipe_def.get(
                            "outputs", []
                        )
                        for output_def in outputs:
                            output_name = (
                                output_def["ref"]
                            )
                            try:
                                out_ds = (
                                    project
                                    .get_dataset(
                                        output_name
                                    )
                                )
                                out_settings = (
                                    out_ds
                                    .get_settings()
                                )
                                out_params = (
                                    out_settings
                                    .get_raw()
                                    .get(
                                        "params",
                                        {}
                                    )
                                )
                                conn_name = (
                                    out_params
                                    .get(
                                        "connection",
                                        "default"
                                    )
                                )

                                cu = (
                                    connection_usage
                                )
                                if conn_name in cu:
                                    if (
                                        "used_by_recipes"  # noqa: E501
                                        not in
                                        cu[conn_name]
                                    ):
                                        cu[
                                            conn_name
                                        ][
                                            "used_by_recipes"  # noqa: E501
                                        ] = []
                                    rbr = cu[
                                        conn_name
                                    ][
                                        "used_by_recipes"  # noqa: E501
                                    ]
                                    rn = recipe[
                                        "name"
                                    ]
                                    if (
                                        rn
                                        not in rbr
                                    ):
                                        rbr.append(
                                            rn
                                        )
                            except Exception:
                                continue

                    except Exception:
                        continue

                project_conn_info = {
                    "project_key": project_key,
                    "connection_usage": (
                        connection_usage
                    ),
                    "unique_connections_used": (
                        len(connection_usage)
                    ),
                    "total_datasets": len(
                        datasets
                    ),
                    "total_recipes": len(
                        recipes
                    )
                }

                result[
                    "project_connection_info"
                ] = project_conn_info

            except Exception as e:
                result[
                    "project_connection_error"
                ] = (
                    "Could not get project"
                    " connection info:"
                    f" {str(e)}"
                )

        # Group connections by type
        connection_types = {}
        for conn in global_connections:
            conn_type = conn["type"]
            if conn_type not in connection_types:
                connection_types[conn_type] = []
            connection_types[conn_type].append(
                conn["name"]
            )

        result[
            "connection_types"
        ] = connection_types
        result[
            "connection_type_count"
        ] = len(connection_types)

        return result

    except Exception as e:
        return {
            "status": "error",
            "message": (
                "Failed to get connections:"
                f" {str(e)}"
            )
        }
