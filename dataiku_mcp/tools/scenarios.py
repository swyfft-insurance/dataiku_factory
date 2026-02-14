"""
Scenario management tools for Dataiku DSS.

This module provides functions for creating, updating,
deleting, and managing scenarios in Dataiku DSS projects
through the dataiku-api-client.
"""

from typing import Any

from dataiku_mcp.client import get_project


def create_scenario(
    project_key: str,
    scenario_name: str,
    scenario_type: str,
    definition: dict[str, Any] | None = None
) -> dict[str, Any]:
    """
    Create a new scenario in a Dataiku DSS project.

    Args:
        project_key: The project key where the scenario
            will be created
        scenario_name: Name of the scenario to create
        scenario_type: Type of scenario
            ('step_based' or 'custom_python')
        definition: Optional scenario definition dict.
            If None, defaults to {'params': {}}

    Returns:
        Dict with status and scenario details or error
        message
    """
    # Validate scenario type before attempting connection
    valid_types = ['step_based', 'custom_python']
    if scenario_type not in valid_types:
        return {
            "status": "error",
            "message": (
                f"Invalid scenario type "
                f"'{scenario_type}'. "
                f"Must be one of: {valid_types}"
            )
        }

    try:
        project = get_project(project_key)

        # Set default definition if not provided
        if definition is None:
            definition = {'params': {}}

        # Create the scenario
        scenario = project.create_scenario(
            scenario_name=scenario_name,
            type=scenario_type,
            definition=definition
        )

        return {
            "status": "ok",
            "scenario_name": scenario_name,
            "scenario_id": scenario.id,
            "scenario_type": scenario_type,
            "project_key": project_key,
            "message": (
                f"Scenario '{scenario_name}' "
                f"created successfully"
            )
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                f"Failed to create scenario "
                f"'{scenario_name}': {str(e)}"
            )
        }


def update_scenario(
    project_key: str,
    scenario_id: str,
    **kwargs
) -> dict[str, Any]:
    """
    Update an existing scenario's settings.

    Args:
        project_key: The project key containing
            the scenario
        scenario_id: ID of the scenario to update
        **kwargs: Update parameters including:
            - name: New scenario name
            - description: Scenario description
            - active: Whether the scenario is active
            - tags: List of tags
            - custom_fields: Dict of custom metadata
            - definition: Scenario definition dict
            - step_script: Python script code to update
              in a custom_python step
            - step_index: Index of the step to update
              (default: 0)

    Returns:
        Dict with status and update details or error
        message
    """
    try:
        project = get_project(project_key)
        scenario = project.get_scenario(scenario_id)

        updated_fields = []

        # Update scenario metadata if provided
        metadata_keys = [
            'description', 'tags', 'custom_fields'
        ]
        if any(key in kwargs for key in metadata_keys):
            metadata = scenario.get_metadata()

            if 'description' in kwargs:
                metadata['description'] = (
                    kwargs['description']
                )
                updated_fields.append('description')

            if 'tags' in kwargs:
                metadata['tags'] = kwargs['tags']
                updated_fields.append('tags')

            if 'custom_fields' in kwargs:
                if 'customFields' not in metadata:
                    metadata['customFields'] = {}
                metadata['customFields'].update(
                    kwargs['custom_fields']
                )
                updated_fields.append('custom_fields')

            scenario.set_metadata(metadata)

        # Update scenario settings
        settings_keys = [
            'name', 'active', 'definition',
            'step_script'
        ]
        if any(
            key in kwargs for key in settings_keys
        ):
            settings = scenario.get_settings()

            if 'name' in kwargs:
                settings.name = kwargs['name']
                updated_fields.append('name')

            if 'active' in kwargs:
                settings.active = kwargs['active']
                updated_fields.append('active')

            if 'definition' in kwargs:
                # Update the definition (this may not
                # work with all DSS versions)
                try:
                    current_definition = (
                        settings.get_definition()
                    )
                    current_definition.update(
                        kwargs['definition']
                    )
                    settings.set_definition(
                        current_definition
                    )
                    updated_fields.append('definition')
                except AttributeError:
                    # Fallback: direct update
                    settings.data.update(
                        kwargs['definition']
                    )
                    updated_fields.append('definition')

            if 'step_script' in kwargs:
                # Update Python script in the first
                # custom_python step
                step_index = kwargs.get(
                    'step_index', 0
                )
                script_code = kwargs['step_script']

                # Access raw steps directly
                raw_steps = settings.raw_steps
                if step_index < len(raw_steps):
                    step = raw_steps[step_index]
                    if step.get('type') == (
                        'custom_python'
                    ):
                        if 'params' not in step:
                            step['params'] = {}
                        step['params']['script'] = (
                            script_code
                        )
                        updated_fields.append(
                            'step_script'
                        )
                    else:
                        raise ValueError(
                            f"Step {step_index} is "
                            f"not a custom_python "
                            f"step"
                        )
                else:
                    raise ValueError(
                        f"Step index {step_index} "
                        f"is out of range"
                    )

            settings.save()

        return {
            "status": "ok",
            "scenario_id": scenario_id,
            "updated_fields": updated_fields,
            "message": (
                f"Scenario '{scenario_id}' "
                f"updated successfully"
            )
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                f"Failed to update scenario "
                f"'{scenario_id}': {str(e)}"
            )
        }


def delete_scenario(
    project_key: str,
    scenario_id: str
) -> dict[str, Any]:
    """
    Delete a scenario from a Dataiku DSS project.

    Args:
        project_key: The project key containing
            the scenario
        scenario_id: ID of the scenario to delete

    Returns:
        Dict with status and deletion details or
        error message
    """
    try:
        project = get_project(project_key)
        scenario = project.get_scenario(scenario_id)

        # Store scenario info before deletion
        scenario_info = {
            "id": scenario_id,
            "name": getattr(
                scenario, 'name', scenario_id
            ),
            "type": getattr(
                scenario, 'type', 'unknown'
            )
        }

        # Delete the scenario
        scenario.delete()

        return {
            "status": "ok",
            "deleted_scenario": scenario_info,
            "message": (
                f"Scenario '{scenario_id}' "
                f"deleted successfully"
            )
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                f"Failed to delete scenario "
                f"'{scenario_id}': {str(e)}"
            )
        }


def add_scenario_trigger(
    project_key: str,
    scenario_id: str,
    trigger_type: str,
    **params
) -> dict[str, Any]:
    """
    Add a trigger to a scenario.

    Args:
        project_key: The project key containing
            the scenario
        scenario_id: ID of the scenario to add
            trigger to
        trigger_type: Type of trigger ('time',
            'dataset', 'periodic', 'hourly',
            'daily', 'monthly')
        **params: Trigger-specific parameters:
            For 'periodic': every_minutes (int)
            For 'hourly': starting_hour (int),
                minute_of_hour (int),
                repeat_every (int)
            For 'daily': hour (int), minute (int),
                year (int), month (int),
                day (int), repeat_every (int)
            For 'monthly': day (int), hour (int),
                minute (int), year (int),
                month (int)
            For 'dataset': dataset_name (str),
                project_key (str, optional)
            For 'time': Use one of the specific
                time trigger types above

    Returns:
        Dict with status and trigger details or
        error message
    """
    # Validate trigger type before attempting connection
    valid_trigger_types = [
        'periodic', 'hourly', 'daily',
        'monthly', 'dataset'
    ]
    if trigger_type == 'time':
        return {
            "status": "error",
            "message": (
                "Use specific time trigger types: "
                "periodic, hourly, daily, or monthly"
            )
        }

    if trigger_type not in valid_trigger_types:
        return {
            "status": "error",
            "message": (
                f"Unsupported trigger type "
                f"'{trigger_type}'. "
                f"Supported types: "
                f"{', '.join(valid_trigger_types)}"
            )
        }

    # Validate dataset trigger parameters
    if (
        trigger_type == 'dataset'
        and 'dataset_name' not in params
    ):
        return {
            "status": "error",
            "message": (
                "dataset_name is required "
                "for dataset triggers"
            )
        }

    try:
        project = get_project(project_key)
        scenario = project.get_scenario(scenario_id)
        settings = scenario.get_settings()

        # Map trigger types to actual methods
        trigger_added = False
        trigger_details = {"type": trigger_type}

        if trigger_type == 'periodic':
            every_minutes = params.get(
                'every_minutes', 60
            )
            settings.add_periodic_trigger(
                every_minutes=every_minutes
            )
            trigger_details['every_minutes'] = (
                every_minutes
            )
            trigger_added = True

        elif trigger_type == 'hourly':
            starting_hour = params.get(
                'starting_hour', 0
            )
            minute_of_hour = params.get(
                'minute_of_hour', 0
            )
            repeat_every = params.get(
                'repeat_every', 1
            )
            settings.add_hourly_trigger(
                starting_hour=starting_hour,
                minute_of_hour=minute_of_hour,
                repeat_every=repeat_every
            )
            trigger_details.update({
                'starting_hour': starting_hour,
                'minute_of_hour': minute_of_hour,
                'repeat_every': repeat_every
            })
            trigger_added = True

        elif trigger_type == 'daily':
            hour = params.get('hour', 2)
            minute = params.get('minute', 0)
            year = params.get('year')
            month = params.get('month')
            day = params.get('day')
            repeat_every = params.get(
                'repeat_every', 1
            )
            timezone = params.get(
                'timezone', 'SERVER'
            )

            settings.add_daily_trigger(
                hour=hour,
                minute=minute,
                year=year,
                month=month,
                day=day,
                repeat_every=repeat_every,
                timezone=timezone
            )
            trigger_details.update({
                'hour': hour,
                'minute': minute,
                'year': year,
                'month': month,
                'day': day,
                'repeat_every': repeat_every,
                'timezone': timezone
            })
            trigger_added = True

        elif trigger_type == 'monthly':
            day = params.get('day', 1)
            hour = params.get('hour', 2)
            minute = params.get('minute', 0)
            year = params.get('year')
            month = params.get('month')

            settings.add_monthly_trigger(
                day=day,
                hour=hour,
                minute=minute,
                year=year,
                month=month
            )
            trigger_details.update({
                'day': day,
                'hour': hour,
                'minute': minute,
                'year': year,
                'month': month
            })
            trigger_added = True

        elif trigger_type == 'dataset':
            dataset_name = params.get(
                'dataset_name'
            )
            dataset_project_key = params.get(
                'project_key', project_key
            )

            settings.add_dataset_trigger(
                dataset_name=dataset_name,
                project_key=dataset_project_key
            )
            trigger_details.update({
                'dataset_name': dataset_name,
                'project_key': (
                    dataset_project_key
                )
            })
            trigger_added = True

        if trigger_added:
            settings.save()

            return {
                "status": "ok",
                "scenario_id": scenario_id,
                "trigger_details": trigger_details,
                "message": (
                    f"Trigger '{trigger_type}' "
                    f"added to scenario "
                    f"'{scenario_id}' successfully"
                )
            }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                f"Failed to add trigger to "
                f"scenario '{scenario_id}': "
                f"{str(e)}"
            )
        }


def remove_scenario_trigger(
    project_key: str,
    scenario_id: str,
    trigger_idx: int
) -> dict[str, Any]:
    """
    Remove a trigger from a scenario by index.

    Args:
        project_key: The project key containing
            the scenario
        scenario_id: ID of the scenario to remove
            trigger from
        trigger_idx: Index of the trigger to remove
            (0-based)

    Returns:
        Dict with status and removal details or
        error message
    """
    try:
        project = get_project(project_key)
        scenario = project.get_scenario(scenario_id)
        settings = scenario.get_settings()

        # Get current triggers
        triggers = settings.get_triggers()

        if (
            trigger_idx < 0
            or trigger_idx >= len(triggers)
        ):
            max_idx = len(triggers) - 1
            return {
                "status": "error",
                "message": (
                    f"Invalid trigger index "
                    f"{trigger_idx}. "
                    f"Valid range: 0-{max_idx}"
                )
            }

        # Get trigger info before removal
        trigger_info = triggers[trigger_idx]

        # Remove the trigger
        del triggers[trigger_idx]

        # Save the settings
        settings.save()

        return {
            "status": "ok",
            "scenario_id": scenario_id,
            "removed_trigger": {
                "index": trigger_idx,
                "type": trigger_info.get(
                    'type', 'unknown'
                ),
                "name": trigger_info.get(
                    'name', 'unnamed'
                )
            },
            "remaining_triggers": (
                len(triggers) - 1
            ),
            "message": (
                f"Trigger at index {trigger_idx} "
                f"removed from scenario "
                f"'{scenario_id}' successfully"
            )
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                f"Failed to remove trigger from "
                f"scenario '{scenario_id}': "
                f"{str(e)}"
            )
        }


def run_scenario(
    project_key: str,
    scenario_id: str,
    wait: bool = True,
    no_fail: bool = False
) -> dict[str, Any]:
    """
    Run a scenario and optionally wait for
    completion.

    Args:
        project_key: The project key containing
            the scenario
        scenario_id: ID of the scenario to run
        wait: Whether to wait for completion
            (default: True)
        no_fail: Whether to suppress failures
            (default: False)

    Returns:
        Dict with status and run details or error
        message
    """
    try:
        project = get_project(project_key)
        scenario = project.get_scenario(scenario_id)

        if wait:
            # Run scenario and wait for completion
            run_result = scenario.run_and_wait(
                no_fail=no_fail
            )

            # Extract run information
            run_info = {
                "scenario_id": scenario_id,
                "run_id": getattr(
                    run_result, 'id', 'unknown'
                ),
                "outcome": getattr(
                    run_result,
                    'outcome',
                    'unknown'
                ),
                "start_time": getattr(
                    run_result,
                    'start_time',
                    None
                ),
                "end_time": getattr(
                    run_result, 'end_time', None
                ),
                "duration": getattr(
                    run_result, 'duration', None
                ),
                "waited_for_completion": True
            }

            # Check if run was successful
            outcome = run_info.get(
                'outcome', ''
            ).upper()
            if outcome == 'SUCCESS':
                status = "ok"
                message = (
                    f"Scenario '{scenario_id}' "
                    f"ran successfully"
                )
            elif outcome == 'FAILED':
                status = (
                    "error" if not no_fail
                    else "ok"
                )
                message = (
                    f"Scenario '{scenario_id}' "
                    f"run failed"
                )
            else:
                status = "ok"
                message = (
                    f"Scenario '{scenario_id}' "
                    f"run completed with "
                    f"outcome: {outcome}"
                )

            return {
                "status": status,
                "run_info": run_info,
                "message": message
            }

        else:
            # Run scenario without waiting
            trigger_fire = scenario.run()

            return {
                "status": "ok",
                "scenario_id": scenario_id,
                "trigger_fire_id": getattr(
                    trigger_fire, 'id', 'unknown'
                ),
                "waited_for_completion": False,
                "message": (
                    f"Scenario '{scenario_id}' "
                    f"run initiated successfully"
                )
            }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                f"Failed to run scenario "
                f"'{scenario_id}': {str(e)}"
            )
        }


def get_scenario_info(
    project_key: str,
    scenario_id: str
) -> dict[str, Any]:
    """
    Get detailed information about a scenario.

    Args:
        project_key: The project key containing
            the scenario
        scenario_id: ID of the scenario to inspect

    Returns:
        Dict with scenario information or error
        message
    """
    try:
        project = get_project(project_key)
        scenario = project.get_scenario(scenario_id)

        # Get scenario metadata
        try:
            metadata = (
                scenario.get_metadata()
                if hasattr(scenario, 'get_metadata')
                else {}
            )
        except Exception:
            metadata = {}

        # Get scenario settings
        settings = scenario.get_settings()

        # Get triggers - try raw_triggers first,
        # then get_triggers()
        try:
            triggers = (
                getattr(
                    settings,
                    'raw_triggers',
                    None
                )
                or settings.get_triggers()
            )
        except Exception:
            triggers = []

        # Get scenario status
        try:
            status = (
                scenario.get_status()
                if hasattr(scenario, 'get_status')
                else {}
            )
        except Exception:
            status = {}

        is_dict = isinstance(metadata, dict)
        description = (
            metadata.get("description", "")
            if is_dict else ""
        )
        tags = (
            metadata.get("tags", [])
            if is_dict else []
        )
        custom_fields = (
            metadata.get("customFields", {})
            if is_dict else {}
        )

        last_run_outcome = (
            getattr(
                status, 'last_run_outcome', None
            )
            if status else None
        )
        last_run_start = (
            getattr(
                status,
                'last_run_start_time',
                None
            )
            if status else None
        )
        last_run_end = (
            getattr(
                status, 'last_run_end_time', None
            )
            if status else None
        )
        last_run_duration = (
            getattr(
                status, 'last_run_duration', None
            )
            if status else None
        )

        next_run = (
            scenario.next_run()
            if hasattr(scenario, 'next_run')
            else None
        )
        is_active = (
            scenario.is_active()
            if hasattr(scenario, 'is_active')
            else None
        )

        return {
            "status": "ok",
            "scenario_info": {
                "id": scenario_id,
                "name": getattr(
                    settings, 'name', scenario_id
                ),
                "type": getattr(
                    settings, 'type', 'unknown'
                ),
                "active": getattr(
                    settings, 'active', False
                ),
                "description": description,
                "tags": tags,
                "custom_fields": custom_fields,
                "triggers": [
                    {
                        "type": trigger.get(
                            'type', 'unknown'
                        ),
                        "name": trigger.get(
                            'name', 'unnamed'
                        ),
                        "active": trigger.get(
                            'active', False
                        ),
                        "params": trigger.get(
                            'params', {}
                        )
                    }
                    for trigger in triggers
                ] if triggers else [],
                "trigger_count": len(triggers),
                "last_run": {
                    "outcome": last_run_outcome,
                    "start_time": last_run_start,
                    "end_time": last_run_end,
                    "duration": last_run_duration
                },
                "next_run": next_run,
                "is_active": is_active
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                f"Failed to get scenario info "
                f"for '{scenario_id}': {str(e)}"
            )
        }


def list_scenarios(
    project_key: str,
    scenario_type: str | None = None,
    active_only: bool = False
) -> dict[str, Any]:
    """
    List all scenarios in a project, optionally
    filtered by type or active status.

    Args:
        project_key: The project key to list
            scenarios from
        scenario_type: Optional filter by scenario
            type ('step_based' or 'custom_python')
        active_only: Whether to list only active
            scenarios (default: False)

    Returns:
        Dict with list of scenarios or error message
    """
    try:
        project = get_project(project_key)

        # Get all scenarios
        all_scenarios = project.list_scenarios()

        scenarios_info = []
        for scenario_data in all_scenarios:
            scenario_id = scenario_data.get(
                'id', scenario_data.get('name')
            )

            try:
                scenario = project.get_scenario(
                    scenario_id
                )
                settings = scenario.get_settings()

                # Apply filters
                stype = getattr(
                    settings, 'type', None
                )
                if (
                    scenario_type
                    and stype != scenario_type
                ):
                    continue

                if active_only and not getattr(
                    settings, 'active', False
                ):
                    continue

                # Get triggers - try raw_triggers
                try:
                    triggers = (
                        getattr(
                            settings,
                            'raw_triggers',
                            None
                        )
                        or settings.get_triggers()
                    )
                    trigger_count = (
                        len(triggers)
                        if triggers else 0
                    )
                except Exception:
                    trigger_count = 0

                scenarios_info.append({
                    "id": scenario_id,
                    "name": getattr(
                        settings,
                        'name',
                        scenario_id
                    ),
                    "type": getattr(
                        settings,
                        'type',
                        'unknown'
                    ),
                    "active": getattr(
                        settings, 'active', False
                    ),
                    "description": (
                        scenario_data.get(
                            'description', ''
                        )
                    ),
                    "tags": scenario_data.get(
                        'tags', []
                    ),
                    "trigger_count": trigger_count
                })

            except Exception as e:
                # Include basic info even if we
                # can't get full details
                scenarios_info.append({
                    "id": scenario_id,
                    "name": scenario_data.get(
                        'name', scenario_id
                    ),
                    "type": scenario_data.get(
                        'type', 'unknown'
                    ),
                    "active": scenario_data.get(
                        'active', False
                    ),
                    "description": (
                        scenario_data.get(
                            'description', ''
                        )
                    ),
                    "tags": scenario_data.get(
                        'tags', []
                    ),
                    "trigger_count": 0,
                    "error": (
                        "Could not get full "
                        f"details: {str(e)}"
                    )
                })

        return {
            "status": "ok",
            "scenarios": scenarios_info,
            "total_count": len(scenarios_info),
            "project_key": project_key,
            "filters": {
                "scenario_type": scenario_type,
                "active_only": active_only
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                f"Failed to list scenarios in "
                f"project '{project_key}': "
                f"{str(e)}"
            )
        }


def get_scenario_run_history(
    project_key: str,
    scenario_id: str,
    limit: int = 10
) -> dict[str, Any]:
    """
    Get the run history for a scenario.

    Args:
        project_key: The project key containing
            the scenario
        scenario_id: ID of the scenario to get
            run history for
        limit: Maximum number of runs to return
            (default: 10)

    Returns:
        Dict with run history or error message
    """
    try:
        project = get_project(project_key)
        scenario = project.get_scenario(scenario_id)

        # Get scenario run history
        if hasattr(scenario, 'get_run_history'):
            run_history = scenario.get_run_history(
                limit=limit
            )
        else:
            # Fallback to get_last_runs
            if hasattr(scenario, 'get_last_runs'):
                run_history = (
                    scenario.get_last_runs(
                        limit=limit
                    )
                )
            else:
                run_history = []

        # Process run history
        processed_runs = []
        for run in run_history:
            run_info = {
                "run_id": getattr(
                    run, 'id', 'unknown'
                ),
                "outcome": getattr(
                    run, 'outcome', 'unknown'
                ),
                "start_time": getattr(
                    run, 'start_time', None
                ),
                "end_time": getattr(
                    run, 'end_time', None
                ),
                "duration": getattr(
                    run, 'duration', None
                ),
                "trigger_name": getattr(
                    run, 'trigger_name', 'unknown'
                ),
                "trigger_type": getattr(
                    run, 'trigger_type', 'unknown'
                )
            }

            # Handle different run object formats
            if hasattr(run, 'get_info'):
                run_details = run.get_info()
                run_info.update({
                    "outcome": run_details.get(
                        'outcome',
                        run_info['outcome']
                    ),
                    "start_time": run_details.get(
                        'startTime',
                        run_info['start_time']
                    ),
                    "end_time": run_details.get(
                        'endTime',
                        run_info['end_time']
                    ),
                    "duration": run_details.get(
                        'duration',
                        run_info['duration']
                    )
                })

            processed_runs.append(run_info)

        num_runs = len(processed_runs)
        return {
            "status": "ok",
            "scenario_id": scenario_id,
            "run_history": processed_runs,
            "total_runs": num_runs,
            "message": (
                f"Retrieved {num_runs} runs "
                f"for scenario '{scenario_id}'"
            )
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                f"Failed to get run history for "
                f"scenario '{scenario_id}': "
                f"{str(e)}"
            )
        }
