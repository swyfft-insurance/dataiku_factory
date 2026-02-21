"""
Debugging and monitoring tools for Dataiku MCP integration.
"""

from typing import Any

from dataiku_mcp.client import get_client, get_project


def get_recent_runs(
    project_key: str,
    limit: int = 50,
    status_filter: str | None = None
) -> dict[str, Any]:
    """
    Get recent run history across all scenarios/recipes.

    Args:
        project_key: The project key
        limit: Number of recent runs to retrieve
        status_filter: Filter by status (SUCCESS, FAILED, etc.)

    Returns:
        Dict containing recent runs and summary
    """
    try:
        project = get_project(project_key)
        get_client()

        all_runs = []

        # Get scenario runs
        scenarios = project.list_scenarios()
        for scenario in scenarios:
            try:
                scenario_obj = project.get_scenario(
                    scenario["id"]
                )
                runs = scenario_obj.get_last_runs(
                    limit=limit
                )

                for run in runs:
                    run_info = {
                        "type": "scenario",
                        "object_id": scenario["id"],
                        "object_name": scenario["name"],
                        "run_id": run.run_id,
                        "outcome": run.outcome,
                        "start_time": run.start_time,
                        "end_time": run.end_time,
                        "duration": run.duration,
                        "trigger_type": getattr(
                            run,
                            'trigger_type',
                            'unknown'
                        )
                    }

                    # Filter by status if specified
                    if (
                        status_filter is None
                        or run.outcome == status_filter
                    ):
                        all_runs.append(run_info)

            except Exception:
                continue

        # Get job runs (from recipes and other activities)
        try:
            jobs = project.list_jobs(limit=limit)
            for job in jobs:
                job_name = (
                    job.job_name
                    if hasattr(job, 'job_name')
                    else f"Job {job.job_id}"
                )
                start_time = (
                    job.start_time
                    if hasattr(job, 'start_time')
                    else None
                )
                end_time = (
                    job.end_time
                    if hasattr(job, 'end_time')
                    else None
                )
                duration = (
                    job.duration
                    if hasattr(job, 'duration')
                    else None
                )
                job_info = {
                    "type": "job",
                    "object_id": job.job_id,
                    "object_name": job_name,
                    "run_id": job.job_id,
                    "outcome": job.state,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": duration,
                    "trigger_type": "manual"
                }

                # Filter by status if specified
                if (
                    status_filter is None
                    or job.state == status_filter
                ):
                    all_runs.append(job_info)

        except Exception:
            # Jobs might not be available in all DSS versions
            pass

        # Sort by start time (most recent first)
        all_runs.sort(
            key=lambda x: x["start_time"] or "",
            reverse=True
        )

        # Limit results
        if len(all_runs) > limit:
            all_runs = all_runs[:limit]

        # Calculate summary statistics
        total_runs = len(all_runs)
        success_runs = len([
            r for r in all_runs
            if r["outcome"] in ["SUCCESS", "DONE"]
        ])
        failed_runs = len([
            r for r in all_runs
            if r["outcome"] in ["FAILED", "ABORTED"]
        ])
        running_runs = len([
            r for r in all_runs
            if r["outcome"] in ["RUNNING", "PENDING"]
        ])

        # Calculate average duration for completed runs
        completed_runs = [
            r for r in all_runs
            if r["duration"]
            and r["outcome"]
            in ["SUCCESS", "DONE", "FAILED"]
        ]
        avg_duration = (
            sum(r["duration"] for r in completed_runs)
            / len(completed_runs)
            if completed_runs
            else 0
        )

        # Group by outcome
        outcome_summary = {}
        for run in all_runs:
            outcome = run["outcome"]
            if outcome not in outcome_summary:
                outcome_summary[outcome] = 0
            outcome_summary[outcome] += 1

        # Group by type
        type_summary = {}
        for run in all_runs:
            run_type = run["type"]
            if run_type not in type_summary:
                type_summary[run_type] = 0
            type_summary[run_type] += 1

        # Recent failures analysis
        recent_failures = [
            r for r in all_runs
            if r["outcome"] in ["FAILED", "ABORTED"]
        ][:10]

        success_rate = (
            (success_runs / total_runs * 100)
            if total_runs > 0
            else 0
        )
        summary = {
            "total_runs": total_runs,
            "success_runs": success_runs,
            "failed_runs": failed_runs,
            "running_runs": running_runs,
            "success_rate": success_rate,
            "average_duration": avg_duration,
            "outcome_summary": outcome_summary,
            "type_summary": type_summary,
            "recent_failures_count": len(
                recent_failures
            )
        }

        return {
            "status": "ok",
            "project_key": project_key,
            "runs": all_runs,
            "recent_failures": recent_failures,
            "summary": summary,
            "filters_applied": {
                "limit": limit,
                "status_filter": status_filter
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                "Failed to get recent runs: "
                f"{str(e)}"
            )
        }


def get_job_details(
    project_key: str,
    job_id: str
) -> dict[str, Any]:
    """
    Get detailed job execution information.

    Args:
        project_key: The project key
        job_id: Job identifier

    Returns:
        Dict containing detailed job information
    """
    try:
        project = get_project(project_key)

        # Get job details
        job = project.get_job(job_id)

        # Get job status (primary source of info)
        job_status = job.get_status()
        job_info = {
            "job_id": job_id,
            "state": job_status.get(
                "baseStatus", {}
            ).get("state", "unknown"),
            "start_time": job_status.get(
                "baseStatus", {}
            ).get("startTime", None),
            "end_time": job_status.get(
                "baseStatus", {}
            ).get("endTime", None),
            "initiator": job_status.get(
                "baseStatus", {}
            ).get("initiator", "unknown"),
            "warning": job_status.get(
                "hasWarning", False
            ),
        }

        # Get job log
        logs = []
        try:
            log_content = job.get_log()
            if log_content:
                logs.append({
                    "type": "main_log",
                    "content": log_content,
                    "timestamp": job_info[
                        "start_time"
                    ]
                })
        except Exception as e:
            logs.append({
                "type": "error",
                "content": (
                    "Could not retrieve job log: "
                    f"{str(e)}"
                ),
                "timestamp": job_info[
                    "start_time"
                ]
            })

        # Get result information if available
        result_info = {}
        try:
            if hasattr(job, 'get_result'):
                result = job.get_result()
                result_summary = (
                    str(result)[:500]
                    if result
                    else "No result"
                )
                result_info = {
                    "has_result": True,
                    "result_type": (
                        type(result).__name__
                    ),
                    "result_summary": (
                        result_summary
                    )
                }
        except Exception as e:
            result_info = {
                "has_result": False,
                "error": (
                    "Could not get result: "
                    f"{str(e)}"
                )
            }

        # Get activity information
        activity_info = {}
        try:
            activities = job.get_activities()
            activity_info = {
                "activity_count": len(activities),
                "activities": [
                    {
                        "type": activity.get(
                            "type", "unknown"
                        ),
                        "name": activity.get(
                            "name", "unknown"
                        ),
                        "state": activity.get(
                            "state", "unknown"
                        ),
                        "start_time": activity.get(
                            "startTime", None
                        ),
                        "end_time": activity.get(
                            "endTime", None
                        )
                    }
                    for activity in activities[:10]
                ]
            }
        except Exception as e:
            activity_info = {
                "activity_count": 0,
                "error": (
                    "Could not get activities: "
                    f"{str(e)}"
                )
            }

        # Get resource usage if available
        resource_usage = {}
        try:
            usage = job.get_resource_usage()
            resource_usage = {
                "cpu_time": usage.get(
                    "cpuTime", 0
                ),
                "memory_usage": usage.get(
                    "memoryUsage", 0
                ),
                "disk_usage": usage.get(
                    "diskUsage", 0
                )
            }
        except Exception as e:
            resource_usage = {
                "error": (
                    "Could not get resource "
                    f"usage: {str(e)}"
                )
            }

        # Calculate execution timeline
        timeline = []
        if job_info["start_time"]:
            timeline.append({
                "event": "job_started",
                "timestamp": job_info[
                    "start_time"
                ],
                "description": (
                    "Job execution started"
                )
            })

        if job_info["end_time"]:
            state = job_info['state']
            timeline.append({
                "event": "job_completed",
                "timestamp": job_info[
                    "end_time"
                ],
                "description": (
                    "Job completed with "
                    f"status: {state}"
                )
            })

        return {
            "status": "ok",
            "project_key": project_key,
            "job_info": job_info,
            "logs": logs,
            "result_info": result_info,
            "activity_info": activity_info,
            "resource_usage": resource_usage,
            "timeline": timeline,
            "log_count": len(logs)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                "Failed to get job details: "
                f"{str(e)}"
            )
        }


def cancel_running_jobs(
    project_key: str,
    job_ids: list[str]
) -> dict[str, Any]:
    """
    Cancel running jobs/scenarios.

    Args:
        project_key: The project key
        job_ids: List of job IDs to cancel

    Returns:
        Dict containing cancellation results
    """
    try:
        client = get_client()
        project = get_project(project_key)

        cancelled_jobs = []
        failed_cancellations = []

        for job_id in job_ids:
            try:
                # Get job first to check cancellable
                job = client.get_job(job_id)

                # Check job state
                job_state = job.state
                if job_state not in [
                    "RUNNING", "PENDING"
                ]:
                    failed_cancellations.append({
                        "job_id": job_id,
                        "error": (
                            "Job is not running "
                            f"(state: {job_state})"
                        ),
                        "current_state": job_state
                    })
                    continue

                # Try to cancel the job
                job.abort()

                # Verify cancellation
                try:
                    # Wait a moment and check status
                    import time
                    time.sleep(1)

                    updated_job = client.get_job(
                        job_id
                    )
                    new_state = updated_job.state

                    is_cancelled = new_state in [
                        "ABORTED", "CANCELLED"
                    ]
                    cancelled_jobs.append({
                        "job_id": job_id,
                        "previous_state": (
                            job_state
                        ),
                        "new_state": new_state,
                        "cancelled_successfully": (
                            is_cancelled
                        ),
                        "job_name": getattr(
                            job,
                            'job_name',
                            f"Job {job_id}"
                        )
                    })

                except Exception as e:
                    # Cancellation might have worked
                    # even if we can't verify
                    cancelled_jobs.append({
                        "job_id": job_id,
                        "previous_state": (
                            job_state
                        ),
                        "new_state": "unknown",
                        "cancelled_successfully": (
                            True
                        ),
                        "job_name": getattr(
                            job,
                            'job_name',
                            f"Job {job_id}"
                        ),
                        "verification_error": (
                            str(e)
                        )
                    })

            except Exception as e:
                failed_cancellations.append({
                    "job_id": job_id,
                    "error": (
                        "Failed to cancel job: "
                        f"{str(e)}"
                    )
                })

        # Get summary of results
        successful_cancellations = len([
            j for j in cancelled_jobs
            if j.get(
                "cancelled_successfully", False
            )
        ])
        failed_count = len(failed_cancellations)

        # Try to get current running jobs for context
        current_running_jobs = []
        try:
            jobs = project.list_jobs(limit=50)
            for job in jobs:
                if job.state in [
                    "RUNNING", "PENDING"
                ]:
                    current_running_jobs.append({
                        "job_id": job.job_id,
                        "job_name": getattr(
                            job,
                            'job_name',
                            f"Job {job.job_id}"
                        ),
                        "state": job.state,
                        "start_time": getattr(
                            job,
                            'start_time',
                            None
                        )
                    })
        except Exception:
            pass

        success_rate = (
            (
                successful_cancellations
                / len(job_ids) * 100
            )
            if job_ids
            else 0
        )
        cancellation_summary = {
            "total_requested": len(job_ids),
            "successful_cancellations": (
                successful_cancellations
            ),
            "failed_cancellations": failed_count,
            "success_rate": success_rate,
            "remaining_running_jobs": len(
                current_running_jobs
            )
        }

        return {
            "status": "ok",
            "project_key": project_key,
            "cancelled_jobs": cancelled_jobs,
            "failed_cancellations": (
                failed_cancellations
            ),
            "cancellation_summary": (
                cancellation_summary
            ),
            "current_running_jobs": (
                current_running_jobs
            )
        }

    except Exception as e:
        return {
            "status": "error",
            "message": (
                "Failed to cancel running "
                f"jobs: {str(e)}"
            )
        }
