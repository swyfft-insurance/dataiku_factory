"""
Deployment tools for Dataiku MCP integration.

Read-only access to API Deployer and Project Deployer status.
"""

from typing import Dict, Any, List, Optional
from dataiku_mcp.client import get_client


def _get_api_deployer():
    """Get API deployer, with clear error if not available."""
    client = get_client()
    try:
        return client.get_api_deployer()
    except Exception as e:
        raise RuntimeError(
            f"API Deployer not available on this DSS instance. "
            f"This feature requires a Deployer node. Error: {str(e)}"
        )


def _get_project_deployer():
    """Get Project deployer, with clear error if not available."""
    client = get_client()
    try:
        return client.get_project_deployer()
    except Exception as e:
        raise RuntimeError(
            f"Project Deployer not available on this DSS instance. "
            f"This feature requires a Deployer node. Error: {str(e)}"
        )


def list_api_deployer_services() -> Dict[str, Any]:
    """
    List all services in the API Deployer.

    Returns:
        Dict containing list of API services
    """
    try:
        deployer = _get_api_deployer()
        services = deployer.list_services()

        service_list = []
        for svc in services:
            svc_data = svc._data if hasattr(svc, '_data') else svc
            if isinstance(svc_data, dict):
                service_list.append({
                    "id": svc_data.get("id"),
                    "creation_tag": svc_data.get("creationTag", {}),
                })
            else:
                service_list.append({
                    "id": getattr(svc, 'id', str(svc)),
                })

        return {
            "status": "ok",
            "services": service_list,
            "service_count": len(service_list)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list API deployer services: {str(e)}"
        }


def list_api_deployer_deployments(
    service_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    List deployments in the API Deployer.

    Args:
        service_id: Optional filter by service ID

    Returns:
        Dict containing list of API deployments
    """
    try:
        deployer = _get_api_deployer()
        deployments = deployer.list_deployments()

        dep_list = []
        for dep in deployments:
            dep_data = dep._data if hasattr(dep, '_data') else dep
            if isinstance(dep_data, dict):
                dep_info = {
                    "id": dep_data.get("id"),
                    "service_id": dep_data.get("serviceId"),
                    "infra_id": dep_data.get("infraId"),
                    "version": dep_data.get("publishedServiceVersion"),
                    "enabled": dep_data.get("enabled", False),
                }
            else:
                dep_info = {
                    "id": getattr(dep, 'id', str(dep)),
                }

            # Filter by service if specified
            if service_id and dep_info.get("service_id") != service_id:
                continue

            dep_list.append(dep_info)

        return {
            "status": "ok",
            "deployments": dep_list,
            "deployment_count": len(dep_list),
            "filter_service_id": service_id
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list API deployer deployments: {str(e)}"
        }


def list_api_deployer_infras() -> Dict[str, Any]:
    """
    List infrastructures in the API Deployer.

    Returns:
        Dict containing list of API deployer infrastructures
    """
    try:
        deployer = _get_api_deployer()
        infras = deployer.list_infras()

        infra_list = []
        for infra in infras:
            infra_data = infra._data if hasattr(infra, '_data') else infra
            if isinstance(infra_data, dict):
                infra_list.append({
                    "id": infra_data.get("id"),
                    "stage": infra_data.get("stage"),
                    "type": infra_data.get("type"),
                })
            else:
                infra_list.append({
                    "id": getattr(infra, 'id', str(infra)),
                })

        return {
            "status": "ok",
            "infras": infra_list,
            "infra_count": len(infra_list)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list API deployer infras: {str(e)}"
        }


def get_api_deployment_status(
    deployment_id: str
) -> Dict[str, Any]:
    """
    Get status of a specific API deployment.

    Args:
        deployment_id: ID of the deployment

    Returns:
        Dict containing deployment status
    """
    try:
        deployer = _get_api_deployer()
        deployment = deployer.get_deployment(deployment_id)

        # Get light status
        try:
            status = deployment.get_status()
            status_data = status._data if hasattr(status, '_data') else status
            if not isinstance(status_data, dict):
                status_data = {"raw": str(status_data)}
        except Exception:
            status_data = {"error": "Could not retrieve status"}

        # Get settings
        try:
            settings = deployment.get_settings()
            settings_data = settings.get_raw() if hasattr(settings, 'get_raw') else {}
        except Exception:
            settings_data = {}

        return {
            "status": "ok",
            "deployment_id": deployment_id,
            "deployment_status": status_data,
            "settings": {
                "service_id": settings_data.get("serviceId"),
                "infra_id": settings_data.get("infraId"),
                "version": settings_data.get("publishedServiceVersion"),
                "enabled": settings_data.get("enabled"),
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get API deployment status: {str(e)}"
        }


def list_project_deployer_projects() -> Dict[str, Any]:
    """
    List all projects in the Project Deployer.

    Returns:
        Dict containing list of deployer projects
    """
    try:
        deployer = _get_project_deployer()
        projects = deployer.list_projects()

        project_list = []
        for proj in projects:
            proj_data = proj._data if hasattr(proj, '_data') else proj
            if isinstance(proj_data, dict):
                project_list.append({
                    "project_key": proj_data.get("projectKey"),
                    "name": proj_data.get("name"),
                    "packages_count": proj_data.get("packagesCount"),
                })
            else:
                project_list.append({
                    "project_key": getattr(proj, 'project_key', str(proj)),
                })

        return {
            "status": "ok",
            "projects": project_list,
            "project_count": len(project_list)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list project deployer projects: {str(e)}"
        }


def list_project_deployer_deployments(
    published_project_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    List deployments in the Project Deployer.

    Args:
        published_project_key: Optional filter by project key

    Returns:
        Dict containing list of project deployments
    """
    try:
        deployer = _get_project_deployer()
        deployments = deployer.list_deployments()

        dep_list = []
        for dep in deployments:
            dep_data = dep._data if hasattr(dep, '_data') else dep
            if isinstance(dep_data, dict):
                dep_info = {
                    "id": dep_data.get("id"),
                    "project_key": dep_data.get("projectKey"),
                    "infra_id": dep_data.get("infraId"),
                    "bundle_id": dep_data.get("bundleId"),
                }
            else:
                dep_info = {
                    "id": getattr(dep, 'id', str(dep)),
                }

            if published_project_key and dep_info.get("project_key") != published_project_key:
                continue

            dep_list.append(dep_info)

        return {
            "status": "ok",
            "deployments": dep_list,
            "deployment_count": len(dep_list),
            "filter_project_key": published_project_key
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list project deployer deployments: {str(e)}"
        }


def list_project_deployer_infras() -> Dict[str, Any]:
    """
    List infrastructures in the Project Deployer.

    Returns:
        Dict containing list of project deployer infrastructures
    """
    try:
        deployer = _get_project_deployer()
        infras = deployer.list_infras()

        infra_list = []
        for infra in infras:
            infra_data = infra._data if hasattr(infra, '_data') else infra
            if isinstance(infra_data, dict):
                infra_list.append({
                    "id": infra_data.get("id"),
                    "stage": infra_data.get("stage"),
                })
            else:
                infra_list.append({
                    "id": getattr(infra, 'id', str(infra)),
                })

        return {
            "status": "ok",
            "infras": infra_list,
            "infra_count": len(infra_list)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list project deployer infras: {str(e)}"
        }


def get_project_deployment_status(
    deployment_id: str
) -> Dict[str, Any]:
    """
    Get status of a specific project deployment.

    Args:
        deployment_id: ID of the deployment

    Returns:
        Dict containing deployment status
    """
    try:
        deployer = _get_project_deployer()
        deployment = deployer.get_deployment(deployment_id)

        # Get light status
        try:
            status = deployment.get_status()
            status_data = status._data if hasattr(status, '_data') else status
            if not isinstance(status_data, dict):
                status_data = {"raw": str(status_data)}
        except Exception:
            status_data = {"error": "Could not retrieve status"}

        # Get settings
        try:
            settings = deployment.get_settings()
            settings_data = settings.get_raw() if hasattr(settings, 'get_raw') else {}
        except Exception:
            settings_data = {}

        return {
            "status": "ok",
            "deployment_id": deployment_id,
            "deployment_status": status_data,
            "settings": {
                "project_key": settings_data.get("projectKey"),
                "infra_id": settings_data.get("infraId"),
                "bundle_id": settings_data.get("bundleId"),
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get project deployment status: {str(e)}"
        }
