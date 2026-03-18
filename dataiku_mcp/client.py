"""
Dataiku DSS client wrapper for MCP integration.
"""

import os

import dataikuapi
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

_CLIENT_INSTANCE: dataikuapi.DSSClient | None = None


def get_client() -> dataikuapi.DSSClient:
    """
    Get a configured Dataiku DSS client instance.

    Returns:
        dataikuapi.DSSClient: Configured DSS client

    Raises:
        ValueError: If required environment variables are missing
        ConnectionError: If connection to DSS fails
    """
    global _CLIENT_INSTANCE

    if _CLIENT_INSTANCE is None:
        _CLIENT_INSTANCE = _create_client()

    return _CLIENT_INSTANCE


def _create_client() -> dataikuapi.DSSClient:
    """
    Create a new DSS client instance.

    Returns:
        dataikuapi.DSSClient: New DSS client instance
    """
    # Get configuration from environment
    dss_host = os.environ.get("DSS_HOST")
    dss_api_key = os.environ.get("DSS_API_KEY")
    insecure_tls = os.environ.get("DSS_INSECURE_TLS", "true").lower() == "true"

    if not dss_host:
        raise ValueError("DSS_HOST environment variable is required")

    if not dss_api_key:
        raise ValueError("DSS_API_KEY environment variable is required")

    try:
        client = dataikuapi.DSSClient(
            dss_host,
            dss_api_key,
            insecure_tls=insecure_tls
        )

        # Test connection
        client.get_instance_info()

        return client

    except Exception as e:
        raise ConnectionError(
            f"Failed to connect to DSS at {dss_host}: {e}"
        ) from e


def reset_client():
    """
    Reset the client instance (useful for testing).
    """
    global _CLIENT_INSTANCE
    _CLIENT_INSTANCE = None


CLAUDE_WRITE_TAG = "claude write"

# Methods that modify project content (not just read)
_WRITE_METHODS = {
    "new_recipe", "create_recipe", "create_dataset", "new_managed_dataset",
    "create_upload_dataset", "create_filesystem_dataset", "create_s3_dataset",
    "create_sql_table_dataset", "create_managed_folder", "create_scenario",
    "create_streaming_endpoint", "set_metadata", "set_permissions",
    "set_variables", "start_job", "new_job",
}


def _has_claude_write_tag(project: dataikuapi.dss.project.DSSProject) -> bool:
    """Check if project has the 'Claude Write' tag (case-insensitive)."""
    try:
        metadata = project.get_metadata()
        tags = metadata.get("tags", [])
        return any(t.lower() == CLAUDE_WRITE_TAG for t in tags)
    except Exception:
        return False


class SafeDSSProject:
    """
    Wrapper around DSSProject that enforces safety guards:

    1. Blocks project deletion entirely (must use DSS UI).
    2. Blocks write operations unless the project has a 'Claude Write' tag
       (case-insensitive). Read operations always pass through.
    """

    _BLOCKED_METHODS = {"delete"}

    def __init__(self, project: dataikuapi.dss.project.DSSProject):
        self._project = project
        self._write_checked = None  # Cache: None = not checked, True/False = result

    def _check_write_access(self, method_name: str):
        """Raise if project lacks 'Claude Write' tag."""
        if self._write_checked is None:
            self._write_checked = _has_claude_write_tag(self._project)
        if not self._write_checked:
            raise PermissionError(
                f"Write operation '{method_name}' is blocked. "
                f"Project '{self._project.project_key}' does not have a 'Claude Write' tag. "
                f"Add this tag in the DSS UI (Project Settings > Tags) to allow modifications, "
                f"or create a new project via MCP (which adds the tag automatically)."
            )

    def __getattr__(self, name: str):
        if name in self._BLOCKED_METHODS:
            raise PermissionError(
                f"Operation '{name}' is blocked by the MCP server safety guard. "
                f"Project deletion must be done through the DSS UI."
            )
        if name in _WRITE_METHODS:
            self._check_write_access(name)
        return getattr(self._project, name)


def get_project(project_key: str) -> SafeDSSProject:
    """
    Get a DSS project by key, wrapped with safety guards.

    The returned object blocks project.delete() and write operations
    on projects without a 'Claude Write' tag.

    Args:
        project_key: The project key

    Returns:
        SafeDSSProject: Safety-wrapped project instance
    """
    client = get_client()
    return SafeDSSProject(client.get_project(project_key))


def get_project_for_write(project_key: str) -> SafeDSSProject:
    """
    Get a DSS project, verifying it has the 'Claude Write' tag.

    Use this instead of get_project() when the tool will modify
    the project (create/update/delete objects, run jobs, etc.).

    Args:
        project_key: The project key

    Returns:
        SafeDSSProject: Safety-wrapped project instance

    Raises:
        PermissionError: If project lacks 'Claude Write' tag
    """
    client = get_client()
    project = client.get_project(project_key)
    if not _has_claude_write_tag(project):
        raise PermissionError(
            f"Project '{project_key}' is read-only (no 'Claude Write' tag). "
            f"Add a 'Claude Write' tag in DSS UI to allow modifications."
        )
    return SafeDSSProject(project)


def list_projects() -> list[str]:
    """
    List all accessible project keys.

    Returns:
        list[str]: List of project keys
    """
    client = get_client()
    return client.list_project_keys()


def get_dss_version() -> str:
    """
    Get the DSS version.

    Returns:
        str: DSS version string
    """
    client = get_client()
    return client.get_instance_info()._data.get("dssVersion", "Unknown")
