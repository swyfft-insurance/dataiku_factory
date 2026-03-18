"""
Managed folder tools for Dataiku MCP integration.

File operations within Dataiku managed folders: list, upload, download, delete.
"""

import base64
import io
from typing import Dict, Any, List, Optional
from dataiku_mcp.client import get_client, get_project, get_project_for_write


def list_managed_folders(
    project_key: str
) -> Dict[str, Any]:
    """
    List all managed folders in a project.

    Args:
        project_key: The project key

    Returns:
        Dict containing list of managed folders with IDs and metadata
    """
    try:
        project = get_project(project_key)
        folders = project.list_managed_folders()

        folder_list = []
        for folder in folders:
            folder_info = {
                "id": folder.get("id"),
                "name": folder.get("name", folder.get("id", "unknown")),
                "type": folder.get("type", "unknown"),
                "description": folder.get("description", ""),
                "tags": folder.get("tags", []),
            }
            folder_list.append(folder_info)

        return {
            "status": "ok",
            "project_key": project_key,
            "folders": folder_list,
            "folder_count": len(folder_list)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to list managed folders: {str(e)}"
        }


def get_managed_folder_contents(
    project_key: str,
    folder_id: str,
    path: str = "/"
) -> Dict[str, Any]:
    """
    List files and subdirectories in a managed folder.

    Args:
        project_key: The project key
        folder_id: ID of the managed folder
        path: Path within the folder (default "/")

    Returns:
        Dict containing file listing with names, sizes, and timestamps
    """
    try:
        project = get_project(project_key)
        folder = project.get_managed_folder(folder_id)
        contents = folder.list_contents()

        items = contents.get("items", [])

        # Filter by path prefix if not root
        if path and path != "/":
            normalized_path = path.rstrip("/") + "/"
            items = [
                item for item in items
                if item.get("path", "").startswith(normalized_path)
            ]

        # Cap at 500 items
        max_items = 500
        truncated = len(items) > max_items
        items = items[:max_items]

        file_list = []
        for item in items:
            file_info = {
                "path": item.get("path"),
                "size": item.get("size"),
                "last_modified": item.get("lastModified"),
            }
            file_list.append(file_info)

        return {
            "status": "ok",
            "project_key": project_key,
            "folder_id": folder_id,
            "path": path,
            "items": file_list,
            "item_count": len(file_list),
            "truncated": truncated
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get folder contents: {str(e)}"
        }


def get_managed_folder_info(
    project_key: str,
    folder_id: str
) -> Dict[str, Any]:
    """
    Get settings and metadata for a managed folder.

    Args:
        project_key: The project key
        folder_id: ID of the managed folder

    Returns:
        Dict containing folder settings: connection, path, type
    """
    try:
        project = get_project(project_key)
        folder = project.get_managed_folder(folder_id)
        settings = folder.get_settings()
        raw = settings.get_raw()

        return {
            "status": "ok",
            "project_key": project_key,
            "folder_id": folder_id,
            "folder_info": {
                "name": raw.get("name", folder_id),
                "type": raw.get("type", "unknown"),
                "connection": raw.get("params", {}).get("connection", "unknown"),
                "path": raw.get("params", {}).get("path", ""),
                "description": raw.get("description", ""),
                "tags": raw.get("tags", []),
                "partitioning": raw.get("partitioning", {}),
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to get folder info: {str(e)}"
        }


def upload_file_to_folder(
    project_key: str,
    folder_id: str,
    path: str,
    content: str,
    is_base64: bool = False
) -> Dict[str, Any]:
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
    try:
        project = get_project_for_write(project_key)
        folder = project.get_managed_folder(folder_id)

        if is_base64:
            file_data = base64.b64decode(content)
        else:
            file_data = content.encode("utf-8")

        folder.put_file(path, io.BytesIO(file_data))

        return {
            "status": "ok",
            "project_key": project_key,
            "folder_id": folder_id,
            "path": path,
            "size_bytes": len(file_data),
            "message": f"File uploaded to '{path}' successfully"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to upload file to '{path}': {str(e)}"
        }


def download_file_from_folder(
    project_key: str,
    folder_id: str,
    path: str,
    max_size_bytes: int = 1048576
) -> Dict[str, Any]:
    """
    Download a file from a managed folder.

    Returns content as text if UTF-8 decodable, otherwise as base64.

    Args:
        project_key: The project key
        folder_id: ID of the managed folder
        path: File path within the folder
        max_size_bytes: Maximum file size to download (default 1MB)

    Returns:
        Dict containing file content (text or base64)
    """
    try:
        project = get_project(project_key)
        folder = project.get_managed_folder(folder_id)

        stream = folder.get_file(path)
        data = stream.read(max_size_bytes + 1)
        stream.close()

        if len(data) > max_size_bytes:
            return {
                "status": "error",
                "message": f"File exceeds max size ({max_size_bytes} bytes). "
                           f"Actual size is at least {len(data)} bytes. "
                           f"Increase max_size_bytes or download via DSS UI.",
                "path": path,
                "size_exceeded": True
            }

        # Try to decode as text
        try:
            text_content = data.decode("utf-8")
            return {
                "status": "ok",
                "project_key": project_key,
                "folder_id": folder_id,
                "path": path,
                "content": text_content,
                "encoding": "utf-8",
                "size_bytes": len(data)
            }
        except UnicodeDecodeError:
            # Return as base64
            return {
                "status": "ok",
                "project_key": project_key,
                "folder_id": folder_id,
                "path": path,
                "content": base64.b64encode(data).decode("ascii"),
                "encoding": "base64",
                "size_bytes": len(data)
            }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to download file '{path}': {str(e)}"
        }


def delete_file_from_folder(
    project_key: str,
    folder_id: str,
    path: str
) -> Dict[str, Any]:
    """
    Delete a file from a managed folder.

    Args:
        project_key: The project key
        folder_id: ID of the managed folder
        path: File path to delete

    Returns:
        Dict containing deletion result
    """
    try:
        project = get_project_for_write(project_key)
        folder = project.get_managed_folder(folder_id)

        folder.delete_file(path)

        return {
            "status": "ok",
            "project_key": project_key,
            "folder_id": folder_id,
            "path": path,
            "message": f"File '{path}' deleted successfully"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to delete file '{path}': {str(e)}"
        }
