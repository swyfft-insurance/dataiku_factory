# Dataiku MCP Server — Developer Guide

## Architecture

```
scripts/mcp_server.py          # Entry point (argparse, stdio/sse transport)
  └─> dataiku_mcp/server.py    # FastMCP instance, all @mcp.tool() registrations
      └─> dataiku_mcp/tools/   # Tool implementations by category (14 files)
      └─> dataiku_mcp/client.py # Singleton DSSClient, SafeDSSProject wrapper
```

- **server.py** — thin registration layer. Each `@mcp.tool()` function delegates to a tool implementation. All 76+ tools registered here.
- **tools/*.py** — business logic. Each file covers a domain (recipes, datasets, scenarios, etc.).
- **client.py** — singleton `get_client()`, `SafeDSSProject` safety wrapper, write-tag enforcement.

## Two Categories of Tools

**1:1 API wrappers** — thin pass-throughs to a single `dataikuapi` method:
```
get_instance_info  →  client.get_instance_info()
list_scenarios     →  project.list_scenarios()
```

**Use-case compositions** — orchestrate multiple API calls into one coherent operation:
```
move_to_zone       →  get flow, find item, move it, verify
batch_update_objects → iterate objects, update metadata on each
propagate_schema   →  get flow, find recipe, propagate, report changes
```

New tools may be either type — use judgment about what's useful as a single MCP operation.

## Tool Design Philosophy

**Each tool returns one level of information.** `list_*` returns IDs, names, and metadata. A separate `get_*` returns details for a specific item. Never recurse the whole tree in one response — large projects produce massive payloads that blow context limits. Let the caller decide when to go deeper.

Example: `list_flow_zones` returns zone IDs/names/colors/counts. `get_flow_zone` takes a zone ID and returns that zone's items.

## Adding a New Tool

| Step | File | What to do |
|------|------|------------|
| 1 | `dataiku_mcp/tools/<category>.py` | Implement function (see pattern below) |
| 2 | `dataiku_mcp/client.py` | Add to `_WRITE_METHODS` if it calls project methods that mutate state. Add to `_BLOCKED_METHODS` if forbidden. Skip for read-only or client-level ops. |
| 3 | `dataiku_mcp/server.py` | Register with `@mcp.tool()` — passthrough with type hints and docstring |
| 4 | Update docs (see Documentation Protocol below) |

### Standard Tool Pattern

```python
def my_tool(project_key: str, param: str, optional: int | None = None) -> dict[str, Any]:
    """Docstring — shown to Claude as the tool description."""
    try:
        # Read-only:
        project = get_project(project_key)
        # Mutating:
        project = get_project_for_write(project_key)
        # Instance-level (no project):
        client = get_client()

        # API calls — handle ._data attribute variations
        result = project.some_method()
        raw = result._data if hasattr(result, '_data') else result

        return {"status": "ok", "result": raw}
    except Exception as e:
        return {"status": "error", "message": f"Failed to do thing: {str(e)}"}
```

### Conventions

- Return type: always `dict[str, Any]`
- Success: `{"status": "ok", ...}` — include relevant data
- Failure: `{"status": "error", "message": "..."}` — include context
- Type hints required on all parameters (MCP schema generation)
- `get_project_for_write()` for any mutation — requires `Claude Write` tag
- `get_project()` for read-only project operations
- `get_client()` directly for instance-level operations (admin, connections, users)
- Poll long-running jobs: `time.sleep(2)` loop, max 600 iterations (10 min)
- Mask sensitive data: use `_mask_sensitive()` pattern from `tools/administration.py`

## Tool Naming Conventions

MCP tool names don't mirror Dataiku API method names 1:1. Follow these conventions:

| Convention | Example | Notes |
|-----------|---------|-------|
| `create_` not `new_` | `create_recipe` (API: `project.new_recipe()`) | Consistent verb across all CRUD |
| `get_` for single objects | `get_scenario_info` | Retrieves one thing |
| `list_` for collections | `list_scenarios`, `list_managed_folders` | Returns arrays |
| Scope prefix when ambiguous | `get_project_flow` not `get_flow` | Disambiguates in flat tool list |
| Object + attribute | `get_recipe_code`, `get_dataset_sample` | Retrieves a specific aspect |
| Verb + object for actions | `build_dataset`, `run_scenario`, `clone_scenario` | Non-CRUD operations |
| `_to/from_` + container | `upload_file_to_folder`, `download_file_from_folder` | Nested object operations |
| Scope-qualified for deployer | `list_api_deployer_services`, `get_project_deployment_status` | Avoid collision between API/Project deployers |
| Use-case names for compositions | `move_to_zone`, `batch_update_objects`, `propagate_schema` | Multi-step workflows |

When naming a new tool:
1. Follow the conventions above — don't just mirror the Dataiku API method name
2. Include enough scope to be unambiguous in a flat tool list
3. For use-case tools, name after what the user wants to accomplish

## SafeDSSProject Contract

**File:** `dataiku_mcp/client.py`

- `_WRITE_METHODS` (line 82): set of Dataiku project method names that require the `Claude Write` tag. Update this when adding tools that call new mutating project methods.
- `_BLOCKED_METHODS` (line 110): methods forbidden entirely (currently: `delete`).
- `get_project(key)` — returns `SafeDSSProject` wrapper. Blocks deletes, enforces write tag on write methods.
- `get_project_for_write(key)` — same, but fails immediately if tag is missing (don't wait for first write call).
- Projects created via `create_project()` MCP tool get the `Claude Write` tag automatically.
- All other projects need the tag added manually in DSS UI (Project Settings > Tags).

## API Coverage Map

Living checklist. Update when adding tools.

| Domain | MCP Coverage | Key Gaps |
|--------|-------------|----------|
| Projects & Client | Good | `get_project_metadata`, `set_project_permissions`, `list_projects` (as tool) |
| Datasets | Good | `rename_dataset`, `get_dataset_usages`, `set_dataset_schema` |
| Recipes | Good | `get_recipe_definition`, `get_recipe_status`, `rename_recipe` |
| Scenarios | Good | `abort_scenario`, `get_last_successful_run`, `get_scenario_average_duration` |
| Managed Folders | Good | `create_managed_folder` |
| Deployment | Good | Minor gaps |
| Data Quality | Good | Minor gaps |
| SQL Execution | Good | Complete |
| Administration | Good | User/group management, API key management |
| Connections & Envs | Partial | `create_connection`, `delete_connection`, `test_connection` |
| ML Tasks & Models | None | Create tasks, train, deploy, saved models |
| LLM Mesh & GenAI | None | Completions, embeddings, knowledge banks |
| Wiki & Notebooks | None | Articles, Jupyter notebooks, SQL notebooks |
| Webapps & Dashboards | None | Web apps, dashboards, insights, code studios |
| Plugins & Macros | None | Plugin management, macro execution |
| Governance | None | Separate `GovernClient` auth — different subsystem |
| Streaming | None | Clusters, streaming endpoints |
| Meanings & Feature Store | None | Data dictionary, feature groups |

## Gotchas for MCP Developers

See also: `../../skills/dataiku/references/gotchas.md` for API-level pitfalls.

MCP-specific:
- `server.py` tool function parameter names and types must exactly match the implementation in `tools/*.py`
- Deeply nested return dicts should be flattened for readability in MCP output
- Large responses (full flow graphs, large dataset samples) may hit context limits — consider summarization or truncation
- The `._data` attribute on Dataiku API objects contains the raw dict — check `hasattr(result, '_data')` before accessing

## Documentation Maintenance Protocol

When adding or modifying tools, keep both repos in sync:

**When a new MCP tool is added:**

| Step | File | What |
|------|------|------|
| 1 | This file (`CLAUDE.md`) | Mark API method as covered in the coverage map above |
| 2 | `../../skills/dataiku/SKILL.md` | Update capability map — flip "No — use API" to "Yes", add tool to workflow sections |
| 3 | `../../skills/dataiku/references/gotchas.md` | Add any new pitfalls discovered during implementation |
| 4 | `../../skills/dataiku/references/api-*.md` | Add/correct API details if reference was incomplete |

**When a gotcha is discovered (even without adding a tool):**
- Add to `gotchas.md` immediately
- If it affects an existing tool, note it in the tool's docstring

**Commit protocol (two repos):**
```bash
# 1. Commit in this submodule
git add -A && git commit -m "Add [tool_name] tool" && git push

# 2. Commit in parent repo (submodule pointer + skill docs)
cd ../..
git add mcp-servers/dataiku_factory skills/dataiku/
git commit -m "Update dataiku: add [tool_name], update skill docs" && git push
```

## Project Structure

```
dataiku_mcp/
├── __init__.py                  # Version
├── client.py                    # Singleton client, SafeDSSProject, write-tag enforcement
├── server.py                    # FastMCP instance, all @mcp.tool() registrations
└── tools/
    ├── administration.py        # Instance info, settings, logs, audit
    ├── advanced_scenarios.py    # Logs, steps, clone
    ├── code_development.py      # Code extraction, validation, dry run
    ├── data_quality.py          # Rules, status, creation
    ├── datasets.py              # CRUD + schema + metrics + post-write
    ├── deployment.py            # API Deployer + Project Deployer
    ├── environment_config.py    # Connections, variables, environments
    ├── managed_folders.py       # List, upload, download, delete files
    ├── monitoring_debug.py      # Runs, job details, cancellation
    ├── productivity.py          # Duplicate, export, batch update, set vars
    ├── project_exploration.py   # Flow, search, sampling
    ├── recipes.py               # CRUD + run + schema updates
    ├── scenarios.py             # CRUD + triggers + metadata
    └── sql_execution.py         # Query execution, connection listing
scripts/
└── mcp_server.py                # Entry point (argparse, stdio/sse transport)
```
