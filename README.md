# Dataiku MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server for managing Dataiku DSS projects from Claude Code or Claude Desktop. Provides tools for recipes, datasets, scenarios, and project exploration.

Swyfft-specific additions:
- `get_dataset_post_write_statements` tool for inspecting hidden ETL logic in post-write SQL
- `get_job_details` fix: uses `project.get_job()` and `get_status()` dict
- Ruff linting cleanup

## Setup

### 1. Clone the repo

```bash
git clone git@github.com:swyfft-insurance/dataiku_factory.git
cd dataiku_factory
```

### 2. Create a virtual environment and install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 3. Configure environment variables

Add your DSS credentials to `~/.swyfft_credentials`:

```bash
export SWYFFT_DSS_HOST=https://your-dss-instance.com:10000
export SWYFFT_DSS_API_KEY=your-api-key-here
```

Add to your `~/.bashrc` (or `~/.zshrc`):

```bash
[ -f ~/.swyfft_credentials ] && source ~/.swyfft_credentials
```

Then reload: `source ~/.bashrc`

The server reads `DSS_HOST` and `DSS_API_KEY` at runtime. The `.mcp.json` config maps `${SWYFFT_DSS_*}` vars to these names (see step 4).

### 4. Add to Claude Code

If you're using the [SwyfftAnalytics-claude-code](https://github.com/swyfft-insurance/SwyfftAnalytics-claude-code) parent repo, run `python3 setup.py` — it generates `.mcp.json` from the template with paths resolved automatically.

If you cloned this repo standalone, add to your project's `.mcp.json` manually:

```json
{
  "mcpServers": {
    "dataiku": {
      "command": "/path/to/dataiku_factory/.venv/bin/python",
      "args": ["/path/to/dataiku_factory/scripts/mcp_server.py"],
      "env": {
        "DSS_HOST": "${SWYFFT_DSS_HOST}",
        "DSS_API_KEY": "${SWYFFT_DSS_API_KEY}"
      }
    }
  }
}
```

Replace `/path/to/dataiku_factory` with the absolute path to your clone.

### 5. Add to Claude Desktop (alternative)

Add to `claude_desktop_config.json`, replacing the path with your clone location:

```json
{
  "mcpServers": {
    "dataiku": {
      "command": "/path/to/dataiku_factory/.venv/bin/python",
      "args": ["/path/to/dataiku_factory/scripts/mcp_server.py"],
      "env": {
        "DSS_HOST": "https://your-dss-instance.com:10000",
        "DSS_API_KEY": "your-api-key-here"
      }
    }
  }
}
```

## Tools

The server exposes 40 tools across 9 categories:

### Recipes
| Tool | Description |
|------|-------------|
| `create_recipe` | Create a new recipe (python, sql, sync, etc.) |
| `update_recipe` | Update recipe settings or code |
| `delete_recipe` | Delete a recipe |
| `run_recipe` | Execute a recipe |
| `get_recipe_info` | Get recipe metadata |
| `list_recipes` | List all recipes in a project |

### Datasets
| Tool | Description |
|------|-------------|
| `create_dataset` | Create a new dataset |
| `update_dataset` | Update dataset settings |
| `delete_dataset` | Delete a dataset |
| `build_dataset` | Build a dataset |
| `inspect_dataset_schema` | Get dataset column schema |
| `check_dataset_metrics` | Get dataset metrics (row count, etc.) |
| `list_datasets` | List all datasets in a project |
| `get_dataset_info` | Get dataset metadata |
| `get_dataset_post_write_statements` | Get pre/post-write SQL statements |
| `clear_dataset` | Clear dataset data |

### Scenarios
| Tool | Description |
|------|-------------|
| `create_scenario` | Create a new scenario |
| `update_scenario` | Update scenario settings |
| `delete_scenario` | Delete a scenario |
| `run_scenario` | Execute a scenario |
| `add_scenario_trigger` | Add a trigger (daily, periodic, dataset-based) |
| `remove_scenario_trigger` | Remove a trigger |
| `get_scenario_info` | Get scenario metadata |
| `list_scenarios` | List all scenarios in a project |
| `get_scenario_run_history` | Get past run results |

### Advanced Scenarios
| Tool | Description |
|------|-------------|
| `get_scenario_logs` | Get detailed run logs and errors |
| `get_scenario_steps` | Get step configuration including code |
| `clone_scenario` | Clone a scenario with modifications |

### Code Development
| Tool | Description |
|------|-------------|
| `get_recipe_code` | Extract Python/SQL code from a recipe |
| `validate_recipe_syntax` | Validate code syntax before running |
| `test_recipe_dry_run` | Test recipe logic without execution |

### Project Exploration
| Tool | Description |
|------|-------------|
| `get_project_flow` | Get complete data flow/pipeline structure |
| `search_project_objects` | Search datasets, recipes, scenarios by pattern |
| `get_dataset_sample` | Get sample rows from a dataset |

### Environment & Configuration
| Tool | Description |
|------|-------------|
| `get_code_environments` | List available Python/R environments |
| `get_project_variables` | Get project-level variables |
| `get_connections` | List available data connections |

### Monitoring & Debugging
| Tool | Description |
|------|-------------|
| `get_recent_runs` | Get recent run history |
| `get_job_details` | Get detailed job execution info |
| `cancel_running_jobs` | Cancel running jobs |

### Productivity
| Tool | Description |
|------|-------------|
| `duplicate_project_structure` | Copy project structure to a new project |
| `export_project_config` | Export project config as JSON/YAML |
| `batch_update_objects` | Bulk-update objects matching a pattern |

## Verify it works

Start a new Claude Code session from the project directory. You should see the `dataiku` server in the status bar. Test with:

```
> list all scenarios in project MYPROJECT
```

## Security notes

- Store credentials in `~/.swyfft_credentials` (never committed) and reference them via `${VAR}` in `.mcp.json`
- Use an API key scoped to the projects you need
- `DSS_INSECURE_TLS=true` disables certificate verification — only use for self-signed certs on internal instances

## Development

```bash
pip install -e .[dev]       # Install with dev dependencies
black dataiku_mcp/ scripts/ # Format
ruff check dataiku_mcp/     # Lint
```

## License

MIT
