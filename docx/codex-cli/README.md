# Codex CLI

Notes for using REFS from Codex CLI.

## Recommended Path

Use the GlassVein Codex package to install the Codex plugin and generate the Codex MCP configuration. The package is an npm CLI package, not a native binary. It installs the `gv-for-codex` command and the bundled Codex plugin files.

Current local registry package:

```bash
npm install -g @opensessiongateway/gv-for-codex --registry http://host.docker.internal:4873/
```

The published package version currently used in this workspace is `@opensessiongateway/gv-for-codex@0.1.0`.

## Prepare REFS Backend

`gv-for-codex` does not build or start REFS by itself. The actual REFS MCP backend is still `rec_mcp_memory_server`.

Preferred Linux x86_64 install:

```bash
mkdir -p ~/.local/share/refs
cd ~/.local/share/refs
curl -L -o refs-linux-x86_64-musl-static.tar.gz \
  https://github.com/Yeamika/RemoteExecutorForSession/releases/download/v0.1.17/refs-linux-x86_64-musl-static.tar.gz
tar -xzf refs-linux-x86_64-musl-static.tar.gz
```

Use the matching REFS release asset on other platforms.

## Register REFS For Codex CLI

After installing the npm package and preparing the REFS backend, register the backend through the GV-managed MCP registry:

```bash
gv-for-codex mcp add \
  --name refs \
  --command ~/.local/share/refs/refs-linux-x86_64-musl-static/rec_mcp_memory_server \
  --activate
```

Useful checks:

```bash
gv-for-codex mcp list
gv-for-codex mcp configure --servers refs
```

This writes the GV Codex MCP registry under `$CODEX_HOME/gv-for-codex/plugins/gv-for-codex/gv-mcp.registry.json` and updates Codex CLI MCP configuration through the bundled configure script.

The default injected field is `ExecutorSessionID`, which REFS tools use for `read`, `FileAction`, `rg`, `exbash`, and executor management.

## Installer Notes

Useful environment variables:

- `CODEX_HOME`: target Codex home, defaulting to `~/.codex`.
- `GV_FOR_CODEX_INSTALL_ROOT`: target directory for the installed GV marketplace files.
- `GV_FOR_CODEX_SKIP_POSTINSTALL=1`: install the npm package only; run `gv-for-codex install` later.

If Codex CLI is already available, npm `postinstall` attempts to add the local marketplace and install `gv-for-codex@gv-for-codex`. If that step was skipped or Codex was installed later, run:

```bash
gv-for-codex install
```

## Static Direct MCP Fallback

Codex CLI can also point directly at `rec_mcp_memory_server` with a normal `mcp_servers.refs` entry. That is useful for quick backend smoke tests, but it bypasses the GV Codex hub and does not provide the GV session ownership injection.

Prefer the `gv-for-codex mcp add ... --activate` flow for normal Codex CLI usage so REFS tool calls receive the expected session context.
