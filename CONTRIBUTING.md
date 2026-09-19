# Contributing

Contributions are welcome. Keep changes focused, preserve the documented
read-only and write-safety boundaries, and do not use real Fabric resources in
tests unless a maintainer has explicitly authorized an isolated fixture.

## Development

Go changes:

```sh
go test ./...
go vet ./...
go build ./cmd/...
```

`fabric-jupyter` changes:

```sh
python -m pip install -e "./python[dev]" build
cd python
ruff check .
mypy src
pytest
python -m build
```

Tests must not contain credentials, tenant or workspace identifiers, personal
paths, captured service responses, or generated environments and packages.
See the [filesystem guide](docs/FILESYSTEM.md) and
[`fabric-jupyter` guide](docs/JUPYTER.md) for architecture and compatibility
details.

Before opening a change, run the smallest relevant tests and verify that no
virtualenv, package archive, cache, log, recovery file, or live-test evidence
is tracked.
