# Repository Guidelines

## Proto

- After editing `proto/`, run `./scripts/gen-proto.sh`.
- Commit regenerated `internal/proto/*.pb.go` files with the proto changes.

## Commits

- Use Conventional Commits: `<type>(<scope>): <summary>`.
- Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`.
- Keep summaries imperative, lowercase when possible, under 72 chars, with no trailing period.
- Mark breaking API/protocol changes with `!` and explain them in the body.
- Example: `feat(proto): add message identity tuple`.
