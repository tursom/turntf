# Repository Guidelines

## Commit Convention

- Use Conventional Commits: `<type>(<scope>): <summary>`.
- Allowed types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`.
- Use an English imperative summary, starting with a lowercase verb when possible.
- Keep the summary under 72 characters and do not end it with a period.
- Use a scope when it clarifies the affected area, for example `store`, `cluster`, `proto`, `api`, or `docs`.
- For breaking API or protocol changes, add `!` after the type or scope and explain the break in the commit body.

Examples:

```text
feat(store): add message triple identity
feat(proto)!: replace message_id with message identity tuple
docs(readme): document anti-entropy snapshots
```
