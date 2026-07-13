# Upstream Contribution Workflow

`graphql-orm-ai` consumes reusable contracts from `agql-auth` and
`graphql-orm`, but its agent does not mutate either sibling repository. This
keeps concurrent agents from overwriting work, committing another agent's
uncommitted files, or merging a dependency revision that downstream crates no
longer resolve.

## Ownership model

Assign one owning agent to each repository. The owner alone may edit that
repository, change its branch, commit, push, update its PR, or merge it. When
multiple tasks target one repository, serialize them onto the owner's branch
or use separate worktrees and PRs with an explicit integration owner.

Downstream agents may inspect upstream source and GitHub state read-only. They
express required changes as copy-ready prompts in the ignored `.handoffs/`
directory and wait for the upstream owner to return a reviewed final commit
SHA.

## Dependency sequence

For the current stack, merge and repin from the bottom upward:

1. The `agql-auth` owner completes its versioned PR, documentation, migration
   guidance, Rustdoc, tests, and merge.
2. The owner reports the final `main` or release-tag commit SHA and merge
   strategy.
3. The `graphql-orm` owner updates its exact `agql-auth` revision, lockfile,
   README, changelog, migration guide, and compatibility checks, then merges.
4. The owner reports the final `graphql-orm` commit SHA.
5. The `graphql-orm-ai` owner repins both dependencies, regenerates the
   lockfile, verifies one Cargo type/source universe, and runs its full release
   matrix before merging.

Do not merge downstream first and hope a moving branch remains compatible.
Exact full Git revisions are intentional review boundaries.

## Merge strategy and exact revisions

If a downstream manifest pins a commit from an open upstream PR:

- a squash or rebase merge creates a different commit, so the downstream pin
  must be replaced;
- a merge commit normally retains the PR commits as ancestors, but the
  downstream crate should still pin the reviewed merge or release-tag commit;
  and
- the upstream owner must report the final SHA instead of asking downstream
  agents to infer it from a branch name.

Tags are useful release names, but Git dependencies should keep a reviewed
full commit revision while these crates remain Git-only.

## Shared-machine safety

- Never run write commands in a sibling worktree owned by another agent.
- Never stage all files in a dirty worktree without proving ownership of every
  change.
- Never resolve conflicts by discarding, stashing, or resetting another
  agent's files.
- Prefer a separate `git worktree` and branch for every concurrent task in the
  same repository.
- Record the owning agent and branch in the PR or issue before parallel work
  starts.

## Handoff prompt contents

Every upstream prompt should state:

- the exact repository, base branch, existing PR/branch, and owning boundary;
- the reusable problem and required public contract, without consumer-domain
  entities or policies;
- compatibility and security invariants;
- expected version, README, changelog, migration, and Rustdoc updates;
- required tests, backend compile checks, and database isolation;
- whether the owner should merge; and
- the final information downstream needs: merge strategy, version, final SHA,
  and any migration or feature changes.

The downstream agent resumes only after receiving that final handoff.
