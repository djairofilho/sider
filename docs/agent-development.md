# Agent development runbook

This runbook applies to work in the [operational-base epic](https://github.com/djairofilho/sider/issues/103),
the [AI-memory epic](https://github.com/djairofilho/sider/issues/104), and
versioned tasks in [the release manifest](../releases/plan.json). The issues are
the post-1.0.1 execution queue; the manifest remains the release contract.
An unattended runner needs a separate scheduler or repeated invocation; this
document does not start one.

## At each invocation

1. Read `AGENTS.md`, the selected issue, its native dependencies, and the relevant
   contract documentation. Check `git status` and preserve existing changes.
2. Select one open task whose listed dependencies are complete. Prefer a task
   that unlocks others. If several are independent, select work with disjoint
   files and one integrator for shared contracts. Never infer that a missing
   issue, test, or gate has passed.
3. Route by the policy in [the development fronts](next-development.md). Record
   model, reasoning effort, task ID, baseline SHA, and intended validation in the
   handoff or issue. Use the issue's `agent:sol` or `agent:luna` label as its
   primary route. The issue body defines the precise division of work and takes
   precedence if the label is stale. Model names are routing hints; required
   evidence is unchanged.
4. Make the smallest buildable change that satisfies the task. Run focused tests
   during implementation, then the checks required by `AGENTS.md` and the issue.
   Record command, result, platform, SHA, and any skipped external gate.
5. Review the diff against acceptance criteria. Update documentation and evidence
   with the code that changes the contract. Leave a concise handoff containing
   completed work, remaining blockers, tests, and exact next task.

## Model routing

| Task shape | Route | Typical effort | Required review |
| --- | --- | --- | --- |
| Inventory, link or wording fix, test data, or example within a frozen interface | `gpt-6-luna` | Low or medium | Focused check and contract comparison |
| Multi-module implementation, new API, concurrency, AOF, replication, quota, or security | `gpt-6.1-sol` | Medium or high | Relevant failure tests and integration review |
| Ambiguous product choice or conflict between contracts and evidence | `gpt-6.1-sol` | High | Written decision and updated acceptance criteria before coding |
| Validation of a Sol change with a mechanical, bounded checklist | `gpt-6-luna` | Medium | Sol resolves any substantive finding before integration |

Do not split one atomic storage/persistence change across models merely to keep
both busy. A Luna handoff must identify exact input files, output, invariants,
validation command, and conditions that trigger escalation. Sol owns integration
when multiple workstreams touch shared contracts.

The `agent:sol` label routes epics, contract decisions, durable and distributed
changes, security, and release integration. The `agent:luna` label routes the
bounded client audit (#106), search corpus (#112), and reference integration
(#113). A Luna issue can still require a Sol decision or review stated in its
body. Labels do not make an issue ready or done: check native `blocked_by`
dependencies and acceptance evidence first.

## Select the next issue

Use the GitHub issue state as the live queue. The two epics organize work and
are not implementation tasks. This repository currently uses one orchestrator
to assign independent worktrees and avoid duplicate claims. Respect the
`AGENTS.md` limit of three active workstreams and one integrator for shared
contracts. Coordinate Cargo build and test jobs so only one heavy job uses this
workspace at a time; run benchmarks on an otherwise idle machine.

```powershell
gh issue list --repo djairofilho/sider --state open --label agent:sol --json number,title,labels
gh issue list --repo djairofilho/sider --state open --label agent:luna --json number,title,labels
gh issue view 110 --repo djairofilho/sider --json number,title,body,labels,state
gh api repos/djairofilho/sider/issues/110/dependencies/blocked_by
```

Replace `110` with each candidate issue number. Select it only when every
native blocker is closed, the issue's acceptance cases can be tested in the
available environment, and no other workstream owns its files. If a release
or external-platform gate cannot run, record its missing evidence and continue
another ready issue. Recheck the queue after each integration; a label change
alone cannot close a blocker. A fresh invocation repeats this selection rather
than trusting an earlier snapshot of the backlog.

## Stop and handoff conditions

- Stop dependent implementation when the task is absent from the active issue
  graph or versioned manifest, a dependency is incomplete, or the acceptance
  criteria leave an observable behavior undecided. Continue independent,
  unblocked work if available.
- Stop publication on any missing, skipped, canceled, or failed required gate.
  Do not turn a planned test or an old SHA's receipt into success evidence.
- Preserve the current release policy: do not publish, merge, commit, push, or
  synchronize GitHub unless the active task or user explicitly authorizes it.
  `cargo xtask sync` is a dry run; `--apply` writes externally.
- A changed AOF format, replication contract, security boundary, or release
  asset requires Sol review and the affected platform and migration checks.
- After two unsuccessful correction cycles, report the observed failure, the
  smallest reproduction, and the specific decision needed. Do not repeat the
  same command without a changed hypothesis.

## Issue definition of ready and done

An issue is **ready** when it names its outcome, dependency IDs, affected
contracts, acceptance cases, and expected evidence. Git actions follow the
active issue and `AGENTS.md` authorization rules.
It is **done** when the change meets those cases, required checks pass at the
recorded SHA, documentation matches behavior, and the PR or handoff links the
evidence. A gate closes only under its release or checkpoint rules.
