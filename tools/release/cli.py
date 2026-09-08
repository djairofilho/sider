"""python -m tools.release.cli --help. Mutations always require --apply."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tomllib
from pathlib import Path

from tools.release.github import GitHubClient, GitHubError
from tools.release.plan import load_plan, release_for_version, render_roadmap
from tools.release.policy import candidate_for, event_context, git, readiness, validate_gate_reports, validate_version_files
from tools.release.prepare import prepare_release
from tools.release.publish import prepare_assets, publish_release, reconcile_published
from tools.release.runner import assemble, build_package, product_gates, write_json
from tools.release.sync import sync_plan

ROOT = Path(__file__).resolve().parents[2]


def preflight(root, plan, client, event):
    merged = event.get("action") == "closed"
    context = event_context(event, plan["repository"], merged=merged)
    if git("rev-parse", "HEAD", cwd=root) != context["sha"]:
        raise ValueError("Checkout não corresponde exatamente ao SHA do evento")
    git("diff", "--quiet", cwd=root)
    git("diff", "--cached", "--quiet", cwd=root)
    release = release_for_version(plan, context["version"])
    validate_version_files(root, context["version"])
    tracking = readiness(plan, release, client)
    candidate = candidate_for(context["version"], context["sha"], client, root)
    published = any(r["tag_name"] == "v" + context["version"] and r["draft"] is False
                    for r in client.paginate(client.repo_path("/releases")))
    return {**context, **tracking, "candidate": candidate, "published": published, "merged": merged}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("validate")
    render = commands.add_parser("render")
    render.add_argument("--write", action="store_true")
    sync = commands.add_parser("sync")
    sync.add_argument("--apply", action="store_true")
    prepare = commands.add_parser("prepare")
    prepare.add_argument("version")
    prepare.add_argument("--notes-file", type=Path, required=True)
    prepare.add_argument("--apply", action="store_true")
    check = commands.add_parser("preflight")
    check.add_argument("--event", type=Path, default=os.environ.get("GITHUB_EVENT_PATH"))
    check.add_argument("--out", type=Path)
    for name in ("package", "gates"):
        command = commands.add_parser(name)
        command.add_argument("version", nargs="?" if name == "package" else None)
        command.add_argument("--target", required=True)
        command.add_argument("--out", type=Path, required=True)
        if name == "package":
            command.add_argument("--bootstrap", action="store_true", help="Simulation only; TCP result remains not_run")
    publish = commands.add_parser("publish")
    publish.add_argument("--event", type=Path, default=os.environ.get("GITHUB_EVENT_PATH"))
    publish.add_argument("--out", type=Path, required=True)
    publish.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    plan = load_plan(ROOT / "releases/plan.json")
    if args.command in {"validate", "render"}:
        expected = render_roadmap(plan)
        path = ROOT / "ROADMAP.md"
        if args.command == "render" and args.write:
            path.write_text(expected, encoding="utf-8", newline="\n")
        elif path.read_text(encoding="utf-8") != expected:
            raise ValueError("ROADMAP.md desatualizado; execute render --write")
        result = {"valid": True, "milestones": len(plan["releases"]),
                  "tasks": sum(len(r["tasks"]) for r in plan["releases"])}
    elif args.command in {"package", "gates"}:
        if args.command == "package":
            version = args.version or tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))["package"]["version"]
            result = build_package(ROOT, args.out, version, args.target, bootstrap=args.bootstrap)
        else:
            result = product_gates(ROOT, args.out, plan, release_for_version(plan, args.version), args.version, args.target)
    else:
        client = GitHubClient(plan["repository"], allow_writes=getattr(args, "apply", False))
        if args.command == "sync":
            result = sync_plan(plan, client, apply=args.apply)
        elif args.command == "prepare":
            result = prepare_release(ROOT, plan, release_for_version(plan, args.version), args.version,
                                     args.notes_file.read_text(encoding="utf-8"), client, apply=args.apply)
        else:
            if not args.event:
                raise ValueError("Arquivo de evento obrigatório")
            event = json.loads(Path(args.event).read_text(encoding="utf-8"))
            context = preflight(ROOT, plan, client, event)
            if args.command == "preflight":
                result = context
                if args.out:
                    write_json(args.out, context)
                if os.environ.get("GITHUB_OUTPUT"):
                    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
                        for key in ("version", "sha", "published", "merged"):
                            value = str(context[key]).lower() if type(context[key]) is bool else context[key]
                            output.write(f"{key}={value}\n")
            else:
                event_context(event, plan["repository"], merged=True)
                release = release_for_version(plan, context["version"])
                notes = validate_version_files(ROOT, context["version"])
                if context["published"]:
                    if not args.apply:
                        result = {"mode": "dry-run", "action": "verify-published-and-reconcile", **context}
                    else:
                        result = reconcile_published(client, context["version"], context["sha"],
                                                     gate_issue=context["gate_issue"], milestone_number=context["milestone_number"],
                                                     required_gates=release["required_gates"], notes=notes)
                else:
                    manifest = assemble(ROOT, args.out, release, context["version"], context["candidate"])
                    validate_gate_reports(release, manifest["gates"], context["sha"])
                    uploads = prepare_assets(manifest, args.out, notes)
                    write_json(args.out / "release-manifest.json", manifest)
                    if args.apply:
                        result = publish_release(client, manifest, args.out, notes,
                                                 gate_issue=context["gate_issue"], milestone_number=context["milestone_number"])
                    else:
                        result = {"mode": "dry-run", "uploads": list(uploads), **context}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, GitHubError, OSError) as error:
        print(f"Erro: {error}", file=sys.stderr)
        sys.exit(1)
