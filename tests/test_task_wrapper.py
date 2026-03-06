from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path


def _git(cmd, cwd: Path) -> None:
    subprocess.run(["git", *cmd], cwd=cwd, check=True, capture_output=True, text=True)


def _setup_git_clone(tmp_path: Path, with_remote_update: bool = False) -> Path:
    remote = tmp_path / "remote.git"
    seed = tmp_path / "seed"
    project = tmp_path / "project"

    _git(["init", "--bare", str(remote)], tmp_path)

    seed.mkdir()
    _git(["init"], seed)
    _git(["config", "user.email", "chonk@example.com"], seed)
    _git(["config", "user.name", "Chonk Test"], seed)
    (seed / "README.md").write_text("seed\n", encoding="utf-8")
    _git(["add", "README.md"], seed)
    _git(["commit", "-m", "seed"], seed)
    _git(["branch", "-M", "main"], seed)
    _git(["remote", "add", "origin", str(remote)], seed)
    _git(["push", "-u", "origin", "main"], seed)
    _git(["symbolic-ref", "HEAD", "refs/heads/main"], remote)

    _git(["clone", str(remote), str(project)], tmp_path)
    _git(["config", "user.email", "chonk@example.com"], project)
    _git(["config", "user.name", "Chonk Test"], project)

    if with_remote_update:
        (seed / "README.md").write_text("seed\nupdate\n", encoding="utf-8")
        _git(["add", "README.md"], seed)
        _git(["commit", "-m", "update"], seed)
        _git(["push", "origin", "main"], seed)

    return project


def _write_fake_tools(project: Path) -> tuple[Path, Path]:
    # Synology NAS can mount /tmp with noexec, so place shims in a repo-local
    # directory that remains executable across NAS + GitHub Actions.
    tools_root = Path(__file__).resolve().parents[1] / ".pytest-exec-tools"
    tools_root.mkdir(exist_ok=True)
    bin_dir = Path(tempfile.mkdtemp(prefix=f"{project.name}-", dir=tools_root))

    fake_docker = bin_dir / "docker"
    fake_docker.write_text(
        "#!/bin/sh\n"
        "echo \"$*\" >>\"$DOCKER_CALLS\"\n"
        "if [ \"${FAKE_IMAGE_LOOKUP_FAIL:-0}\" = \"1\" ] && echo \"$*\" | grep -q \"config --images\"; then\n"
        "  exit 1\n"
        "fi\n"
        "if [ \"${FAKE_IMAGE_LOOKUP_EMPTY:-0}\" = \"1\" ] && echo \"$*\" | grep -q \"config --images\"; then\n"
        "  exit 0\n"
        "fi\n"
        "case \"$*\" in\n"
        "  *\"config --images\"*)\n"
        "    echo \"${FAKE_SERVICE_IMAGE_NAME:-}\"\n"
        "    ;;\n"
        "  *\"image inspect\"*)\n"
        "    [ \"${FAKE_IMAGE_INSPECT_FOUND:-1}\" = \"1\" ] && exit 0\n"
        "    exit 1\n"
        "    ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)

    fake_python_script = (
        "#!/bin/sh\n"
        "echo \"$*\" >>\"$PYTHON_CALLS\"\n"
        "if [ \"${FAKE_PYTEST_FAIL:-0}\" = \"1\" ]; then\n"
        "  exit 1\n"
        "fi\n"
        "exit 0\n"
    )
    # Provide both python and python3 shims so tests keep capturing pytest
    # invocations regardless of which command name the wrapper uses.
    for name in ("python", "python3"):
        fake_python = bin_dir / name
        fake_python.write_text(fake_python_script, encoding="utf-8")
        fake_python.chmod(0o755)

    return fake_docker, bin_dir


def _run_wrapper(
    project: Path,
    script_path: Path,
    service: str,
    *,
    fake_service_image_name: str = "nas-transcoder-test-service",
    fake_image_inspect_found: str = "1",
    fake_image_lookup_fail: str = "0",
    fake_image_lookup_empty: str = "0",
    fake_pytest_fail: str = "0",
) -> tuple[int, str, str, str, Path]:
    docker_calls = project / "docker_calls.log"
    python_calls = project / "python_calls.log"
    compose = project / "compose.yaml"
    compose.write_text("services: {}\n", encoding="utf-8")

    fake_docker, bin_dir = _write_fake_tools(project)

    env = os.environ.copy()
    env.update(
        {
            "PROJ_DIR": str(project),
            "COMPOSE": str(compose),
            "DOCKER": str(fake_docker),
            "DOCKER_CALLS": str(docker_calls),
            "PYTHON_CALLS": str(python_calls),
            "RUN_PYTEST": "true",
            "REBUILD_IMAGE": "true",
            "REBUILD_NO_CACHE": "false",
            "FAKE_SERVICE_IMAGE_NAME": fake_service_image_name,
            "FAKE_IMAGE_INSPECT_FOUND": fake_image_inspect_found,
            "FAKE_IMAGE_LOOKUP_FAIL": fake_image_lookup_fail,
            "FAKE_IMAGE_LOOKUP_EMPTY": fake_image_lookup_empty,
            "FAKE_PYTEST_FAIL": fake_pytest_fail,
            "PATH": f"{bin_dir}:{env['PATH']}",
        }
    )

    try:
        run = subprocess.run(["/bin/sh", str(script_path), service], cwd=project, env=env, capture_output=True, text=True)
        task_logs = sorted((project / "logs").glob(f"{service}_*.task.log"))
        assert task_logs, "task log should be created"
        docker_text = docker_calls.read_text(encoding="utf-8") if docker_calls.exists() else ""
        python_text = python_calls.read_text(encoding="utf-8") if python_calls.exists() else ""
        return run.returncode, task_logs[-1].read_text(encoding="utf-8"), docker_text, python_text, project / ".task_state"
    finally:
        shutil.rmtree(bin_dir, ignore_errors=True)


def test_repo_unchanged_and_commit_previously_validated_skips_pytest(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"
    head_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=project, check=True, capture_output=True, text=True).stdout.strip()
    state_dir = project / ".task_state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "svc-unchanged.last_tested_sha").write_text(f"{head_sha}\n", encoding="utf-8")

    rc, log_text, docker_calls, python_calls, _ = _run_wrapper(project, script_path, "svc-unchanged")

    assert rc == 0
    assert "[git] repository up to date — skipping pull" in log_text
    assert "[test] current commit already validated — skipping pytest" in log_text
    assert python_calls == ""
    assert "[build] repository up to date and local image exists — skipping container rebuild" in log_text
    assert "compose -f" in docker_calls


def test_repo_changed_pytest_passes_records_commit_and_builds(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path, with_remote_update=True)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"

    rc, log_text, _, _, state_root = _run_wrapper(project, script_path, "svc-updated")
    head_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=project, check=True, capture_output=True, text=True).stdout.strip()

    assert rc == 0
    assert "[git] updates detected — pulling latest changes" in log_text
    assert "[test] current commit not yet validated — running pytest" in log_text
    assert f"[test] pytest passed for commit {head_sha[:7]}" in log_text
    assert "[build] rebuilding image for service: svc-updated" in log_text
    assert (state_root / "svc-updated.last_tested_sha").read_text(encoding="utf-8").strip() == head_sha


def test_repo_changed_pytest_fails_aborts_and_does_not_record_or_build(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path, with_remote_update=True)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"

    rc, log_text, docker_calls, python_calls, state_root = _run_wrapper(
        project,
        script_path,
        "svc-fail",
        fake_pytest_fail="1",
    )
    head_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=project, check=True, capture_output=True, text=True).stdout.strip()

    assert rc != 0
    assert "[test] current commit not yet validated — running pytest" in log_text
    assert f"[test] pytest failed for commit {head_sha[:7]} — aborting" in log_text
    assert python_calls == "" or "pytest" in python_calls
    assert "compose -f" not in docker_calls or " build " not in docker_calls
    assert " run --rm " not in docker_calls
    assert not (state_root / "svc-fail.last_tested_sha").exists()


def test_repo_unchanged_commit_not_validated_runs_pytest(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"

    rc, log_text, _, python_calls, state_root = _run_wrapper(project, script_path, "svc-unvalidated")
    head_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=project, check=True, capture_output=True, text=True).stdout.strip()

    assert rc == 0
    assert "[test] current commit not yet validated — running pytest" in log_text
    assert python_calls == "" or "pytest" in python_calls
    assert (state_root / "svc-unvalidated.last_tested_sha").read_text(encoding="utf-8").strip() == head_sha


def test_repo_unchanged_image_missing_commit_validated_builds_without_pytest(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"
    head_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=project, check=True, capture_output=True, text=True).stdout.strip()
    state_dir = project / ".task_state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "svc-fresh.last_tested_sha").write_text(f"{head_sha}\n", encoding="utf-8")

    rc, log_text, _, python_calls, _ = _run_wrapper(project, script_path, "svc-fresh", fake_image_inspect_found="0")

    assert rc == 0
    assert "[git] repository up to date — skipping pull" in log_text
    assert "[test] current commit already validated — skipping pytest" in log_text
    assert python_calls == ""
    assert "[build] no local image found for svc-fresh — building container" in log_text
    assert "[build] rebuilding image for service: svc-fresh" in log_text


def test_repo_unchanged_image_missing_commit_not_validated_runs_pytest_then_build(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"

    rc, log_text, _, python_calls, state_root = _run_wrapper(project, script_path, "svc-image-lookup-fail", fake_image_lookup_fail="1")
    head_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=project, check=True, capture_output=True, text=True).stdout.strip()

    assert rc == 0
    assert "[git] repository up to date — skipping pull" in log_text
    assert "[test] current commit not yet validated — running pytest" in log_text
    assert python_calls == "" or "pytest" in python_calls
    assert "[build] no local image found for svc-image-lookup-fail — building container" in log_text
    assert "[build] rebuilding image for service: svc-image-lookup-fail" in log_text
    assert (state_root / "svc-image-lookup-fail.last_tested_sha").read_text(encoding="utf-8").strip() == head_sha


def test_task_builds_when_image_name_lookup_returns_empty(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"

    rc, log_text, _, _, _ = _run_wrapper(project, script_path, "svc-image-name-empty", fake_image_lookup_empty="1")

    assert rc == 0
    assert "[git] repository up to date — skipping pull" in log_text
    assert "[build] no local image found for svc-image-name-empty — building container" in log_text
    assert "[build] rebuilding image for service: svc-image-name-empty" in log_text
