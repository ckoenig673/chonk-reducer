from __future__ import annotations

import os
import subprocess
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


def _write_fake_tools(tmp_path: Path) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()

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

    fake_python = bin_dir / "python3"
    fake_python.write_text(
        "#!/bin/sh\n"
        "echo \"$*\" >>\"$PYTHON_CALLS\"\n"
        "exit 0\n",
        encoding="utf-8",
    )
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
) -> tuple[int, str]:
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
            "PATH": f"{bin_dir}:{env['PATH']}",
        }
    )

    run = subprocess.run(["/bin/sh", str(script_path), service], cwd=project, env=env, capture_output=True, text=True)
    task_logs = sorted((project / "logs").glob(f"{service}_*.task.log"))
    assert task_logs, "task log should be created"
    return run.returncode, task_logs[-1].read_text(encoding="utf-8")


def test_task_skips_build_and_pytest_when_repo_unchanged(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"

    rc, log_text = _run_wrapper(project, script_path, "svc-unchanged")

    assert rc == 0
    assert "[git] repository up to date — skipping pull" in log_text
    assert "[test] repository unchanged — skipping pytest" in log_text
    assert "[build] repository up to date and local image exists — skipping container rebuild" in log_text


def test_task_pulls_and_rebuilds_when_updates_detected(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path, with_remote_update=True)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"

    rc, log_text = _run_wrapper(project, script_path, "svc-updated")

    assert rc == 0
    assert "[git] updates detected — pulling latest changes" in log_text
    assert "[test] running pytest..." in log_text
    assert "[build] rebuilding image for service: svc-updated" in log_text


def test_task_builds_when_image_missing_even_without_repo_updates(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"

    rc, log_text = _run_wrapper(project, script_path, "svc-fresh", fake_image_inspect_found="0")

    assert rc == 0
    assert "[git] repository up to date — skipping pull" in log_text
    assert "[build] no local image found for svc-fresh — building container" in log_text
    assert "[build] rebuilding image for service: svc-fresh" in log_text


def test_task_builds_when_image_lookup_fails_even_without_repo_updates(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"

    rc, log_text = _run_wrapper(project, script_path, "svc-image-lookup-fail", fake_image_lookup_fail="1")

    assert rc == 0
    assert "[git] repository up to date — skipping pull" in log_text
    assert "[build] no local image found for svc-image-lookup-fail — building container" in log_text
    assert "[build] rebuilding image for service: svc-image-lookup-fail" in log_text


def test_task_builds_when_image_name_lookup_returns_empty(tmp_path: Path) -> None:
    project = _setup_git_clone(tmp_path)
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "chonkreducer_task.sh"

    rc, log_text = _run_wrapper(project, script_path, "svc-image-name-empty", fake_image_lookup_empty="1")

    assert rc == 0
    assert "[git] repository up to date — skipping pull" in log_text
    assert "[build] no local image found for svc-image-name-empty — building container" in log_text
    assert "[build] rebuilding image for service: svc-image-name-empty" in log_text
