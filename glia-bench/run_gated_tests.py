#!/usr/bin/env python3
"""Run the curated Ray Data test suite against the artifact's Ray.

Two modes:

  --mode record   Run all tests against the current artifact source (which
                  must be unmodified vanilla Ray at this point, since this
                  is called from ``setup_environment.sh``). Record each
                  test's pass/fail into baseline/tests_baseline.json. Tests
                  that fail here are considered "pre-existing failures" and
                  are ignored by the gate.

  --mode gate     Run the same tests against the artifact's (possibly
                  modified) Ray source and compare against baseline. Prints
                  JSON with a ``regressed`` list of tests that passed on
                  baseline but fail now.

Ray is pip-installed editable from ``<artifact>/python``, so ``import ray``
resolves to the artifact's source tree directly in every process — driver,
pytest subprocess, and any Ray workers the tests spawn. No path manipulation
or overlay is required.

Per-test granularity is achieved by running pytest with JUnit XML output
and parsing it.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from xml.etree import ElementTree

# Import the curated test list from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_list import (  # noqa: E402
    FAST_TEST_NODES,
    SENSITIVE_TEST_RUNS,
    SENSITIVE_TESTS,
    TEST_NODES,
)


def _run_pytest_one_file(
    artifact_dir: str,
    test_node: str,
    timeout_per_test: int,
    junit_path: str,
) -> int:
    """Run pytest for a single test node in an isolated subprocess.

    Ray is pip-installed editable from ``<artifact>/python``, so ``import
    ray`` resolves to the artifact's tree directly. That includes
    ``ray.data.tests`` and ``ray.tests``, which are part of the source tree
    and picked up via the editable install.

    We launch each file in its own pytest invocation because Ray tests rely
    on module-scoped fixtures that initialize/teardown Ray — sharing a
    single pytest process across all files causes state leakage
    (``ray_start_regular_shared`` vs ``ray_start_10_cpus_shared`` etc. can
    conflict).

    Pytest runs from a neutral temp cwd so stray ``ray`` subdirs
    (e.g. ``/tmp/ray/session_*``) aren't interpreted as a namespace package.
    """
    env = os.environ.copy()
    env.setdefault("RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE", "1")
    env.setdefault("RAY_DATA_DISABLE_PROGRESS_BARS", "1")
    # Force Ray to bind to 127.0.0.1 so tests can start Ray from the agent's
    # sandboxed shell, which runs in a network namespace where Ray's default
    # host-IP detection is unreachable.
    env.setdefault("RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER", "0")

    artifact_abs = os.path.abspath(artifact_dir)
    # Convert the test node to an absolute path.
    if "::" in test_node:
        path, rest = test_node.split("::", 1)
        abs_node = os.path.join(artifact_abs, path) + "::" + rest
    else:
        abs_node = os.path.join(artifact_abs, test_node)

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "--timeout",
        str(timeout_per_test),
        "-q",
        "--no-header",
        f"--junitxml={junit_path}",
        "-p",
        "no:cacheprovider",
        abs_node,
    ]

    # Use a dedicated empty cwd (not /tmp) so pytest/Python doesn't pick up
    # stray ``ray`` subdirectories as a namespace package. Ray's
    # ``ray.init()`` creates /tmp/ray/session_* which is otherwise treated
    # as a namespace package entry for ``ray``.
    clean_cwd = tempfile.mkdtemp(prefix="gated-tests-cwd-")
    # Capture stderr to a tempfile so we can surface pytest collection errors
    # when the JUnit XML ends up empty. stdout is still discarded.
    stderr_fd, stderr_path = tempfile.mkstemp(
        prefix="gated-tests-stderr-", suffix=".log"
    )
    try:
        with open(os.devnull, "w") as devnull, os.fdopen(stderr_fd, "w") as stderr_f:
            proc = subprocess.run(
                cmd,
                cwd=clean_cwd,
                env=env,
                stdout=devnull,
                stderr=stderr_f,
            )
    finally:
        try:
            os.rmdir(clean_cwd)
        except OSError:
            pass
    return proc.returncode, stderr_path


def _run_all_tests(
    artifact_dir: str,
    timeout_per_test: int,
    test_nodes: list = None,
) -> dict:
    """Run each test file in its own pytest subprocess; merge results."""
    if test_nodes is None:
        test_nodes = TEST_NODES
    all_results: dict = {}
    for i, test_node in enumerate(test_nodes):
        with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
            junit_path = f.name
        try:
            print(
                f"  [{i+1}/{len(test_nodes)}] {test_node}",
                file=sys.stderr,
            )
            returncode, stderr_path = _run_pytest_one_file(
                artifact_dir, test_node, timeout_per_test, junit_path
            )
            try:
                file_results: dict = {}
                parse_error: str | None = None
                try:
                    file_results = _parse_junit(junit_path, test_file=test_node)
                except ElementTree.ParseError as e:
                    parse_error = f"JUnit XML unparseable: {e}"
                except Exception as e:
                    parse_error = f"JUnit parse crashed: {type(e).__name__}: {e}"

                # Guard against collection errors OR junit-write failures — if
                # no tests were reported, log the node as an error and surface
                # the pytest stderr tail so the gate's caller can see *why*
                # collection / pytest startup failed (the only way to debug
                # sandbox-specific issues like missing loopback or shm sizing).
                if not file_results:
                    file_results[test_node] = "error"
                    msg_parts = [
                        f"    ! pytest produced no usable junit for {test_node}"
                    ]
                    if parse_error:
                        msg_parts.append(f"      parse: {parse_error}")
                    msg_parts.append(f"      pytest exit code: {returncode}")
                    try:
                        with open(stderr_path) as sf:
                            tail = sf.read()[-4000:]
                        if tail.strip():
                            msg_parts.append("      pytest stderr tail:")
                            msg_parts.extend(
                                "        " + ln for ln in tail.splitlines()[-40:]
                            )
                    except OSError:
                        pass
                    print("\n".join(msg_parts), file=sys.stderr)
                all_results.update(file_results)
            finally:
                if os.path.exists(stderr_path):
                    os.unlink(stderr_path)
        finally:
            if os.path.exists(junit_path):
                os.unlink(junit_path)
    return all_results


def _canonical_dotted_path(raw: str, test_file: str) -> str:
    """Strip the filesystem-path-derived prefix from a dotted module path.

    With ``--import-mode=importlib``, pytest derives the testcase ``classname``
    (and for collection errors, the ``name``) from the absolute test-file
    path — e.g. ``workspace.glia.task-repo.worktrees.<agent>.python.ray.data.
    tests.test_foo.TestBar``. That prefix changes between the setup-time run
    (cwd = task repo root) and evaluate-time runs (cwd = a per-agent
    worktree), so the baseline keys wouldn't match current-run keys.
    Normalize by stripping everything before the rightmost segment that
    matches the test file's basename. Safe no-op when the input doesn't
    contain the basename (e.g. normal ``TestClass`` classnames or
    ``test_method[param]`` names).
    """
    file_basename = os.path.splitext(os.path.basename(test_file.split("::")[0]))[0]
    if not file_basename:
        return raw
    parts = raw.split(".")
    try:
        idx = len(parts) - 1 - parts[::-1].index(file_basename)
    except ValueError:
        return raw
    return ".".join(parts[idx:])


# Backward-compat alias (the original name was classname-only).
_canonical_classname = _canonical_dotted_path


def _parse_junit(junit_path: str, test_file: str = "") -> dict:
    """Parse JUnit XML into ``{nodeid: "passed"|"failed"|"error"|"skipped"}``."""
    results: dict = {}
    if not os.path.isfile(junit_path):
        return results
    tree = ElementTree.parse(junit_path)
    root = tree.getroot()
    # JUnit can have <testsuite> as root OR <testsuites><testsuite>...
    suites = root.iter("testcase")
    for case in suites:
        classname = case.attrib.get("classname", "")
        name = case.attrib.get("name", "")
        # On collection errors pytest sometimes emits
        # <testcase classname="" name="module.path">, putting the path in
        # ``name`` instead of ``classname``. Normalize both fields so keys
        # stay stable regardless of cwd.
        if test_file:
            classname = _canonical_dotted_path(classname, test_file)
            name = _canonical_dotted_path(name, test_file)
        nodeid = f"{classname}::{name}" if classname else name

        status = "passed"
        if case.find("failure") is not None:
            status = "failed"
        elif case.find("error") is not None:
            status = "error"
        elif case.find("skipped") is not None:
            status = "skipped"
        results[nodeid] = status
    return results


def _normalize_sensitive_nodeid(test_node: str) -> str:
    """Convert a SENSITIVE_TESTS path-based nodeid to the normalized form
    used by ``_parse_junit`` (``<file_basename>::<test_part>``), so that
    sensitive-test entries in baseline and gate results match the keys
    produced by normal runs and don't duplicate them.
    """
    file_part, _, test_part = test_node.partition("::")
    basename = os.path.splitext(os.path.basename(file_part))[0]
    return f"{basename}::{test_part}" if test_part else basename


def _run_sensitive_tests(artifact_dir: str, timeout_per_test: int,
                         n_runs: int = SENSITIVE_TEST_RUNS) -> dict:
    """Run each SENSITIVE_TESTS nodeid ``n_runs`` times.

    Returns a dict ``{normalized_nodeid: "passed"|"failed"|"error"|"skipped"}``
    where the status is the strictest observed across the N runs:
      - "passed" only if ALL N runs passed
      - otherwise the worst observed status (failed > error > skipped)

    Keys are returned in the SAME normalized form that ``_parse_junit``
    produces (``<file_basename>::<test>``), so callers can directly merge
    into single-run results without creating duplicates.
    """
    STATUS_RANK = {"passed": 0, "skipped": 1, "error": 2, "failed": 3}
    results: dict = {}
    for i, test_node in enumerate(SENSITIVE_TESTS):
        worst = "passed"
        for run in range(n_runs):
            with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
                junit_path = f.name
            try:
                print(
                    f"  [sensitive {i+1}/{len(SENSITIVE_TESTS)}] {test_node} "
                    f"run {run+1}/{n_runs}",
                    file=sys.stderr,
                )
                _, stderr_path = _run_pytest_one_file(
                    artifact_dir, test_node, timeout_per_test, junit_path
                )
                try:
                    parsed = _parse_junit(junit_path, test_file=test_node)
                    # For a nodeid-specific invocation, expect exactly one
                    # result. Take the single entry's status.
                    if parsed:
                        status = next(iter(parsed.values()))
                    else:
                        status = "error"
                        try:
                            with open(stderr_path) as sf:
                                tail = sf.read()[-2000:]
                            if tail.strip():
                                print(
                                    f"    ! sensitive test produced no result; "
                                    f"pytest stderr tail:\n"
                                    + "\n".join(
                                        "      " + ln for ln in tail.splitlines()[-20:]
                                    ),
                                    file=sys.stderr,
                                )
                        except OSError:
                            pass
                finally:
                    if os.path.exists(stderr_path):
                        os.unlink(stderr_path)
                if STATUS_RANK.get(status, 99) > STATUS_RANK.get(worst, 99):
                    worst = status
            finally:
                if os.path.exists(junit_path):
                    os.unlink(junit_path)
        results[_normalize_sensitive_nodeid(test_node)] = worst
    return results


def cmd_record(artifact_dir: str, baseline_path: str, timeout: int) -> int:
    """Run tests against the artifact source at the time of setup, record
    pass/fail. Must be called before any agent modifications.

    Always records the full TEST_NODES list; the fast subset is a strict
    subset so no separate fast-mode baseline is needed.

    SENSITIVE_TESTS are additionally re-run N times; their baseline status is
    the strictest observed across the N runs. This prevents a single lucky
    pass at record time from baselining a determinism-sensitive test as
    "passed" when it only passes probabilistically.
    """
    results = _run_all_tests(artifact_dir, timeout_per_test=timeout)

    # Override sensitive-test results with multi-run aggregation.
    print(
        f"  [sensitive] running {len(SENSITIVE_TESTS)} sensitive test(s) "
        f"{SENSITIVE_TEST_RUNS}x each for baseline",
        file=sys.stderr,
    )
    sensitive_results = _run_sensitive_tests(artifact_dir, timeout)
    results.update(sensitive_results)

    os.makedirs(os.path.dirname(baseline_path), exist_ok=True)
    with open(baseline_path, "w") as f:
        json.dump({"results": results}, f, indent=2, sort_keys=True)

    summary = {}
    for v in results.values():
        summary[v] = summary.get(v, 0) + 1

    print(json.dumps({"path": baseline_path, "total": len(results), "summary": summary}))
    return 0


def cmd_gate(artifact_dir: str, baseline_path: str, timeout: int, fast: bool) -> int:
    """Run tests against OVERLAYED ray, detect regressions vs baseline.

    ``fast=True`` runs only FAST_TEST_NODES. Regressions are reported only
    for tests that ran in this mode; tests outside the subset are ignored.
    The full gate (fast=False) runs the complete TEST_NODES list.
    """
    if not os.path.isfile(baseline_path):
        print(json.dumps({
            "regressed": [],
            "error": f"baseline not found at {baseline_path}",
        }))
        return 1

    with open(baseline_path) as f:
        baseline = json.load(f).get("results", {})

    test_nodes = FAST_TEST_NODES if fast else TEST_NODES
    current = _run_all_tests(
        artifact_dir, timeout_per_test=timeout, test_nodes=test_nodes
    )

    # For sensitive tests (determinism-class contracts), override the
    # single-shot result with an N-run aggregation. A single pass is not
    # evidence of correctness for probabilistic bugs. The strictest observed
    # status across N runs is used.
    sensitive_in_scope = [
        t for t in SENSITIVE_TESTS
        if any(
            os.path.splitext(os.path.basename(t.split("::")[0]))[0]
            in os.path.splitext(os.path.basename(n.split("::")[0]))[0]
            for n in test_nodes
        )
    ]
    if sensitive_in_scope:
        print(
            f"  [sensitive] re-running {len(sensitive_in_scope)} sensitive test(s) "
            f"{SENSITIVE_TEST_RUNS}x each",
            file=sys.stderr,
        )
        # _run_sensitive_tests returns normalized keys, so a plain update
        # replaces the single-run entry written by _run_all_tests.
        current.update(_run_sensitive_tests(artifact_dir, timeout))

    # A "regression" is: a test that passed on baseline but now fails or errors.
    # Missing-in-current is also a regression (test disappeared / collection error).
    regressed = []
    fixed = []  # passed in current but failed in baseline — informational only
    unknown = []  # in current but not in baseline (new or renamed tests)

    # In fast mode we only check the subset of baseline entries that
    # correspond to files we actually ran, to avoid reporting every other
    # test as "missing".
    if fast:
        # Match baseline entries to the files currently run, via classname.
        fast_file_stems = set()
        for node in FAST_TEST_NODES:
            fast_file_stems.add(
                os.path.splitext(os.path.basename(node.split("::")[0]))[0]
            )
        relevant_baseline = {
            k: v for k, v in baseline.items()
            if any(stem in k for stem in fast_file_stems)
        }
    else:
        relevant_baseline = baseline

    for nodeid, baseline_status in relevant_baseline.items():
        current_status = current.get(nodeid)
        if baseline_status == "passed":
            if current_status in (None, "failed", "error"):
                regressed.append({
                    "test": nodeid,
                    "baseline": baseline_status,
                    "current": current_status or "missing",
                })
        else:
            # Previously failed/errored/skipped; a now-passing one is "fixed".
            if current_status == "passed":
                fixed.append(nodeid)

    for nodeid, current_status in current.items():
        if nodeid not in baseline:
            unknown.append({"test": nodeid, "status": current_status})

    summary = {
        "baseline_total": len(baseline),
        "current_total": len(current),
        "regressed_count": len(regressed),
        "fixed_count": len(fixed),
        "unknown_count": len(unknown),
    }

    result = {
        "summary": summary,
        "regressed": regressed,
        "fixed": fixed[:50],  # cap to keep output readable
        "unknown": unknown[:50],
    }
    print(json.dumps(result))
    # Exit 0 regardless — caller decides; evaluate script parses the JSON.
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=["record", "gate"],
        required=True,
        help="record: baseline unmodified Ray; gate: check artifact vs baseline",
    )
    parser.add_argument(
        "--artifact-dir",
        default=".",
        help="Artifact root (defaults to CWD).",
    )
    parser.add_argument(
        "--baseline",
        default="baseline/tests_baseline.json",
        help="Path to the baseline JSON (relative to artifact-dir or absolute).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=180,
        help="Per-test timeout in seconds.",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help=(
            "Gate mode only: run only FAST_TEST_NODES (~1-2 min), a strict "
            "subset covering the scheduler, backpressure policies, ranker, "
            "and issue detector. Intended for mid-session correctness "
            "pre-checks before running the full gate."
        ),
    )
    args = parser.parse_args()

    artifact_dir = os.path.abspath(args.artifact_dir)
    baseline = args.baseline
    if not os.path.isabs(baseline):
        baseline = os.path.join(artifact_dir, baseline)

    if args.mode == "record":
        if args.fast:
            print(json.dumps({
                "error": "--fast is only valid with --mode gate"
            }))
            return 2
        return cmd_record(artifact_dir, baseline, args.timeout)
    else:
        return cmd_gate(artifact_dir, baseline, args.timeout, fast=args.fast)


if __name__ == "__main__":
    sys.exit(main())
