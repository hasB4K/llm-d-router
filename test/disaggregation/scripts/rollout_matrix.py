#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import time
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen


REVISION_LABEL = "disaggregatedset.x-k8s.io/revision"
ROLE_LABEL = "disaggregatedset.x-k8s.io/role"
SET_LABEL = "disaggregatedset.x-k8s.io/name=revision-rollout"
REVISION_HEADER = "x-disagg-revision"
PREFERRED_ROLE_HEADER = "x-preferred-role"
CHAT_PATH = "/v1/chat/completions"
CHAT_BODY = {"model": "test", "messages": [{"role": "user", "content": "hi"}]}


@dataclass(frozen=True)
class Cell:
    topology: str
    mode: str
    prefill_url: str
    decode_url: str | None = None

    @property
    def name(self) -> str:
        return f"{self.topology}/{self.mode}"


@dataclass
class Sample:
    revisions: Counter[str]
    total: int
    failures: list[str]


@dataclass(frozen=True)
class StepResult:
    state: str
    observed: dict[str, float]
    expected: dict[str, float]


@dataclass(frozen=True)
class PreferenceResult:
    shape: str
    cell: str
    requested_role: str


@dataclass(frozen=True)
class HTTPResponse:
    status: int
    body: dict[str, Any]
    headers: dict[str, str]


SHAPES = ((2, 10), (10, 10), (10, 2))
SHAPES_BY_NAME = {
    f"{prefill}p{decode}d": (prefill, decode) for prefill, decode in SHAPES
}
CELLS = (
    Cell("single-epp", "sum", "http://127.0.0.1:18081"),
    Cell("single-epp", "max-role", "http://127.0.0.1:18082"),
    Cell(
        "two-epp",
        "sum",
        "http://127.0.0.1:18083",
        "http://127.0.0.1:18084",
    ),
    Cell(
        "two-epp",
        "max-role",
        "http://127.0.0.1:18085",
        "http://127.0.0.1:18086",
    ),
)
PREFERENCE_CELL = Cell("generic", "prefer", "http://127.0.0.1:18087")


def run(*args: str, input_text: str | None = None) -> str:
    result = subprocess.run(
        args,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(args)}\n{result.stderr}"
        )
    return result.stdout


def kubectl(namespace: str, *args: str) -> str:
    return run("kubectl", "-n", namespace, *args)


def pod_is_ready(pod: dict[str, Any]) -> bool:
    if pod["metadata"].get("deletionTimestamp") is not None:
        return False
    return any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in pod.get("status", {}).get("conditions", [])
    )


def pods(namespace: str) -> list[dict[str, Any]]:
    raw = kubectl(namespace, "get", "pods", "-l", SET_LABEL, "-o", "json")
    return json.loads(raw)["items"]


def ready_counts(current_pods: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for pod in current_pods:
        if not pod_is_ready(pod):
            continue
        labels = pod["metadata"].get("labels", {})
        revision = labels.get(REVISION_LABEL, "")
        role = labels.get(ROLE_LABEL, "")
        if revision and role:
            counts[revision][role] += 1
    return {revision: dict(per_role) for revision, per_role in counts.items()}


def pod_ip_revisions(current_pods: list[dict[str, Any]]) -> dict[str, str]:
    result = {}
    for pod in current_pods:
        if not pod_is_ready(pod):
            continue
        ip = pod.get("status", {}).get("podIP")
        revision = pod["metadata"].get("labels", {}).get(REVISION_LABEL)
        if ip and revision:
            result[ip] = revision
    return result


def expected_shares(
    counts: dict[str, dict[str, int]], mode: str
) -> dict[str, float]:
    covered = {}
    for revision, roles in counts.items():
        prefill = roles.get("prefill", 0)
        decode = roles.get("decode", 0)
        if prefill == 0 or decode == 0:
            continue
        covered[revision] = roles
    if mode == "sum":
        weights = {
            revision: roles.get("prefill", 0) + roles.get("decode", 0)
            for revision, roles in covered.items()
        }
    else:
        role_totals = {
            role: sum(roles.get(role, 0) for roles in covered.values())
            for role in ("prefill", "decode")
        }
        dominant_role = max(role_totals, key=role_totals.get)
        weights = {
            revision: roles.get(dominant_role, 0)
            for revision, roles in covered.items()
        }
    total = sum(weights.values())
    return {revision: weight / total for revision, weight in weights.items()} if total else {}


def host_from_host_port(value: str) -> str:
    if value.startswith("["):
        return value[1:].split("]", 1)[0]
    return value.rsplit(":", 1)[0]


def post_json(url: str, headers: dict[str, str] | None = None) -> HTTPResponse:
    request_headers = {"content-type": "application/json"}
    request_headers.update(headers or {})
    request = Request(
        url,
        data=json.dumps(CHAT_BODY).encode(),
        headers=request_headers,
        method="POST",
    )
    try:
        response = urlopen(request, timeout=20)
    except HTTPError as error:
        response = error
    with response:
        payload = response.read().decode()
        try:
            body = json.loads(payload)
        except json.JSONDecodeError:
            body = {"raw": payload[:200]}
        return HTTPResponse(
            status=response.status,
            body=body,
            headers={key.lower(): value for key, value in response.headers.items()},
        )


def post_json_with_retry(
    url: str,
    headers: dict[str, str] | None = None,
    attempts: int = 3,
) -> HTTPResponse:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = post_json(url, headers)
            if response.status < 500 or attempt == attempts - 1:
                return response
        except OSError as error:
            last_error = error
            if attempt == attempts - 1:
                raise
        time.sleep(0.05 * (2**attempt))
    assert last_error is not None
    raise last_error


async def one_single(
    cell: Cell, ip_revisions: dict[str, str]
) -> tuple[str | None, str | None]:
    response = await asyncio.to_thread(
        post_json_with_retry, cell.prefill_url + CHAT_PATH
    )
    if response.status != 200:
        return None, f"single EPP returned {response.status}: {response.body}"
    decode_revision = response.body.get("revision")
    header_revision = response.headers.get(REVISION_HEADER)
    prefiller = response.body.get("prefiller_host_port")
    if not decode_revision or header_revision != decode_revision:
        return None, "single EPP response revision/header mismatch"
    if not prefiller:
        return None, "single EPP did not schedule a prefill endpoint"
    prefill_revision = ip_revisions.get(host_from_host_port(prefiller))
    if prefill_revision != decode_revision:
        return None, (
            f"single EPP crossed revisions: prefill={prefill_revision} "
            f"decode={decode_revision}"
        )
    return decode_revision, None


async def check_role_preference(shape_name: str) -> list[PreferenceResult]:
    results = []
    for preferred_role in ("prefill", "decode"):
        response = await asyncio.to_thread(
            post_json_with_retry,
            PREFERENCE_CELL.prefill_url + CHAT_PATH,
            {PREFERRED_ROLE_HEADER: preferred_role},
        )
        if response.status != 200:
            raise AssertionError(
                f"{PREFERENCE_CELL.name}: role {preferred_role} returned "
                f"{response.status}: {response.body}"
            )
        response_role = response.headers.get(PREFERRED_ROLE_HEADER)
        selected_role = response.body.get("role")
        if (response_role, selected_role) != (preferred_role, preferred_role):
            raise AssertionError(
                f"{PREFERENCE_CELL.name}: requested role={preferred_role}, "
                f"response={response_role}, selected={selected_role}"
            )
        print(
            f"    PASS {PREFERENCE_CELL.name:<19} preferred role {preferred_role}: "
            "selected endpoint and response header matched"
        )
        results.append(
            PreferenceResult(
                shape=shape_name,
                cell=PREFERENCE_CELL.name,
                requested_role=preferred_role,
            )
        )
    return results


async def one_two(cell: Cell) -> tuple[str | None, str | None]:
    prefill = await asyncio.to_thread(
        post_json_with_retry, cell.prefill_url + CHAT_PATH
    )
    if prefill.status != 200:
        return None, f"prefill EPP returned {prefill.status}: {prefill.body}"
    revision = prefill.headers.get(REVISION_HEADER)
    if not revision or prefill.body.get("revision") != revision:
        return None, "prefill response revision/header mismatch"
    assert cell.decode_url is not None
    decode = await asyncio.to_thread(
        post_json_with_retry,
        cell.decode_url + CHAT_PATH,
        {REVISION_HEADER: revision},
    )
    if decode.status != 200:
        return None, (
            f"decode EPP returned {decode.status} for revision {revision}: "
            f"{decode.body}"
        )
    if decode.body.get("revision") != revision:
        return None, (
            f"two EPPs crossed revisions: prefill={revision} "
            f"decode={decode.body.get('revision')}"
        )
    return revision, None


async def sample_cell(
    cell: Cell,
    requests: int,
    concurrency: int,
    ip_revisions: dict[str, str],
) -> Sample:
    semaphore = asyncio.Semaphore(concurrency)
    revisions: Counter[str] = Counter()
    failures = []

    async def one() -> None:
        async with semaphore:
            try:
                if cell.topology == "single-epp":
                    revision, error = await one_single(cell, ip_revisions)
                else:
                    revision, error = await one_two(cell)
            except Exception as exception:
                revision, error = None, str(exception)
            if error:
                if len(failures) < 10:
                    failures.append(error)
            elif revision:
                revisions[revision] += 1

    await asyncio.gather(*(one() for _ in range(requests)))
    return Sample(revisions=revisions, total=requests, failures=failures)


def assert_sample(
    cell: Cell,
    sample: Sample,
    expected: dict[str, float],
    tolerance: float,
) -> None:
    successful = sum(sample.revisions.values())
    if sample.failures or successful != sample.total:
        raise AssertionError(
            f"{cell.name}: {sample.total - successful} failed requests: "
            f"{sample.failures}"
        )
    unexpected = set(sample.revisions) - set(expected)
    if unexpected:
        raise AssertionError(
            f"{cell.name}: unexpected revisions received traffic: {unexpected}"
        )
    for revision, expected_share in expected.items():
        observed = sample.revisions[revision] / successful
        if abs(observed - expected_share) > tolerance:
            raise AssertionError(
                f"{cell.name}: revision {revision} observed={observed:.3%} "
                f"expected={expected_share:.3%} tolerance={tolerance:.1%}"
            )


def format_counts(counts: dict[str, dict[str, int]]) -> str:
    return " ".join(
        f"{revision[:8]}={roles.get('prefill', 0)}p/{roles.get('decode', 0)}d"
        for revision, roles in sorted(counts.items())
    )


async def checkpoint(
    namespace: str,
    shape_name: str,
    requests: int,
    concurrency: int,
    tolerance: float,
    cells: tuple[Cell, ...],
    results: dict[tuple[str, str, str], list[StepResult]],
) -> bool:
    before_pods = pods(namespace)
    before = ready_counts(before_pods)
    if not expected_shares(before, "sum"):
        print(f"  checkpoint skipped: no complete revision ({format_counts(before)})")
        return False

    stable_pods = before_pods
    stable = before
    quiet_seconds = 0
    for _ in range(30):
        await asyncio.sleep(1)
        stable_pods = pods(namespace)
        stable = ready_counts(stable_pods)
        if stable != before:
            print("  checkpoint skipped: Ready counts still changing")
            return False
        if any(pod["metadata"].get("deletionTimestamp") for pod in stable_pods):
            quiet_seconds = 0
            continue
        quiet_seconds += 1
        if quiet_seconds == 5:
            break
    else:
        print("  checkpoint skipped: terminating pods did not drain")
        return False

    print(f"  checkpoint {format_counts(stable)}")
    ip_revisions = pod_ip_revisions(stable_pods)
    samples = await asyncio.gather(
        *(sample_cell(cell, requests, concurrency, ip_revisions) for cell in cells)
    )
    after = ready_counts(pods(namespace))
    if after != stable:
        raise RuntimeError(
            f"Ready counts changed during sampling: before={stable}, after={after}"
        )
    for cell, sample in zip(cells, samples, strict=True):
        expected = expected_shares(stable, cell.mode)
        assert_sample(cell, sample, expected, tolerance)
        successful = sum(sample.revisions.values())
        observed_shares = {
            revision: sample.revisions[revision] / successful
            for revision in sorted(sample.revisions)
        }
        observed = ", ".join(
            f"{revision[:8]}={share:.1%}"
            for revision, share in observed_shares.items()
        )
        target = ", ".join(
            f"{revision[:8]}={share:.1%}" for revision, share in sorted(expected.items())
        )
        print(f"    PASS {cell.name:<19} observed [{observed}] expected [{target}]")
        results[(shape_name, cell.topology, cell.mode)].append(
            StepResult(
                state=format_counts(stable),
                observed=observed_shares,
                expected=expected,
            )
        )
    return True


def render_disaggregated_set(
    template: str,
    namespace: str,
    prefill: int,
    decode: int,
    backend_image: str,
    auto_ready: bool,
    rollout_token: str,
) -> str:
    replacements = {
        "__NAMESPACE__": namespace,
        "__PREFILL_REPLICAS__": str(prefill),
        "__DECODE_REPLICAS__": str(decode),
        "__BACKEND_IMAGE__": backend_image,
        "__AUTO_READY__": str(auto_ready).lower(),
        "__ROLLOUT_TOKEN__": rollout_token,
    }
    rendered = template
    for key, value in replacements.items():
        rendered = rendered.replace(key, value)
    return rendered


def apply_yaml(yaml_text: str) -> None:
    run("kubectl", "apply", "-f", "-", input_text=yaml_text)


def wait_initial_ready(namespace: str, prefill: int, decode: int) -> str:
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        counts = ready_counts(pods(namespace))
        if len(counts) == 1:
            revision, roles = next(iter(counts.items()))
            if roles.get("prefill") == prefill and roles.get("decode") == decode:
                return revision
        time.sleep(1)
    raise TimeoutError("initial DisaggregatedSet did not become Ready")


def wait_pods_deleted(namespace: str) -> None:
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if not pods(namespace):
            return
        time.sleep(1)
    raise TimeoutError("previous DisaggregatedSet pods were not deleted")


def target_unready_pods(
    namespace: str, old_revision: str
) -> list[dict[str, Any]]:
    candidates = []
    for pod in pods(namespace):
        labels = pod["metadata"].get("labels", {})
        if labels.get(REVISION_LABEL) in (None, "", old_revision):
            continue
        if pod["metadata"].get("deletionTimestamp") is not None or pod_is_ready(pod):
            continue
        if pod.get("status", {}).get("phase") == "Running":
            candidates.append(pod)
    return candidates


def choose_pod_to_promote(
    candidates: list[dict[str, Any]],
    counts: dict[str, dict[str, int]],
    old_revision: str,
    prefill: int,
    decode: int,
) -> dict[str, Any]:
    desired = {"prefill": prefill, "decode": decode}
    target_counts: dict[str, int] = defaultdict(int)
    for revision, roles in counts.items():
        if revision == old_revision:
            continue
        for role, count in roles.items():
            target_counts[role] += count
    return min(
        candidates,
        key=lambda pod: (
            target_counts[pod["metadata"]["labels"][ROLE_LABEL]]
            / desired[pod["metadata"]["labels"][ROLE_LABEL]],
            pod["metadata"]["labels"][ROLE_LABEL],
            pod["metadata"]["name"],
        ),
    )


async def rollout_shape(
    namespace: str,
    template: str,
    backend_image: str,
    prefill: int,
    decode: int,
    requests: int,
    concurrency: int,
    tolerance: float,
    cells: tuple[Cell, ...],
    results: dict[tuple[str, str, str], list[StepResult]],
    preference_results: list[PreferenceResult],
) -> None:
    shape_name = f"{prefill}p{decode}d"
    print(f"\n=== Real DisaggregatedSet rollout: {shape_name} ===")
    kubectl(
        namespace,
        "delete",
        "disaggregatedset",
        "revision-rollout",
        "--ignore-not-found",
        "--wait=true",
    )
    wait_pods_deleted(namespace)

    initial = render_disaggregated_set(
        template,
        namespace,
        prefill,
        decode,
        backend_image,
        True,
        shape_name + "-a",
    )
    apply_yaml(initial)
    old_revision = wait_initial_ready(namespace, prefill, decode)
    print(f"  initial revision: {old_revision}")
    await checkpoint(
        namespace,
        shape_name,
        min(requests, 100),
        concurrency,
        tolerance,
        cells,
        results,
    )
    preference_results.extend(await check_role_preference(shape_name))

    target = render_disaggregated_set(
        template,
        namespace,
        prefill,
        decode,
        backend_image,
        False,
        shape_name + "-b",
    )
    apply_yaml(target)

    seen_states = {format_counts(ready_counts(pods(namespace)))}
    progress_deadline = time.monotonic() + 900
    while time.monotonic() < progress_deadline:
        current_pods = pods(namespace)
        counts = ready_counts(current_pods)
        state = format_counts(counts)
        if state not in seen_states:
            sampled = await checkpoint(
                namespace,
                shape_name,
                requests,
                concurrency,
                tolerance,
                cells,
                results,
            )
            if sampled:
                seen_states.add(state)
                # Sampling thousands of requests can take longer than the
                # rollout itself. Start a fresh no-progress window after each
                # completed checkpoint.
                progress_deadline = time.monotonic() + 900

        target_revisions = [
            revision for revision in counts if revision != old_revision
        ]
        if (
            len(target_revisions) == 1
            and old_revision not in counts
            and counts[target_revisions[0]].get("prefill") == prefill
            and counts[target_revisions[0]].get("decode") == decode
        ):
            print(f"  rollout complete: {target_revisions[0]}")
            return

        candidates = target_unready_pods(namespace, old_revision)
        if not candidates:
            await asyncio.sleep(1)
            continue
        chosen = choose_pod_to_promote(
            candidates, counts, old_revision, prefill, decode
        )
        name = chosen["metadata"]["name"]
        role = chosen["metadata"]["labels"][ROLE_LABEL]
        print(f"  promote {role} pod {name}")
        kubectl(namespace, "exec", name, "-c", "server", "--", "touch", "/tmp/ready")
        progress_deadline = time.monotonic() + 900
        await asyncio.sleep(2)
    raise TimeoutError(f"rollout {shape_name} made no progress for 900 seconds")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", default="llm-d-test-disagg-matrix")
    parser.add_argument(
        "--backend-image",
        default="localhost:5000/disagg-rollout-backend:latest",
    )
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--tolerance", type=float, default=0.06)
    parser.add_argument(
        "--shape",
        action="append",
        choices=tuple(SHAPES_BY_NAME),
        help="rollout shape to run; repeat to select multiple (default: all)",
    )
    parser.add_argument(
        "--topology",
        action="append",
        choices=("single-epp", "two-epp"),
        help="topology to test; repeat to select both (default: both)",
    )
    parser.add_argument(
        "--mode",
        action="append",
        choices=("sum", "max-role"),
        help="gating mode to test; repeat to select both (default: both)",
    )
    parser.add_argument("--table-report", type=Path)
    return parser.parse_args()


def format_distribution(shares: dict[str, float]) -> str:
    return ", ".join(
        f"{revision[:8]}={share:.1%}" for revision, share in sorted(shares.items())
    )


def write_table_report(
    path: Path,
    shapes: tuple[tuple[int, int], ...],
    cells: tuple[Cell, ...],
    requests: int,
    tolerance: float,
    results: dict[tuple[str, str, str], list[StepResult]],
    preference_results: list[PreferenceResult],
) -> None:
    lines = [
        "# Disaggregation rollout matrix",
        "",
        f"Requests per transition cell: {requests}",
        f"Tolerance: +/-{tolerance:.1%}",
        "",
        "| Shape | Step | Ready pods by revision | Topology | Mode | Observed | Expected | Max delta |",
        "|---|---:|---|---|---|---|---|---:|",
    ]
    for prefill, decode in shapes:
        shape = f"{prefill}p{decode}d"
        for cell in cells:
            checkpoints = results[(shape, cell.topology, cell.mode)]
            for step, result in enumerate(checkpoints, start=1):
                revisions = set(result.observed) | set(result.expected)
                max_delta = max(
                    (
                        abs(
                            result.observed.get(revision, 0)
                            - result.expected.get(revision, 0)
                        )
                        for revision in revisions
                    ),
                    default=0.0,
                )
                lines.append(
                    f"| {shape} | {step} | {result.state} | {cell.topology} | "
                    f"{cell.mode} | {format_distribution(result.observed)} | "
                    f"{format_distribution(result.expected)} | {max_delta:.1%} |"
                )
    if preference_results:
        lines.extend(
            [
                "",
                "## Generic header-to-label preference",
                "",
                "| Shape | EPP case | Requested role | Result |",
                "|---|---|---:|---|",
            ]
        )
        for result in preference_results:
            lines.append(
                f"| {result.shape} | {result.cell} | "
                f"{result.requested_role} | selected endpoint and header matched |"
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


async def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    template = (root / "k8s" / "disaggregatedset.yaml.tpl").read_text()
    shape_names = tuple(dict.fromkeys(args.shape or SHAPES_BY_NAME))
    shapes = tuple(SHAPES_BY_NAME[name] for name in shape_names)
    topologies = set(args.topology or ("single-epp", "two-epp"))
    modes = set(args.mode or ("sum", "max-role"))
    cells = tuple(
        cell for cell in CELLS if cell.topology in topologies and cell.mode in modes
    )
    results: dict[tuple[str, str, str], list[StepResult]] = defaultdict(list)
    preference_results: list[PreferenceResult] = []

    for prefill, decode in shapes:
        await rollout_shape(
            args.namespace,
            template,
            args.backend_image,
            prefill,
            decode,
            args.requests,
            args.concurrency,
            args.tolerance,
            cells,
            results,
            preference_results,
        )

    print(f"\n=== {len(shapes) * len(cells)}-case matrix ===")
    failures = 0
    for prefill, decode in shapes:
        shape = f"{prefill}p{decode}d"
        for cell in cells:
            checkpoints = results[(shape, cell.topology, cell.mode)]
            passed = len(checkpoints) >= 2
            failures += int(not passed)
            print(
                f"  {'PASS' if passed else 'FAIL'} {shape:<7} {cell.name:<20} "
                f"checkpoints={len(checkpoints)}"
            )
    expected_preference_results = len(shapes) * 2
    preference_passed = len(preference_results) == expected_preference_results
    failures += int(not preference_passed)
    print(
        f"  {'PASS' if preference_passed else 'FAIL'} generic preference "
        f"checks={len(preference_results)}/{expected_preference_results}"
    )
    if args.table_report is not None:
        write_table_report(
            args.table_report,
            shapes,
            cells,
            args.requests,
            args.tolerance,
            results,
            preference_results,
        )
        print(f"Table report: {args.table_report}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
