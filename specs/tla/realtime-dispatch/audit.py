#!/usr/bin/env python3
"""复核已完成的 TLC 证据，并用真实工具故障检查运行器分类；不运行 Go。"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

import check as runner


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--legacy", type=Path, required=True)
    parser.add_argument("--previous", type=Path, help="可选：上一版原始 TLC 目录，用于核对历史模型/配置保留")
    parser.add_argument("--output", type=Path, required=True, help="尚不存在的证据目录")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "audit.py").write_bytes(Path(__file__).read_bytes())
    checks = []

    def verify(label, condition):
        checks.append({"check": label, "passed": bool(condition)})

    summaries = []
    negatives = []
    for directory in (args.current.resolve(), args.legacy.resolve()):
        summary = json.loads((directory / "summary.json").read_text())
        summaries.append(summary)
        matrix = json.loads((directory / summary["matrix"]).read_text())
        cases = {c["name"]: c for c in matrix["cases"]}
        verify(f"{summary['model']}:complete", summary["all_matched"] and len(cases) == len(summary["cases"]))
        verify(f"{summary['model']}:runner hash", sha((directory / "check.py").read_bytes()) == summary["runner_sha256"])
        verify(f"{summary['model']}:matrix hash", sha((directory / summary["matrix"]).read_bytes()) == summary["matrix_sha256"])
        for result in summary["cases"]:
            case_dir = directory / result["name"]
            text = (case_dir / "tlc.log").read_text()
            trace = runner.trace_from_output(text)
            label = f"{summary['model']}:{result['name']}"
            config, _ = runner.configuration(matrix, cases[result["name"]])
            verify(label + ":artifacts", all((
                sha((case_dir / f"{summary['model']}.tla").read_bytes()) == summary["spec_sha256"],
                sha((case_dir / "model.cfg").read_bytes()) == result["config_sha256"] == sha(config.encode()),
                sha(text.encode()) == result["log_sha256"],
                result == json.loads((case_dir / "result.json").read_text()),
                not result["timed_out"],
                runner.classify(cases[result["name"]], result["exit_code"], text, trace),
            )))
            if result["expected"]["kind"] != "pass":
                negatives.append((label, cases[result["name"]], result, text, trace))
                verify(label + ":trace", trace == json.loads((case_dir / "counterexample.json").read_text()))

    if args.previous:
        previous = args.previous.resolve()
        old_model = (previous / "current-isolation-1" / "RealtimeDispatch.tla").read_text()
        renamed_model = (args.legacy / "current-isolation-1" / "LegacyDispatch.tla").read_text()
        verify("historical model preserved except module name",
               renamed_model.replace("MODULE LegacyDispatch", "MODULE RealtimeDispatch") == old_model)
        for case in summaries[1]["cases"]:
            verify("historical config preserved:" + case["name"],
                   (previous / case["name"] / "model.cfg").read_bytes()
                   == (args.legacy / case["name"] / "model.cfg").read_bytes())

    # 真正执行 SANY 错误和 TLC 求值错误；不是伪造退出码/日志。
    fault_results = []
    for name, source, expected_code in (
        ("BadParse", "---- MODULE BadParse ----\nVARIABLE x\nInit == (\n====\n", 150),
        ("BadEval", "---- MODULE BadEval ----\nVARIABLE x\nInit == x = (<<>>)[1]\nNext == UNCHANGED x\nSpec == Init /\\ [][Next]_x\n====\n", 75),
    ):
        directory = output / name
        directory.mkdir()
        (directory / f"{name}.tla").write_text(source)
        (directory / "model.cfg").write_text("SPECIFICATION Spec\nCHECK_DEADLOCK FALSE\n")
        command = [summaries[0]["cases"][0]["command"][0], "-Xmx512m", "-cp", summaries[0]["jar"],
                   "tlc2.TLC", "-workers", "1", "-fp", "0", "-seed", "0", "-config", "model.cfg", name]
        with (directory / "tlc.log").open("w") as log:
            process = subprocess.run(command, cwd=directory, stdout=log, stderr=subprocess.STDOUT, timeout=30)
        text = (directory / "tlc.log").read_text()
        trace = runner.trace_from_output(text)
        fault_results.append({"name": name, "exit_code": process.returncode, "command": command,
                              "log_sha256": sha(text.encode())})
        verify(name + ":actual failure code", process.returncode == expected_code)
        for label, case, _, _, _ in negatives:
            verify(label + ":reject " + name, not runner.classify(case, process.returncode, text, trace))

    for label, case, result, text, trace in negatives:
        verify(label + ":reject missing trace", not runner.classify(case, result["exit_code"], text, {"states": [], "loops": []}))
        verify(label + ":reject wrong exit", not runner.classify(case, 150, text, trace))
        verify(label + ":reject timeout", not runner.classify(case, None, text, trace))
        if case["expect"]["kind"] == "invariant":
            wrong = text.replace(f"Invariant {case['expect']['name']} is violated.", "Invariant DifferentName is violated.")
            verify(label + ":reject wrong invariant", not runner.classify(case, 12, wrong, trace))
        else:
            wrong_case = {**case, "properties": ["DifferentName"]}
            verify(label + ":reject wrong temporal configuration", not runner.classify(wrong_case, 13, text, trace))

    def states(name):
        return json.loads((args.current / name / "counterexample.json").read_text())["states"]

    def values(state):
        return "\n".join(state["tla_values"])

    def scalar(state, name):
        return re.search(r"^/\\ " + name + r" = (.*)$", values(state), re.M)[1]

    def phases(state):
        return re.findall(r'"([A-Za-z]+)"', scalar(state, "phase"))

    # 不把 JSON 当 TLA 求值器，只核对这些小见证的具体终态和动作顺序。
    for name in ("witness-healthy-without-close", "witness-full-key-without-close"):
        final = states(name)[-1]
        verify(name + ":open healthy progress with blocked head", scalar(final, "connection") == '"open"'
               and phases(final) == ["route", "queued", "done", "done", "done"]
               and scalar(final, "routed") == "<<3, 4>>")
    final = states("witness-rejected-request")[-1]
    verify("rejected request is not handler success", scalar(final, "connection") == '"open"'
           and scalar(final, "started") == "{1}" and scalar(final, "rejected") == "{2}"
           and phases(final) == ["remote", "rejected", "done", "done"]
           and 'status |-> "service_unavailable"' in values(final))
    history = states("witness-cancel-writer")
    actions = [s["action"].split(" line")[0].lstrip("<") for s in history]
    verify("external cancel closes transport before blocked write returns",
           actions.index("Cancel") < actions.index("CloseTransport") < actions.index("FinishWrite")
           and scalar(history[-1], "connection") == '"closed"')
    history = states("witness-lock-wait-deadline")
    expired = next(s for s in history if s["action"].startswith("<Expire"))
    stop = scalar(expired, "stop") + "\n" + values(expired).split("/\\ stop = ", 1)[1].split("/\\", 1)[0]
    # Snapshot 的 writer/expired 分行记录，比较同一记录里的整数。
    writer = int(re.search(r"writer \|-> (\d+)", stop)[1])
    request = int(re.search(r"expired \|-> (\d+)", stop)[1])
    verify("deadline can expire on lock waiter", writer != 0 and request != 0 and writer != request)
    history = states("witness-active-response-counted")
    overflow = next(s for s in history if s["action"].startswith("<Overflow"))
    verify("response limit includes active writer", scalar(overflow, "responseActive") != "0"
           and scalar(overflow, "responseQueue").startswith("<<")
           and "responses |-> 2" in values(overflow))

    report = {"invocation": [sys.executable, *sys.argv], "audit_sha256": sha(Path(__file__).read_bytes()),
              "summaries": [str(args.current.resolve() / "summary.json"), str(args.legacy.resolve() / "summary.json")],
              "negative_cases": len(negatives), "faults": fault_results,
              "checks": checks, "all_passed": all(c["passed"] for c in checks)}
    (output / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"all_passed": report["all_passed"], "checks": len(checks),
                      "failures": [c["check"] for c in checks if not c["passed"]],
                      "report": str(output / "report.json")}, ensure_ascii=False))
    return 0 if report["all_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
