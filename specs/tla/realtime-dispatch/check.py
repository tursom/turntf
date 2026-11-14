#!/usr/bin/env python3
"""运行有限 TLC 场景，严格区分预期反例与工具失败；输出到独立目录。"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent
LEGACY_PARAMETERS = {
    "capacity": "Capacity", "input": "Input", "bypass_barrier": "BypassBarrier",
    "remote_responds": "RemoteResponds", "writer_responds": "WriterResponds",
    "router_responds": "RouterResponds", "allow_cancel": "AllowCancel",
    "corrupt_replies": "CorruptReplies",
}
LEGACY_INPUTS = {"IsolationInput", "RelayInput", "CapacityInput", "MixedInput", "StateInput", "MultiFrameInput", "EmptyInput"}
PARAMETERS = {
    "capacity": "Capacity", "stream_limit": "StreamLimit", "stream_bytes": "StreamBytes",
    "response_limit": "ResponseLimit", "response_bytes": "ResponseBytes", "input": "Input",
    "remote_responds": "RemoteResponds", "writer_responds": "WriterResponds",
    "slow_route": "SlowRoute", "allow_cancel": "AllowCancel", "deadlines": "Deadlines",
    "serial_client": "SerialClient", "reject_full": "RejectFull", "corrupt_replies": "CorruptReplies",
    "split_epoch_key": "SplitEpochKey", "break_fifo": "BreakFIFO", "skip_barrier": "SkipBarrier",
}
NUMBERS = {"capacity": {1, 2}, "stream_limit": {1, 2, 4}, "stream_bytes": {1, 2, 4, 8},
           "response_limit": {1, 2, 4}, "response_bytes": {1, 2, 4, 8}}
INPUTS = {"IsolationInput", "RelayInput", "CapacityInput", "NormalInput", "StateInput",
          "StreamBarrierInput", "ResponseBarrierInput", "MultiFrameInput", "KeyInput",
          "StreamOverflowInput", "StreamByteInput", "ResponseOverflowInput", "ResponseByteInput", "EmptyInput"}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def configuration(matrix, case):
    legacy = matrix["schema"] == 1
    parameters = LEGACY_PARAMETERS if legacy else PARAMETERS
    inputs = LEGACY_INPUTS if legacy else INPUTS
    params = {**matrix["defaults"], **case.get("parameters", {})}
    if set(params) != set(parameters) or params["input"] not in inputs:
        raise ValueError("unknown/missing model parameter")
    if type(params["capacity"]) is not int or params["capacity"] not in (1, 2):
        raise ValueError("capacity must be 1 or 2")
    lines = ["SPECIFICATION Spec", "CHECK_DEADLOCK FALSE", "CONSTANTS"]
    for key, name in parameters.items():
        value = params[key]
        if key == "input":
            lines.append(f"  {name} <- {value}")
        elif key in NUMBERS:
            if type(value) is not int or value not in NUMBERS[key]:
                raise ValueError(f"invalid small bound: {key}")
            lines.append(f"  {name} = {value}")
        else:
            if type(value) is not bool:
                raise ValueError(f"{key} must be boolean")
            lines.append(f"  {name} = {'TRUE' if value else 'FALSE'}")
    omitted = case.get("omit_invariants", [])
    if not set(omitted) <= set(matrix["invariants"]):
        raise ValueError("unknown omitted invariant")
    invariants = [n for n in matrix["invariants"] if n not in omitted]
    invariants += case.get("extra_invariants", [])
    for name in invariants + case["properties"]:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*", name):
            raise ValueError("invalid property name")
    lines.extend("INVARIANT " + n for n in invariants)
    lines.extend("PROPERTY " + n for n in case["properties"])
    expected = case["expect"]
    if expected["kind"] == "invariant" and expected["name"] not in invariants:
        raise ValueError("expected invariant is not checked")
    if expected["kind"] == "temporal" and case["properties"] != [expected["name"]]:
        raise ValueError("a negative temporal case must check exactly its named property")
    if expected["kind"] not in {"pass", "invariant", "temporal"}:
        raise ValueError("unknown expected result")
    return "\n".join(lines) + "\n", params


def trace_from_output(text):
    """保留 TLC 原始 TLA 值，不尝试把集合/函数误转换成 Python 对象。"""
    states, loops = [], []
    state = None
    for line in text.splitlines():
        match = re.match(r"State (\d+):\s*(.*)", line)
        if match:
            state = {"number": int(match[1]), "action": match[2], "tla_values": []}
            states.append(state)
        elif "Back to state" in line:
            loops.append(line.strip())
            state = None
        elif state is not None and (line.startswith("/\\ ") or line.startswith(" ")):
            state["tla_values"].append(line)
        elif state is not None and line and not line.startswith("/"):
            state = None
    return {"states": states, "loops": loops}


def classify(case, exit_code, output, trace):
    # 先排除工具故障；不能仅因日志同时出现违例文字就接受。
    faults = ("Parsing or semantic analysis failed", "OutOfMemoryError", "Exception in thread",
              "Attempted to check if the value", "TLC threw an unexpected exception")
    if any(fault in output for fault in faults):
        return False
    expected = case["expect"]
    if expected["kind"] == "pass":
        return exit_code == 0 and "Model checking completed. No error has been found." in output
    if len(trace["states"]) < 2:
        return False
    if expected["kind"] == "invariant":
        violated = re.findall(r"Invariant ([A-Za-z][A-Za-z0-9]*) is violated\.", output)
        return exit_code == 12 and violated == [expected["name"]]
    return (exit_code == 13 and case["properties"] == [expected["name"]]
            and "Temporal properties were violated." in output
            and "Invariant " not in output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", default=os.environ.get("TLA2TOOLS_JAR", "/usr/share/java/tla2tools.jar"))
    parser.add_argument("--java", default="java")
    parser.add_argument("--case", action="append", help="只运行指定场景，可重复")
    parser.add_argument("--legacy", action="store_true", help="运行保留的旧模型及 27 场景")
    parser.add_argument("--output", type=Path, help="新建的结果目录；省略时使用系统临时目录")
    parser.add_argument("--timeout", type=int, default=120, help="每场景截止秒数")
    args = parser.parse_args()
    jar = Path(args.jar).resolve()
    if not jar.is_file() or args.timeout <= 0:
        parser.error("需要有效的 tla2tools.jar 和正的 timeout")
    matrix_path = ROOT / ("legacy-cases.json" if args.legacy else "cases.json")
    matrix_bytes = matrix_path.read_bytes()
    matrix = json.loads(matrix_bytes)
    if matrix["schema"] not in {1, 2}:
        parser.error("unsupported case matrix schema")
    model_name = matrix.get("model", "RealtimeDispatch")
    if model_name != ("LegacyDispatch" if args.legacy else "RealtimeDispatch"):
        parser.error("unexpected model name")
    model = (ROOT / f"{model_name}.tla").read_bytes()
    names = [c["name"] for c in matrix["cases"]]
    if len(names) != len(set(names)) or any(not re.fullmatch(r"[a-z0-9-]+", n) for n in names):
        parser.error("case names must be unique safe directory names")
    if args.case and not set(args.case) <= set(names):
        parser.error("unknown case name")
    cases = [c for c in matrix["cases"] if not args.case or c["name"] in args.case]
    # 在创建产物或启动 TLC 前验证全部配置，避免只运行了一半才发现矩阵错误。
    configs = {c["name"]: configuration(matrix, c) for c in cases}
    if args.output:
        destination = args.output.resolve()
        destination.mkdir(parents=True, exist_ok=False)
    else:
        destination = Path(tempfile.mkdtemp(prefix="turntf-tlc-realtime-"))
    version = subprocess.run([args.java, "-version"], capture_output=True, text=True, timeout=10, check=True)
    summary = {
        "schema": 1, "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "java": version.stdout + version.stderr, "jar": str(jar),
        "jar_sha256": digest(jar.read_bytes()), "spec_sha256": digest(model),
        "matrix_sha256": digest(matrix_bytes), "runner_sha256": digest(Path(__file__).read_bytes()),
        "output": str(destination), "cases": [],
        "model": model_name, "matrix": matrix_path.name,
        "invocation": [sys.executable, *sys.argv], "cwd": str(Path.cwd()),
    }
    (destination / matrix_path.name).write_bytes(matrix_bytes)
    (destination / "check.py").write_bytes(Path(__file__).read_bytes())
    print(json.dumps({"output": str(destination)}, ensure_ascii=False), flush=True)
    for case in cases:
        directory = destination / case["name"]
        directory.mkdir()
        (directory / f"{model_name}.tla").write_bytes(model)
        config, params = configs[case["name"]]
        (directory / "model.cfg").write_text(config)
        command = [args.java, "-XX:+UseParallelGC", "-Xmx512m", "-cp", str(jar),
                   "tlc2.TLC", "-workers", "1", "-fp", "0", "-seed", "0",
                   "-cleanup", "-metadir", str(directory / "states"),
                   "-config", "model.cfg", model_name]
        began = time.monotonic()
        timed_out = False
        with (directory / "tlc.log").open("w") as log:
            try:
                process = subprocess.run(command, cwd=directory, stdout=log, stderr=subprocess.STDOUT,
                                         timeout=args.timeout)
                exit_code = process.returncode
            except subprocess.TimeoutExpired:
                timed_out = True
                exit_code = None
        text = (directory / "tlc.log").read_text()
        trace = trace_from_output(text)
        if trace["states"]:
            (directory / "counterexample.json").write_text(json.dumps(trace, ensure_ascii=False, indent=2) + "\n")
        stats = re.search(r"([\d,]+) states generated, ([\d,]+) distinct states found", text)
        result = {
            "name": case["name"], "expected": case["expect"], "parameters": params,
            "matched": not timed_out and classify(case, exit_code, text, trace),
            "exit_code": exit_code, "timed_out": timed_out,
            "seconds": round(time.monotonic() - began, 3), "command": command,
            "config_sha256": digest(config.encode()), "log_sha256": digest(text.encode()),
            "generated_states": int(stats[1].replace(",", "")) if stats else None,
            "distinct_states": int(stats[2].replace(",", "")) if stats else None,
            "trace_states": len(trace["states"]), "trace_loops": trace["loops"],
            "tlc_version": next((line for line in text.splitlines() if line.startswith("TLC2 Version")), None),
        }
        result["classification"] = ("pass" if exit_code == 0 and result["matched"]
                                    else "expected_violation" if result["matched"]
                                    else "tool_failure" if timed_out or exit_code not in (0, 12, 13)
                                    else "unexpected_result")
        summary["cases"].append(result)
        (directory / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        (destination / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps({k: result[k] for k in ["name", "matched", "exit_code", "distinct_states", "trace_states", "seconds"]}), flush=True)
    summary["all_matched"] = all(c["matched"] for c in summary["cases"])
    summary["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (destination / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    return 0 if summary["all_matched"] else 1


if __name__ == "__main__":
    sys.exit(main())
