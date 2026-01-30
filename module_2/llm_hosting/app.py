# -*- coding: utf-8 -*-
"""Flask + tiny local LLM standardizer with incremental JSONL CLI output and batch processing."""

from __future__ import annotations

import json
import os
import re
import sys
import time
import difflib
from typing import Any, Dict, List, Tuple

from flask import Flask, jsonify, request
from huggingface_hub import hf_hub_download
from llama_cpp import Llama  # CPU-only by default if N_GPU_LAYERS=0

app = Flask(__name__)

# ---------------- Model config ----------------
MODEL_REPO = os.getenv(
    "MODEL_REPO",
    "TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF",
)
MODEL_FILE = os.getenv(
    "MODEL_FILE",
    "tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf",
)

N_THREADS = int(os.getenv("N_THREADS", "2"))  # dual-core CPU
N_CTX = int(os.getenv("N_CTX", "512"))
N_GPU_LAYERS = int(os.getenv("N_GPU_LAYERS", "0"))  # CPU-only

CANON_UNIS_PATH = os.getenv("CANON_UNIS_PATH", "canon_universities.txt")
CANON_PROGS_PATH = os.getenv("CANON_PROGS_PATH", "canon_programs.txt")

# Precompiled, non-greedy JSON object matcher
JSON_OBJ_RE = re.compile(r"\{.*?\}", re.DOTALL)

# ---------------- Canonical lists + abbrev maps ----------------
def _read_lines(path: str) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip()]
    except FileNotFoundError:
        return []

CANON_UNIS = _read_lines(CANON_UNIS_PATH)
CANON_PROGS = _read_lines(CANON_PROGS_PATH)

ABBREV_UNI: Dict[str, str] = {
    r"(?i)^mcg(\.|ill)?$": "McGill University",
    r"(?i)^(ubc|u\.?b\.?c\.?)$": "University of British Columbia",
    r"(?i)^uoft$": "University of Toronto",
}

COMMON_UNI_FIXES: Dict[str, str] = {
    "McGiill University": "McGill University",
    "Mcgill University": "McGill University",
    "University Of British Columbia": "University of British Columbia",
}

COMMON_PROG_FIXES: Dict[str, str] = {
    "Mathematic": "Mathematics",
    "Info Studies": "Information Studies",
}

# ---------------- Few-shot prompt ----------------
SYSTEM_PROMPT = (
    "You are a data cleaning assistant. Standardize degree program and university "
    "names.\n\n"
    "Rules:\n"
    "- Input provides 'program-major' and 'university'.\n"
    "- Trim extra spaces and commas.\n"
    '- Expand obvious abbreviations (e.g., "McG" -> "McGill University", '
    '"UBC" -> "University of British Columbia").\n'
    "- Use Title Case for program; use official capitalization for university.\n"
    '- Ensure correct spelling.\n'
    '- If university cannot be inferred, return "Unknown".\n\n'
    "Return JSON ONLY with keys:\n"
    "  standardized_program, standardized_university\n"
)

FEW_SHOTS: List[Tuple[Dict[str, str], Dict[str, str]]] = [
    (
        {"program": "Information Studies, McGill University"},
        {
            "standardized_program": "Information Studies",
            "standardized_university": "McGill University",
        },
    ),
    (
        {"program": "Information, McG"},
        {
            "standardized_program": "Information Studies",
            "standardized_university": "McGill University",
        },
    ),
    (
        {"program": "Mathematics, University Of British Columbia"},
        {
            "standardized_program": "Mathematics",
            "standardized_university": "University of British Columbia",
        },
    ),
]

_LLM: Llama | None = None

# ---------------- LLM load ----------------
def _load_llm() -> Llama:
    global _LLM
    if _LLM is not None:
        return _LLM

    model_path = hf_hub_download(
        repo_id=MODEL_REPO,
        filename=MODEL_FILE,
        local_dir="models",
        local_dir_use_symlinks=False,
        force_filename=MODEL_FILE,
    )

    _LLM = Llama(
        model_path=model_path,
        n_ctx=N_CTX,
        n_threads=N_THREADS,
        n_gpu_layers=N_GPU_LAYERS,
        verbose=False,
    )
    return _LLM

# ---------------- Normalization helpers ----------------
def _best_match(name: str, candidates: List[str], cutoff: float = 0.86) -> str | None:
    if not name or not candidates:
        return None
    matches = difflib.get_close_matches(name, candidates, n=1, cutoff=cutoff)
    return matches[0] if matches else None

def _post_normalize_program(prog: str) -> str:
    p = (prog or "").strip()
    p = COMMON_PROG_FIXES.get(p, p)
    p = p.title()
    if p in CANON_PROGS:
        return p
    match = _best_match(p, CANON_PROGS, cutoff=0.84)
    return match or p

def _post_normalize_university(uni: str) -> str:
    u = (uni or "").strip()
    for pat, full in ABBREV_UNI.items():
        if re.fullmatch(pat, u):
            u = full
            break
    u = COMMON_UNI_FIXES.get(u, u)
    if u:
        u = re.sub(r"\bOf\b", "of", u.title())
    if u in CANON_UNIS:
        return u
    match = _best_match(u, CANON_UNIS, cutoff=0.86)
    return match or u or "Unknown"

# ---------------- Batch LLM call ----------------
def _call_llm_batch(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Process a batch of rows in a single LLM call.
    Each row should have 'program-major' and 'university'.
    Returns a list of standardized results.
    """
    llm = _load_llm()
    batch_input = [{"program-major": r.get("program-major", ""), "university": r.get("university", "")} for r in rows]

    # Build messages for few-shot + batch input
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for x_in, x_out in FEW_SHOTS:
        messages.append({"role": "user", "content": json.dumps(x_in)})
        messages.append({"role": "assistant", "content": json.dumps(x_out)})

    messages.append({"role": "user", "content": json.dumps({"rows": batch_input}, ensure_ascii=False)})

    out = llm.create_chat_completion(
        messages=messages,
        temperature=0.0,
        max_tokens=32,
        top_p=1.0,
    )

    text = (out["choices"][0]["message"]["content"] or "").strip()
    results: List[Dict[str, str]] = []

    try:
        json_objects = JSON_OBJ_RE.findall(text)
        if not json_objects:
            json_objects = [text]

        for obj_text in json_objects:
            obj = json.loads(obj_text)
            std_prog = _post_normalize_program(obj.get("standardized_program", obj.get("program-major", "")))
            std_uni = _post_normalize_university(obj.get("standardized_university", obj.get("university", "")))
            results.append({"standardized_program": std_prog, "standardized_university": std_uni})

    except Exception:
        for r in batch_input:
            std_prog = _post_normalize_program(r.get("program-major", ""))
            std_uni = _post_normalize_university(r.get("university", ""))
            results.append({"standardized_program": std_prog, "standardized_university": std_uni})

    while len(results) < len(rows):
        results.append({"standardized_program": "", "standardized_university": "Unknown"})

    return results

# ---------------- Parallel batch processing (incremental write) ----------------
def _parallel_process(rows: List[Dict[str, Any]], sink=None, batch_size: int = 5) -> List[Dict[str, Any]]:
    """
    Process rows in batches, writing each batch to `sink` if provided.
    Returns the list of processed rows.
    """
    total = len(rows)
    processed_rows: List[Dict[str, Any]] = []
    start_time = time.time()

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        results = _call_llm_batch(batch)

        for row, result in zip(batch, results):
            row["llm-generated-program"] = result["standardized_program"]
            row["llm-generated-university"] = result["standardized_university"]
            processed_rows.append(row)

            # Write immediately if sink provided
            if sink is not None:
                json.dump(row, sink, ensure_ascii=False)
                sink.write("\n")
                sink.flush()

        elapsed = time.time() - start_time
        completed = min(i + batch_size, total)
        avg_per_row = elapsed / completed
        remaining = total - completed
        eta = remaining * avg_per_row
        print(f"[{completed}/{total}] Elapsed: {elapsed:.1f}s, ETA: {eta:.1f}s, Avg: {avg_per_row:.2f}s/row")

    return processed_rows

# ---------------- Input normalization ----------------
def _normalize_input(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        return payload["rows"]
    return []

# ---------------- Flask endpoints ----------------
@app.get("/")
def health() -> Any:
    return jsonify({"ok": True})

@app.post("/standardize")
def standardize() -> Any:
    payload = request.get_json(force=True, silent=True)
    rows = _normalize_input(payload)
    rows = _parallel_process(rows, batch_size=5)
    return jsonify({"rows": rows})

# ---------------- CLI processing ----------------
def _cli_process_file(in_path: str, out_path: str | None, append: bool, to_stdout: bool) -> None:
    with open(in_path, "r", encoding="utf-8") as f:
        rows = _normalize_input(json.load(f))

    sink = sys.stdout if to_stdout else None
    if not to_stdout:
        out_path = out_path or (in_path + ".jsonl")
        mode = "a" if append else "w"
        sink = open(out_path, mode, encoding="utf-8")

    assert sink is not None

    try:
        # Pass sink into parallel process for incremental writing
        _parallel_process(rows, sink=sink, batch_size=5)
    finally:
        if sink is not sys.stdout:
            sink.close()

# ---------------- Main ----------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Standardize program/university with a tiny local LLM.")
    parser.add_argument("--file", help="Path to JSON input", default=None)
    parser.add_argument("--serve", action="store_true", help="Run HTTP server instead of CLI.")
    parser.add_argument("--out", default=None, help="Output path for JSON Lines.")
    parser.add_argument("--append", action="store_true", help="Append instead of overwrite.")
    parser.add_argument("--stdout", action="store_true", help="Write JSONL to stdout.")

    args = parser.parse_args()

    if args.serve or args.file is None:
        port = int(os.getenv("PORT", "8000"))
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        _cli_process_file(in_path=args.file, out_path=args.out, append=args.append, to_stdout=args.stdout)
