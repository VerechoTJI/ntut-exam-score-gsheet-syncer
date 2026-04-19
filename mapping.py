from utils import parse_puzzle_results, is_test_passed


def build_sheet_mapping(ws):
    rows = ws.get_all_values()
    if len(rows) < 2:
        raise RuntimeError("Sheet must have at least two header rows.")
    h1 = rows[0]
    h2 = rows[1]
    max_len = max(len(h1), len(h2))
    h1 = h1 + [""] * (max_len - len(h1))
    h2 = h2 + [""] * (max_len - len(h2))

    last = ""
    for i in range(max_len):
        if h1[i].strip():
            last = h1[i].strip()
        else:
            h1[i] = last

    mapping = [None] * max_len

    for i in range(max_len):
        a = h1[i].strip()
        b = h2[i].strip()
        la = a.lower()
        lb = b.lower()
        if la in ("學號", "student_id", "student id") or lb in ("學號",):
            mapping[i] = {"type": "student_id"}
            continue
        if la in ("最後成績",) or la in ("原始分數",):
            mapping[i] = {"type": "final"}
            continue

    groups = {}
    for i in range(max_len):
        if mapping[i] is not None:
            continue
        a = h1[i].strip()
        key = a if a != "" else "__unknown__"
        groups.setdefault(key, []).append(i)

    for key, cols in groups.items():
        pidx = None
        try:
            pidx = int(key) - 1
        except Exception:
            pidx = None

        subtask_counter = 0
        for i in cols:
            b = h2[i].strip()
            lb = b.lower()
            if "規則" in b or "rule" in lb:
                mapping[i] = {"type": "rule", "pidx": pidx}
            else:
                mapping[i] = {"type": "subtask", "pidx": pidx, "sidx": subtask_counter}
                subtask_counter += 1

    class_idx = None
    student_col_idx = next(
        (i for i, m in enumerate(mapping) if m and m.get("type") == "student_id"), None
    )
    name_idx = None

    rightmost = student_col_idx if student_col_idx is not None else -1
    data_start = rightmost + 1

    last_data = len(mapping) - 1
    for i in range(data_start, len(mapping)):
        if mapping[i] and mapping[i].get("type") == "final":
            last_data = i - 1
            break

    return mapping, class_idx, student_col_idx, name_idx, data_start, last_data


def find_student_row(sheet_student_col_vals, db_student_id):
    for i, v in enumerate(sheet_student_col_vals):
        if v.strip() == str(db_student_id).strip():
            return i + 1
    return None


def build_values_for_row(r, puzzle_col_name, mapping, data_start_col, last_data_col):
    values = []
    raw = r.get(puzzle_col_name) if puzzle_col_name else None
    puzzles = parse_puzzle_results(raw)

    for col_idx in range(data_start_col, last_data_col + 1):
        info = mapping[col_idx]
        if not info or info.get("type") in ("unknown", "final"):
            values.append("")
            continue

        if info["type"] == "subtask":
            pidx = info["pidx"]
            sidx = info["sidx"]
            p_entry = None
            if isinstance(puzzles, dict):
                p_entry = puzzles.get(str(pidx)) or puzzles.get(pidx)
            elif isinstance(puzzles, list) and pidx < len(puzzles):
                p_entry = puzzles[pidx]

            if not p_entry:
                values.append("")
                continue

            subtasks = p_entry.get("subtasks") if isinstance(p_entry, dict) else None
            subtasks = subtasks or []
            if sidx < len(subtasks):
                sub = subtasks[sidx]
                tests = []
                if isinstance(sub, dict):
                    for arr_name in ("visible", "hidden", "tests", "cases"):
                        arr = sub.get(arr_name)
                        if isinstance(arr, list):
                            tests.extend(arr)
                elif isinstance(sub, list):
                    tests = sub

                passed = False
                if tests:
                    passed = all(is_test_passed(t) for t in tests)
                values.append(1 if passed else "")
            else:
                values.append("")

        elif info["type"] == "rule":
            pidx = info["pidx"]
            p_entry = None
            if isinstance(puzzles, dict):
                p_entry = puzzles.get(str(pidx)) or puzzles.get(pidx)
            elif isinstance(puzzles, list) and pidx < len(puzzles):
                p_entry = puzzles[pidx]

            rules = (
                p_entry.get("specialRuleResults") if isinstance(p_entry, dict) else []
            )
            rules = rules or []
            if rules:
                try:
                    rule_passed = all(
                        rr.get("passed") in (True, "True", "true", 1, "1")
                        for rr in rules
                        if isinstance(rr, dict)
                    )
                except Exception:
                    rule_passed = False
                values.append(1 if rule_passed else "")
            else:
                values.append("")

        else:
            values.append("")

    return values
