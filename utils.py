import json
import ast


def parse_puzzle_results(val):
    if val is None:
        return {}
    parsed = {}
    if isinstance(val, dict):
        parsed = val
    elif isinstance(val, (list, tuple)):
        return val
    elif isinstance(val, str):
        s = val.strip()
        try:
            parsed = json.loads(s)
        except Exception:
            try:
                parsed = ast.literal_eval(s)
            except Exception:
                pass

    if isinstance(parsed, dict):
        normalized = {}
        for k, v in parsed.items():
            k_str = str(k).upper()
            if k_str.startswith("Q"):
                try:
                    new_k = str(int(k_str[1:]) - 1)
                    normalized[new_k] = v
                except ValueError:
                    normalized[k] = v
            else:
                normalized[k] = v
        return normalized
        
    return parsed


def is_test_passed(test):
    if isinstance(test, dict):
        status = test.get("status")
        if status in ("AC", "Accepted", "PASS", "Passed", True):
            return True
        passed = test.get("passed")
        if passed in (True, "True", "true", 1, "1"):
            return True
        return False
    if isinstance(test, str):
        return test in ("AC", "Accepted", "PASS", "Passed")
    if isinstance(test, bool):
        return test
    return False


def pick_column(available_cols, candidates):
    for cand in candidates:
        if cand in available_cols:
            return cand
    lower_map = {c.lower(): c for c in available_cols}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def col_num_to_a1(n):
    s = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s
