"""Recognize infrastructure failures present in client-uploaded results."""
from utils import parse_puzzle_results


def error_reason(value):
    if isinstance(value, dict):
        status = str(value.get('status', value.get('statusCode', ''))).upper()
        if status in ('SE', 'SYSTEMERROR', 'CTLE', 'ABORTED') or str(value.get('attribution', '')).lower() == 'system':
            return str(value.get('message') or value.get('reason') or status or 'system-attributed failure')
        children = value.values()
    elif isinstance(value, list):
        children = value
    else:
        return None
    for child in children:
        reason = error_reason(child)
        if reason:
            return reason
    return None


def errored_questions(raw):
    values = parse_puzzle_results(raw)
    if isinstance(values, dict):
        entries = values.items()
    elif isinstance(values, list):
        entries = enumerate(values)
    else:
        return {}
    return {str(key): reason for key, question in entries
            if (reason := error_reason(question))}
