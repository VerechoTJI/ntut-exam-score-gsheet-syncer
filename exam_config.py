"""The scoring projection of v2 config; never log the complete exam document."""
import json
import math
from pathlib import Path

from utils import parse_puzzle_results


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value, label, maximum=math.inf):
    require(type(value) in (int, float) and math.isfinite(value)
            and 0 <= value <= maximum, f'{label}: expected a finite nonnegative number <= {maximum}')
    return value


def puzzles(config):
    return [q for section in config['sections'] for q in section['puzzles']]


def load_config(path):
    config = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    require(isinstance(config, dict) and isinstance(config.get('sections'), list)
            and config['sections'], 'Full exam config requires nonempty sections')
    section_ids = set()
    question_ids = set()
    for section in config['sections']:
        require(isinstance(section, dict), 'Invalid section')
        sid = section.get('id')
        require(isinstance(sid, str) and sid.strip() and sid not in section_ids, 'Invalid/duplicate section id')
        section_ids.add(sid)
        require(isinstance(section.get('title'), str), 'Section title required')
        number(section.get('maxScore'), 'Section maxScore')
        require(isinstance(section.get('puzzles'), list) and section['puzzles'], 'Section puzzles required')
        for question in section['puzzles']:
            require(isinstance(question, dict), 'Invalid question')
            qid = question.get('id')
            require(isinstance(qid, str) and qid.strip() and qid not in question_ids, 'Invalid/duplicate question id')
            question_ids.add(qid)
            require(isinstance(question.get('title'), str), 'Question title required')
            number(question.get('score'), 'Question score')
            question['multiplier'] = number(question.get('multiplier', 0.5), 'Question multiplier', 1)
            require(isinstance(question.get('subtasks'), list) and question['subtasks'], 'Question subtasks required')
            for subtask in question['subtasks']:
                require(isinstance(subtask, dict), 'Invalid subtask')
                number(subtask.get('score'), 'Subtask score')
                require(isinstance(subtask.get('visible'), list)
                        and isinstance(subtask.get('hidden'), list), 'Subtask visible/hidden arrays required')
    return config


def question_mapping(config, path=None):
    positions = {q['id']: i for i, q in enumerate(puzzles(config))}
    if path:
        explicit = json.loads(Path(path).read_text(encoding='utf-8-sig'))
        require(isinstance(explicit, dict), 'Question mapping must be an object')
        for qid, group in explicit.items():
            require(qid in positions and type(group) is int and 1 <= group <= len(positions),
                    'Question mapping requires known IDs and valid one-based groups')
            positions[qid] = group - 1
    require(len(set(positions.values())) == len(positions), 'Question mapping has duplicate groups')
    return positions


def normalize_rows(rows, column, config, positions):
    ordered = puzzles(config)
    output = []
    for row in rows:
        source = parse_puzzle_results(row.get(column), normalize=False)
        require(isinstance(source, (dict, list)), 'Question results must be an object or array')
        entries = source.items() if isinstance(source, dict) else enumerate(source)
        mapped = {}
        for key, value in entries:
            text = str(key)
            candidates = set()
            if text in positions:
                candidates.add(positions[text])
            if text.startswith('Q') and text[1:].isdigit():
                index = int(text[1:]) - 1
                require(0 <= index < len(ordered), f'Unknown question {text}')
                candidates.add(positions[ordered[index]['id']])
            elif text.isdigit():
                index = int(text)
                require(0 <= index < len(ordered), f'Unknown question {text}')
                candidates.add(positions[ordered[index]['id']])
            require(len(candidates) == 1, f'Unknown or ambiguous question {text}')
            destination = str(candidates.pop())
            require(destination not in mapped, f'Duplicate question aliases for {text}')
            require(isinstance(value, dict) and isinstance(value.get('subtasks'), list),
                    f'Invalid results for question {text}')
            expected = next(q for q in ordered if positions[q['id']] == int(destination))
            require(len(value['subtasks']) == len(expected['subtasks']), f'Subtask count mismatch for {text}')
            mapped[destination] = value
        output.append({**row, column: mapped})
    return output


def validate_layout(mapping, config, positions):
    actual = [(m['pidx'], m.get('sidx'), m['type']) for m in mapping
              if m['type'] in ('subtask', 'rule')]
    expected = []
    for question in puzzles(config):
        index = positions[question['id']]
        expected.extend((index, i, 'subtask') for i in range(len(question['subtasks'])))
        expected.append((index, None, 'rule'))
    require(sorted(actual, key=str) == sorted(expected, key=str), 'Sheet layout does not match exam config')
