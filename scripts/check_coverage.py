"""Reject incomplete branch data, including production files absent from reports."""
import json
from pathlib import Path
import sys


def production_files(root):
    return {p.relative_to(root).as_posix() for p in root.rglob('*.py')
            if not any(part.startswith('.') for part in p.relative_to(root).parts)
            and 'tests' not in p.relative_to(root).parts
            and p.name != 'test_db_parse.py'}


def validate(report, root):
    errors = []
    if not report.get('meta', {}).get('branch_coverage'):
        errors.append('Report was not collected with branch coverage')
    files = {name.replace('\\', '/'): data for name, data in report['files'].items()}
    for name in sorted(production_files(root)):
        if name not in files:
            errors.append(f'{name}: absent from coverage report')
            continue
        data = files[name]
        summary = data['summary']
        if (summary['covered_lines'] != summary['num_statements']
                or data['missing_lines']):
            errors.append(f'{name}: missing statements {data["missing_lines"]}')
        if (summary['covered_branches'] != summary['num_branches']
                or data['missing_branches']):
            errors.append(f'{name}: missing branches {data["missing_branches"]}')
    return errors


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    report = json.loads(Path(args[0]).read_text(encoding='utf-8'))
    errors = validate(report, Path.cwd())
    if errors:
        print('\n'.join(errors))
        return 1
    print('PASS: every production statement and branch covered (100%).')
    return 0


if __name__ == '__main__':
    sys.exit(main())
