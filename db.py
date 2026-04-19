import os
import psycopg2
from dotenv import load_dotenv
from utils import pick_column


def fetch_scoreboard_rows(limit=None):
    load_dotenv()
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        cur = conn.cursor()

        cur.execute("SELECT * FROM score_boards LIMIT 0;")
        available_cols = [desc[0] for desc in cur.description]

        student_id_col = pick_column(
            available_cols, ["student_ID", "student_id", "studentId"]
        )
        puzzle_results_col = pick_column(
            available_cols, ["puzzle_results", "puzzleResults", "puzzles", "results"]
        )
        puzzle_amount_col = (
            pick_column(available_cols, ["puzzle_amount"])
            or pick_column(available_cols, ["puzzles_amount"])
            or None
        )
        subtask_amount_col = pick_column(available_cols, ["subtask_amount"])
        id_col = pick_column(available_cols, ["id"])

        selected_cols = []
        for c in (
            student_id_col,
            puzzle_results_col,
            puzzle_amount_col,
            subtask_amount_col,
            id_col,
        ):
            if c and c not in selected_cols:
                selected_cols.append(c)

        if not selected_cols:
            selected_cols = available_cols

        def quote(c):
            return '"' + str(c).replace('"', '""') + '"'

        select_clause = ", ".join(quote(c) for c in selected_cols)
        query = f"SELECT {select_clause} FROM score_boards"
        if limit:
            query += f" LIMIT {int(limit)}"

        cur.execute(query)
        rows = cur.fetchall()
        row_dicts = [dict(zip(selected_cols, row)) for row in rows]
        try:
            cur.close()
        except Exception:
            pass
        return row_dicts, selected_cols
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
