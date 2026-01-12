import base64
import numbers
import sqlite3
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


DB_PATH = "backlog.db"
BACKLOG_TEAMS = ["Team 1", "Team 2"]
DEPENDENCY_TEAMS = ["PC", "BC", "CC", "Integration", "Auth", "Digital"]
SPRINTS = [f"Sprint {i}" for i in range(1, 12)]
PLACEHOLDER_OPTION = "Choose options"


def with_placeholder(options):
    return [PLACEHOLDER_OPTION] + list(options)


def normalize_choice(value):
    return None if value == PLACEHOLDER_OPTION else value


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    with get_conn() as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS backlog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task TEXT NOT NULL,
                task_details TEXT,
                lob TEXT,
                image_blob BLOB,
                theme TEXT NOT NULL,
                evaluation TEXT,
                estimation INTEGER,
                team TEXT,
                sprint TEXT
            );

            CREATE TABLE IF NOT EXISTS dependency (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task TEXT NOT NULL,
                sub_task TEXT,
                team TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS theme (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS evaluation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                note TEXT
            );

            CREATE TABLE IF NOT EXISTS backlog_dependency (
                backlog_id INTEGER NOT NULL,
                dependency_id INTEGER NOT NULL,
                PRIMARY KEY (backlog_id, dependency_id),
                FOREIGN KEY (backlog_id) REFERENCES backlog(id) ON DELETE CASCADE,
                FOREIGN KEY (dependency_id) REFERENCES dependency(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS sub_backlog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backlog_id INTEGER,
                title TEXT NOT NULL,
                note TEXT
            );

            CREATE TABLE IF NOT EXISTS sub_backlog_backlog (
                sub_backlog_id INTEGER NOT NULL,
                backlog_id INTEGER NOT NULL,
                PRIMARY KEY (sub_backlog_id, backlog_id),
                FOREIGN KEY (sub_backlog_id) REFERENCES sub_backlog(id) ON DELETE CASCADE,
                FOREIGN KEY (backlog_id) REFERENCES backlog(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS meeting (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                meeting_datetime TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS meeting_note (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                meeting_id INTEGER,
                meeting_date TEXT,
                topic TEXT,
                note_type TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                FOREIGN KEY (meeting_id) REFERENCES meeting(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS meeting_note_backlog (
                meeting_note_id INTEGER NOT NULL,
                backlog_id INTEGER NOT NULL,
                PRIMARY KEY (meeting_note_id, backlog_id),
                FOREIGN KEY (meeting_note_id) REFERENCES meeting_note(id) ON DELETE CASCADE,
                FOREIGN KEY (backlog_id) REFERENCES backlog(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS meeting_note_dependency (
                meeting_note_id INTEGER NOT NULL,
                dependency_id INTEGER NOT NULL,
                PRIMARY KEY (meeting_note_id, dependency_id),
                FOREIGN KEY (meeting_note_id) REFERENCES meeting_note(id) ON DELETE CASCADE,
                FOREIGN KEY (dependency_id) REFERENCES dependency(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS meeting_note_theme (
                meeting_note_id INTEGER NOT NULL,
                theme_id INTEGER NOT NULL,
                PRIMARY KEY (meeting_note_id, theme_id),
                FOREIGN KEY (meeting_note_id) REFERENCES meeting_note(id) ON DELETE CASCADE,
                FOREIGN KEY (theme_id) REFERENCES theme(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS meeting_note_evaluation (
                meeting_note_id INTEGER NOT NULL,
                evaluation_id INTEGER NOT NULL,
                PRIMARY KEY (meeting_note_id, evaluation_id),
                FOREIGN KEY (meeting_note_id) REFERENCES meeting_note(id) ON DELETE CASCADE,
                FOREIGN KEY (evaluation_id) REFERENCES evaluation(id) ON DELETE CASCADE
            );
            """
        )
        backlog_info = conn.execute("PRAGMA table_info(backlog)").fetchall()
        backlog_columns = [row["name"] for row in backlog_info]
        sub_task_info = next(
            (row for row in backlog_info if row["name"] == "task_details"),
            None,
        )
        if "task" not in backlog_columns:
            conn.execute("ALTER TABLE backlog ADD COLUMN task TEXT NOT NULL DEFAULT ''")
        if "task_details" not in backlog_columns:
            conn.execute("ALTER TABLE backlog ADD COLUMN task_details TEXT")
            if "sub_task" in backlog_columns:
                conn.execute(
                    """
                    UPDATE backlog
                    SET task_details = sub_task
                    WHERE (task_details IS NULL OR TRIM(task_details) = '')
                        AND sub_task IS NOT NULL
                        AND TRIM(sub_task) != ''
                    """
                )
        if "lob" not in backlog_columns:
            conn.execute("ALTER TABLE backlog ADD COLUMN lob TEXT")
        if "image_blob" not in backlog_columns:
            conn.execute("ALTER TABLE backlog ADD COLUMN image_blob BLOB")
        if "evaluation" not in backlog_columns:
            conn.execute("ALTER TABLE backlog ADD COLUMN evaluation TEXT")
        if sub_task_info and sub_task_info["notnull"]:
            conn.executescript(
                """
                CREATE TABLE backlog_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task TEXT NOT NULL,
                    task_details TEXT,
                    lob TEXT,
                    image_blob BLOB,
                    theme TEXT NOT NULL,
                    evaluation TEXT,
                    estimation INTEGER,
                    team TEXT,
                    sprint TEXT
                );
                INSERT INTO backlog_new (id, task, task_details, lob, image_blob, theme, evaluation, estimation, team, sprint)
                SELECT id, task, task_details, lob, image_blob, theme, evaluation, estimation, team, sprint FROM backlog;
                DROP TABLE backlog;
                ALTER TABLE backlog_new RENAME TO backlog;
                """
            )
        if "name" in backlog_columns:
            conn.execute(
                "UPDATE backlog SET task = name WHERE task = '' AND name IS NOT NULL"
            )
        estimation_info = next(
            (row for row in backlog_info if row["name"] == "estimation"), None
        )
        if estimation_info and (
            estimation_info["type"].upper() != "INTEGER" or estimation_info["notnull"]
        ):
            conn.executescript(
                """
                CREATE TABLE backlog_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task TEXT NOT NULL,
                    task_details TEXT,
                    lob TEXT,
                    image_blob BLOB,
                    theme TEXT NOT NULL,
                    evaluation TEXT,
                    estimation INTEGER,
                    team TEXT,
                    sprint TEXT
                );
                INSERT INTO backlog_new (id, task, task_details, lob, image_blob, theme, evaluation, estimation, team, sprint)
                SELECT id, task, task_details, lob, image_blob, theme, evaluation, CAST(estimation AS INTEGER), team, sprint FROM backlog;
                DROP TABLE backlog;
                ALTER TABLE backlog_new RENAME TO backlog;
                """
            )
        backlog_info = conn.execute("PRAGMA table_info(backlog)").fetchall()
        team_info = next((row for row in backlog_info if row["name"] == "team"), None)
        sprint_info = next((row for row in backlog_info if row["name"] == "sprint"), None)
        sub_task_info = next(
            (row for row in backlog_info if row["name"] == "task_details"),
            None,
        )
        estimation_info = next(
            (row for row in backlog_info if row["name"] == "estimation"), None
        )
        if (
            (team_info and team_info["notnull"])
            or (sprint_info and sprint_info["notnull"])
            or (sub_task_info and sub_task_info["notnull"])
            or (estimation_info and estimation_info["type"].upper() != "INTEGER")
            or (estimation_info and estimation_info["notnull"])
        ):
            conn.executescript(
                """
                CREATE TABLE backlog_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task TEXT NOT NULL,
                    task_details TEXT,
                    lob TEXT,
                    image_blob BLOB,
                    theme TEXT NOT NULL,
                    evaluation TEXT,
                    estimation INTEGER,
                    team TEXT,
                    sprint TEXT
                );
                INSERT INTO backlog_new (id, task, task_details, lob, image_blob, theme, evaluation, estimation, team, sprint)
                SELECT id, task, task_details, lob, image_blob, theme, evaluation, CAST(estimation AS INTEGER), team, sprint FROM backlog;
                DROP TABLE backlog;
                ALTER TABLE backlog_new RENAME TO backlog;
                """
            )

        dependency_info = conn.execute("PRAGMA table_info(dependency)").fetchall()
        dependency_columns = [row["name"] for row in dependency_info]
        if "task" not in dependency_columns:
            conn.execute("ALTER TABLE dependency ADD COLUMN task TEXT NOT NULL DEFAULT ''")
        if "sub_task" not in dependency_columns:
            conn.execute("ALTER TABLE dependency ADD COLUMN sub_task TEXT")
        if "name" in dependency_columns and "task" in dependency_columns:
            conn.execute(
                "UPDATE dependency SET task = name WHERE task = '' AND name IS NOT NULL"
            )
        if "name" in dependency_columns or "sprint" in dependency_columns:
            conn.executescript(
                """
                CREATE TABLE dependency_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task TEXT NOT NULL,
                    sub_task TEXT,
                    team TEXT NOT NULL
                );
                INSERT INTO dependency_new (id, task, sub_task, team)
                SELECT id, task, sub_task, team FROM dependency;
                DROP TABLE dependency;
                ALTER TABLE dependency_new RENAME TO dependency;
                """
            )
        evaluation_info = conn.execute("PRAGMA table_info(evaluation)").fetchall()
        evaluation_columns = [row["name"] for row in evaluation_info]
        if "note" not in evaluation_columns:
            conn.execute("ALTER TABLE evaluation ADD COLUMN note TEXT")

        legacy_sub_task = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sub_task'"
        ).fetchone()
        sub_backlog_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sub_backlog'"
        ).fetchone()
        if legacy_sub_task and not sub_backlog_exists:
            conn.execute("ALTER TABLE sub_task RENAME TO sub_backlog")

        sub_backlog_info = conn.execute("PRAGMA table_info(sub_backlog)").fetchall()
        sub_backlog_columns = [row["name"] for row in sub_backlog_info]
        backlog_id_info = next(
            (row for row in sub_backlog_info if row["name"] == "backlog_id"),
            None,
        )
        if backlog_id_info and backlog_id_info["notnull"]:
            conn.executescript(
                """
                CREATE TABLE sub_backlog_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    backlog_id INTEGER,
                    title TEXT NOT NULL,
                    note TEXT
                );
                INSERT INTO sub_backlog_new (id, backlog_id, title, note)
                SELECT id, backlog_id, COALESCE(title, ''), note FROM sub_backlog;
                DROP TABLE sub_backlog;
                ALTER TABLE sub_backlog_new RENAME TO sub_backlog;
                """
            )
            sub_backlog_info = conn.execute("PRAGMA table_info(sub_backlog)").fetchall()
            sub_backlog_columns = [row["name"] for row in sub_backlog_info]
        if "backlog_id" not in sub_backlog_columns:
            conn.execute("ALTER TABLE sub_backlog ADD COLUMN backlog_id INTEGER")
        if "title" not in sub_backlog_columns:
            conn.execute(
                "ALTER TABLE sub_backlog ADD COLUMN title TEXT NOT NULL DEFAULT ''"
            )
        if "note" not in sub_backlog_columns:
            conn.execute("ALTER TABLE sub_backlog ADD COLUMN note TEXT")

        sub_backlog_backlog_info = conn.execute(
            "PRAGMA table_info(sub_backlog_backlog)"
        ).fetchall()
        if not sub_backlog_backlog_info:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sub_backlog_backlog (
                    sub_backlog_id INTEGER NOT NULL,
                    backlog_id INTEGER NOT NULL,
                    PRIMARY KEY (sub_backlog_id, backlog_id),
                    FOREIGN KEY (sub_backlog_id) REFERENCES sub_backlog(id) ON DELETE CASCADE,
                    FOREIGN KEY (backlog_id) REFERENCES backlog(id) ON DELETE CASCADE
                )
                """
            )
        if "backlog_id" in sub_backlog_columns:
            conn.execute(
                """
                INSERT OR IGNORE INTO sub_backlog_backlog (sub_backlog_id, backlog_id)
                SELECT id, backlog_id
                FROM sub_backlog
                WHERE backlog_id IS NOT NULL
                """
            )

        meeting_note_info = conn.execute("PRAGMA table_info(meeting_note)").fetchall()
        meeting_note_columns = [row["name"] for row in meeting_note_info]
        if "meeting_id" not in meeting_note_columns:
            conn.execute("ALTER TABLE meeting_note ADD COLUMN meeting_id INTEGER")
        if "meeting_date" not in meeting_note_columns:
            conn.execute("ALTER TABLE meeting_note ADD COLUMN meeting_date TEXT")
        if "topic" not in meeting_note_columns:
            conn.execute("ALTER TABLE meeting_note ADD COLUMN topic TEXT")
        if "note" not in meeting_note_columns:
            conn.execute("ALTER TABLE meeting_note ADD COLUMN note TEXT NOT NULL DEFAULT ''")
        if "note_type" not in meeting_note_columns:
            conn.execute("ALTER TABLE meeting_note ADD COLUMN note_type TEXT NOT NULL DEFAULT ''")
        if "status" not in meeting_note_columns:
            conn.execute("ALTER TABLE meeting_note ADD COLUMN status TEXT NOT NULL DEFAULT 'open'")
        conn.execute(
            """
            INSERT OR IGNORE INTO theme (name)
            SELECT DISTINCT theme FROM backlog
            WHERE theme IS NOT NULL AND TRIM(theme) != ''
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO evaluation (name)
            SELECT DISTINCT evaluation FROM backlog
            WHERE evaluation IS NOT NULL AND TRIM(evaluation) != ''
            """
        )
        conn.execute("PRAGMA foreign_keys = ON")


def fetch_themes():
    with get_conn() as conn:
        rows = conn.execute("SELECT name FROM theme ORDER BY name").fetchall()
    return [row["name"] for row in rows]

def fetch_evaluations():
    with get_conn() as conn:
        rows = conn.execute("SELECT name FROM evaluation ORDER BY name").fetchall()
    return [row["name"] for row in rows]


def fetch_theme_rows():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                t.id,
                t.name,
                COUNT(b.id) AS backlog_count
            FROM theme t
            LEFT JOIN backlog b ON b.theme = t.name
            GROUP BY t.id, t.name
            ORDER BY t.name
            """
        ).fetchall()
    return rows

def fetch_evaluation_rows():
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM evaluation ORDER BY name").fetchall()
    return rows


def fetch_meeting_notes():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, meeting_id, meeting_date, topic, note_type, note, status
            FROM meeting_note
            ORDER BY
                CASE WHEN meeting_date IS NULL OR TRIM(meeting_date) = '' THEN 1 ELSE 0 END,
                meeting_date DESC,
                id DESC
            """
        ).fetchall()
    return rows


def fetch_todo_meeting_notes(include_completed=False):
    status_filter = ""
    if include_completed:
        status_filter = "AND LOWER(status) IN ('open', 'in-progress', 'completed')"
    else:
        status_filter = "AND LOWER(status) IN ('open', 'in-progress')"
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT id, meeting_id, meeting_date, topic, note_type, note, status
            FROM meeting_note
            WHERE LOWER(note_type) = 'todo'
                {status_filter}
            ORDER BY
                CASE WHEN meeting_date IS NULL OR TRIM(meeting_date) = '' THEN 1 ELSE 0 END,
                meeting_date DESC,
                id DESC
            """
        ).fetchall()
    return rows


def fetch_meeting_notes_for_backlog(backlog_id):
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT mn.id, mn.meeting_id, mn.meeting_date, mn.topic, mn.note_type, mn.note, mn.status
            FROM meeting_note mn
            INNER JOIN meeting_note_backlog mnb ON mnb.meeting_note_id = mn.id
            WHERE mnb.backlog_id = ?
            ORDER BY mn.meeting_date DESC, mn.id DESC
            """,
            (backlog_id,),
        ).fetchall()
    return rows


def fetch_meeting_notes_for_dependency(dependency_id):
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT mn.id, mn.meeting_id, mn.meeting_date, mn.topic, mn.note_type, mn.note, mn.status
            FROM meeting_note mn
            INNER JOIN meeting_note_dependency mnd ON mnd.meeting_note_id = mn.id
            WHERE mnd.dependency_id = ?
            ORDER BY mn.meeting_date DESC, mn.id DESC
            """,
            (dependency_id,),
        ).fetchall()
    return rows


def fetch_meeting_notes_for_theme(theme_id):
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT mn.id, mn.meeting_id, mn.meeting_date, mn.topic, mn.note_type, mn.note, mn.status
            FROM meeting_note mn
            INNER JOIN meeting_note_theme mnt ON mnt.meeting_note_id = mn.id
            WHERE mnt.theme_id = ?
            ORDER BY mn.meeting_date DESC, mn.id DESC
            """,
            (theme_id,),
        ).fetchall()
    return rows


def fetch_meeting_notes_for_evaluation(evaluation_id):
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT mn.id, mn.meeting_id, mn.meeting_date, mn.topic, mn.note_type, mn.note, mn.status
            FROM meeting_note mn
            INNER JOIN meeting_note_evaluation mne ON mne.meeting_note_id = mn.id
            WHERE mne.evaluation_id = ?
            ORDER BY mn.meeting_date DESC, mn.id DESC
            """,
            (evaluation_id,),
        ).fetchall()
    return rows


def fetch_meeting_notes_for_sub_backlog(sub_backlog_id):
    backlog_ids = fetch_backlog_ids_for_sub_backlog(sub_backlog_id)
    if not backlog_ids:
        return []
    placeholders = ",".join(["?"] * len(backlog_ids))
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT mn.id, mn.meeting_id, mn.meeting_date, mn.topic, mn.note_type, mn.note, mn.status
            FROM meeting_note mn
            INNER JOIN meeting_note_backlog mnb ON mnb.meeting_note_id = mn.id
            WHERE mnb.backlog_id IN ({placeholders})
            ORDER BY mn.meeting_date DESC, mn.id DESC
            """,
            tuple(backlog_ids),
        ).fetchall()
    return rows


def render_meeting_notes_table(rows):
    st.markdown("Associated meeting notes")
    if rows:
        notes_df = pd.DataFrame([dict(row) for row in rows])
        st.dataframe(notes_df, width="stretch", hide_index=True)
    else:
        st.info("No associated meeting notes yet.")


def fetch_meetings():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, title, meeting_datetime
            FROM meeting
            ORDER BY meeting_datetime DESC, id DESC
            """
        ).fetchall()
    return rows




def parse_meeting_date(value):
    if not value:
        return datetime.now().date()
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        pass
    try:
        return datetime.strptime(value.split()[0], "%Y-%m-%d").date()
    except ValueError:
        return datetime.now().date()


def fetch_dependencies():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM dependency ORDER BY id"
        ).fetchall()
    return rows


def fetch_backlogs():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                b.id,
                b.task,
                b.task_details,
                b.lob,
                b.image_blob,
                b.theme,
                b.evaluation,
                b.estimation,
                b.team,
                b.sprint,
                COUNT(d.id) AS dependency_count
            FROM backlog b
            LEFT JOIN backlog_dependency bd ON b.id = bd.backlog_id
            LEFT JOIN dependency d ON d.id = bd.dependency_id
            GROUP BY b.id
            ORDER BY b.id
            """
        ).fetchall()
    return rows


def fetch_backlog_dependency_ids(backlog_id):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT dependency_id FROM backlog_dependency WHERE backlog_id = ?",
            (backlog_id,),
        ).fetchall()
    return [row["dependency_id"] for row in rows]


def fetch_backlogs_for_dependency(dependency_id):
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                b.id,
                b.task,
                b.task_details,
                b.theme,
                b.evaluation,
                b.estimation,
                b.team,
                b.sprint
            FROM backlog b
            INNER JOIN backlog_dependency bd ON b.id = bd.backlog_id
            WHERE bd.dependency_id = ?
            ORDER BY b.id
            """,
            (dependency_id,),
        ).fetchall()
    return rows


def fetch_sub_backlogs():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                st.id,
                st.title,
                st.note,
                GROUP_CONCAT(b.task, ' | ') AS backlog_tasks
            FROM sub_backlog st
            LEFT JOIN sub_backlog_backlog sbb ON sbb.sub_backlog_id = st.id
            LEFT JOIN backlog b ON b.id = sbb.backlog_id
            GROUP BY st.id, st.title, st.note
            ORDER BY st.id
            """
        ).fetchall()
    return rows


def fetch_backlog_dependency_rows():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                b.id AS backlog_id,
                b.task AS backlog_task,
                b.task_details AS backlog_task_details,
                b.theme AS backlog_theme,
                b.evaluation AS backlog_evaluation,
                b.team AS backlog_team,
                b.sprint AS backlog_sprint,
                d.id AS dependency_id,
                d.task AS dependency_task,
                d.sub_task AS dependency_sub_task,
                d.team AS dependency_team
            FROM backlog b
            LEFT JOIN backlog_dependency bd ON b.id = bd.backlog_id
            LEFT JOIN dependency d ON d.id = bd.dependency_id
            ORDER BY b.id, d.id
            """
        ).fetchall()
    return rows


def fetch_backlog_sub_backlog_rows():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                b.id AS backlog_id,
                b.task AS backlog_task,
                b.task_details AS backlog_task_details,
                b.theme AS backlog_theme,
                b.evaluation AS backlog_evaluation,
                b.team AS backlog_team,
                b.sprint AS backlog_sprint,
                sb.id AS sub_backlog_id,
                sb.title AS sub_backlog_title,
                sb.note AS sub_backlog_note
            FROM backlog b
            LEFT JOIN sub_backlog_backlog sbb ON b.id = sbb.backlog_id
            LEFT JOIN sub_backlog sb ON sb.id = sbb.sub_backlog_id
            ORDER BY b.id, sb.id
            """
        ).fetchall()
    return rows


def fetch_backlog_sub_backlog_dependency_rows():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                b.id AS backlog_id,
                b.task AS backlog_task,
                b.task_details AS backlog_task_details,
                b.theme AS backlog_theme,
                b.evaluation AS backlog_evaluation,
                b.team AS backlog_team,
                b.sprint AS backlog_sprint,
                sb.id AS sub_backlog_id,
                sb.title AS sub_backlog_title,
                sb.note AS sub_backlog_note,
                d.id AS dependency_id,
                d.task AS dependency_task,
                d.sub_task AS dependency_sub_task,
                d.team AS dependency_team
            FROM backlog b
            LEFT JOIN sub_backlog_backlog sbb ON b.id = sbb.backlog_id
            LEFT JOIN sub_backlog sb ON sb.id = sbb.sub_backlog_id
            LEFT JOIN backlog_dependency bd ON b.id = bd.backlog_id
            LEFT JOIN dependency d ON d.id = bd.dependency_id
            ORDER BY b.id, sb.id, d.id
            """
        ).fetchall()
    return rows


def fetch_backlogs_for_sub_backlog(sub_backlog_id):
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                b.id,
                b.task,
                b.task_details,
                b.lob,
                b.theme,
                b.evaluation,
                b.estimation,
                b.team,
                b.sprint
            FROM backlog b
            INNER JOIN sub_backlog_backlog sbb ON b.id = sbb.backlog_id
            WHERE sbb.sub_backlog_id = ?
            ORDER BY b.id
            """,
            (sub_backlog_id,),
        ).fetchall()
    return rows


def fetch_sub_backlog_ids_for_backlog(backlog_id):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT sub_backlog_id FROM sub_backlog_backlog WHERE backlog_id = ?",
            (backlog_id,),
        ).fetchall()
    return [row["sub_backlog_id"] for row in rows]


def fetch_backlog_ids_for_sub_backlog(sub_backlog_id):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT backlog_id FROM sub_backlog_backlog WHERE sub_backlog_id = ?",
            (sub_backlog_id,),
        ).fetchall()
    return [row["backlog_id"] for row in rows]


def fetch_meeting_note_backlog_ids(meeting_note_id):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT backlog_id FROM meeting_note_backlog WHERE meeting_note_id = ?",
            (meeting_note_id,),
        ).fetchall()
    return [row["backlog_id"] for row in rows]


def fetch_meeting_note_dependency_ids(meeting_note_id):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT dependency_id FROM meeting_note_dependency WHERE meeting_note_id = ?",
            (meeting_note_id,),
        ).fetchall()
    return [row["dependency_id"] for row in rows]


def fetch_meeting_note_theme_ids(meeting_note_id):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT theme_id FROM meeting_note_theme WHERE meeting_note_id = ?",
            (meeting_note_id,),
        ).fetchall()
    return [row["theme_id"] for row in rows]


def fetch_meeting_note_evaluation_ids(meeting_note_id):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT evaluation_id FROM meeting_note_evaluation WHERE meeting_note_id = ?",
            (meeting_note_id,),
        ).fetchall()
    return [row["evaluation_id"] for row in rows]


def insert_dependency(conn, task, sub_task, team):
    cursor = conn.execute(
        "INSERT INTO dependency (task, sub_task, team) VALUES (?, ?, ?)",
        (task, sub_task, team),
    )
    return cursor.lastrowid


def insert_theme(conn, name):
    cursor = conn.execute(
        "INSERT OR IGNORE INTO theme (name) VALUES (?)",
        (name,),
    )
    return cursor.lastrowid

def insert_evaluation(conn, name, note=None):
    cursor = conn.execute(
        "INSERT OR IGNORE INTO evaluation (name, note) VALUES (?, ?)",
        (name, note),
    )
    return cursor.lastrowid


def insert_meeting_note(conn, meeting_id, meeting_date, topic, note_type, note, status="open"):
    cursor = conn.execute(
        """
        INSERT INTO meeting_note (meeting_id, meeting_date, topic, note_type, note, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (meeting_id, meeting_date, topic, note_type, note, status),
    )
    return cursor.lastrowid


def insert_meeting(conn, title, meeting_datetime):
    cursor = conn.execute(
        "INSERT INTO meeting (title, meeting_datetime) VALUES (?, ?)",
        (title, meeting_datetime),
    )
    return cursor.lastrowid


def insert_sub_backlog(conn, title, note):
    cursor = conn.execute(
        "INSERT INTO sub_backlog (title, note) VALUES (?, ?)",
        (title, note),
    )
    return cursor.lastrowid


def upsert_sub_backlog_backlogs(conn, sub_backlog_id, backlog_ids):
    conn.execute(
        "DELETE FROM sub_backlog_backlog WHERE sub_backlog_id = ?",
        (sub_backlog_id,),
    )
    for backlog_id in backlog_ids:
        conn.execute(
            """
            INSERT OR IGNORE INTO sub_backlog_backlog (sub_backlog_id, backlog_id)
            VALUES (?, ?)
            """,
            (sub_backlog_id, backlog_id),
        )


def upsert_backlog_sub_backlogs(conn, backlog_id, sub_backlog_ids):
    conn.execute(
        "DELETE FROM sub_backlog_backlog WHERE backlog_id = ?",
        (backlog_id,),
    )
    for sub_backlog_id in sub_backlog_ids:
        conn.execute(
            """
            INSERT OR IGNORE INTO sub_backlog_backlog (sub_backlog_id, backlog_id)
            VALUES (?, ?)
            """,
            (sub_backlog_id, backlog_id),
        )


def upsert_meeting_note_backlogs(conn, meeting_note_id, backlog_ids):
    conn.execute(
        "DELETE FROM meeting_note_backlog WHERE meeting_note_id = ?",
        (meeting_note_id,),
    )
    for backlog_id in backlog_ids:
        conn.execute(
            "INSERT OR IGNORE INTO meeting_note_backlog (meeting_note_id, backlog_id) VALUES (?, ?)",
            (meeting_note_id, backlog_id),
        )


def upsert_meeting_note_dependencies(conn, meeting_note_id, dependency_ids):
    conn.execute(
        "DELETE FROM meeting_note_dependency WHERE meeting_note_id = ?",
        (meeting_note_id,),
    )
    for dependency_id in dependency_ids:
        conn.execute(
            "INSERT OR IGNORE INTO meeting_note_dependency (meeting_note_id, dependency_id) VALUES (?, ?)",
            (meeting_note_id, dependency_id),
        )


def upsert_meeting_note_themes(conn, meeting_note_id, theme_ids):
    conn.execute(
        "DELETE FROM meeting_note_theme WHERE meeting_note_id = ?",
        (meeting_note_id,),
    )
    for theme_id in theme_ids:
        conn.execute(
            "INSERT OR IGNORE INTO meeting_note_theme (meeting_note_id, theme_id) VALUES (?, ?)",
            (meeting_note_id, theme_id),
        )


def upsert_meeting_note_evaluations(conn, meeting_note_id, evaluation_ids):
    conn.execute(
        "DELETE FROM meeting_note_evaluation WHERE meeting_note_id = ?",
        (meeting_note_id,),
    )
    for evaluation_id in evaluation_ids:
        conn.execute(
            "INSERT OR IGNORE INTO meeting_note_evaluation (meeting_note_id, evaluation_id) VALUES (?, ?)",
            (meeting_note_id, evaluation_id),
        )


def insert_backlog(
    conn,
    task,
    task_details,
    lob,
    image_blob,
    theme,
    evaluation,
    estimation,
    team,
    sprint,
):
    cursor = conn.execute(
        """
        INSERT INTO backlog (task, task_details, lob, image_blob, theme, evaluation, estimation, team, sprint)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task,
            task_details,
            lob,
            image_blob,
            theme,
            evaluation,
            estimation,
            team,
            sprint,
        ),
    )
    return cursor.lastrowid


def upsert_backlog_dependencies(conn, backlog_id, dependency_ids):
    conn.execute(
        "DELETE FROM backlog_dependency WHERE backlog_id = ?",
        (backlog_id,),
    )
    for dep_id in dependency_ids:
        conn.execute(
            "INSERT OR IGNORE INTO backlog_dependency (backlog_id, dependency_id) VALUES (?, ?)",
            (backlog_id, dep_id),
        )


def upsert_dependency_backlogs(conn, dependency_id, backlog_ids):
    conn.execute(
        "DELETE FROM backlog_dependency WHERE dependency_id = ?",
        (dependency_id,),
    )
    for backlog_id in backlog_ids:
        conn.execute(
            "INSERT OR IGNORE INTO backlog_dependency (backlog_id, dependency_id) VALUES (?, ?)",
            (backlog_id, dependency_id),
        )


def dependency_label(dep_row):
    return f"{dep_row['task']} / {dep_row['sub_task'] or ''} [{dep_row['team']}]"


_PASTE_COMPONENT = components.declare_component(
    "paste_image",
    path=str(Path(__file__).parent / "components" / "paste_image"),
)


def paste_image_component(label, key):
    st.caption(label)
    data_url = _PASTE_COMPONENT(key=key, default=None)
    if isinstance(data_url, str) and data_url.startswith("data:image/"):
        try:
            image_bytes = base64.b64decode(data_url.split(",", 1)[1])
        except (ValueError, IndexError):
            image_bytes = None
        if image_bytes:
            st.session_state[f"{key}_data"] = image_bytes
    return st.session_state.get(f"{key}_data")


def backlog_label(backlog_row):
    estimation_value = backlog_row["estimation"]
    estimation_display = "-" if estimation_value is None else estimation_value
    team_display = backlog_row["team"] or "-"
    sprint_display = backlog_row["sprint"] or "-"
    return (
        f"{backlog_row['id']}: {backlog_row['task']} / {backlog_row['task_details'] or ''} "
        f"(est {estimation_display}, {team_display}, {sprint_display})"
    )


def parse_estimation(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None, None
    if isinstance(value, numbers.Number):
        if float(value).is_integer():
            return int(value), None
        return None, "invalid_estimation"
    text = str(value).strip()
    if text == "":
        return None, None
    text = text.replace(",", "")
    try:
        numeric = float(text)
    except ValueError:
        return None, "invalid_estimation"
    if not numeric.is_integer():
        return None, "invalid_estimation"
    return int(numeric), None


init_db()

st.set_page_config(page_title="Backlog Manager", layout="wide")
st.title("Backlog Manager")

st.markdown(
    """
    <style>
    div[role="dialog"] {
        width: 80vw !important;
        max-width: 80vw !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

tab_choice = st.radio(
    "View",
    [
        "Meeting Notes",
        "Todo Notes",
        "Meetings",
        "Themes",
        "Backlog",
        "Sub-backlogs",
        "Evaluations",
        "Dependencies",
        "Sprint x Team",
        "Backlog x Dependencies",
        "Backlog x Sub-backlogs",
        "Backlog x Sub-backlogs x Dependencies",
    ],
    horizontal=True,
    key="active_tab",
    label_visibility="collapsed",
)

if tab_choice == "Backlog":
    themes = fetch_themes()
    evaluations = fetch_evaluations()
    dependency_rows = fetch_dependencies()
    dependency_choices = {dependency_label(row): row["id"] for row in dependency_rows}
    existing_dependency_labels = list(dependency_choices.keys())
    sub_backlog_rows = fetch_sub_backlogs()
    sub_backlog_choices = {
        f"{row['title']} (#{row['id']})": row["id"] for row in sub_backlog_rows
    }
    existing_sub_backlog_labels = list(sub_backlog_choices.keys())
    backlog_rows = fetch_backlogs()
    dependency_df = (
        pd.DataFrame([dict(row) for row in dependency_rows])
        if dependency_rows
        else pd.DataFrame(columns=["id", "task", "sub_task", "team"])
    )

    @st.dialog("Add backlog")
    def add_backlog_dialog():
        add_new_dep_count = st.number_input(
            "New dependency count",
            min_value=0,
            max_value=10,
            value=0,
            step=1,
            key="add_dep_count",
        )
        add_new_sub_backlog_count = st.number_input(
            "New sub-backlog count",
            min_value=0,
            max_value=10,
            value=0,
            step=1,
            key="add_sub_backlog_count",
        )
        with st.form("add_backlog_form"):
            image_col, middle_col, right_col = st.columns(3, gap="large")
            with image_col:
                pasted_image = paste_image_component(
                    "Paste image (optional)",
                    key="add_backlog_image_paste",
                )
                if pasted_image:
                    st.image(pasted_image, caption="Pasted image")
            with middle_col:
                task = st.text_input("Task")
                theme_options = with_placeholder(themes)
                theme_choice = st.selectbox("Theme", theme_options, index=0)
                lob = st.text_input("LOB (optional)")
                team = st.selectbox("Team", with_placeholder(BACKLOG_TEAMS), index=0)
                evaluation_options = with_placeholder(evaluations)
                evaluation_choice = st.selectbox(
                    "Evaluation",
                    evaluation_options,
                    index=0,
                    key="add_evaluation_choice",
                )
            with right_col:
                task_details = st.text_input("Task details (optional)")
                new_theme = st.text_input("New theme (optional)")
                estimation_input_right = st.number_input(
                    "Estimation",
                    min_value=0,
                    step=1,
                    value=0,
                    key="add_estimation_right",
                )
                sprint = st.selectbox("Sprint", with_placeholder(SPRINTS), index=0)
            selected_dependency_labels = st.multiselect(
                "Existing dependencies",
                options=existing_dependency_labels,
                default=[],
                placeholder=PLACEHOLDER_OPTION,
                key="add_existing_deps",
            )
            selected_sub_backlog_labels = st.multiselect(
                "Assign existing sub-backlogs",
                options=existing_sub_backlog_labels,
                default=[],
                placeholder=PLACEHOLDER_OPTION,
                key="add_existing_sub_backlogs",
            )

            new_dependencies = []
            for i in range(add_new_dep_count):
                st.markdown(f"New dependency {i + 1}")
                team_col, task_col, sub_task_col = st.columns(3, gap="large")
                with team_col:
                    dep_team = st.selectbox(
                        f"Dependency team {i + 1}",
                        with_placeholder(DEPENDENCY_TEAMS),
                        key=f"add_dep_team_{i}",
                    )
                with task_col:
                    dep_task = st.text_input(
                        f"Dependency task {i + 1}",
                        key=f"add_dep_task_{i}",
                    )
                with sub_task_col:
                    dep_sub_task = st.text_input(
                        f"Dependency sub-task {i + 1} (optional)",
                        key=f"add_dep_sub_task_{i}",
                    )
                new_dependencies.append((dep_task, dep_sub_task, dep_team))

            new_sub_backlogs = []
            for i in range(add_new_sub_backlog_count):
                st.markdown(f"New sub-backlog {i + 1}")
                title_col, note_col = st.columns(2, gap="large")
                with title_col:
                    title = st.text_input(
                        f"Sub-backlog title {i + 1}",
                        key=f"add_sub_backlog_title_{i}",
                    )
                with note_col:
                    note = st.text_area(
                        f"Sub-backlog note {i + 1} (optional)",
                        height=80,
                        key=f"add_sub_backlog_note_{i}",
                    )
                new_sub_backlogs.append((title, note))

            render_meeting_notes_table([])

            submitted = st.form_submit_button("Add backlog")
            if submitted:
                if not task.strip():
                    st.error("Backlog task is required.")
                else:
                    invalid_dep_team = [
                        dep_task
                        for dep_task, _, dep_team in new_dependencies
                        if dep_task.strip() and dep_team == PLACEHOLDER_OPTION
                    ]
                    if invalid_dep_team:
                        st.error("Dependency team is required for new dependencies.")
                        return
                    theme = new_theme.strip() or normalize_choice(theme_choice)
                    if not theme:
                        st.error("Backlog theme is required.")
                        return
                    evaluation_value = normalize_choice(evaluation_choice)
                    team_value = normalize_choice(team)
                    sprint_value = normalize_choice(sprint)
                    image_bytes = pasted_image
                    estimation_value = int(estimation_input_right)
                    task_details_value = task_details.strip() or None
                    lob_value = lob.strip() or None
                    with get_conn() as conn:
                        insert_theme(conn, theme.strip())
                        backlog_id = insert_backlog(
                            conn,
                            task.strip(),
                            task_details_value,
                            lob_value,
                            image_bytes,
                            theme.strip(),
                            evaluation_value,
                            estimation_value,
                            team_value,
                            sprint_value,
                        )

                        dependency_ids = []
                        for label in selected_dependency_labels:
                            dependency_ids.append(dependency_choices[label])

                        for dep_task, dep_sub_task, dep_team in new_dependencies:
                            if dep_task.strip():
                                dep_id = insert_dependency(
                                    conn,
                                    dep_task.strip(),
                                    dep_sub_task.strip() or None,
                                    dep_team,
                                )
                                dependency_ids.append(dep_id)

                        upsert_backlog_dependencies(conn, backlog_id, dependency_ids)

                        if selected_sub_backlog_labels:
                            sub_backlog_ids = [
                                sub_backlog_choices[label]
                                for label in selected_sub_backlog_labels
                            ]
                            upsert_backlog_sub_backlogs(
                                conn, backlog_id, sub_backlog_ids
                            )

                        for title, note in new_sub_backlogs:
                            if title.strip():
                                sub_backlog_id = insert_sub_backlog(
                                    conn,
                                    title.strip(),
                                    note.strip() or None,
                                )
                                upsert_sub_backlog_backlogs(
                                    conn, sub_backlog_id, [backlog_id]
                                )

                    st.success("Backlog added.")
                    st.session_state.pop("add_backlog_image_paste", None)
                    st.session_state.pop("selected_backlog_ids", None)
                    st.rerun()

    @st.dialog("Edit backlog")
    def edit_backlog_dialog(backlog_row):
        edit_new_dep_count = st.number_input(
            "New dependency count (edit)",
            min_value=0,
            max_value=10,
            value=0,
            step=1,
            key="edit_dep_count",
        )
        edit_new_sub_backlog_count = st.number_input(
            "New sub-backlog count (edit)",
            min_value=0,
            max_value=10,
            value=0,
            step=1,
            key="edit_sub_backlog_count",
        )
        selected_dep_ids = fetch_backlog_dependency_ids(backlog_row["id"])
        selected_dep_labels = [
            dependency_label(row)
            for row in dependency_rows
            if row["id"] in selected_dep_ids
        ]
        with st.form("edit_backlog_form"):
            image_col, middle_col, right_col = st.columns(3, gap="large")
            with image_col:
                current_image = backlog_row["image_blob"]
                if current_image:
                    if isinstance(current_image, memoryview):
                        current_image = current_image.tobytes()
                    st.image(current_image, caption="Current image")
                pasted_replace_image = paste_image_component(
                    "Paste image to replace (optional)",
                    key="edit_backlog_image_paste",
                )
                if pasted_replace_image:
                    st.image(pasted_replace_image, caption="New image preview")
                remove_image = st.checkbox("Remove image", key="remove_backlog_image")
            with middle_col:
                edit_task = st.text_input("Task", value=backlog_row["task"])
                theme_options = with_placeholder(themes)
                theme_value = backlog_row["theme"] or PLACEHOLDER_OPTION
                if theme_value and theme_value not in theme_options:
                    theme_options.append(theme_value)
                default_theme = (
                    theme_options.index(theme_value) if theme_value in theme_options else 0
                )
                edit_theme_choice = st.selectbox(
                    "Theme",
                    theme_options,
                    index=default_theme,
                    key="edit_theme_choice",
                )
                edit_lob = st.text_input(
                    "LOB (optional)",
                    value=backlog_row["lob"] or "",
                )
                team_options = with_placeholder(BACKLOG_TEAMS)
                team_value = backlog_row["team"] or PLACEHOLDER_OPTION
                edit_team = st.selectbox(
                    "Team",
                    team_options,
                    index=team_options.index(team_value) if team_value in team_options else 0,
                )
                evaluation_options = with_placeholder(evaluations)
                evaluation_value = backlog_row["evaluation"] or PLACEHOLDER_OPTION
                if evaluation_value and evaluation_value not in evaluation_options:
                    evaluation_options.append(evaluation_value)
                default_evaluation = (
                    evaluation_options.index(evaluation_value)
                    if evaluation_value in evaluation_options
                    else 0
                )
                edit_evaluation_choice = st.selectbox(
                    "Evaluation",
                    evaluation_options,
                    index=default_evaluation,
                    key="edit_evaluation_choice",
                )
            with right_col:
                edit_task_details = st.text_input(
                    "Task details (optional)",
                    value=backlog_row["task_details"] or "",
                )
                edit_new_theme = st.text_input("New theme (optional)")
                estimation_default = (
                    0 if backlog_row["estimation"] is None else int(backlog_row["estimation"])
                )
                edit_estimation_input = st.number_input(
                    "Estimation",
                    min_value=0,
                    step=1,
                    value=estimation_default,
                    key="edit_estimation_value",
                )
                sprint_options = with_placeholder(SPRINTS)
                sprint_value = backlog_row["sprint"] or PLACEHOLDER_OPTION
                edit_sprint = st.selectbox(
                    "Sprint",
                    sprint_options,
                    index=sprint_options.index(sprint_value) if sprint_value in sprint_options else 0,
                )
            default_dependency_labels = [
                label
                for label in selected_dep_labels
                if label in existing_dependency_labels
            ]
            edit_selected_dependency_labels = st.multiselect(
                "Existing dependencies",
                options=existing_dependency_labels,
                default=default_dependency_labels,
                placeholder=PLACEHOLDER_OPTION,
                key="edit_existing_deps",
            )
            linked_sub_backlog_ids = fetch_sub_backlog_ids_for_backlog(backlog_row["id"])
            default_sub_backlog_labels = [
                label
                for label, sub_id in sub_backlog_choices.items()
                if sub_id in linked_sub_backlog_ids
            ]
            edit_selected_sub_backlog_labels = st.multiselect(
                "Assign existing sub-backlogs",
                options=existing_sub_backlog_labels,
                default=default_sub_backlog_labels,
                placeholder=PLACEHOLDER_OPTION,
                key="edit_existing_sub_backlogs",
            )

            edit_new_dependencies = []
            for i in range(edit_new_dep_count):
                st.markdown(f"New dependency {i + 1}")
                team_col, task_col, sub_task_col = st.columns(3, gap="large")
                with team_col:
                    dep_team = st.selectbox(
                        f"Dependency team {i + 1}",
                        with_placeholder(DEPENDENCY_TEAMS),
                        key=f"edit_dep_team_{i}",
                    )
                with task_col:
                    dep_task = st.text_input(
                        f"Dependency task {i + 1}",
                        key=f"edit_dep_task_{i}",
                    )
                with sub_task_col:
                    dep_sub_task = st.text_input(
                        f"Dependency sub-task {i + 1} (optional)",
                        key=f"edit_dep_sub_task_{i}",
                    )
                edit_new_dependencies.append((dep_task, dep_sub_task, dep_team))

            edit_new_sub_backlogs = []
            for i in range(edit_new_sub_backlog_count):
                st.markdown(f"New sub-backlog {i + 1}")
                title_col, note_col = st.columns(2, gap="large")
                with title_col:
                    title = st.text_input(
                        f"Sub-backlog title {i + 1}",
                        key=f"edit_sub_backlog_title_{i}",
                    )
                with note_col:
                    note = st.text_area(
                        f"Sub-backlog note {i + 1} (optional)",
                        height=80,
                        key=f"edit_sub_backlog_note_{i}",
                    )
                edit_new_sub_backlogs.append((title, note))

            linked_dependencies = [
                row for row in dependency_rows if row["id"] in selected_dep_ids
            ]
            if linked_dependencies:
                linked_dependency_df = pd.DataFrame(
                    [dict(row) for row in linked_dependencies]
                )
                st.markdown("Existing dependencies (table)")
                st.dataframe(
                    linked_dependency_df[["task", "sub_task", "team"]],
                    width="stretch",
                    hide_index=True,
                )
            else:
                st.info("No dependencies linked to this backlog yet.")

            render_meeting_notes_table(
                fetch_meeting_notes_for_backlog(backlog_row["id"])
            )

            updated = st.form_submit_button("Update backlog")
            if updated:
                if not edit_task.strip():
                    st.error("Backlog task is required.")
                else:
                    invalid_dep_team = [
                        dep_task
                        for dep_task, _, dep_team in edit_new_dependencies
                        if dep_task.strip() and dep_team == PLACEHOLDER_OPTION
                    ]
                    if invalid_dep_team:
                        st.error("Dependency team is required for new dependencies.")
                        return
                    edit_theme = edit_new_theme.strip() or normalize_choice(edit_theme_choice)
                    if not edit_theme:
                        st.error("Theme is required.")
                        return
                    edit_evaluation_value = normalize_choice(edit_evaluation_choice)
                    edit_team_value = normalize_choice(edit_team)
                    edit_sprint_value = normalize_choice(edit_sprint)
                    if remove_image:
                        image_bytes = None
                    elif pasted_replace_image:
                        image_bytes = pasted_replace_image
                    else:
                        image_bytes = backlog_row["image_blob"]
                        if isinstance(image_bytes, memoryview):
                            image_bytes = image_bytes.tobytes()
                    edit_estimation_value = int(edit_estimation_input)
                    edit_task_details_value = edit_task_details.strip() or None
                    edit_lob_value = edit_lob.strip() or None
                    with get_conn() as conn:
                        insert_theme(conn, edit_theme.strip())
                        conn.execute(
                            """
                            UPDATE backlog
                            SET task = ?, task_details = ?, lob = ?, image_blob = ?, theme = ?, evaluation = ?, estimation = ?, team = ?, sprint = ?
                            WHERE id = ?
                            """,
                            (
                                edit_task.strip(),
                                edit_task_details_value,
                                edit_lob_value,
                                image_bytes,
                                edit_theme.strip(),
                                edit_evaluation_value,
                                edit_estimation_value,
                                edit_team_value,
                                edit_sprint_value,
                                backlog_row["id"],
                            ),
                        )

                        dependency_ids = []
                        for label in edit_selected_dependency_labels:
                            dependency_ids.append(dependency_choices[label])

                        for dep_task, dep_sub_task, dep_team in edit_new_dependencies:
                            if dep_task.strip():
                                dep_id = insert_dependency(
                                    conn,
                                    dep_task.strip(),
                                    dep_sub_task.strip() or None,
                                    dep_team,
                                )
                                dependency_ids.append(dep_id)

                        upsert_backlog_dependencies(conn, backlog_row["id"], dependency_ids)

                        sub_backlog_ids = [
                            sub_backlog_choices[label]
                            for label in edit_selected_sub_backlog_labels
                        ]
                        upsert_backlog_sub_backlogs(
                            conn, backlog_row["id"], sub_backlog_ids
                        )

                        for title, note in edit_new_sub_backlogs:
                            if title.strip():
                                sub_backlog_id = insert_sub_backlog(
                                    conn,
                                    title.strip(),
                                    note.strip() or None,
                                )
                                upsert_sub_backlog_backlogs(
                                    conn, sub_backlog_id, [backlog_row["id"]]
                                )

                    st.success("Backlog updated.")
                    st.session_state.pop("edit_backlog_image_paste", None)
                    st.rerun()

    @st.dialog("Delete backlog")
    def delete_backlog_dialog(selected_ids, backlog_lookup):
        items = []
        for item_id in selected_ids:
            row = backlog_lookup.get(item_id)
            if row:
                items.append(f"{row['task']} / {row['task_details'] or ''}")
        st.write(f"Delete {len(selected_ids)} backlog item(s)?")
        if items:
            st.write(items)
        if st.button("Confirm delete", type="primary"):
            placeholders = ",".join(["?"] * len(selected_ids))
            with get_conn() as conn:
                conn.execute(
                    f"DELETE FROM backlog WHERE id IN ({placeholders})",
                    tuple(selected_ids),
                )
            st.session_state.pop("selected_backlog_ids", None)
            st.success("Backlog deleted.")
            st.rerun()

    @st.dialog("Merge backlogs")
    def merge_backlog_dialog(selected_ids, backlog_lookup):
        selected_rows = [backlog_lookup.get(item_id) for item_id in selected_ids]
        selected_rows = [row for row in selected_rows if row]
        if len(selected_rows) < 2:
            st.info("Select at least two backlog items to merge.")
            return

        selected_labels = [backlog_label(row) for row in selected_rows]
        st.write("Merging these items:")
        st.write(selected_labels)

        primary_options = with_placeholder([row["id"] for row in selected_rows])
        primary_id = st.selectbox(
            "Merge into",
            options=primary_options,
            format_func=lambda item_id: (
                PLACEHOLDER_OPTION
                if item_id == PLACEHOLDER_OPTION
                else backlog_label(backlog_lookup[item_id])
            ),
            key="merge_primary_id",
        )
        if primary_id == PLACEHOLDER_OPTION:
            st.info("Choose a primary backlog to continue.")
            return
        primary_row = backlog_lookup[primary_id]

        theme_values = {row["theme"] for row in selected_rows if row["theme"]}
        if len(theme_values) > 1:
            st.caption("Themes differ across selected items. Choose the merged theme.")

        combined_dep_ids = set()
        for row in selected_rows:
            combined_dep_ids.update(fetch_backlog_dependency_ids(row["id"]))
        default_dep_labels = [
            dependency_label(row)
            for row in dependency_rows
            if row["id"] in combined_dep_ids
        ]
        default_dep_labels = [
            label for label in default_dep_labels if label in existing_dependency_labels
        ]

        estimation_sum = sum(
            int(row["estimation"]) if row["estimation"] is not None else 0
            for row in selected_rows
        )

        with st.form(f"merge_backlog_form_{primary_id}"):
            merge_col_left, merge_col_mid, merge_col_right = st.columns(3, gap="large")
            with merge_col_left:
                merge_task = st.text_input(
                    "Task",
                    value=primary_row["task"],
                    key=f"merge_task_{primary_id}",
                )
                theme_options = with_placeholder(themes)
                theme_value = primary_row["theme"] or PLACEHOLDER_OPTION
                if theme_value and theme_value not in theme_options:
                    theme_options.append(theme_value)
                merge_theme_choice = st.selectbox(
                    "Theme",
                    theme_options,
                    index=theme_options.index(theme_value) if theme_value in theme_options else 0,
                    key=f"merge_theme_choice_{primary_id}",
                )
                merge_lob = st.text_input(
                    "LOB (optional)",
                    value=primary_row["lob"] or "",
                    key=f"merge_lob_{primary_id}",
                )
                team_options = with_placeholder(BACKLOG_TEAMS)
                team_value = primary_row["team"] or PLACEHOLDER_OPTION
                merge_team = st.selectbox(
                    "Team",
                    team_options,
                    index=team_options.index(team_value) if team_value in team_options else 0,
                    key=f"merge_team_{primary_id}",
                )
                evaluation_options = with_placeholder(evaluations)
                evaluation_value = primary_row["evaluation"] or PLACEHOLDER_OPTION
                if evaluation_value and evaluation_value not in evaluation_options:
                    evaluation_options.append(evaluation_value)
                merge_evaluation_choice = st.selectbox(
                    "Evaluation",
                    evaluation_options,
                    index=(
                        evaluation_options.index(evaluation_value)
                        if evaluation_value in evaluation_options
                        else 0
                    ),
                    key=f"merge_evaluation_choice_{primary_id}",
                )
            with merge_col_mid:
                merge_task_details = st.text_input(
                    "Task details (optional)",
                    value=primary_row["task_details"] or "",
                    key=f"merge_task_details_{primary_id}",
                )
                merge_new_theme = st.text_input(
                    "New theme (optional)",
                    key=f"merge_new_theme_{primary_id}",
                )
                merge_estimation = st.number_input(
                    "Estimation",
                    min_value=0,
                    step=1,
                    value=estimation_sum,
                    key=f"merge_estimation_{primary_id}",
                )
                sprint_options = with_placeholder(SPRINTS)
                sprint_value = primary_row["sprint"] or PLACEHOLDER_OPTION
                merge_sprint = st.selectbox(
                    "Sprint",
                    sprint_options,
                    index=sprint_options.index(sprint_value) if sprint_value in sprint_options else 0,
                    key=f"merge_sprint_{primary_id}",
                )
            with merge_col_right:
                merge_selected_dependency_labels = st.multiselect(
                    "Existing dependencies",
                    options=existing_dependency_labels,
                    default=default_dep_labels,
                    placeholder=PLACEHOLDER_OPTION,
                    key=f"merge_existing_deps_{primary_id}",
                )

            merge_submit = st.form_submit_button("Merge backlogs")
            if merge_submit:
                if not merge_task.strip():
                    st.error("Backlog task is required.")
                    return
                merge_theme = merge_new_theme.strip() or normalize_choice(merge_theme_choice)
                if not merge_theme:
                    st.error("Theme is required.")
                    return

                merge_evaluation_value = normalize_choice(merge_evaluation_choice)
                merge_team_value = normalize_choice(merge_team)
                merge_sprint_value = normalize_choice(merge_sprint)
                merge_task_details_value = merge_task_details.strip() or None
                merge_lob_value = merge_lob.strip() or None
                merge_estimation_value = int(merge_estimation)
                image_bytes = primary_row["image_blob"]
                if isinstance(image_bytes, memoryview):
                    image_bytes = image_bytes.tobytes()

                with get_conn() as conn:
                    insert_theme(conn, merge_theme.strip())
                    conn.execute(
                        """
                        UPDATE backlog
                        SET task = ?, task_details = ?, lob = ?, image_blob = ?, theme = ?, evaluation = ?, estimation = ?, team = ?, sprint = ?
                        WHERE id = ?
                        """,
                        (
                            merge_task.strip(),
                            merge_task_details_value,
                            merge_lob_value,
                            image_bytes,
                            merge_theme.strip(),
                            merge_evaluation_value,
                            merge_estimation_value,
                            merge_team_value,
                            merge_sprint_value,
                            primary_row["id"],
                        ),
                    )

                    dependency_ids = [
                        dependency_choices[label]
                        for label in merge_selected_dependency_labels
                    ]
                    upsert_backlog_dependencies(conn, primary_row["id"], dependency_ids)

                    delete_ids = [
                        row["id"] for row in selected_rows if row["id"] != primary_row["id"]
                    ]
                    if delete_ids:
                        placeholders = ",".join(["?"] * len(delete_ids))
                        conn.execute(
                            f"DELETE FROM backlog WHERE id IN ({placeholders})",
                            tuple(delete_ids),
                        )

                st.success("Backlogs merged.")
                st.session_state.pop("selected_backlog_ids", None)
                st.rerun()

    @st.dialog("Split backlog")
    def split_backlog_dialog(backlog_row):
        current_dep_ids = fetch_backlog_dependency_ids(backlog_row["id"])
        current_dep_labels = [
            dependency_label(row)
            for row in fetch_dependencies()
            if row["id"] in current_dep_ids
        ]
        if current_dep_labels:
            st.caption("Dependencies will be copied to all split items.")
            st.write(", ".join(current_dep_labels))
        else:
            st.caption("No dependencies linked to this backlog.")

        original_estimation = backlog_row["estimation"]
        original_display = "-" if original_estimation is None else original_estimation
        st.caption(f"Original estimation: {original_display}")

        split_count = st.number_input(
            "Number of split items",
            min_value=2,
            max_value=10,
            value=2,
            step=1,
            key="split_count_dialog",
        )

        with st.form("split_backlog_form_dialog"):
            split_items = []
            for i in range(split_count):
                st.markdown(f"Split item {i + 1}")
                task_col, task_details_col, est_col = st.columns(3, gap="large")
                with task_col:
                    item_task = st.text_input(
                        f"Task {i + 1}",
                        value=backlog_row["task"],
                        key=f"split_task_dialog_{i}",
                    )
                default_task_details = backlog_row["task_details"]
                if default_task_details:
                    default_task_details = f"{default_task_details} ({i + 1})"
                else:
                    default_task_details = f"Part {i + 1}"
                with task_details_col:
                    item_task_details = st.text_input(
                        f"Task details {i + 1} (optional)",
                        value=default_task_details,
                        key=f"split_task_details_dialog_{i}",
                    )
                with est_col:
                    item_estimation = st.number_input(
                        f"Estimation {i + 1}",
                        min_value=0,
                        step=1,
                        key=f"split_est_dialog_{i}",
                    )
                split_items.append((item_task, item_task_details, item_estimation))

            split_submit = st.form_submit_button("Split backlog")
            if split_submit:
                invalid = [item for item in split_items if not item[0].strip()]
                if invalid:
                    st.error("All split items need a task.")
                else:
                    if backlog_row["estimation"] is None:
                        st.error("Estimation is required to split this backlog.")
                        return
                    total = sum(item[2] for item in split_items)
                    if total != int(backlog_row["estimation"]):
                        st.error(
                            f"Total estimation ({total}) must match original "
                            f"({backlog_row['estimation']})."
                        )
                        return

                    with get_conn() as conn:
                        new_ids = []
                        image_bytes = backlog_row["image_blob"]
                        if isinstance(image_bytes, memoryview):
                            image_bytes = image_bytes.tobytes()
                        for item_task, item_task_details, item_estimation in split_items:
                            new_id = insert_backlog(
                                conn,
                                item_task.strip(),
                                item_task_details.strip(),
                                backlog_row["lob"],
                                image_bytes,
                                backlog_row["theme"],
                                backlog_row["evaluation"],
                                item_estimation,
                                backlog_row["team"],
                                backlog_row["sprint"],
                            )
                            new_ids.append(new_id)

                        for new_id in new_ids:
                            for dep_id in current_dep_ids:
                                conn.execute(
                                    """
                                    INSERT OR IGNORE INTO backlog_dependency
                                    (backlog_id, dependency_id)
                                    VALUES (?, ?)
                                    """,
                                    (new_id, dep_id),
                                )

                        conn.execute(
                            "DELETE FROM backlog WHERE id = ?",
                            (backlog_row["id"],),
                        )

                    st.success("Backlog split completed.")
                    st.session_state.pop("selected_backlog_ids", None)
                    st.rerun()

    with st.expander("Import CSV"):
        backlog_file = st.file_uploader(
            "CSV file (Backlog)",
            type=["csv"],
            key="backlog_csv_file",
        )
        if backlog_file:
            try:
                backlog_csv = pd.read_csv(backlog_file)
            except Exception as exc:
                st.error(f"Failed to read CSV: {exc}")
            else:
                st.dataframe(backlog_csv.head(), width="stretch")
                columns = with_placeholder(backlog_csv.columns)
                map_task = st.selectbox(
                    "Map: task (required)",
                    columns,
                    key="map_backlog_task",
                )
                map_task_details = st.selectbox(
                    "Map: task_details (optional)",
                    columns,
                    key="map_backlog_task_details",
                )
                map_lob = st.selectbox(
                    "Map: lob (optional)",
                    columns,
                    key="map_backlog_lob",
                )
                map_theme = st.selectbox(
                    "Map: theme (required)",
                    columns,
                    key="map_backlog_theme",
                )
                map_evaluation = st.selectbox(
                    "Map: evaluation (optional)",
                    columns,
                    key="map_backlog_evaluation",
                )
                map_estimation = st.selectbox(
                    "Map: estimation (optional)",
                    columns,
                    key="map_backlog_estimation",
                )
                map_team = st.selectbox(
                    "Map: team (optional)",
                    columns,
                    key="map_backlog_team",
                )
                map_sprint = st.selectbox(
                    "Map: sprint (optional)",
                    columns,
                    key="map_backlog_sprint",
                )
                if st.button("Import backlog", key="import_backlog_btn"):
                    missing = []
                    if map_task == PLACEHOLDER_OPTION:
                        missing.append("task")
                    if map_theme == PLACEHOLDER_OPTION:
                        missing.append("theme")
                    if missing:
                        st.error(f"Required mappings missing: {', '.join(missing)}")
                    else:
                        imported = 0
                        skipped = 0
                        skip_reasons = {
                            "missing_task": 0,
                            "missing_theme": 0,
                            "invalid_estimation": 0,
                        }

                        def get_cell(row, column):
                            if not column or column == PLACEHOLDER_OPTION:
                                return None
                            value = row[column]
                            if pd.isna(value):
                                return None
                            text = str(value).strip()
                            return text if text != "" else None

                        with get_conn() as conn:
                            for _, row in backlog_csv.iterrows():
                                task_value = get_cell(row, map_task)
                                if not task_value:
                                    skipped += 1
                                    skip_reasons["missing_task"] += 1
                                    continue
                                theme_value = get_cell(row, map_theme)
                                if not theme_value:
                                    skipped += 1
                                    skip_reasons["missing_theme"] += 1
                                    continue
                                task_details_value = get_cell(row, map_task_details)
                                lob_value = get_cell(row, map_lob)
                                evaluation_value = get_cell(row, map_evaluation)
                                team_value = get_cell(row, map_team)
                                sprint_value = get_cell(row, map_sprint)
                                estimation_value = None
                                estimation_cell = (
                                    row[map_estimation]
                                    if map_estimation != PLACEHOLDER_OPTION
                                    else None
                                )
                                estimation_value, err = parse_estimation(estimation_cell)
                                if err:
                                    skipped += 1
                                    skip_reasons["invalid_estimation"] += 1
                                    continue
                                insert_theme(conn, theme_value)
                                if evaluation_value:
                                    insert_evaluation(conn, evaluation_value)
                                insert_backlog(
                                    conn,
                                    task_value,
                                    task_details_value,
                                    lob_value,
                                    None,
                                    theme_value,
                                    evaluation_value,
                                    estimation_value,
                                    team_value,
                                    sprint_value,
                                )
                                imported += 1
                        st.success(f"Imported {imported} rows. Skipped {skipped} rows.")
                        if skipped:
                            st.info(
                                "Skipped: "
                                f"missing task={skip_reasons['missing_task']}, "
                                f"missing theme={skip_reasons['missing_theme']}, "
                                f"invalid estimation={skip_reasons['invalid_estimation']}"
                            )
                        st.rerun()

    st.subheader("Backlog list")
    backlog_filter_row1 = st.columns(4, gap="small")
    with backlog_filter_row1[0]:
        backlog_task_filter = st.text_input(
            "Task (filter)",
            key="backlog_task_filter",
        )
    with backlog_filter_row1[1]:
        backlog_task_details_filter = st.text_input(
            "Task details (filter)",
            key="backlog_task_details_filter",
        )
    with backlog_filter_row1[2]:
        backlog_lob_filter = st.text_input(
            "LOB (filter)",
            key="backlog_lob_filter",
        )
    with backlog_filter_row1[3]:
        backlog_theme_filter = st.selectbox(
            "Theme (filter)",
            with_placeholder(themes),
            index=0,
            key="backlog_theme_filter",
        )
    backlog_filter_row2 = st.columns(4, gap="small")
    with backlog_filter_row2[0]:
        backlog_team_filter = st.selectbox(
            "Team (filter)",
            with_placeholder(BACKLOG_TEAMS),
            index=0,
            key="backlog_team_filter",
        )
    with backlog_filter_row2[1]:
        backlog_sprint_filter = st.selectbox(
            "Sprint (filter)",
            with_placeholder(SPRINTS),
            index=0,
            key="backlog_sprint_filter",
        )
    with backlog_filter_row2[2]:
        backlog_evaluation_filter = st.selectbox(
            "Evaluation (filter)",
            with_placeholder(evaluations),
            index=0,
            key="backlog_evaluation_filter",
        )
    with backlog_filter_row2[3]:
        backlog_search = st.text_input(
            "Search",
            key="backlog_search",
            help="Filter by task/task details/lob/theme/evaluation",
        )
    if backlog_rows:
        backlog_df = pd.DataFrame([dict(row) for row in backlog_rows])
        filtered_backlog_df = backlog_df.copy()
        if backlog_task_filter.strip():
            query = backlog_task_filter.strip()
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["task"]
                .fillna("")
                .str.contains(query, case=False, na=False)
            ]
        if backlog_task_details_filter.strip():
            query = backlog_task_details_filter.strip()
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["task_details"]
                .fillna("")
                .str.contains(query, case=False, na=False)
            ]
        if backlog_lob_filter.strip():
            query = backlog_lob_filter.strip()
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["lob"]
                .fillna("")
                .str.contains(query, case=False, na=False)
            ]
        if backlog_team_filter != PLACEHOLDER_OPTION:
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["team"] == backlog_team_filter
            ]
        if backlog_sprint_filter != PLACEHOLDER_OPTION:
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["sprint"] == backlog_sprint_filter
            ]
        if backlog_theme_filter != PLACEHOLDER_OPTION:
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["theme"] == backlog_theme_filter
            ]
        if backlog_evaluation_filter != PLACEHOLDER_OPTION:
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["evaluation"] == backlog_evaluation_filter
            ]
        if backlog_search.strip():
            query = backlog_search.strip().lower()
            searchable = (
                filtered_backlog_df[["task", "task_details", "lob", "theme", "evaluation"]]
                .fillna("")
                .agg(" ".join, axis=1)
                .str.lower()
            )
            filtered_backlog_df = filtered_backlog_df[searchable.str.contains(query)]
        display_df = filtered_backlog_df.drop(columns=["image_blob"], errors="ignore")
        selection = st.dataframe(
            display_df,
            width="stretch",
            on_select="rerun",
            selection_mode="multi-row",
        )
        if selection and selection.selection.rows:
            selected_ids = [
                int(display_df.iloc[index]["id"])
                for index in selection.selection.rows
            ]
            st.session_state["selected_backlog_ids"] = selected_ids
        else:
            st.session_state.pop("selected_backlog_ids", None)
    else:
        st.info("No backlog items yet.")

    backlog_by_id = {row["id"]: row for row in backlog_rows}
    selected_ids = st.session_state.get("selected_backlog_ids", [])
    selected_backlog = backlog_by_id.get(selected_ids[0]) if len(selected_ids) == 1 else None

    if selected_ids:
        if len(selected_ids) == 1 and selected_backlog:
            st.caption(f"Selected: {selected_backlog['task']}")
        else:
            st.caption(f"Selected: {len(selected_ids)} items")
    else:
        st.caption("Selected: none")

    if len(selected_ids) > 1:
        with st.form("bulk_assign_backlog_dependencies_form"):
            st.caption("Bulk assign dependencies (replaces existing assignments).")
            bulk_dependencies = st.multiselect(
                "Assign dependencies",
                existing_dependency_labels,
                key="bulk_backlog_dependencies",
                placeholder=PLACEHOLDER_OPTION,
            )
            bulk_submit = st.form_submit_button("Apply dependencies")
            if bulk_submit:
                dependency_ids = [
                    dependency_choices[label] for label in bulk_dependencies
                ]
                with get_conn() as conn:
                    for backlog_id in selected_ids:
                        upsert_backlog_dependencies(conn, backlog_id, dependency_ids)
                st.success("Dependencies updated.")
                st.rerun()

        with st.form("bulk_assign_backlog_sub_backlogs_form"):
            st.caption("Bulk assign sub-backlogs (replaces existing assignments).")
            bulk_sub_backlogs = st.multiselect(
                "Assign sub-backlogs",
                existing_sub_backlog_labels,
                key="bulk_backlog_sub_backlogs",
                placeholder=PLACEHOLDER_OPTION,
            )
            bulk_sub_submit = st.form_submit_button("Apply sub-backlogs")
            if bulk_sub_submit:
                sub_backlog_ids = [
                    sub_backlog_choices[label] for label in bulk_sub_backlogs
                ]
                with get_conn() as conn:
                    for backlog_id in selected_ids:
                        upsert_backlog_sub_backlogs(conn, backlog_id, sub_backlog_ids)
                st.success("Sub-backlogs updated.")
                st.rerun()

        with st.form("bulk_assign_backlog_evaluation_form"):
            st.caption("Bulk assign evaluation (sets the same value for all selected backlogs).")
            bulk_evaluation = st.selectbox(
                "Assign evaluation",
                with_placeholder(evaluations),
                key="bulk_backlog_evaluation",
            )
            eval_submit = st.form_submit_button("Apply evaluation")
            if eval_submit:
                evaluation_value = normalize_choice(bulk_evaluation)
                with get_conn() as conn:
                    placeholders = ",".join(["?"] * len(selected_ids))
                    conn.execute(
                        f"UPDATE backlog SET evaluation = ? WHERE id IN ({placeholders})",
                        (evaluation_value, *selected_ids),
                    )
                st.success("Evaluation updated.")
                st.rerun()

    action_cols = st.columns(5, gap="small")
    with action_cols[0]:
        if st.button("Add backlog"):
            st.session_state.pop("add_backlog_image_paste_data", None)
            add_backlog_dialog()
    with action_cols[1]:
        edit_disabled = selected_backlog is None
        if st.button("Edit selected backlog", disabled=edit_disabled):
            st.session_state.pop("edit_backlog_image_paste", None)
            st.session_state.pop("edit_backlog_image_paste_data", None)
            st.session_state.pop("remove_backlog_image", None)
            edit_backlog_dialog(selected_backlog)
    with action_cols[2]:
        split_disabled = selected_backlog is None
        if st.button("Split selected backlog", disabled=split_disabled):
            split_backlog_dialog(selected_backlog)
    with action_cols[3]:
        merge_disabled = len(selected_ids) < 2
        if st.button("Merge selected backlogs", disabled=merge_disabled):
            merge_backlog_dialog(selected_ids, backlog_by_id)
    with action_cols[4]:
        delete_disabled = not selected_ids
        if st.button("Delete selected backlog", disabled=delete_disabled):
            delete_backlog_dialog(selected_ids, backlog_by_id)

    if backlog_rows and not selected_ids:
        st.info("Select backlog items from the list to edit or delete.")
    elif backlog_rows and len(selected_ids) > 1:
        st.info("Multiple items selected. Edit/Split are disabled; Merge/Delete are enabled.")

if tab_choice == "Dependencies":
    dependency_rows = fetch_dependencies()
    backlog_rows = fetch_backlogs()
    backlog_choices = {backlog_label(row): row["id"] for row in backlog_rows}
    backlog_labels = list(backlog_choices.keys())

    @st.dialog("Add dependency")
    def add_dependency_dialog():
        with st.form("add_dependency_form"):
            team_col, task_col, sub_task_col = st.columns(3, gap="large")
            with team_col:
                dep_team = st.selectbox(
                    "Team",
                    with_placeholder(DEPENDENCY_TEAMS),
                    index=0,
                )
            with task_col:
                dep_task = st.text_input("Task")
            with sub_task_col:
                dep_sub_task = st.text_input("Sub-task (optional)")

            render_meeting_notes_table([])

            dep_submitted = st.form_submit_button("Add dependency")
            if dep_submitted:
                if not dep_task.strip():
                    st.error("Dependency task is required.")
                elif dep_team == PLACEHOLDER_OPTION:
                    st.error("Dependency team is required.")
                else:
                    with get_conn() as conn:
                        insert_dependency(
                            conn,
                            dep_task.strip(),
                            dep_sub_task.strip() or None,
                            dep_team,
                        )
                    st.success("Dependency added.")
                    st.session_state.pop("selected_dependency_ids", None)
                    st.rerun()

    @st.dialog("Edit dependency")
    def edit_dependency_dialog(dep_row):
        with st.form("edit_dependency_form"):
            team_col, task_col, sub_task_col = st.columns(3, gap="large")
            with team_col:
                dep_team_options = with_placeholder(DEPENDENCY_TEAMS)
                edit_dep_team = st.selectbox(
                    "Team",
                    dep_team_options,
                    index=dep_team_options.index(dep_row["team"])
                    if dep_row["team"] in dep_team_options
                    else 0,
                )
            with task_col:
                edit_dep_task = st.text_input("Task", value=dep_row["task"])
            with sub_task_col:
                edit_dep_sub_task = st.text_input(
                    "Sub-task (optional)",
                    value=dep_row["sub_task"] or "",
                )
            render_meeting_notes_table(
                fetch_meeting_notes_for_dependency(dep_row["id"])
            )
            dep_updated = st.form_submit_button("Update dependency")
            if dep_updated:
                if not edit_dep_task.strip():
                    st.error("Dependency task is required.")
                elif edit_dep_team == PLACEHOLDER_OPTION:
                    st.error("Dependency team is required.")
                else:
                    with get_conn() as conn:
                        conn.execute(
                            """
                            UPDATE dependency
                            SET task = ?, sub_task = ?, team = ?
                            WHERE id = ?
                            """,
                            (
                                edit_dep_task.strip(),
                                edit_dep_sub_task.strip() or None,
                                edit_dep_team,
                                dep_row["id"],
                            ),
                        )
                    st.success("Dependency updated.")
                    st.rerun()

    @st.dialog("Delete dependency")
    def delete_dependency_dialog(selected_ids, dependency_lookup):
        items = []
        for item_id in selected_ids:
            row = dependency_lookup.get(item_id)
            if row:
                items.append(f"{row['task']} / {row['sub_task'] or ''}")
        st.write(f"Delete {len(selected_ids)} dependency item(s)?")
        if items:
            st.write(items)
        if st.button("Confirm delete", type="primary"):
            placeholders = ",".join(["?"] * len(selected_ids))
            with get_conn() as conn:
                conn.execute(
                    f"DELETE FROM dependency WHERE id IN ({placeholders})",
                    tuple(selected_ids),
                )
            st.session_state.pop("selected_dependency_ids", None)
            st.success("Dependency deleted.")
            st.rerun()

    @st.dialog("Dependency details")
    def dependency_detail_dialog(dep_row):
        st.write(f"Task: {dep_row['task']}")
        st.write(f"Sub-task: {dep_row['sub_task'] or ''}")
        st.write(f"Team: {dep_row['team']}")

        linked_rows = fetch_backlogs_for_dependency(dep_row["id"])
        st.subheader("Linked backlogs")
        if linked_rows:
            st.dataframe([dict(row) for row in linked_rows], width="stretch")
        else:
            st.info("No linked backlogs.")

        if st.button("Close"):
            st.session_state["show_dependency_detail"] = False
            st.rerun()

    with st.expander("Import CSV"):
        dep_file = st.file_uploader(
            "CSV file (Dependency)",
            type=["csv"],
            key="dependency_csv_file",
        )
        if dep_file:
            try:
                dep_csv = pd.read_csv(dep_file)
            except Exception as exc:
                st.error(f"Failed to read CSV: {exc}")
            else:
                st.dataframe(dep_csv.head(), width="stretch")
                columns = with_placeholder(dep_csv.columns)
                map_task = st.selectbox(
                    "Map: task (required)",
                    columns,
                    key="map_dependency_task",
                )
                map_sub_task = st.selectbox(
                    "Map: sub_task (optional)",
                    columns,
                    key="map_dependency_sub_task",
                )
                map_team = st.selectbox(
                    "Map: team (required)",
                    columns,
                    key="map_dependency_team",
                )
                if st.button("Import dependency", key="import_dependency_btn"):
                    missing = []
                    if map_task == PLACEHOLDER_OPTION:
                        missing.append("task")
                    if map_team == PLACEHOLDER_OPTION:
                        missing.append("team")
                    if missing:
                        st.error(f"Required mappings missing: {', '.join(missing)}")
                    else:
                        imported = 0
                        skipped = 0
                        skip_reasons = {"missing_task": 0, "missing_team": 0}

                        def get_cell(row, column):
                            if not column or column == PLACEHOLDER_OPTION:
                                return None
                            value = row[column]
                            if pd.isna(value):
                                return None
                            text = str(value).strip()
                            return text if text != "" else None

                        with get_conn() as conn:
                            for _, row in dep_csv.iterrows():
                                task_value = get_cell(row, map_task)
                                if not task_value:
                                    skipped += 1
                                    skip_reasons["missing_task"] += 1
                                    continue
                                team_value = get_cell(row, map_team)
                                if not team_value:
                                    skipped += 1
                                    skip_reasons["missing_team"] += 1
                                    continue
                                sub_task_value = get_cell(row, map_sub_task)
                                insert_dependency(
                                    conn,
                                    task_value,
                                    sub_task_value,
                                    team_value,
                                )
                                imported += 1
                        st.success(f"Imported {imported} rows. Skipped {skipped} rows.")
                        if skipped:
                            st.info(
                                "Skipped: "
                                f"missing task={skip_reasons['missing_task']}, "
                                f"missing team={skip_reasons['missing_team']}"
                            )
                        st.rerun()

    st.subheader("Dependency list")
    dependency_filter_cols = st.columns(4, gap="small")
    with dependency_filter_cols[0]:
        dependency_task_filter = st.text_input(
            "Task (filter)",
            key="dependency_task_filter",
        )
    with dependency_filter_cols[1]:
        dependency_sub_task_filter = st.text_input(
            "Sub-task (filter)",
            key="dependency_sub_task_filter",
        )
    with dependency_filter_cols[2]:
        dependency_team_filter = st.selectbox(
            "Team (filter)",
            with_placeholder(DEPENDENCY_TEAMS),
            index=0,
            key="dependency_team_filter",
        )
    with dependency_filter_cols[3]:
        dependency_search = st.text_input(
            "Search",
            key="dependency_search",
            help="Filter by task/sub-task/team",
        )
    if dependency_rows:
        dependency_df = pd.DataFrame([dict(row) for row in dependency_rows])
        filtered_dependency_df = dependency_df.copy()
        if dependency_task_filter.strip():
            query = dependency_task_filter.strip()
            filtered_dependency_df = filtered_dependency_df[
                filtered_dependency_df["task"]
                .fillna("")
                .str.contains(query, case=False, na=False)
            ]
        if dependency_sub_task_filter.strip():
            query = dependency_sub_task_filter.strip()
            filtered_dependency_df = filtered_dependency_df[
                filtered_dependency_df["sub_task"]
                .fillna("")
                .str.contains(query, case=False, na=False)
            ]
        if dependency_team_filter != PLACEHOLDER_OPTION:
            filtered_dependency_df = filtered_dependency_df[
                filtered_dependency_df["team"] == dependency_team_filter
            ]
        if dependency_search.strip():
            query = dependency_search.strip().lower()
            searchable = (
                filtered_dependency_df[["task", "sub_task", "team"]]
                .fillna("")
                .agg(" ".join, axis=1)
                .str.lower()
            )
            filtered_dependency_df = filtered_dependency_df[
                searchable.str.contains(query)
            ]
        selection = st.dataframe(
            filtered_dependency_df,
            width="stretch",
            on_select="rerun",
            selection_mode="multi-row",
        )
        if selection and selection.selection.rows:
            selected_ids = [
                int(filtered_dependency_df.iloc[index]["id"])
                for index in selection.selection.rows
            ]
            st.session_state["selected_dependency_ids"] = selected_ids
        else:
            st.session_state.pop("selected_dependency_ids", None)
            st.session_state["show_dependency_detail"] = False
    else:
        st.info("No dependencies yet.")

    dependency_by_id = {row["id"]: row for row in dependency_rows}
    selected_ids = st.session_state.get("selected_dependency_ids", [])
    selected_dependency = (
        dependency_by_id.get(selected_ids[0]) if len(selected_ids) == 1 else None
    )

    if selected_ids:
        if len(selected_ids) == 1 and selected_dependency:
            st.caption(f"Selected: {selected_dependency['task']}")
        else:
            st.caption(f"Selected: {len(selected_ids)} items")
    else:
        st.caption("Selected: none")

    if len(selected_ids) > 1:
        with st.form("bulk_assign_dependency_backlogs_form"):
            st.caption("Bulk assign backlogs (replaces existing assignments).")
            bulk_backlogs = st.multiselect(
                "Assign backlogs",
                backlog_labels,
                key="bulk_dependency_backlogs",
                placeholder=PLACEHOLDER_OPTION,
            )
            bulk_submit = st.form_submit_button("Apply backlogs")
            if bulk_submit:
                backlog_ids = [backlog_choices[label] for label in bulk_backlogs]
                with get_conn() as conn:
                    for dependency_id in selected_ids:
                        upsert_dependency_backlogs(conn, dependency_id, backlog_ids)
                st.success("Backlogs updated.")
                st.rerun()

    action_cols = st.columns(4, gap="small")
    with action_cols[0]:
        if st.button("Add dependency"):
            add_dependency_dialog()
    with action_cols[1]:
        edit_disabled = selected_dependency is None
        if st.button("Edit selected dependency", disabled=edit_disabled):
            edit_dependency_dialog(selected_dependency)
    with action_cols[2]:
        delete_disabled = not selected_ids
        if st.button("Delete selected dependency", disabled=delete_disabled):
            delete_dependency_dialog(selected_ids, dependency_by_id)

    if dependency_rows and not selected_ids:
        st.info("Select dependencies from the list to edit or delete.")
    elif dependency_rows and len(selected_ids) > 1:
        st.info("Multiple items selected. Edit is disabled; Delete is enabled.")

if tab_choice == "Backlog x Dependencies":
    st.subheader("Backlog x Dependencies")
    join_rows = fetch_backlog_dependency_rows()
    if join_rows:
        join_df = pd.DataFrame([dict(row) for row in join_rows])
        st.dataframe(join_df, width="stretch")
    else:
        st.info("No backlog/dependency links yet.")

if tab_choice == "Backlog x Sub-backlogs":
    st.subheader("Backlog x Sub-backlogs")
    join_rows = fetch_backlog_sub_backlog_rows()
    if join_rows:
        join_df = pd.DataFrame([dict(row) for row in join_rows])
        st.dataframe(join_df, width="stretch")
    else:
        st.info("No backlog/sub-backlog links yet.")

if tab_choice == "Backlog x Sub-backlogs x Dependencies":
    st.subheader("Backlog x Sub-backlogs x Dependencies")
    join_rows = fetch_backlog_sub_backlog_dependency_rows()
    if join_rows:
        join_df = pd.DataFrame([dict(row) for row in join_rows])
        st.dataframe(join_df, width="stretch")
    else:
        st.info("No backlog/sub-backlog/dependency links yet.")

if tab_choice == "Sub-backlogs":
    backlog_rows = fetch_backlogs()
    sub_backlog_rows = fetch_sub_backlogs()
    backlog_choices = {backlog_label(row): row["id"] for row in backlog_rows}
    backlog_labels = list(backlog_choices.keys())

    @st.dialog("Add sub-backlog")
    def add_sub_backlog_dialog():
        with st.form("add_sub_backlog_form"):
            title = st.text_input("Title")
            note = st.text_area("Note (optional)", height=120)
            st.caption("Associated backlogs")
            st.info("No associated backlogs yet.")
            render_meeting_notes_table([])
            submitted = st.form_submit_button("Add sub-backlog")
            if submitted:
                if not title.strip():
                    st.error("Sub-backlog title is required.")
                else:
                    with get_conn() as conn:
                        sub_backlog_id = insert_sub_backlog(
                            conn,
                            title.strip(),
                            note.strip() or None,
                        )
                    st.success("Sub-backlog added.")
                    st.rerun()

    @st.dialog("Edit sub-backlog")
    def edit_sub_backlog_dialog(sub_backlog_row):
        with st.form("edit_sub_backlog_form"):
            title = st.text_input("Title", value=sub_backlog_row["title"])
            note = st.text_area(
                "Note (optional)",
                value=sub_backlog_row["note"] or "",
                height=120,
            )
            st.caption("Associated backlogs")
            associated_rows = fetch_backlogs_for_sub_backlog(sub_backlog_row["id"])
            if associated_rows:
                associated_df = pd.DataFrame([dict(row) for row in associated_rows])
                st.dataframe(associated_df, width="stretch")
            else:
                st.info("No associated backlogs.")
            render_meeting_notes_table(
                fetch_meeting_notes_for_sub_backlog(sub_backlog_row["id"])
            )
            submitted = st.form_submit_button("Update sub-backlog")
            if submitted:
                if not title.strip():
                    st.error("Sub-backlog title is required.")
                else:
                    with get_conn() as conn:
                        conn.execute(
                            """
                            UPDATE sub_backlog
                            SET title = ?, note = ?
                            WHERE id = ?
                            """,
                            (
                                title.strip(),
                                note.strip() or None,
                                sub_backlog_row["id"],
                            ),
                        )
                    st.success("Sub-backlog updated.")
                    st.rerun()

    @st.dialog("Delete sub-backlog")
    def delete_sub_backlog_dialog(selected_ids, sub_backlog_lookup):
        items = []
        for item_id in selected_ids:
            row = sub_backlog_lookup.get(item_id)
            if row:
                items.append(row["title"])
        st.write(f"Delete {len(selected_ids)} sub-backlog item(s)?")
        if items:
            st.write(items)
        if st.button("Confirm delete", type="primary"):
            placeholders = ",".join(["?"] * len(selected_ids))
            with get_conn() as conn:
                conn.execute(
                    f"DELETE FROM sub_backlog WHERE id IN ({placeholders})",
                    tuple(selected_ids),
                )
            st.success("Sub-backlog deleted.")
            st.rerun()

    st.subheader("Sub-backlog list")
    if sub_backlog_rows:
        sub_backlog_df = pd.DataFrame([dict(row) for row in sub_backlog_rows])
        selection = st.dataframe(
            sub_backlog_df,
            width="stretch",
            on_select="rerun",
            selection_mode="multi-row",
        )
        if selection and selection.selection.rows:
            selected_ids = [
                int(sub_backlog_df.iloc[index]["id"])
                for index in selection.selection.rows
            ]
            st.session_state["selected_sub_backlog_ids"] = selected_ids
        else:
            st.session_state.pop("selected_sub_backlog_ids", None)
    else:
        st.info("No sub-backlogs yet.")

    sub_backlog_by_id = {row["id"]: row for row in sub_backlog_rows}
    selected_ids = st.session_state.get("selected_sub_backlog_ids", [])
    selected_sub_backlog = (
        sub_backlog_by_id.get(selected_ids[0]) if len(selected_ids) == 1 else None
    )

    if selected_ids:
        if len(selected_ids) == 1 and selected_sub_backlog:
            st.caption(f"Selected: {selected_sub_backlog['title']}")
        else:
            st.caption(f"Selected: {len(selected_ids)} items")
    else:
        st.caption("Selected: none")

    action_cols = st.columns(3, gap="small")
    with action_cols[0]:
        if st.button("Add sub-backlog"):
            add_sub_backlog_dialog()
    with action_cols[1]:
        edit_disabled = selected_sub_backlog is None
        if st.button("Edit selected sub-backlog", disabled=edit_disabled):
            edit_sub_backlog_dialog(selected_sub_backlog)
    with action_cols[2]:
        delete_disabled = not selected_ids
        if st.button("Delete selected sub-backlog", disabled=delete_disabled):
            delete_sub_backlog_dialog(selected_ids, sub_backlog_by_id)

    if sub_backlog_rows and not selected_ids:
        st.info("Select sub-backlogs from the list to edit or delete.")
    elif sub_backlog_rows and len(selected_ids) > 1:
        st.info("Multiple items selected. Edit is disabled; Delete is enabled.")

if tab_choice == "Themes":
    theme_rows = fetch_theme_rows()

    @st.dialog("Add theme")
    def add_theme_dialog():
        with st.form("add_theme_form"):
            theme_name = st.text_input("Name")
            render_meeting_notes_table([])
            submitted = st.form_submit_button("Add theme")
            if submitted:
                if not theme_name.strip():
                    st.error("Theme name is required.")
                else:
                    with get_conn() as conn:
                        insert_theme(conn, theme_name.strip())
                    st.success("Theme added.")
                    st.rerun()

    @st.dialog("Edit theme")
    def edit_theme_dialog(theme_row):
        with st.form("edit_theme_form"):
            new_name = st.text_input("Name", value=theme_row["name"])
            render_meeting_notes_table(
                fetch_meeting_notes_for_theme(theme_row["id"])
            )
            submitted = st.form_submit_button("Update theme")
            if submitted:
                if not new_name.strip():
                    st.error("Theme name is required.")
                else:
                    old_name = theme_row["name"]
                    try:
                        with get_conn() as conn:
                            conn.execute(
                                "UPDATE theme SET name = ? WHERE id = ?",
                                (new_name.strip(), theme_row["id"]),
                            )
                            conn.execute(
                                "UPDATE backlog SET theme = ? WHERE theme = ?",
                                (new_name.strip(), old_name),
                            )
                        st.success("Theme updated.")
                        st.rerun()
                    except sqlite3.IntegrityError:
                        st.error("Theme name already exists.")

    @st.dialog("Delete theme")
    def delete_theme_dialog(selected_ids, theme_lookup):
        items = []
        for item_id in selected_ids:
            row = theme_lookup.get(item_id)
            if row:
                items.append(row["name"])
        st.write(f"Delete {len(selected_ids)} theme item(s)?")
        if items:
            st.write(items)
        if st.button("Confirm delete", type="primary"):
            placeholders = ",".join(["?"] * len(selected_ids))
            with get_conn() as conn:
                conn.execute(
                    f"DELETE FROM theme WHERE id IN ({placeholders})",
                    tuple(selected_ids),
                )
                conn.execute(
                    f"UPDATE backlog SET theme = '' WHERE theme IN ({placeholders})",
                    tuple(items),
                )
            st.success("Theme deleted.")
            st.rerun()

    st.subheader("Theme list")
    if theme_rows:
        theme_df = pd.DataFrame([dict(row) for row in theme_rows]).rename(
            columns={"backlog_count": "Backlog count"}
        )
        selection = st.dataframe(
            theme_df,
            width="stretch",
            on_select="rerun",
            selection_mode="multi-row",
        )
        if selection and selection.selection.rows:
            selected_ids = [
                int(theme_df.iloc[index]["id"]) for index in selection.selection.rows
            ]
            st.session_state["selected_theme_ids"] = selected_ids
        else:
            st.session_state.pop("selected_theme_ids", None)
    else:
        st.info("No themes yet.")

    theme_by_id = {row["id"]: row for row in theme_rows}
    selected_ids = st.session_state.get("selected_theme_ids", [])
    selected_theme = theme_by_id.get(selected_ids[0]) if len(selected_ids) == 1 else None

    if selected_ids:
        if len(selected_ids) == 1 and selected_theme:
            st.caption(f"Selected: {selected_theme['name']}")
        else:
            st.caption(f"Selected: {len(selected_ids)} items")
    else:
        st.caption("Selected: none")

    action_cols = st.columns(3, gap="small")
    with action_cols[0]:
        if st.button("Add theme"):
            add_theme_dialog()
    with action_cols[1]:
        edit_disabled = selected_theme is None
        if st.button("Edit selected theme", disabled=edit_disabled):
            edit_theme_dialog(selected_theme)
    with action_cols[2]:
        delete_disabled = not selected_ids
        if st.button("Delete selected theme", disabled=delete_disabled):
            delete_theme_dialog(selected_ids, theme_by_id)

    if theme_rows and not selected_ids:
        st.info("Select themes from the list to edit or delete.")
    elif theme_rows and len(selected_ids) > 1:
        st.info("Multiple items selected. Edit is disabled; Delete is enabled.")

if tab_choice == "Evaluations":
    evaluation_rows = fetch_evaluation_rows()

    @st.dialog("Add evaluation")
    def add_evaluation_dialog():
        with st.form("add_evaluation_form"):
            evaluation_name = st.text_input("Name")
            evaluation_note = st.text_area("Note", height=120)
            render_meeting_notes_table([])
            submitted = st.form_submit_button("Add evaluation")
            if submitted:
                if not evaluation_name.strip():
                    st.error("Evaluation name is required.")
                else:
                    with get_conn() as conn:
                        insert_evaluation(
                            conn,
                            evaluation_name.strip(),
                            evaluation_note.strip() or None,
                        )
                    st.success("Evaluation added.")
                    st.rerun()

    @st.dialog("Edit evaluation")
    def edit_evaluation_dialog(evaluation_row):
        with st.form("edit_evaluation_form"):
            new_name = st.text_input("Name", value=evaluation_row["name"])
            new_note = st.text_area(
                "Note",
                value=evaluation_row["note"] or "",
                height=120,
            )
            render_meeting_notes_table(
                fetch_meeting_notes_for_evaluation(evaluation_row["id"])
            )
            submitted = st.form_submit_button("Update evaluation")
            if submitted:
                if not new_name.strip():
                    st.error("Evaluation name is required.")
                else:
                    old_name = evaluation_row["name"]
                    try:
                        with get_conn() as conn:
                            conn.execute(
                                "UPDATE evaluation SET name = ?, note = ? WHERE id = ?",
                                (new_name.strip(), new_note.strip() or None, evaluation_row["id"]),
                            )
                            conn.execute(
                                "UPDATE backlog SET evaluation = ? WHERE evaluation = ?",
                                (new_name.strip(), old_name),
                            )
                        st.success("Evaluation updated.")
                        st.rerun()
                    except sqlite3.IntegrityError:
                        st.error("Evaluation name already exists.")

    @st.dialog("Delete evaluation")
    def delete_evaluation_dialog(selected_ids, evaluation_lookup):
        items = []
        for item_id in selected_ids:
            row = evaluation_lookup.get(item_id)
            if row:
                items.append(row["name"])
        st.write(f"Delete {len(selected_ids)} evaluation item(s)?")
        if items:
            st.write(items)
        if st.button("Confirm delete", type="primary"):
            placeholders = ",".join(["?"] * len(selected_ids))
            with get_conn() as conn:
                conn.execute(
                    f"DELETE FROM evaluation WHERE id IN ({placeholders})",
                    tuple(selected_ids),
                )
                conn.execute(
                    f"UPDATE backlog SET evaluation = '' WHERE evaluation IN ({placeholders})",
                    tuple(items),
                )
            st.success("Evaluation deleted.")
            st.rerun()

    st.subheader("Evaluation list")
    if evaluation_rows:
        evaluation_df = pd.DataFrame([dict(row) for row in evaluation_rows])
        selection = st.dataframe(
            evaluation_df,
            width="stretch",
            on_select="rerun",
            selection_mode="multi-row",
        )
        if selection and selection.selection.rows:
            selected_ids = [
                int(evaluation_df.iloc[index]["id"]) for index in selection.selection.rows
            ]
            st.session_state["selected_evaluation_ids"] = selected_ids
        else:
            st.session_state.pop("selected_evaluation_ids", None)
    else:
        st.info("No evaluations yet.")

    evaluation_by_id = {row["id"]: row for row in evaluation_rows}
    selected_ids = st.session_state.get("selected_evaluation_ids", [])
    selected_evaluation = (
        evaluation_by_id.get(selected_ids[0]) if len(selected_ids) == 1 else None
    )

    if selected_ids:
        if len(selected_ids) == 1 and selected_evaluation:
            st.caption(f"Selected: {selected_evaluation['name']}")
        else:
            st.caption(f"Selected: {len(selected_ids)} items")
    else:
        st.caption("Selected: none")

    action_cols = st.columns(3, gap="small")
    with action_cols[0]:
        if st.button("Add evaluation"):
            add_evaluation_dialog()
    with action_cols[1]:
        edit_disabled = selected_evaluation is None
        if st.button("Edit selected evaluation", disabled=edit_disabled):
            edit_evaluation_dialog(selected_evaluation)
    with action_cols[2]:
        delete_disabled = not selected_ids
        if st.button("Delete selected evaluation", disabled=delete_disabled):
            delete_evaluation_dialog(selected_ids, evaluation_by_id)

    if evaluation_rows and not selected_ids:
        st.info("Select evaluations from the list to edit or delete.")
    elif evaluation_rows and len(selected_ids) > 1:
        st.info("Multiple items selected. Edit is disabled; Delete is enabled.")

if tab_choice == "Meetings":
    meeting_rows = fetch_meetings()

    @st.dialog("Add meeting")
    def add_meeting_dialog():
        with st.form("add_meeting_form"):
            meeting_title = st.text_input("Title")
            meeting_date = st.date_input("Date", value=datetime.now().date())
            submitted = st.form_submit_button("Add meeting")
            if submitted:
                if not meeting_title.strip():
                    st.error("Meeting title is required.")
                else:
                    with get_conn() as conn:
                        insert_meeting(
                            conn,
                            meeting_title.strip(),
                            meeting_date.strftime("%Y-%m-%d"),
                        )
                    st.success("Meeting added.")
                    st.rerun()

    @st.dialog("Edit meeting")
    def edit_meeting_dialog(meeting_row):
        with st.form("edit_meeting_form"):
            new_title = st.text_input("Title", value=meeting_row["title"])
            new_date = st.date_input(
                "Date",
                value=parse_meeting_date(meeting_row["meeting_datetime"]),
            )
            submitted = st.form_submit_button("Update meeting")
            if submitted:
                if not new_title.strip():
                    st.error("Meeting title is required.")
                else:
                    with get_conn() as conn:
                        conn.execute(
                            """
                            UPDATE meeting
                            SET title = ?, meeting_datetime = ?
                            WHERE id = ?
                            """,
                            (
                                new_title.strip(),
                                new_date.strftime("%Y-%m-%d"),
                                meeting_row["id"],
                            ),
                        )
                    st.success("Meeting updated.")
                    st.rerun()

    @st.dialog("Delete meeting")
    def delete_meeting_dialog(selected_ids, meeting_lookup):
        items = []
        for item_id in selected_ids:
            row = meeting_lookup.get(item_id)
            if row:
                items.append(f"{row['meeting_datetime']} | {row['title']}")
        st.write(f"Delete {len(selected_ids)} meeting item(s)?")
        if items:
            st.write(items)
        if st.button("Confirm delete", type="primary"):
            placeholders = ",".join(["?"] * len(selected_ids))
            with get_conn() as conn:
                conn.execute(
                    f"DELETE FROM meeting WHERE id IN ({placeholders})",
                    tuple(selected_ids),
                )
            st.success("Meeting deleted.")
            st.rerun()

    st.subheader("Meeting list")
    if meeting_rows:
        meeting_df = pd.DataFrame([dict(row) for row in meeting_rows])
        selection = st.dataframe(
            meeting_df,
            width="stretch",
            on_select="rerun",
            selection_mode="multi-row",
        )
        if selection and selection.selection.rows:
            selected_ids = [
                int(meeting_df.iloc[index]["id"]) for index in selection.selection.rows
            ]
            st.session_state["selected_meeting_ids"] = selected_ids
        else:
            st.session_state.pop("selected_meeting_ids", None)
    else:
        st.info("No meetings yet.")

    meeting_by_id = {row["id"]: row for row in meeting_rows}
    selected_ids = st.session_state.get("selected_meeting_ids", [])
    selected_meeting = (
        meeting_by_id.get(selected_ids[0]) if len(selected_ids) == 1 else None
    )

    if selected_ids:
        if len(selected_ids) == 1 and selected_meeting:
            st.caption(f"Selected: {selected_meeting['title']}")
        else:
            st.caption(f"Selected: {len(selected_ids)} items")
    else:
        st.caption("Selected: none")

    action_cols = st.columns(3, gap="small")
    with action_cols[0]:
        if st.button("Add meeting"):
            add_meeting_dialog()
    with action_cols[1]:
        edit_disabled = selected_meeting is None
        if st.button("Edit selected meeting", disabled=edit_disabled):
            edit_meeting_dialog(selected_meeting)
    with action_cols[2]:
        delete_disabled = not selected_ids
        if st.button("Delete selected meeting", disabled=delete_disabled):
            delete_meeting_dialog(selected_ids, meeting_by_id)

    if meeting_rows and not selected_ids:
        st.info("Select meetings from the list to edit or delete.")
    elif meeting_rows and len(selected_ids) > 1:
        st.info("Multiple items selected. Edit is disabled; Delete is enabled.")


if tab_choice == "Meeting Notes":
    meeting_rows = fetch_meeting_notes()
    note_type_options = ["Todo", "Decision"]
    backlog_rows = fetch_backlogs()
    dependency_rows = fetch_dependencies()
    theme_rows = fetch_theme_rows()
    evaluation_rows = fetch_evaluation_rows()
    meeting_list = fetch_meetings()

    backlog_choices = {
        backlog_label(row): row["id"] for row in backlog_rows
    }
    dependency_choices = {
        dependency_label(row): row["id"] for row in dependency_rows
    }
    theme_choices = {row["name"]: row["id"] for row in theme_rows}
    evaluation_choices = {row["name"]: row["id"] for row in evaluation_rows}
    meeting_choices = {
        f"{row['meeting_datetime']} | {row['title']}": row["id"]
        for row in meeting_list
    }

    backlog_labels = list(backlog_choices.keys())
    dependency_labels = list(dependency_choices.keys())
    theme_labels = list(theme_choices.keys())
    evaluation_labels = list(evaluation_choices.keys())
    meeting_labels = list(meeting_choices.keys())

    @st.dialog("Edit meeting note")
    def edit_meeting_note_dialog(note_row):
        selected_backlog_ids = fetch_meeting_note_backlog_ids(note_row["id"])
        selected_dependency_ids = fetch_meeting_note_dependency_ids(note_row["id"])
        selected_theme_ids = fetch_meeting_note_theme_ids(note_row["id"])
        selected_evaluation_ids = fetch_meeting_note_evaluation_ids(note_row["id"])

        backlog_selected_labels = [
            label
            for label, item_id in backlog_choices.items()
            if item_id in selected_backlog_ids
        ]
        dependency_selected_labels = [
            label
            for label, item_id in dependency_choices.items()
            if item_id in selected_dependency_ids
        ]
        theme_selected_labels = [
            label for label, item_id in theme_choices.items() if item_id in selected_theme_ids
        ]
        evaluation_selected_labels = [
            label
            for label, item_id in evaluation_choices.items()
            if item_id in selected_evaluation_ids
        ]

        with st.form("edit_meeting_note_form"):
            meeting_selected_label = next(
                (
                    label
                    for label, item_id in meeting_choices.items()
                    if item_id == note_row["meeting_id"]
                ),
                PLACEHOLDER_OPTION,
            )
            left_col, right_col = st.columns(2, gap="large")
            with left_col:
                meeting_date = st.text_input(
                    "Meeting date (optional)",
                    value=note_row["meeting_date"] or "",
                )
                topic = st.text_input(
                    "Topic (optional)",
                    value=note_row["topic"] or "",
                )
                selected_backlogs = st.multiselect(
                    "Assign to backlogs",
                    backlog_labels,
                    default=backlog_selected_labels,
                    placeholder=PLACEHOLDER_OPTION,
                )
                selected_dependencies = st.multiselect(
                    "Assign to dependencies",
                    dependency_labels,
                    default=dependency_selected_labels,
                    placeholder=PLACEHOLDER_OPTION,
                )
            with right_col:
                meeting_label = st.selectbox(
                    "Meeting (optional)",
                    with_placeholder(meeting_labels),
                    index=(
                        with_placeholder(meeting_labels).index(meeting_selected_label)
                        if meeting_selected_label in meeting_labels
                        else 0
                    ),
                )
                selected_note_type = (
                    note_row["note_type"].strip().lower()
                    if note_row["note_type"]
                    else "todo"
                )
                note_type_value = "Decision" if selected_note_type == "decision" else "Todo"
                note_type_options_with_placeholder = with_placeholder(note_type_options)
                note_type_index = (
                    note_type_options_with_placeholder.index(note_type_value)
                    if note_type_value in note_type_options_with_placeholder
                    else 0
                )
                note_type = st.selectbox(
                    "Type",
                    note_type_options_with_placeholder,
                    index=note_type_index,
                )
                selected_themes = st.multiselect(
                    "Assign to themes",
                    theme_labels,
                    default=theme_selected_labels,
                    placeholder=PLACEHOLDER_OPTION,
                )
                selected_evaluations = st.multiselect(
                    "Assign to evaluations",
                    evaluation_labels,
                    default=evaluation_selected_labels,
                    placeholder=PLACEHOLDER_OPTION,
                )
            note = st.text_area(
                "Note (bullet)",
                value=note_row["note"] or "",
                height=140,
            )
            submitted = st.form_submit_button("Update note")
            if submitted:
                if not note.strip():
                    st.error("Note is required.")
                else:
                    backlog_ids = [backlog_choices[label] for label in selected_backlogs]
                    dependency_ids = [
                        dependency_choices[label] for label in selected_dependencies
                    ]
                    theme_ids = [theme_choices[label] for label in selected_themes]
                    evaluation_ids = [
                        evaluation_choices[label] for label in selected_evaluations
                    ]
                    with get_conn() as conn:
                        meeting_id = meeting_choices.get(meeting_label)
                        if note_type == PLACEHOLDER_OPTION:
                            st.error("Note type is required.")
                            return
                        conn.execute(
                            """
                            UPDATE meeting_note
                            SET meeting_id = ?, meeting_date = ?, topic = ?, note_type = ?, note = ?
                            WHERE id = ?
                            """,
                            (
                                meeting_id,
                                meeting_date.strip() or None,
                                topic.strip() or None,
                                note_type.lower(),
                                note.strip(),
                                note_row["id"],
                            ),
                        )
                        upsert_meeting_note_backlogs(
                            conn, note_row["id"], backlog_ids
                        )
                        upsert_meeting_note_dependencies(
                            conn, note_row["id"], dependency_ids
                        )
                        upsert_meeting_note_themes(conn, note_row["id"], theme_ids)
                        upsert_meeting_note_evaluations(
                            conn, note_row["id"], evaluation_ids
                        )
                    st.success("Meeting note updated.")
                    st.rerun()

    @st.dialog("Delete meeting note")
    def delete_meeting_note_dialog(selected_ids, note_lookup):
        items = []
        for item_id in selected_ids:
            row = note_lookup.get(item_id)
            if row:
                preview = row["note"] or ""
                items.append(preview[:80])
        st.write(f"Delete {len(selected_ids)} meeting note(s)?")
        if items:
            st.write(items)
        if st.button("Confirm delete", type="primary"):
            placeholders = ",".join(["?"] * len(selected_ids))
            with get_conn() as conn:
                conn.execute(
                    f"DELETE FROM meeting_note WHERE id IN ({placeholders})",
                    tuple(selected_ids),
                )
            st.success("Meeting note deleted.")
            st.rerun()

    with st.expander("Add meeting note", expanded=True):
        default_meeting_date = datetime.now().strftime("%Y-%m-%d %H:%M")
        date_col, meeting_col, topic_col = st.columns(3, gap="large")
        with date_col:
            meeting_date = st.text_input(
                "Meeting date (optional)",
                value=default_meeting_date,
                key="meeting_note_date",
            )
        with meeting_col:
            meeting_label = st.selectbox(
                "Meeting (optional)",
                with_placeholder(meeting_labels),
                key="meeting_note_meeting",
            )
        with topic_col:
            topic = st.text_input(
                "Topic (optional)",
                key="meeting_note_topic",
            )
        note_text = st.chat_input("Add a meeting note")
        if note_text is not None:
            if not note_text.strip():
                st.error("Note is required.")
            else:
                with get_conn() as conn:
                    meeting_id = meeting_choices.get(meeting_label)
                    note_id = insert_meeting_note(
                        conn,
                        meeting_id,
                        meeting_date.strip() or None,
                        topic.strip() or None,
                        "todo",
                        note_text.strip(),
                    )
                st.success("Meeting note added.")
                st.rerun()

    with st.expander("Meeting notes table", expanded=True):
        if meeting_rows:
            meeting_df = pd.DataFrame([dict(row) for row in meeting_rows])
            selection = st.dataframe(
                meeting_df,
                width="stretch",
                on_select="rerun",
                selection_mode="multi-row",
            )
            if selection and selection.selection.rows:
                selected_ids = [
                    int(meeting_df.iloc[index]["id"]) for index in selection.selection.rows
                ]
                st.session_state["selected_meeting_note_ids"] = selected_ids
            else:
                st.session_state.pop("selected_meeting_note_ids", None)
        else:
            st.info("No meeting notes yet.")

        note_by_id = {row["id"]: row for row in meeting_rows}
        selected_ids = st.session_state.get("selected_meeting_note_ids", [])
        selected_note = note_by_id.get(selected_ids[0]) if len(selected_ids) == 1 else None

        if selected_ids:
            if len(selected_ids) == 1 and selected_note:
                st.caption(f"Selected: {selected_note['note'][:60]}")
            else:
                st.caption(f"Selected: {len(selected_ids)} items")
        else:
            st.caption("Selected: none")

        if len(selected_ids) > 1:
            with st.form("bulk_assign_meeting_notes_form"):
                st.caption("Bulk assign (replaces existing assignments).")
                left_col, right_col = st.columns(2, gap="large")
                with left_col:
                    bulk_backlogs = st.multiselect(
                        "Assign to backlogs",
                        backlog_labels,
                        key="bulk_meeting_note_backlogs",
                        placeholder=PLACEHOLDER_OPTION,
                    )
                    bulk_dependencies = st.multiselect(
                        "Assign to dependencies",
                        dependency_labels,
                        key="bulk_meeting_note_dependencies",
                        placeholder=PLACEHOLDER_OPTION,
                    )
                with right_col:
                    bulk_themes = st.multiselect(
                        "Assign to themes",
                        theme_labels,
                        key="bulk_meeting_note_themes",
                        placeholder=PLACEHOLDER_OPTION,
                    )
                    bulk_evaluations = st.multiselect(
                        "Assign to evaluations",
                        evaluation_labels,
                        key="bulk_meeting_note_evaluations",
                        placeholder=PLACEHOLDER_OPTION,
                    )
                bulk_submit = st.form_submit_button("Apply assignments")
                if bulk_submit:
                    backlog_ids = [backlog_choices[label] for label in bulk_backlogs]
                    dependency_ids = [
                        dependency_choices[label] for label in bulk_dependencies
                    ]
                    theme_ids = [theme_choices[label] for label in bulk_themes]
                    evaluation_ids = [
                        evaluation_choices[label] for label in bulk_evaluations
                    ]
                    with get_conn() as conn:
                        for note_id in selected_ids:
                            upsert_meeting_note_backlogs(conn, note_id, backlog_ids)
                            upsert_meeting_note_dependencies(
                                conn, note_id, dependency_ids
                            )
                            upsert_meeting_note_themes(conn, note_id, theme_ids)
                            upsert_meeting_note_evaluations(
                                conn, note_id, evaluation_ids
                            )
                    st.success("Assignments updated.")
                    st.rerun()

        action_cols = st.columns(2, gap="small")
        with action_cols[0]:
            edit_disabled = selected_note is None
            if st.button("Edit selected note", disabled=edit_disabled):
                edit_meeting_note_dialog(selected_note)
        with action_cols[1]:
            delete_disabled = not selected_ids
            if st.button("Delete selected note", disabled=delete_disabled):
                delete_meeting_note_dialog(selected_ids, note_by_id)

        if meeting_rows and not selected_ids:
            st.info("Select meeting notes from the list to edit or delete.")
        elif meeting_rows and len(selected_ids) > 1:
            st.info("Multiple items selected. Edit is disabled; Delete is enabled.")

if tab_choice == "Todo Notes":
    st.subheader("Todo meeting notes")
    show_completed = st.checkbox("Show completed", value=False)
    todo_rows = fetch_todo_meeting_notes(include_completed=show_completed)
    if todo_rows:
        todo_df = pd.DataFrame([dict(row) for row in todo_rows])
        editable_df = st.data_editor(
            todo_df,
            width="stretch",
            hide_index=True,
            column_config={
                "status": st.column_config.SelectboxColumn(
                    "Status",
                    options=["open", "in-progress", "completed"],
                )
            },
            disabled=[
                "id",
                "meeting_id",
                "meeting_date",
                "topic",
                "note_type",
                "note",
            ],
        )
        if st.button("Save statuses", type="primary"):
            updates = []
            for _, row in editable_df.iterrows():
                original = todo_df.loc[todo_df["id"] == row["id"], "status"].iloc[0]
                if row["status"] != original:
                    updates.append((row["status"], int(row["id"])))
            if updates:
                with get_conn() as conn:
                    conn.executemany(
                        "UPDATE meeting_note SET status = ? WHERE id = ?",
                        updates,
                    )
                st.success("Statuses updated.")
                st.rerun()
            else:
                st.info("No status changes detected.")
    else:
        st.info("No todo meeting notes yet.")

if tab_choice == "Sprint x Team":
    st.subheader("Sprint x Team points")
    backlog_rows = fetch_backlogs()
    if backlog_rows:
        backlog_df = pd.DataFrame([dict(row) for row in backlog_rows])
        metric_df = backlog_df.dropna(subset=["team", "sprint"]).copy()
        metric_df["estimation"] = metric_df["estimation"].fillna(0)
        pivot = (
            metric_df.pivot_table(
                index="sprint",
                columns="team",
                values="estimation",
                aggfunc="sum",
                fill_value=0,
            )
            .sort_index()
        )
        if pivot.empty:
            st.info("No points available for Sprint x Team yet.")
        else:
            st.dataframe(pivot, width="stretch")
    else:
        st.info("No backlog items yet.")
