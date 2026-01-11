import base64
import numbers
import sqlite3
from pathlib import Path
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


DB_PATH = "backlog.db"
BACKLOG_TEAMS = ["Team 1", "Team 2"]
DEPENDENCY_TEAMS = ["PC", "BC", "CC", "Integration", "Auth"]
SPRINTS = [f"Sprint {i}" for i in range(1, 12)]


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
                sub_task TEXT,
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
            """
        )
        backlog_info = conn.execute("PRAGMA table_info(backlog)").fetchall()
        backlog_columns = [row["name"] for row in backlog_info]
        sub_task_info = next((row for row in backlog_info if row["name"] == "sub_task"), None)
        if "task" not in backlog_columns:
            conn.execute("ALTER TABLE backlog ADD COLUMN task TEXT NOT NULL DEFAULT ''")
        if "sub_task" not in backlog_columns:
            conn.execute("ALTER TABLE backlog ADD COLUMN sub_task TEXT")
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
                    sub_task TEXT,
                    lob TEXT,
                    image_blob BLOB,
                    theme TEXT NOT NULL,
                    evaluation TEXT,
                    estimation INTEGER,
                    team TEXT,
                    sprint TEXT
                );
                INSERT INTO backlog_new (id, task, sub_task, lob, image_blob, theme, evaluation, estimation, team, sprint)
                SELECT id, task, sub_task, lob, image_blob, theme, evaluation, estimation, team, sprint FROM backlog;
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
                    sub_task TEXT,
                    lob TEXT,
                    image_blob BLOB,
                    theme TEXT NOT NULL,
                    evaluation TEXT,
                    estimation INTEGER,
                    team TEXT,
                    sprint TEXT
                );
                INSERT INTO backlog_new (id, task, sub_task, lob, image_blob, theme, evaluation, estimation, team, sprint)
                SELECT id, task, sub_task, lob, image_blob, theme, evaluation, CAST(estimation AS INTEGER), team, sprint FROM backlog;
                DROP TABLE backlog;
                ALTER TABLE backlog_new RENAME TO backlog;
                """
            )
        backlog_info = conn.execute("PRAGMA table_info(backlog)").fetchall()
        team_info = next((row for row in backlog_info if row["name"] == "team"), None)
        sprint_info = next((row for row in backlog_info if row["name"] == "sprint"), None)
        sub_task_info = next((row for row in backlog_info if row["name"] == "sub_task"), None)
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
                    sub_task TEXT,
                    lob TEXT,
                    image_blob BLOB,
                    theme TEXT NOT NULL,
                    evaluation TEXT,
                    estimation INTEGER,
                    team TEXT,
                    sprint TEXT
                );
                INSERT INTO backlog_new (id, task, sub_task, lob, image_blob, theme, evaluation, estimation, team, sprint)
                SELECT id, task, sub_task, lob, image_blob, theme, evaluation, CAST(estimation AS INTEGER), team, sprint FROM backlog;
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
                b.sub_task,
                b.lob,
                b.image_blob,
                b.theme,
                b.evaluation,
                b.estimation,
                b.team,
                b.sprint,
                GROUP_CONCAT(d.task, ', ') AS dependency_names
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
                b.sub_task,
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


def insert_backlog(
    conn, task, sub_task, lob, image_blob, theme, evaluation, estimation, team, sprint
):
    cursor = conn.execute(
        """
        INSERT INTO backlog (task, sub_task, lob, image_blob, theme, evaluation, estimation, team, sprint)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (task, sub_task, lob, image_blob, theme, evaluation, estimation, team, sprint),
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
        f"{backlog_row['id']}: {backlog_row['task']} / {backlog_row['sub_task'] or ''} "
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
    ["Backlog", "Dependencies", "Themes", "Evaluations", "Sprint x Team"],
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
                theme_options = [""] + themes
                theme_choice = st.selectbox("Theme", theme_options, index=0)
                lob = st.text_input("LOB (optional)")
                team = st.selectbox("Team", [""] + BACKLOG_TEAMS, index=0)
                evaluation_options = [""] + evaluations
                evaluation_choice = st.selectbox(
                    "Evaluation",
                    evaluation_options,
                    index=0,
                    key="add_evaluation_choice",
                )
            with right_col:
                sub_task = st.text_input("Sub-task (optional)")
                new_theme = st.text_input("New theme (optional)")
                estimation_input_right = st.number_input(
                    "Estimation",
                    min_value=0,
                    step=1,
                    value=0,
                    key="add_estimation_right",
                )
                sprint = st.selectbox("Sprint", [""] + SPRINTS, index=0)
            selected_dependency_labels = st.multiselect(
                "Existing dependencies",
                options=existing_dependency_labels,
                default=[],
                key="add_existing_deps",
            )

            new_dependencies = []
            for i in range(add_new_dep_count):
                st.markdown(f"New dependency {i + 1}")
                team_col, task_col, sub_task_col = st.columns(3, gap="large")
                with team_col:
                    dep_team = st.selectbox(
                        f"Dependency team {i + 1}",
                        [""] + DEPENDENCY_TEAMS,
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

            submitted = st.form_submit_button("Add backlog")
            if submitted:
                if not task.strip():
                    st.error("Backlog task is required.")
                else:
                    invalid_dep_team = [
                        dep_task
                        for dep_task, _, dep_team in new_dependencies
                        if dep_task.strip() and dep_team == ""
                    ]
                    if invalid_dep_team:
                        st.error("Dependency team is required for new dependencies.")
                        return
                    theme = new_theme.strip() or theme_choice
                    if theme == "":
                        st.error("Backlog theme is required.")
                        return
                    evaluation_value = evaluation_choice or None
                    team_value = team or None
                    sprint_value = sprint or None
                    image_bytes = pasted_image
                    estimation_value = int(estimation_input_right)
                    sub_task_value = sub_task.strip() or None
                    lob_value = lob.strip() or None
                    with get_conn() as conn:
                        insert_theme(conn, theme.strip())
                        backlog_id = insert_backlog(
                            conn,
                            task.strip(),
                            sub_task_value,
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
                theme_options = [""] + themes
                theme_value = backlog_row["theme"] or ""
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
                team_options = [""] + BACKLOG_TEAMS
                team_value = backlog_row["team"] or ""
                edit_team = st.selectbox(
                    "Team",
                    team_options,
                    index=team_options.index(team_value) if team_value in team_options else 0,
                )
                evaluation_options = [""] + evaluations
                evaluation_value = backlog_row["evaluation"] or ""
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
                edit_sub_task = st.text_input(
                    "Sub-task (optional)",
                    value=backlog_row["sub_task"] or "",
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
                sprint_options = [""] + SPRINTS
                sprint_value = backlog_row["sprint"] or ""
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
                key="edit_existing_deps",
            )

            edit_new_dependencies = []
            for i in range(edit_new_dep_count):
                st.markdown(f"New dependency {i + 1}")
                team_col, task_col, sub_task_col = st.columns(3, gap="large")
                with team_col:
                    dep_team = st.selectbox(
                        f"Dependency team {i + 1}",
                        [""] + DEPENDENCY_TEAMS,
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

            updated = st.form_submit_button("Update backlog")
            if updated:
                if not edit_task.strip():
                    st.error("Backlog task is required.")
                else:
                    invalid_dep_team = [
                        dep_task
                        for dep_task, _, dep_team in edit_new_dependencies
                        if dep_task.strip() and dep_team == ""
                    ]
                    if invalid_dep_team:
                        st.error("Dependency team is required for new dependencies.")
                        return
                    edit_theme = edit_new_theme.strip() or edit_theme_choice
                    if edit_theme == "":
                        st.error("Theme is required.")
                        return
                    edit_evaluation_value = edit_evaluation_choice or None
                    edit_team_value = edit_team or None
                    edit_sprint_value = edit_sprint or None
                    if remove_image:
                        image_bytes = None
                    elif pasted_replace_image:
                        image_bytes = pasted_replace_image
                    else:
                        image_bytes = backlog_row["image_blob"]
                        if isinstance(image_bytes, memoryview):
                            image_bytes = image_bytes.tobytes()
                    edit_estimation_value = int(edit_estimation_input)
                    edit_sub_task_value = edit_sub_task.strip() or None
                    edit_lob_value = edit_lob.strip() or None
                    with get_conn() as conn:
                        insert_theme(conn, edit_theme.strip())
                        conn.execute(
                            """
                            UPDATE backlog
                            SET task = ?, sub_task = ?, lob = ?, image_blob = ?, theme = ?, evaluation = ?, estimation = ?, team = ?, sprint = ?
                            WHERE id = ?
                            """,
                            (
                                edit_task.strip(),
                                edit_sub_task_value,
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

                    st.success("Backlog updated.")
                    st.session_state.pop("edit_backlog_image_paste", None)
                    st.rerun()

    @st.dialog("Delete backlog")
    def delete_backlog_dialog(selected_ids, backlog_lookup):
        items = []
        for item_id in selected_ids:
            row = backlog_lookup.get(item_id)
            if row:
                items.append(f"{row['task']} / {row['sub_task'] or ''}")
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

        primary_options = [row["id"] for row in selected_rows]
        primary_id = st.selectbox(
            "Merge into",
            options=primary_options,
            format_func=lambda item_id: backlog_label(backlog_lookup[item_id]),
            key="merge_primary_id",
        )
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
                theme_options = [""] + themes
                theme_value = primary_row["theme"] or ""
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
                team_options = [""] + BACKLOG_TEAMS
                team_value = primary_row["team"] or ""
                merge_team = st.selectbox(
                    "Team",
                    team_options,
                    index=team_options.index(team_value) if team_value in team_options else 0,
                    key=f"merge_team_{primary_id}",
                )
                evaluation_options = [""] + evaluations
                evaluation_value = primary_row["evaluation"] or ""
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
                merge_sub_task = st.text_input(
                    "Sub-task (optional)",
                    value=primary_row["sub_task"] or "",
                    key=f"merge_sub_task_{primary_id}",
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
                sprint_options = [""] + SPRINTS
                sprint_value = primary_row["sprint"] or ""
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
                    key=f"merge_existing_deps_{primary_id}",
                )

            merge_submit = st.form_submit_button("Merge backlogs")
            if merge_submit:
                if not merge_task.strip():
                    st.error("Backlog task is required.")
                    return
                merge_theme = merge_new_theme.strip() or merge_theme_choice
                if merge_theme == "":
                    st.error("Theme is required.")
                    return

                merge_evaluation_value = merge_evaluation_choice or None
                merge_team_value = merge_team or None
                merge_sprint_value = merge_sprint or None
                merge_sub_task_value = merge_sub_task.strip() or None
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
                        SET task = ?, sub_task = ?, lob = ?, image_blob = ?, theme = ?, evaluation = ?, estimation = ?, team = ?, sprint = ?
                        WHERE id = ?
                        """,
                        (
                            merge_task.strip(),
                            merge_sub_task_value,
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
                task_col, sub_task_col, est_col = st.columns(3, gap="large")
                with task_col:
                    item_task = st.text_input(
                        f"Task {i + 1}",
                        value=backlog_row["task"],
                        key=f"split_task_dialog_{i}",
                    )
                default_sub_task = backlog_row["sub_task"]
                if default_sub_task:
                    default_sub_task = f"{default_sub_task} ({i + 1})"
                else:
                    default_sub_task = f"Part {i + 1}"
                with sub_task_col:
                    item_sub_task = st.text_input(
                        f"Sub-task {i + 1} (optional)",
                        value=default_sub_task,
                        key=f"split_sub_task_dialog_{i}",
                    )
                with est_col:
                    item_estimation = st.number_input(
                        f"Estimation {i + 1}",
                        min_value=0,
                        step=1,
                        key=f"split_est_dialog_{i}",
                    )
                split_items.append((item_task, item_sub_task, item_estimation))

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
                        for item_task, item_sub_task, item_estimation in split_items:
                            new_id = insert_backlog(
                                conn,
                                item_task.strip(),
                                item_sub_task.strip(),
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
                columns = [""] + list(backlog_csv.columns)
                map_task = st.selectbox(
                    "Map: task (required)",
                    columns,
                    key="map_backlog_task",
                )
                map_sub_task = st.selectbox(
                    "Map: sub_task (optional)",
                    columns,
                    key="map_backlog_sub_task",
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
                    if map_task == "":
                        missing.append("task")
                    if map_theme == "":
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
                            if not column:
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
                                sub_task_value = get_cell(row, map_sub_task)
                                lob_value = get_cell(row, map_lob)
                                evaluation_value = get_cell(row, map_evaluation)
                                team_value = get_cell(row, map_team)
                                sprint_value = get_cell(row, map_sprint)
                                estimation_value = None
                                estimation_cell = row[map_estimation] if map_estimation else None
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
                                    sub_task_value,
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
        backlog_sub_task_filter = st.text_input(
            "Sub-task (filter)",
            key="backlog_sub_task_filter",
        )
    with backlog_filter_row1[2]:
        backlog_lob_filter = st.text_input(
            "LOB (filter)",
            key="backlog_lob_filter",
        )
    with backlog_filter_row1[3]:
        backlog_theme_filter = st.selectbox(
            "Theme (filter)",
            [""] + themes,
            index=0,
            key="backlog_theme_filter",
        )
    backlog_filter_row2 = st.columns(4, gap="small")
    with backlog_filter_row2[0]:
        backlog_team_filter = st.selectbox(
            "Team (filter)",
            [""] + BACKLOG_TEAMS,
            index=0,
            key="backlog_team_filter",
        )
    with backlog_filter_row2[1]:
        backlog_sprint_filter = st.selectbox(
            "Sprint (filter)",
            [""] + SPRINTS,
            index=0,
            key="backlog_sprint_filter",
        )
    with backlog_filter_row2[2]:
        backlog_search = st.text_input(
            "Search",
            key="backlog_search",
            help="Filter by task/sub-task/lob/theme/evaluation",
        )
    with backlog_filter_row2[3]:
        backlog_evaluation_filter = st.selectbox(
            "Evaluation (filter)",
            [""] + evaluations,
            index=0,
            key="backlog_evaluation_filter",
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
        if backlog_sub_task_filter.strip():
            query = backlog_sub_task_filter.strip()
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["sub_task"]
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
        if backlog_team_filter:
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["team"] == backlog_team_filter
            ]
        if backlog_sprint_filter:
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["sprint"] == backlog_sprint_filter
            ]
        if backlog_theme_filter:
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["theme"] == backlog_theme_filter
            ]
        if backlog_evaluation_filter:
            filtered_backlog_df = filtered_backlog_df[
                filtered_backlog_df["evaluation"] == backlog_evaluation_filter
            ]
        if backlog_search.strip():
            query = backlog_search.strip().lower()
            searchable = (
                filtered_backlog_df[["task", "sub_task", "lob", "theme", "evaluation"]]
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

    @st.dialog("Add dependency")
    def add_dependency_dialog():
        with st.form("add_dependency_form"):
            team_col, task_col, sub_task_col = st.columns(3, gap="large")
            with team_col:
                dep_team = st.selectbox("Team", [""] + DEPENDENCY_TEAMS, index=0)
            with task_col:
                dep_task = st.text_input("Task")
            with sub_task_col:
                dep_sub_task = st.text_input("Sub-task (optional)")

            dep_submitted = st.form_submit_button("Add dependency")
            if dep_submitted:
                if not dep_task.strip():
                    st.error("Dependency task is required.")
                elif dep_team == "":
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
                edit_dep_team = st.selectbox(
                    "Team",
                    DEPENDENCY_TEAMS,
                    index=DEPENDENCY_TEAMS.index(dep_row["team"]),
                )
            with task_col:
                edit_dep_task = st.text_input("Task", value=dep_row["task"])
            with sub_task_col:
                edit_dep_sub_task = st.text_input(
                    "Sub-task (optional)",
                    value=dep_row["sub_task"] or "",
                )
            dep_updated = st.form_submit_button("Update dependency")
            if dep_updated:
                if not edit_dep_task.strip():
                    st.error("Dependency task is required.")
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
                columns = [""] + list(dep_csv.columns)
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
                    if map_task == "":
                        missing.append("task")
                    if map_team == "":
                        missing.append("team")
                    if missing:
                        st.error(f"Required mappings missing: {', '.join(missing)}")
                    else:
                        imported = 0
                        skipped = 0
                        skip_reasons = {"missing_task": 0, "missing_team": 0}

                        def get_cell(row, column):
                            if not column:
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
    dependency_filter_cols = st.columns(2, gap="small")
    with dependency_filter_cols[0]:
        dependency_search = st.text_input(
            "Search",
            key="dependency_search",
            help="Filter by task/sub-task/team",
        )
    with dependency_filter_cols[1]:
        dependency_team_filter = st.selectbox(
            "Team (filter)",
            [""] + DEPENDENCY_TEAMS,
            index=0,
            key="dependency_team_filter",
        )
    if dependency_rows:
        dependency_df = pd.DataFrame([dict(row) for row in dependency_rows])
        filtered_dependency_df = dependency_df.copy()
        if dependency_team_filter:
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

    action_cols = st.columns(4, gap="small")
    with action_cols[0]:
        if st.button("Add dependency"):
            add_dependency_dialog()
    with action_cols[1]:
        view_disabled = selected_dependency is None
        if st.button("View selected dependency", disabled=view_disabled):
            dependency_detail_dialog(selected_dependency)
    with action_cols[2]:
        edit_disabled = selected_dependency is None
        if st.button("Edit selected dependency", disabled=edit_disabled):
            edit_dependency_dialog(selected_dependency)
    with action_cols[3]:
        delete_disabled = not selected_ids
        if st.button("Delete selected dependency", disabled=delete_disabled):
            delete_dependency_dialog(selected_ids, dependency_by_id)

    if dependency_rows and not selected_ids:
        st.info("Select dependencies from the list to edit or delete.")
    elif dependency_rows and len(selected_ids) > 1:
        st.info("Multiple items selected. Edit is disabled; Delete is enabled.")

if tab_choice == "Themes":
    theme_rows = fetch_theme_rows()

    @st.dialog("Add theme")
    def add_theme_dialog():
        with st.form("add_theme_form"):
            theme_name = st.text_input("Name")
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
