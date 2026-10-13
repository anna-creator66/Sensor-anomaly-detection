#!/usr/bin/env python3
"""
Real-Time IoT Sensor Stream & Anomaly Detection Pipeline  (Tkinter frontend)
===========================================================================

Tabs
----
1. Dashboard      - device selector, live KPI cards (Temperature / Voltage / Status),
                    embedded Matplotlib live line chart (anomalies marked in red).
2. Anomaly Log    - ttk.Treeview of flagged anomalies, Refresh button, auto-polling,
                    "Acknowledge Selected" action.
3. Simulator      - Start/Stop background sensor generator, "Inject Anomaly" button,
                    live event log.

Anomaly detection (all in SQL)
------------------------------
* THRESHOLD : an AFTER INSERT trigger on `sensor_readings` flags hard-limit
              violations the instant a row lands (also fires for injected rows).
* ZSCORE    : a window-function query (AVG(...) OVER (... ROWS BETWEEN N PRECEDING
              AND 1 PRECEDING)) computes a rolling mean/variance per device and
              flags readings that deviate by more than ZSCORE_LIMIT sigmas.

Run
---
    pip install matplotlib
    python iot_pipeline_app.py

Switching to PostgreSQL
-----------------------
    pip install psycopg2-binary
    set DB_BACKEND = "postgres" and edit PG_DSN below.
    The Database class translates '?' placeholders to '%s' and uses the PG DDL
    (including a plpgsql trigger function) defined further down.
"""

from __future__ import annotations

import queue
import random
import sqlite3
import threading
import tkinter as tk
from datetime import datetime
from tkinter import messagebox, ttk

import matplotlib

matplotlib.use("TkAgg")
import matplotlib.dates as mdates  # noqa: E402
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg  # noqa: E402
from matplotlib.figure import Figure  # noqa: E402

# =============================================================================
# 1. CONFIGURATION
# =============================================================================
DB_BACKEND = "sqlite"                      # "sqlite" | "postgres"
SQLITE_PATH = "iot_pipeline.db"
PG_DSN = "host=localhost port=5432 dbname=iot user=postgres password=postgres"

POLL_MS = 1000                             # UI polling interval (root.after)
CHART_POINTS = 60                          # readings shown on the live chart
ALERT_ROWS = 500                           # max rows shown in the alert table
KEEP_READINGS = 20000                      # retention: newest N readings kept

# Hard limits enforced by the DB trigger
TEMP_MIN, TEMP_MAX = 5.0, 40.0             # degrees C
VOLT_MIN, VOLT_MAX = 11.0, 13.0            # volts (12 V DC rail)

# Statistical detection (window-function query)
ZSCORE_LIMIT = 3.5
ZSCORE_WINDOW = 30                         # preceding rows used for mean/variance
ZSCORE_MIN_SAMPLES = 10

DEFAULT_DEVICES = [
    ("Sensor_Floor_1", "Floor 1 - Assembly"),
    ("Sensor_Floor_2", "Floor 2 - Packaging"),
    ("Sensor_Floor_3", "Floor 3 - Warehouse"),
]

# Dark theme palette
BG = "#0f1419"
PANEL = "#171d25"
CARD = "#1f2733"
BORDER = "#2b3544"
FG = "#e6edf3"
MUTED = "#8b98a8"
ACCENT = "#2f81f7"
ACCENT_DARK = "#1f5fbf"
OK = "#3fb950"
BAD = "#f85149"
WARN = "#d29922"
TEMP_COLOR = "#ff9f43"
VOLT_COLOR = "#4fc3f7"


# =============================================================================
# 2. SCHEMA (DDL)
# =============================================================================
SQLITE_DDL = f"""
CREATE TABLE IF NOT EXISTS devices (
    device_id   TEXT PRIMARY KEY,
    location    TEXT,
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sensor_readings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL REFERENCES devices(device_id),
    ts           TEXT NOT NULL,
    temperature  REAL NOT NULL,
    voltage      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_readings_device_id ON sensor_readings(device_id, id);

CREATE TABLE IF NOT EXISTS anomaly_alerts (
    alert_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id         TEXT NOT NULL,
    metric_name       TEXT NOT NULL,
    value             REAL NOT NULL,
    ts                TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'OPEN',
    detection_method  TEXT NOT NULL DEFAULT 'THRESHOLD',
    reading_id        INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_reading_metric
    ON anomaly_alerts(reading_id, metric_name);
CREATE INDEX IF NOT EXISTS idx_alert_device ON anomaly_alerts(device_id, reading_id);

-- Hard-limit detection: fires on every insert, including manual injections.
CREATE TRIGGER IF NOT EXISTS trg_threshold_alert
AFTER INSERT ON sensor_readings
BEGIN
    INSERT OR IGNORE INTO anomaly_alerts
        (device_id, metric_name, value, ts, status, detection_method, reading_id)
    SELECT NEW.device_id, 'temperature', NEW.temperature, NEW.ts, 'OPEN', 'THRESHOLD', NEW.id
    WHERE NEW.temperature > {TEMP_MAX} OR NEW.temperature < {TEMP_MIN};

    INSERT OR IGNORE INTO anomaly_alerts
        (device_id, metric_name, value, ts, status, detection_method, reading_id)
    SELECT NEW.device_id, 'voltage', NEW.voltage, NEW.ts, 'OPEN', 'THRESHOLD', NEW.id
    WHERE NEW.voltage > {VOLT_MAX} OR NEW.voltage < {VOLT_MIN};
END;
"""

# PostgreSQL equivalent (scaffold - not exercised by the SQLite test path).
PG_DDL = f"""
CREATE TABLE IF NOT EXISTS devices (
    device_id   TEXT PRIMARY KEY,
    location    TEXT,
    created_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sensor_readings (
    id           BIGSERIAL PRIMARY KEY,
    device_id    TEXT NOT NULL REFERENCES devices(device_id),
    ts           TIMESTAMP NOT NULL,
    temperature  DOUBLE PRECISION NOT NULL,
    voltage      DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_readings_device_id ON sensor_readings(device_id, id);

CREATE TABLE IF NOT EXISTS anomaly_alerts (
    alert_id          BIGSERIAL PRIMARY KEY,
    device_id         TEXT NOT NULL,
    metric_name       TEXT NOT NULL,
    value             DOUBLE PRECISION NOT NULL,
    ts                TIMESTAMP NOT NULL,
    status            TEXT NOT NULL DEFAULT 'OPEN',
    detection_method  TEXT NOT NULL DEFAULT 'THRESHOLD',
    reading_id        BIGINT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_alert_reading_metric
    ON anomaly_alerts(reading_id, metric_name);
CREATE INDEX IF NOT EXISTS idx_alert_device ON anomaly_alerts(device_id, reading_id);

CREATE OR REPLACE FUNCTION fn_threshold_alert() RETURNS trigger AS $$
BEGIN
    IF NEW.temperature > {TEMP_MAX} OR NEW.temperature < {TEMP_MIN} THEN
        INSERT INTO anomaly_alerts (device_id, metric_name, value, ts, status, detection_method, reading_id)
        VALUES (NEW.device_id, 'temperature', NEW.temperature, NEW.ts, 'OPEN', 'THRESHOLD', NEW.id)
        ON CONFLICT DO NOTHING;
    END IF;
    IF NEW.voltage > {VOLT_MAX} OR NEW.voltage < {VOLT_MIN} THEN
        INSERT INTO anomaly_alerts (device_id, metric_name, value, ts, status, detection_method, reading_id)
        VALUES (NEW.device_id, 'voltage', NEW.voltage, NEW.ts, 'OPEN', 'THRESHOLD', NEW.id)
        ON CONFLICT DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_threshold_alert ON sensor_readings;
CREATE TRIGGER trg_threshold_alert
AFTER INSERT ON sensor_readings
FOR EACH ROW EXECUTE FUNCTION fn_threshold_alert();
"""


# =============================================================================
# 3. DATABASE LAYER
# =============================================================================
class DatabaseError(Exception):
    """Raised for any database failure (wraps driver-specific exceptions)."""


def to_datetime(value) -> datetime:
    """Normalise a timestamp from either backend (str in SQLite, datetime in PG)."""
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


class Database:
    """
    Thin wrapper around one DB connection.

    Connections are NOT shared between threads: the UI thread and the simulator
    thread each create their own Database() instance.
    """

    def __init__(self) -> None:
        self.backend = DB_BACKEND
        self.conn = None

    # ---- connection hooks -------------------------------------------------
    def connect(self) -> None:
        """Open the connection. Edit here to customise PostgreSQL settings."""
        try:
            if self.backend == "sqlite":
                self.conn = sqlite3.connect(SQLITE_PATH, timeout=10)
                self.conn.execute("PRAGMA journal_mode=WAL")   # readers don't block writer
                self.conn.execute("PRAGMA synchronous=NORMAL")
            elif self.backend == "postgres":
                import psycopg2  # imported lazily so SQLite users need no driver

                self.conn = psycopg2.connect(PG_DSN)
            else:
                raise DatabaseError(f"Unknown DB_BACKEND '{self.backend}'")
        except DatabaseError:
            raise
        except Exception as exc:
            raise DatabaseError(f"Could not connect to {self.backend}: {exc}") from exc

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

    # ---- low-level helpers ------------------------------------------------
    def _q(self, sql: str) -> str:
        """Translate '?' placeholders to the active driver's paramstyle."""
        return sql.replace("?", "%s") if self.backend == "postgres" else sql

    def _query(self, sql: str, params: tuple = ()) -> list:
        try:
            cur = self.conn.cursor()
            cur.execute(self._q(sql), params)
            rows = cur.fetchall()
            cur.close()
            if self.backend == "postgres":
                self.conn.commit()  # end the read transaction
            return rows
        except Exception as exc:
            self._safe_rollback()
            raise DatabaseError(str(exc)) from exc

    def _safe_rollback(self) -> None:
        try:
            self.conn.rollback()
        except Exception:
            pass

    # ---- schema -----------------------------------------------------------
    def init_schema(self) -> None:
        """Create tables, indexes, trigger and seed the default devices."""
        try:
            if self.backend == "sqlite":
                self.conn.executescript(SQLITE_DDL)
            else:
                cur = self.conn.cursor()
                cur.execute(PG_DDL)
                cur.close()
            cur = self.conn.cursor()
            for device_id, location in DEFAULT_DEVICES:
                cur.execute(
                    self._q("INSERT INTO devices (device_id, location) VALUES (?, ?) "
                            "ON CONFLICT DO NOTHING"),
                    (device_id, location),
                )
            cur.close()
            self.conn.commit()
        except Exception as exc:
            self._safe_rollback()
            raise DatabaseError(f"Schema initialisation failed: {exc}") from exc

    # ---- writes -----------------------------------------------------------
    def insert_reading(self, device_id: str, temperature: float, voltage: float):
        """
        Insert one reading, run window-function detection, commit.
        Returns (reading_id, number_of_alerts_for_this_reading).
        The threshold trigger fires automatically inside the INSERT.
        """
        now = datetime.now()
        ts = now.isoformat(sep=" ", timespec="microseconds") if self.backend == "sqlite" else now
        try:
            cur = self.conn.cursor()
            if self.backend == "sqlite":
                cur.execute(
                    "INSERT INTO sensor_readings (device_id, ts, temperature, voltage) "
                    "VALUES (?, ?, ?, ?)",
                    (device_id, ts, temperature, voltage),
                )
                reading_id = cur.lastrowid
            else:
                cur.execute(
                    "INSERT INTO sensor_readings (device_id, ts, temperature, voltage) "
                    "VALUES (%s, %s, %s, %s) RETURNING id",
                    (device_id, ts, temperature, voltage),
                )
                reading_id = cur.fetchone()[0]
            cur.close()

            self._detect_zscore(reading_id, device_id, ts)

            cur = self.conn.cursor()
            cur.execute(self._q("SELECT COUNT(*) FROM anomaly_alerts WHERE reading_id = ?"),
                        (reading_id,))
            n_alerts = cur.fetchone()[0]
            cur.close()
            self.conn.commit()
            return reading_id, n_alerts
        except Exception as exc:
            self._safe_rollback()
            raise DatabaseError(f"Insert failed: {exc}") from exc

    def _detect_zscore(self, reading_id: int, device_id: str, ts) -> None:
        """
        Window-function anomaly detection for the reading just inserted.

        For each metric: rolling AVG(x) and AVG(x*x) over the previous
        ZSCORE_WINDOW rows of the same device give mean and variance
        (var = E[x^2] - E[x]^2). A reading is flagged when
        (x - mean)^2 > Z^2 * var. Comparing squares avoids needing sqrt().
        """
        z2 = ZSCORE_LIMIT ** 2
        cond = (f"id = ? AND n >= {ZSCORE_MIN_SAMPLES} "
                "AND ({sq} - {m}*{m}) > 0.000001 "
                f"AND ({{x}} - {{m}})*({{x}} - {{m}}) > {z2} * ({{sq}} - {{m}}*{{m}})")
        temp_cond = cond.format(x="t", m="t_mean", sq="t_sq")
        volt_cond = cond.format(x="v", m="v_mean", sq="v_sq")
        sql = f"""
            WITH w AS (
                SELECT id,
                       temperature AS t,
                       voltage     AS v,
                       AVG(temperature)             OVER win AS t_mean,
                       AVG(temperature*temperature) OVER win AS t_sq,
                       AVG(voltage)                 OVER win AS v_mean,
                       AVG(voltage*voltage)         OVER win AS v_sq,
                       COUNT(*)                     OVER win AS n
                FROM sensor_readings
                WHERE device_id = ? AND id > ? AND id <= ?
                WINDOW win AS (ORDER BY id
                               ROWS BETWEEN {ZSCORE_WINDOW} PRECEDING AND 1 PRECEDING)
            )
            SELECT 'temperature' AS metric, t AS val FROM w WHERE {temp_cond}
            UNION ALL
            SELECT 'voltage' AS metric, v AS val FROM w WHERE {volt_cond}
        """
        # Scan range is bounded so the query stays fast on long histories.
        params = (device_id, reading_id - 400, reading_id, reading_id, reading_id)
        cur = self.conn.cursor()
        cur.execute(self._q(sql), params)
        flagged = cur.fetchall()
        for metric, value in flagged:
            cur.execute(
                self._q("INSERT INTO anomaly_alerts "
                        "(device_id, metric_name, value, ts, status, detection_method, reading_id) "
                        "VALUES (?, ?, ?, ?, 'OPEN', 'ZSCORE', ?) ON CONFLICT DO NOTHING"),
                (device_id, metric, value, ts, reading_id),
            )
        cur.close()

    def acknowledge(self, alert_ids: list[int]) -> None:
        try:
            cur = self.conn.cursor()
            cur.executemany(
                self._q("UPDATE anomaly_alerts SET status = 'ACKNOWLEDGED' WHERE alert_id = ?"),
                [(a,) for a in alert_ids],
            )
            cur.close()
            self.conn.commit()
        except Exception as exc:
            self._safe_rollback()
            raise DatabaseError(f"Acknowledge failed: {exc}") from exc

    def prune(self) -> None:
        """Retention: keep only the newest KEEP_READINGS rows."""
        try:
            cur = self.conn.cursor()
            cur.execute(
                self._q("DELETE FROM sensor_readings "
                        "WHERE id < (SELECT MAX(id) FROM sensor_readings) - ?"),
                (KEEP_READINGS,),
            )
            cur.close()
            self.conn.commit()
        except Exception as exc:
            self._safe_rollback()
            raise DatabaseError(f"Prune failed: {exc}") from exc

    # ---- reads ------------------------------------------------------------
    def list_devices(self) -> list[str]:
        return [r[0] for r in self._query("SELECT device_id FROM devices ORDER BY device_id")]

    def latest_reading(self, device_id: str):
        """Returns (id, ts, temperature, voltage) or None."""
        rows = self._query(
            "SELECT id, ts, temperature, voltage FROM sensor_readings "
            "WHERE device_id = ? ORDER BY id DESC LIMIT 1", (device_id,))
        return rows[0] if rows else None

    def recent_readings(self, device_id: str, limit: int) -> list:
        rows = self._query(
            "SELECT id, ts, temperature, voltage FROM sensor_readings "
            "WHERE device_id = ? ORDER BY id DESC LIMIT ?", (device_id, limit))
        return list(reversed(rows))

    def flagged_metrics(self, device_id: str, min_reading_id: int) -> dict[int, set[str]]:
        """Map reading_id -> set of flagged metric names (for chart markers / KPI colour)."""
        rows = self._query(
            "SELECT reading_id, metric_name FROM anomaly_alerts "
            "WHERE device_id = ? AND reading_id >= ?", (device_id, min_reading_id))
        out: dict[int, set[str]] = {}
        for rid, metric in rows:
            out.setdefault(rid, set()).add(metric)
        return out

    def recent_alerts(self, limit: int) -> list:
        return self._query(
            "SELECT alert_id, device_id, metric_name, value, ts, status, detection_method "
            "FROM anomaly_alerts ORDER BY alert_id DESC LIMIT ?", (limit,))


# =============================================================================
# 4. SENSOR SIMULATOR (background thread)
# =============================================================================
class Simulator(threading.Thread):
    """Generates realistic readings for every device and inserts them into SQL."""

    def __init__(self, log_q: queue.Queue, devices: list[str],
                 interval: float, anomaly_prob: float) -> None:
        super().__init__(daemon=True, name="SensorSimulator")
        self.log_q = log_q
        self.devices = devices
        self.interval = interval            # seconds between rounds (live-adjustable)
        self.anomaly_prob = anomaly_prob    # 0..1 chance per reading (live-adjustable)
        self.stop_event = threading.Event()

    def stop(self) -> None:
        self.stop_event.set()

    @staticmethod
    def _bases(index: int) -> tuple[float, float]:
        return 23.0 + 1.5 * index, 12.0

    def _next_values(self, state: dict, index: int):
        """Mean-reverting random walk; occasionally returns an anomalous reading."""
        t_base, v_base = self._bases(index)
        state["t"] = t_base + 0.8 * (state["t"] - t_base) + random.gauss(0, 0.25)
        state["v"] = v_base + 0.8 * (state["v"] - v_base) + random.gauss(0, 0.04)
        temp, volt = state["t"], state["v"]
        if random.random() < self.anomaly_prob:
            kind = random.choice(["temp", "volt", "both"])
            if kind in ("temp", "both"):
                temp += random.uniform(18, 35)
            if kind in ("volt", "both"):
                volt -= random.uniform(1.5, 4.0)
        return temp, volt

    def run(self) -> None:
        db = Database()
        try:
            db.connect()
        except DatabaseError as exc:
            self.log_q.put(("error", f"Simulator could not connect: {exc}"))
            return

        state = {d: {"t": self._bases(i)[0], "v": self._bases(i)[1]}
                 for i, d in enumerate(self.devices)}
        failures, count = 0, 0
        self.log_q.put(("info", f"Simulator started for {len(self.devices)} device(s)"))
        try:
            while not self.stop_event.is_set():
                for i, dev in enumerate(self.devices):
                    if self.stop_event.is_set():
                        break
                    try:
                        temp, volt = self._next_values(state[dev], i)
                        _, n_alerts = db.insert_reading(dev, round(temp, 3), round(volt, 3))
                        failures = 0
                        count += 1
                        if n_alerts:
                            self.log_q.put(("warn",
                                            f"{dev}: {n_alerts} alert(s) raised "
                                            f"(T={temp:.1f} C, V={volt:.2f} V)"))
                        if count % 500 == 0:
                            db.prune()
                    except DatabaseError as exc:
                        failures += 1
                        self.log_q.put(("error", f"Simulator write failed ({failures}/5): {exc}"))
                        if failures >= 5:
                            self.log_q.put(("error", "Too many consecutive failures - stopping."))
                            return
                self.stop_event.wait(max(0.1, self.interval))
        finally:
            db.close()
            self.log_q.put(("info", f"Simulator stopped after {count} readings"))


# =============================================================================
# 5. THEME
# =============================================================================
def apply_theme(root: tk.Tk) -> None:
    """Configure a dark ttk theme (based on 'clam' so colours are honoured everywhere)."""
    style = ttk.Style(root)
    style.theme_use("clam")
    root.configure(bg=BG)

    base_font = ("Helvetica", 10)
    style.configure(".", background=BG, foreground=FG, fieldbackground=PANEL,
                    bordercolor=BORDER, lightcolor=BORDER, darkcolor=BORDER,
                    troughcolor=PANEL, font=base_font)
    style.configure("TFrame", background=BG)
    style.configure("TLabel", background=BG, foreground=FG)
    style.configure("Muted.TLabel", foreground=MUTED)
    style.configure("Title.TLabel", font=("Helvetica", 13, "bold"))

    style.configure("TNotebook", background=BG, borderwidth=0)
    style.configure("TNotebook.Tab", padding=(18, 8), background=PANEL, foreground=MUTED)
    style.map("TNotebook.Tab",
              background=[("selected", CARD)], foreground=[("selected", FG)])

    style.configure("TButton", padding=(12, 7), background=CARD, foreground=FG,
                    borderwidth=1, focusthickness=0)
    style.map("TButton",
              background=[("active", BORDER), ("disabled", PANEL)],
              foreground=[("disabled", MUTED)])
    style.configure("Accent.TButton", background=ACCENT, foreground="#ffffff")
    style.map("Accent.TButton", background=[("active", ACCENT_DARK), ("disabled", PANEL)])
    style.configure("Danger.TButton", background="#b62324", foreground="#ffffff")
    style.map("Danger.TButton", background=[("active", "#8e1b1c"), ("disabled", PANEL)])

    style.configure("TCombobox", fieldbackground=PANEL, background=CARD,
                    foreground=FG, arrowcolor=FG, padding=4)
    style.map("TCombobox", fieldbackground=[("readonly", PANEL)],
              foreground=[("readonly", FG)])
    root.option_add("*TCombobox*Listbox.background", PANEL)
    root.option_add("*TCombobox*Listbox.foreground", FG)
    root.option_add("*TCombobox*Listbox.selectBackground", ACCENT)
    style.configure("TSpinbox", fieldbackground=PANEL, foreground=FG,
                    arrowcolor=FG, background=CARD)

    style.configure("TLabelframe", background=BG, bordercolor=BORDER)
    style.configure("TLabelframe.Label", background=BG, foreground=MUTED)

    style.configure("Treeview", background=PANEL, fieldbackground=PANEL,
                    foreground=FG, rowheight=26, borderwidth=0)
    style.configure("Treeview.Heading", background=CARD, foreground=FG,
                    relief="flat", font=("Helvetica", 10, "bold"), padding=6)
    style.map("Treeview", background=[("selected", ACCENT)],
              foreground=[("selected", "#ffffff")])
    style.map("Treeview.Heading", background=[("active", BORDER)])


# =============================================================================
# 6. MAIN APPLICATION
# =============================================================================
class App:
    def __init__(self, root: tk.Tk, db: Database) -> None:
        self.root = root
        self.db = db
        self.sim: Simulator | None = None
        self.log_q: queue.Queue = queue.Queue()
        self._alert_signature = None
        self._poll_job = None

        root.title("IoT Sensor Stream & Anomaly Detection Pipeline")
        root.geometry("1120x760")
        root.minsize(900, 640)
        apply_theme(root)

        self.device_var = tk.StringVar()
        self.interval_var = tk.DoubleVar(value=1.0)
        self.prob_var = tk.DoubleVar(value=3.0)          # percent
        self.inject_metric_var = tk.StringVar(value="Temperature spike")
        self.inject_device_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Ready")

        self._build_layout()
        self._load_devices()
        self.interval_var.trace_add("write", lambda *_: self._sync_sim_settings())
        self.prob_var.trace_add("write", lambda *_: self._sync_sim_settings())

        self._poll()  # start the root.after() polling loop

    # ------------------------------------------------------------------ layout
    def _build_layout(self) -> None:
        self.status_bar = tk.Label(self.root, textvariable=self.status_var, anchor="w",
                                   bg=PANEL, fg=MUTED, padx=10, pady=4)
        self.status_bar.pack(side="bottom", fill="x")

        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=(10, 4))

        self.tab_dash = ttk.Frame(self.notebook, padding=10)
        self.tab_alerts = ttk.Frame(self.notebook, padding=10)
        self.tab_sim = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.tab_dash, text="  Dashboard & Live Stream  ")
        self.notebook.add(self.tab_alerts, text="  Anomaly Alert Log  ")
        self.notebook.add(self.tab_sim, text="  Simulator Control Panel  ")

        self._build_dashboard()
        self._build_alert_tab()
        self._build_simulator_tab()

    def _build_dashboard(self) -> None:
        top = ttk.Frame(self.tab_dash)
        top.pack(fill="x")
        ttk.Label(top, text="Active device:", style="Title.TLabel").pack(side="left")
        self.device_combo = ttk.Combobox(top, textvariable=self.device_var,
                                         state="readonly", width=24)
        self.device_combo.pack(side="left", padx=10)
        self.device_combo.bind("<<ComboboxSelected>>", lambda e: self._refresh_dashboard())
        self.last_update_lbl = ttk.Label(top, text="", style="Muted.TLabel")
        self.last_update_lbl.pack(side="right")

        cards = ttk.Frame(self.tab_dash)
        cards.pack(fill="x", pady=12)
        for c in range(3):
            cards.columnconfigure(c, weight=1, uniform="card")
        self.temp_val, self.temp_sub = self._make_card(cards, 0, "TEMPERATURE", "deg C")
        self.volt_val, self.volt_sub = self._make_card(cards, 1, "VOLTAGE", "V")
        self.status_val, self.status_sub = self._make_card(cards, 2, "STATUS", "")

        # Matplotlib live chart
        self.fig = Figure(figsize=(8, 4), dpi=100, facecolor=BG)
        self.ax = self.fig.add_subplot(111)
        self.ax2 = self.ax.twinx()
        self._style_chart()
        (self.line_t,) = self.ax.plot([], [], color=TEMP_COLOR, lw=1.8, label="Temperature (C)")
        (self.line_v,) = self.ax2.plot([], [], color=VOLT_COLOR, lw=1.8, label="Voltage (V)")
        (self.pts_t,) = self.ax.plot([], [], "o", color=BAD, ms=7, mfc=BAD, mec="white",
                                     label="Anomaly")
        (self.pts_v,) = self.ax2.plot([], [], "o", color=BAD, ms=7, mfc=BAD, mec="white")
        self.ax.legend(handles=[self.line_t, self.line_v, self.pts_t], loc="upper left",
                       facecolor=PANEL, edgecolor=BORDER, labelcolor=FG, fontsize=9)
        self.fig.tight_layout()

        self.canvas = FigureCanvasTkAgg(self.fig, master=self.tab_dash)
        self.canvas.get_tk_widget().configure(bg=BG, highlightthickness=0)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)

    def _style_chart(self) -> None:
        self.ax.set_facecolor(PANEL)
        for ax in (self.ax, self.ax2):
            for spine in ax.spines.values():
                spine.set_color(BORDER)
        self.ax.tick_params(axis="x", colors=MUTED, labelsize=8)
        self.ax.tick_params(axis="y", colors=TEMP_COLOR, labelsize=8)
        self.ax2.tick_params(axis="y", colors=VOLT_COLOR, labelsize=8)
        self.ax.set_ylabel("Temperature (C)", color=TEMP_COLOR)
        self.ax2.set_ylabel("Voltage (V)", color=VOLT_COLOR)
        self.ax.grid(True, color=BORDER, alpha=0.5, lw=0.6)
        self.ax.xaxis_date()
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

    def _make_card(self, parent, col: int, title: str, unit: str):
        card = tk.Frame(parent, bg=CARD, highlightbackground=BORDER, highlightthickness=1)
        card.grid(row=0, column=col, sticky="nsew", padx=6)
        tk.Label(card, text=title, bg=CARD, fg=MUTED,
                 font=("Helvetica", 9, "bold")).pack(anchor="w", padx=14, pady=(10, 0))
        row = tk.Frame(card, bg=CARD)
        row.pack(anchor="w", padx=14)
        value = tk.Label(row, text="--", bg=CARD, fg=FG, font=("Helvetica", 28, "bold"))
        value.pack(side="left")
        tk.Label(row, text=f" {unit}", bg=CARD, fg=MUTED,
                 font=("Helvetica", 11)).pack(side="left", pady=(10, 0))
        sub = tk.Label(card, text="", bg=CARD, fg=MUTED, font=("Helvetica", 9))
        sub.pack(anchor="w", padx=14, pady=(0, 10))
        return value, sub

    def _build_alert_tab(self) -> None:
        bar = ttk.Frame(self.tab_alerts)
        bar.pack(fill="x", pady=(0, 8))
        ttk.Button(bar, text="Refresh Data", style="Accent.TButton",
                   command=self._refresh_alerts_forced).pack(side="left")
        ttk.Button(bar, text="Acknowledge Selected",
                   command=self._acknowledge_selected).pack(side="left", padx=8)
        self.alert_count_lbl = ttk.Label(bar, text="", style="Muted.TLabel")
        self.alert_count_lbl.pack(side="right")

        cols = ("alert_id", "device_id", "metric", "value", "timestamp", "status", "method")
        heads = ("Alert ID", "Device ID", "Metric Name", "Value", "Timestamp", "Status", "Method")
        widths = (80, 140, 110, 90, 190, 120, 100)
        frame = ttk.Frame(self.tab_alerts)
        frame.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(frame, columns=cols, show="headings", selectmode="extended")
        for c, h, w in zip(cols, heads, widths):
            self.tree.heading(c, text=h)
            self.tree.column(c, width=w, anchor="center" if c != "timestamp" else "w")
        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        self.tree.tag_configure("open", foreground="#ff7b72")
        self.tree.tag_configure("ack", foreground=MUTED)

    def _build_simulator_tab(self) -> None:
        left = ttk.Frame(self.tab_sim)
        left.pack(side="left", fill="y", padx=(0, 12))

        sim_box = ttk.LabelFrame(left, text=" Sensor Simulator ", padding=12)
        sim_box.pack(fill="x")
        btns = ttk.Frame(sim_box)
        btns.pack(fill="x")
        self.start_btn = ttk.Button(btns, text="Start Sensor Simulator",
                                    style="Accent.TButton", command=self._start_sim)
        self.start_btn.pack(side="left")
        self.stop_btn = ttk.Button(btns, text="Stop Simulator", style="Danger.TButton",
                                   command=self._stop_sim, state="disabled")
        self.stop_btn.pack(side="left", padx=8)

        opts = ttk.Frame(sim_box)
        opts.pack(fill="x", pady=(12, 0))
        ttk.Label(opts, text="Interval (s):").grid(row=0, column=0, sticky="w")
        ttk.Spinbox(opts, from_=0.2, to=10, increment=0.2, width=7,
                    textvariable=self.interval_var).grid(row=0, column=1, padx=8, pady=3)
        ttk.Label(opts, text="Random anomaly rate (%):").grid(row=1, column=0, sticky="w")
        ttk.Spinbox(opts, from_=0, to=50, increment=1, width=7,
                    textvariable=self.prob_var).grid(row=1, column=1, padx=8, pady=3)
        self.sim_state_lbl = ttk.Label(sim_box, text="State: stopped", style="Muted.TLabel")
        self.sim_state_lbl.pack(anchor="w", pady=(10, 0))

        inj = ttk.LabelFrame(left, text=" Manual Anomaly Injection ", padding=12)
        inj.pack(fill="x", pady=(14, 0))
        ttk.Label(inj, text="Device:").grid(row=0, column=0, sticky="w")
        self.inject_device_combo = ttk.Combobox(inj, textvariable=self.inject_device_var,
                                                state="readonly", width=22)
        self.inject_device_combo.grid(row=0, column=1, padx=8, pady=3)
        ttk.Label(inj, text="Type:").grid(row=1, column=0, sticky="w")
        ttk.Combobox(inj, textvariable=self.inject_metric_var, state="readonly", width=22,
                     values=["Temperature spike", "Voltage drop", "Both"]
                     ).grid(row=1, column=1, padx=8, pady=3)
        ttk.Button(inj, text="Inject Anomaly", style="Danger.TButton",
                   command=self._inject_anomaly).grid(row=2, column=0, columnspan=2,
                                                      sticky="ew", pady=(10, 0))
        ttk.Label(inj, text=f"Trigger limits: {TEMP_MIN:g}-{TEMP_MAX:g} C, "
                            f"{VOLT_MIN:g}-{VOLT_MAX:g} V", style="Muted.TLabel"
                  ).grid(row=3, column=0, columnspan=2, sticky="w", pady=(8, 0))

        right = ttk.LabelFrame(self.tab_sim, text=" Event Log ", padding=8)
        right.pack(side="left", fill="both", expand=True)
        self.log_text = tk.Text(right, bg=PANEL, fg=FG, bd=0, wrap="word", state="disabled",
                                font=("Courier", 9), insertbackground=FG, height=10)
        sb = ttk.Scrollbar(right, orient="vertical", command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=sb.set)
        self.log_text.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")
        self.log_text.tag_configure("info", foreground=FG)
        self.log_text.tag_configure("warn", foreground=WARN)
        self.log_text.tag_configure("error", foreground=BAD)
        self.log_text.tag_configure("ok", foreground=OK)

    # ----------------------------------------------------------------- helpers
    def _set_status(self, text: str, error: bool = False) -> None:
        self.status_var.set(text)
        self.status_bar.configure(fg=BAD if error else MUTED)

    def _log(self, level: str, text: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"[{stamp}] {text}\n", level)
        # Keep the log bounded
        if int(self.log_text.index("end-1c").split(".")[0]) > 500:
            self.log_text.delete("1.0", "50.0")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _load_devices(self) -> None:
        try:
            devices = self.db.list_devices()
        except DatabaseError as exc:
            self._set_status(f"DB error: {exc}", error=True)
            return
        self.device_combo["values"] = devices
        self.inject_device_combo["values"] = devices
        if devices:
            if self.device_var.get() not in devices:
                self.device_var.set(devices[0])
            if self.inject_device_var.get() not in devices:
                self.inject_device_var.set(devices[0])

    # --------------------------------------------------------- polling (after)
    def _poll(self) -> None:
        """Runs every POLL_MS on the Tk event loop; never blocks for long."""
        try:
            self._drain_log_queue()
            self._check_sim_alive()
            self._refresh_dashboard()
            self._refresh_alerts()
            if str(self.status_bar.cget("fg")) != BAD:  # keep DB errors visible
                self._set_status(f"Live - last poll {datetime.now():%H:%M:%S} ({DB_BACKEND})")
        except Exception as exc:  # never let an error kill the polling loop
            self._set_status(f"Polling error: {exc}", error=True)
        finally:
            self._poll_job = self.root.after(POLL_MS, self._poll)

    def _drain_log_queue(self) -> None:
        try:
            while True:
                level, text = self.log_q.get_nowait()
                self._log(level, text)
        except queue.Empty:
            pass

    def _check_sim_alive(self) -> None:
        if self.sim is not None and not self.sim.is_alive():
            self.sim = None
            self._set_sim_buttons(running=False)

    # -------------------------------------------------------------- dashboard
    def _refresh_dashboard(self) -> None:
        device = self.device_var.get()
        if not device:
            return
        try:
            latest = self.db.latest_reading(device)
            if latest is None:
                self._show_no_data()
                return
            rows = self.db.recent_readings(device, CHART_POINTS)
            flagged = self.db.flagged_metrics(device, rows[0][0])
        except DatabaseError as exc:
            self._set_status(f"DB error: {exc}", error=True)
            return

        self.status_bar.configure(fg=MUTED)
        rid, ts, temp, volt = latest
        bad = flagged.get(rid, set())
        age = (datetime.now() - to_datetime(ts)).total_seconds()

        self.temp_val.configure(text=f"{temp:.2f}", fg=BAD if "temperature" in bad else FG)
        self.volt_val.configure(text=f"{volt:.2f}", fg=BAD if "voltage" in bad else FG)
        self.temp_sub.configure(text=f"limits {TEMP_MIN:g} - {TEMP_MAX:g}")
        self.volt_sub.configure(text=f"limits {VOLT_MIN:g} - {VOLT_MAX:g}")
        if bad:
            self.status_val.configure(text="Anomalous", fg=BAD)
            self.status_sub.configure(text="flagged: " + ", ".join(sorted(bad)))
        else:
            self.status_val.configure(text="Normal", fg=OK)
            self.status_sub.configure(text="all metrics within range")
        stale = age > max(5.0, 3 * float(self.interval_var.get() or 1))
        self.last_update_lbl.configure(
            text=f"Last reading {to_datetime(ts):%H:%M:%S}" + ("  (stale - no new data)" if stale else ""),
            foreground=WARN if stale else MUTED)

        self._update_chart(rows, flagged)

    def _show_no_data(self) -> None:
        for lbl in (self.temp_val, self.volt_val):
            lbl.configure(text="--", fg=FG)
        self.status_val.configure(text="No data", fg=MUTED)
        self.status_sub.configure(text="start the simulator")
        self.temp_sub.configure(text="")
        self.volt_sub.configure(text="")
        self.last_update_lbl.configure(text="", foreground=MUTED)
        for ln in (self.line_t, self.line_v, self.pts_t, self.pts_v):
            ln.set_data([], [])
        self.canvas.draw_idle()

    def _update_chart(self, rows: list, flagged: dict) -> None:
        ids = [r[0] for r in rows]
        xs = [mdates.date2num(to_datetime(r[1])) for r in rows]
        temps = [r[2] for r in rows]
        volts = [r[3] for r in rows]

        self.line_t.set_data(xs, temps)
        self.line_v.set_data(xs, volts)
        self.pts_t.set_data([x for x, i in zip(xs, ids) if "temperature" in flagged.get(i, ())],
                            [y for y, i in zip(temps, ids) if "temperature" in flagged.get(i, ())])
        self.pts_v.set_data([x for x, i in zip(xs, ids) if "voltage" in flagged.get(i, ())],
                            [y for y, i in zip(volts, ids) if "voltage" in flagged.get(i, ())])

        # Rescale both axes to the visible window with a little headroom.
        if len(xs) > 1:
            self.ax.set_xlim(xs[0], xs[-1])
        for ax, ys in ((self.ax, temps), (self.ax2, volts)):
            lo, hi = min(ys), max(ys)
            pad = max((hi - lo) * 0.15, 0.5)
            ax.set_ylim(lo - pad, hi + pad)
        self.canvas.draw_idle()

    # ------------------------------------------------------------- alert table
    def _refresh_alerts_forced(self) -> None:
        self._alert_signature = None
        self._load_devices()
        self._refresh_alerts()

    def _refresh_alerts(self) -> None:
        try:
            rows = self.db.recent_alerts(ALERT_ROWS)
        except DatabaseError as exc:
            self._set_status(f"DB error: {exc}", error=True)
            return
        signature = tuple(rows)
        if signature == self._alert_signature:
            return  # nothing changed - avoid flicker / lost scroll position
        self._alert_signature = signature

        selected = set(self.tree.selection())
        self.tree.delete(*self.tree.get_children())
        for alert_id, dev, metric, value, ts, status, method in rows:
            tag = "open" if status == "OPEN" else "ack"
            self.tree.insert("", "end", iid=str(alert_id), tags=(tag,), values=(
                alert_id, dev, metric, f"{value:.2f}",
                f"{to_datetime(ts):%Y-%m-%d %H:%M:%S}", status, method))
        keep = [i for i in selected if self.tree.exists(i)]
        if keep:
            self.tree.selection_set(keep)
        open_count = sum(1 for r in rows if r[5] == "OPEN")
        self.alert_count_lbl.configure(text=f"{len(rows)} shown - {open_count} open")

    def _acknowledge_selected(self) -> None:
        ids = [int(i) for i in self.tree.selection()]
        if not ids:
            messagebox.showinfo("Acknowledge", "Select one or more alerts first.")
            return
        try:
            self.db.acknowledge(ids)
        except DatabaseError as exc:
            messagebox.showerror("Database error", str(exc))
            return
        self._refresh_alerts_forced()

    # --------------------------------------------------------------- simulator
    def _set_sim_buttons(self, running: bool) -> None:
        self.start_btn.configure(state="disabled" if running else "normal")
        self.stop_btn.configure(state="normal" if running else "disabled")
        self.sim_state_lbl.configure(text="State: RUNNING" if running else "State: stopped",
                                     foreground=OK if running else MUTED)

    def _read_settings(self) -> tuple[float, float]:
        try:
            interval = float(self.interval_var.get())
            prob = float(self.prob_var.get()) / 100.0
        except (tk.TclError, ValueError):
            raise ValueError("Interval and anomaly rate must be numbers.")
        if not 0.1 <= interval <= 60:
            raise ValueError("Interval must be between 0.1 and 60 seconds.")
        if not 0 <= prob <= 1:
            raise ValueError("Anomaly rate must be between 0 and 100 %.")
        return interval, prob

    def _sync_sim_settings(self) -> None:
        """Apply spinbox edits to a running simulator immediately."""
        if self.sim is None:
            return
        try:
            self.sim.interval, self.sim.anomaly_prob = self._read_settings()
        except ValueError:
            pass  # ignore half-typed values

    def _start_sim(self) -> None:
        if self.sim is not None and self.sim.is_alive():
            return
        try:
            interval, prob = self._read_settings()
            devices = self.db.list_devices()
        except (ValueError, DatabaseError) as exc:
            messagebox.showerror("Cannot start simulator", str(exc))
            return
        self.sim = Simulator(self.log_q, devices, interval, prob)
        self.sim.start()
        self._set_sim_buttons(running=True)

    def _stop_sim(self) -> None:
        if self.sim is not None:
            self.sim.stop()  # thread exits after its current insert; poll loop resets buttons
        self.stop_btn.configure(state="disabled")

    def _inject_anomaly(self) -> None:
        device = self.inject_device_var.get()
        if not device:
            messagebox.showwarning("Inject Anomaly", "Choose a device first.")
            return
        kind = self.inject_metric_var.get()
        temp = round(random.gauss(24, 0.3), 3)
        volt = round(random.gauss(12, 0.05), 3)
        if kind in ("Temperature spike", "Both"):
            temp = round(random.uniform(65, 95), 2)
        if kind in ("Voltage drop", "Both"):
            volt = round(random.uniform(6, 9), 2)
        try:
            _, n = self.db.insert_reading(device, temp, volt)
        except DatabaseError as exc:
            messagebox.showerror("Database error", str(exc))
            return
        self._log("warn", f"INJECTED on {device}: T={temp} C, V={volt} V -> {n} alert(s)")
        self.device_var.set(device)       # jump the dashboard to the injected device
        self._alert_signature = None
        self._refresh_dashboard()
        self._refresh_alerts()

    # ---------------------------------------------------------------- shutdown
    def shutdown(self) -> None:
        if self._poll_job is not None:
            self.root.after_cancel(self._poll_job)
        if self.sim is not None:
            self.sim.stop()
            self.sim.join(timeout=3)
        self.db.close()
        self.root.destroy()


# =============================================================================
# 7. ENTRY POINT
# =============================================================================
def main() -> None:
    root = tk.Tk()
    db = Database()
    try:
        db.connect()
        db.init_schema()
    except DatabaseError as exc:
        root.withdraw()
        messagebox.showerror("Startup error", str(exc))
        return

    app = App(root, db)
    root.protocol("WM_DELETE_WINDOW", app.shutdown)
    root.mainloop()


if __name__ == "__main__":
    main()
