import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import backend
COLORS = {
    "bg": "#f0f0f0",
    "card": "#ffffff",
    "text": "#000000",
    "line": "#378ADD",
    "band": "#cfe8dc",
    "alert": "#e03a3e",
    "grid": "#dddddd",
}
REFRESH_MS = 2000
W, H = 900, 300
class App:
    def __init__(self, root):
        self.root = root
        self.running = True
        root.title("Sensor Anomaly Monitor")
        root.configure(bg=COLORS["bg"])
        self.build_controls()
        self.build_stats()
        self.build_chart()
        self.build_tables()
        root.protocol("WM_DELETE_WINDOW", self.close)
        self.refresh()
    def build_controls(self):
        bar = tk.Frame(self.root, bg=COLORS["bg"])
        bar.pack(fill="x", padx=10, pady=5)
        self.pause_btn = tk.Button(bar, text="Pause display", command=self.toggle)
        self.pause_btn.pack(side="left", padx=3)
        tk.Button(bar, text="Inject voltage spike", command=self.spike).pack(side="left", padx=3)
        self.connect_btn = tk.Button(bar, text="Connect to database", command=self.connect)
        self.connect_btn.pack(side="left", padx=3)
        tk.Label(bar, text="Device", bg=COLORS["bg"], fg=COLORS["text"]).pack(side="left", padx=3)
        self.device = ttk.Combobox(bar, values=backend.get_devices(), state="readonly", width=10)
        self.device.current(0)
        self.device.pack(side="left")
        tk.Label(bar, text="Window size", bg=COLORS["bg"], fg=COLORS["text"]).pack(side="left", padx=3)
        self.win = tk.Scale(bar, from_=5, to=30, orient="horizontal", length=120, command=self.apply, bg=COLORS["bg"])
        self.win.set(20)
        self.win.pack(side="left")
        tk.Label(bar, text="Alert if |z| above", bg=COLORS["bg"], fg=COLORS["text"]).pack(side="left", padx=3)
        self.thr = tk.Scale(bar, from_=2, to=6, resolution=0.5, orient="horizontal", length=120, command=self.apply, bg=COLORS["bg"])
        self.thr.set(3)
        self.thr.pack(side="left")
    def build_stats(self):
        box = tk.Frame(self.root, bg=COLORS["bg"])
        box.pack(fill="x", padx=10, pady=5)
        self.v_latest = tk.StringVar(value="-")
        self.v_std = tk.StringVar(value="-")
        self.v_rows = tk.StringVar(value="-")
        self.v_alerts = tk.StringVar(value="-")
        items = [("Latest voltage", self.v_latest), ("Rolling std dev", self.v_std),
                 ("Raw rows now", self.v_rows), ("Total alerts", self.v_alerts)]
        for i, (name, var) in enumerate(items):
            card = tk.Frame(box, bg=COLORS["card"], bd=1, relief="solid")
            card.grid(row=0, column=i, padx=5, sticky="ew")
            box.columnconfigure(i, weight=1)
            tk.Label(card, textvariable=var, font=("Arial", 16, "bold"), bg=COLORS["card"], fg=COLORS["text"]).pack()
            tk.Label(card, text=name, bg=COLORS["card"], fg=COLORS["text"]).pack()
    def build_chart(self):
        frame = tk.LabelFrame(self.root, text="Live voltage and rolling band", bg=COLORS["bg"], fg=COLORS["text"])
        frame.pack(fill="x", padx=10, pady=5)
        self.canvas = tk.Canvas(frame, width=W, height=H, bg=COLORS["card"], highlightthickness=0)
        self.canvas.pack()
    def make_table(self, parent, title, cols, col):
        frame = tk.LabelFrame(parent, text=title, bg=COLORS["bg"], fg=COLORS["text"])
        frame.grid(row=0, column=col, padx=5, sticky="nsew")
        parent.columnconfigure(col, weight=1)
        tree = ttk.Treeview(frame, columns=cols, show="headings", height=7)
        for c in cols:
            tree.heading(c, text=c)
            tree.column(c, width=80, anchor="center")
        tree.pack(fill="both", expand=True)
        tree.tag_configure("HIGH", foreground=COLORS["alert"])
        return tree
    def build_tables(self):
        box = tk.Frame(self.root, bg=COLORS["bg"])
        box.pack(fill="both", expand=True, padx=10, pady=5)
        self.t_alerts = self.make_table(box, "Alerts", ("Time", "Device", "Volts", "z", "Level"), 0)
        self.t_parts = self.make_table(box, "Partitions", ("Partition", "Rows"), 1)
        self.t_sum = self.make_table(box, "Summary", ("Bucket", "Avg V", "Min", "Max", "Readings"), 2)
        self.err = tk.StringVar()
        tk.Label(self.root, textvariable=self.err, bg=COLORS["bg"], fg=COLORS["alert"]).pack()
    def connect(self):
        password = simpledialog.askstring("MySQL", "Enter your MySQL password:", show="*", parent=self.root)
        if password is None:
            return
        backend.set_password(password)
        try:
            backend.start()
            self.connect_btn.config(text="Connected", state="disabled")
        except Exception as e:
            messagebox.showerror("Database error", str(e))
    def toggle(self):
        self.running = not self.running
        self.pause_btn.config(text="Pause display" if self.running else "Resume display")
    def spike(self):
        backend.inject_spike(self.device.get())
    def apply(self, _=None):
        backend.set_settings(self.win.get(), self.thr.get())
    def fill(self, tree, rows):
        tree.delete(*tree.get_children())
        for values, tag in rows:
            tree.insert("", "end", values=values, tags=(tag,))
    def draw_chart(self, rows):
        c = self.canvas
        c.delete("all")
        if len(rows) < 2:
            return
        z = backend.SETTINGS["threshold"]
        lows, highs = [], []
        for r in rows:
            vals = [r["voltage"]]
            if r["roll_avg"] is not None and r["roll_std"] is not None:
                vals += [r["roll_avg"] - z * r["roll_std"], r["roll_avg"] + z * r["roll_std"]]
            lows.append(min(vals))
            highs.append(max(vals))
        lo, hi = min(lows) - 2, max(highs) + 2
        n = len(rows) - 1
        X = lambda i: 40 + i * (W - 60) / n
        Y = lambda v: H - 20 - (v - lo) / (hi - lo) * (H - 40)
        for i in range(5):
            v = lo + (hi - lo) * i / 4
            c.create_line(40, Y(v), W, Y(v), fill=COLORS["grid"])
            c.create_text(20, Y(v), text=f"{v:.0f}", fill=COLORS["text"])
        upper, lower = [], []
        for i, r in enumerate(rows):
            if r["roll_avg"] is not None and r["roll_std"] is not None:
                upper.append((X(i), Y(r["roll_avg"] + z * r["roll_std"])))
                lower.append((X(i), Y(r["roll_avg"] - z * r["roll_std"])))
        if len(upper) >= 2:
            points = [p for xy in upper + lower[::-1] for p in xy]
            c.create_polygon(points, fill=COLORS["band"], outline="")
        line = [p for i, r in enumerate(rows) for p in (X(i), Y(r["voltage"]))]
        c.create_line(line, fill=COLORS["line"], width=2)
        for i, r in enumerate(rows):
            if r["roll_avg"] is not None and r["roll_std"]:
                if abs(r["voltage"] - r["roll_avg"]) / r["roll_std"] > z:
                    c.create_oval(X(i) - 5, Y(r["voltage"]) - 5, X(i) + 5, Y(r["voltage"]) + 5, fill=COLORS["alert"], outline="")
                    c.create_text(X(i) + 10, Y(r["voltage"]), text=f"{r['voltage']:.0f} V", fill=COLORS["alert"], anchor="w")
    def refresh(self):
        if self.running and backend.state["running"]:
            try:
                rows = backend.get_recent(self.device.get())
                self.draw_chart(rows)
                if rows:
                    last = rows[-1]
                    self.v_latest.set(f"{last['voltage']:.1f} V")
                    self.v_std.set(f"{last['roll_std']:.2f} V" if last["roll_std"] else "-")
                stats = backend.get_stats()
                self.v_rows.set(stats["rows_now"])
                self.v_alerts.set(stats["alerts_total"])
                self.err.set(stats["error"])
                self.fill(self.t_alerts, [((str(a["ts"]), a["device_id"], f"{a['voltage']:.1f}", f"{a['z_score']:.1f}", a["severity"]), a["severity"]) for a in backend.get_alerts()])
                self.fill(self.t_parts, [((p["name"], p["row_count"]), "") for p in backend.get_partitions()])
                self.fill(self.t_sum, [((s["bucket"], f"{s['avg_v']:.2f}", f"{s['min_v']:.1f}", f"{s['max_v']:.1f}", s["cnt"]), "") for s in backend.get_summary(self.device.get())])
            except Exception as e:
                self.err.set(str(e))
        self.root.after(REFRESH_MS, self.refresh)
    def close(self):
        backend.stop()
        self.root.destroy()
if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()