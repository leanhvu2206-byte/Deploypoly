from flask import Flask, render_template, request, redirect, url_for, session, send_file, flash
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import sqlite3, os, csv, io, json
from datetime import datetime, timedelta, timezone

# ---------------------- Config ----------------------
app = Flask(__name__)
app.config["SECRET_KEY"] = "change-this-secret-in-production"
DB_PATH = os.path.join(os.path.dirname(__file__), "app.db")

# ---------------------- DB Helpers ----------------------
def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def ensure_column(cur, table, column, type_sql):
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {type_sql}")

def init_db():
    con = get_db()
    cur = con.cursor()
    # Users
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    ''')
    # Measurements (bảng ban đầu)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            value REAL NOT NULL,
            created_at TEXT NOT NULL,
            created_by INTEGER,
            FOREIGN KEY(created_by) REFERENCES users(id)
        )
    ''')

    # Specs
    ensure_column(cur, "measurements", "item_code", "TEXT")
    ensure_column(cur, "measurements", "id_size", "REAL")
    ensure_column(cur, "measurements", "id_tol", "TEXT")
    ensure_column(cur, "measurements", "od1_size", "REAL")
    ensure_column(cur, "measurements", "od1_tol", "TEXT")
    ensure_column(cur, "measurements", "od2_size", "REAL")
    ensure_column(cur, "measurements", "od2_tol", "TEXT")
    ensure_column(cur, "measurements", "measured_by", "TEXT")
    ensure_column(cur, "measurements", "area", "TEXT")
    ensure_column(cur, "measurements", "note", "TEXT")

    # Extra checks + actuals + verdicts
    ensure_column(cur, "measurements", "extra_checks", "TEXT")
    ensure_column(cur, "measurements", "actual_id", "REAL")
    ensure_column(cur, "measurements", "actual_od1", "REAL")
    ensure_column(cur, "measurements", "actual_od2", "REAL")
    ensure_column(cur, "measurements", "verdict_id", "INTEGER")
    ensure_column(cur, "measurements", "verdict_od1", "INTEGER")
    ensure_column(cur, "measurements", "verdict_od2", "INTEGER")
    ensure_column(cur, "measurements", "verdict_overall", "INTEGER")

    # (Khuyến nghị) Index cho hiệu năng
    cur.execute("CREATE INDEX IF NOT EXISTS idx_measurements_item_code ON measurements(item_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_measurements_created_at_date ON measurements(substr(created_at,1,10))")

    con.commit()

    # Seed default user
    cur.execute("SELECT id FROM users WHERE username = ?", ("admin",))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO users (username, password_hash, created_at) VALUES (?, ?, ?)",
            ("admin", generate_password_hash("admin123"), datetime.now(timezone.utc).isoformat())
        )
        con.commit()
    con.close()

@app.template_filter("fmt_dt")
def fmt_dt(value):
    if not value:
        return ""
    try:
        v = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(v)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return value

@app.template_filter("extract_extra_summary")
def extract_extra_summary(extra_checks_str):
    """Hiển thị các hạng mục bổ sung, KHÔNG hiển thị PASS/FAIL riêng."""
    import json
    try:
        data = json.loads(extra_checks_str or "null")
    except Exception:
        return ""
    spec_list = []
    if isinstance(data, dict) and "spec" in data:
        spec_list = data["spec"]
    elif isinstance(data, list):
        spec_list = data
    html = '<div class="extras">'
    for sp in spec_list:
        name = sp.get("name", "")
        nom = sp.get("nominal", "")
        tp = sp.get("tol_plus", "")
        tm = sp.get("tol_minus", "")
        actual = None
        # Lấy actual nếu có
        if isinstance(data, dict) and "actuals" in data:
            for a in data["actuals"]:
                if a.get("name") == name:
                    actual = a.get("actual")
                    break
        html += f"""
        <span class="chip chip--none">
          <span class="chip__name">{name}</span>
          <span class="chip__dim">{nom}</span>
          <span class="chip__tol">(+{tp}/-{tm})</span>
          <span class="chip__actual">Actual={actual or '—'}</span>
        </span>"""
    html += "</div>"
    return html

@app.template_filter("extract_extra_results")
def extract_extra_results(extra_checks_str):
    """
    Trả về danh sách [{name, actual, pass}] cho từng hạng mục bổ sung.
    Ưu tiên dùng extra_checks.verdict_items (đã tính sẵn).
    Nếu không có, sẽ tự tính từ spec + actuals.
    """
    try:
        obj = json.loads(extra_checks_str or "null")
    except Exception:
        obj = None

    out = []

    if isinstance(obj, dict):
        # 1) đã có kết quả chấm sẵn
        verdict_items = obj.get("verdict_items")
        if isinstance(verdict_items, list):
            for it in verdict_items:
                out.append({
                    "name": (it.get("name") or "").strip(),
                    "actual": it.get("actual"),
                    "pass": it.get("pass"),
                })
            return out

        # 2) tự tính từ spec + actuals
        spec = obj.get("spec") or []
        actuals = obj.get("actuals") or []
        actual_map = {(a.get("name") or "").strip(): a.get("actual") for a in actuals}

        for sp in spec:
            name = (sp.get("name") or "").strip()
            nominal = to_float(sp.get("nominal"))
            tol_plus = to_float(sp.get("tol_plus"), 0.0)
            tol_minus = to_float(sp.get("tol_minus"), 0.0)
            actual = to_float(actual_map.get(name))
            v, _, _ = judge(actual, nominal, tol_plus, tol_minus)
            out.append({"name": name, "actual": actual, "pass": v})

    elif isinstance(obj, list):
        # fallback: chỉ có danh sách spec
        for sp in obj:
            out.append({
                "name": (sp.get("name") or "").strip(),
                "actual": None,
                "pass": None
            })

    return out

@app.template_filter("render_extras")
def render_extras(extra_checks_str: str) -> str:
    """
    Trả về HTML cho cột Extras:
    - Mỗi hạng mục là 1 'chip'
    - Hiển thị: Tên | nominal (+tol/-tol) | Actual | PASS/FAIL màu
    """
    import html

    try:
        obj = json.loads(extra_checks_str or "null")
    except Exception:
        obj = None

    items = []
    if isinstance(obj, dict):
        # Ưu tiên 'verdict_items' (đã có PASS/FAIL + actuals)
        vitems = obj.get("verdict_items")
        if isinstance(vitems, list) and vitems:
            items = vitems
        else:
            # Nếu là specs gốc hoặc bản KQ chưa kết án -> ghép spec + actuals
            spec = obj.get("spec") or []
            actuals = obj.get("actuals") or []
            amap = {(a.get("name") or "").strip(): a.get("actual") for a in actuals}
            for sp in spec:
                name = (sp.get("name") or "").strip()
                if not name:
                    continue
                items.append({
                    "name": name,
                    "nominal": sp.get("nominal"),
                    "tol_plus": sp.get("tol_plus"),
                    "tol_minus": sp.get("tol_minus"),
                    "actual": amap.get(name),
                    "pass": None
                })

    if not items:
        return '<span class="text-muted">—</span>'

    def fmt_esc(x):
        s = "—" if x is None else str(x)
        return html.escape(s)

    chips = []
    for it in items:
        name     = it.get("name") or ""
        nominal  = it.get("nominal")
        tp       = it.get("tol_plus")
        tm       = it.get("tol_minus")
        actual   = it.get("actual")
        p        = it.get("pass")  # True/False/None

        # Nhãn PASS/FAIL + màu
        verdict_cls = "chip--none"
        verdict_txt = ""
        if p is True:
            verdict_cls = "chip--pass"
            verdict_txt = "PASS"
        elif p is False:
            verdict_cls = "chip--fail"
            verdict_txt = "FAIL"

        chips.append(
            f'''
            <div class="chip {verdict_cls}">
              <span class="chip__name">{html.escape(str(name))}</span>
              <span class="chip__dim"> {fmt_esc(nominal)}
                <span class="chip__tol">(+{fmt_esc(tp)}/ -{fmt_esc(tm)})</span>
              </span>
              <span class="chip__actual">Actual: {fmt_esc(actual)}</span>
              {'<span class="chip__badge">'+verdict_txt+'</span>' if verdict_txt else ''}
            </div>
            '''.strip()
        )

    return '<div class="extras chips">'+ "".join(chips) + '</div>'

@app.template_filter("extract_extra_actuals")
def extract_extra_actuals(extra_checks_str):
    """
    Trả về list [{"name": "...", "actual": ...}, ...] từ cột extra_checks.
    Hỗ trợ cả 2 dạng:
    - {"spec": [...], "actuals": [...]}  (dạng mới khi lưu kết quả)
    - [ ... ]                            (dạng cũ, nếu có)
    """
    try:
        data = json.loads(extra_checks_str or "null")
    except Exception:
        return []

    # Xác định mảng actuals
    if isinstance(data, dict):
        actuals = data.get("actuals") or []
    elif isinstance(data, list):
        actuals = data
    else:
        actuals = []

    out = []
    for e in actuals:
        if not isinstance(e, dict):
            continue
        name = (e.get("name") or "").strip()
        actual = e.get("actual", None)
        if name and actual is not None:
            out.append({"name": name, "actual": actual})
    return out

# ---------------------- Utilities ----------------------
def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT * FROM users WHERE id = ?", (uid,))
    row = cur.fetchone()
    con.close()
    return row

def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user():
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapped

def to_float(val, default=None):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default

def parse_tol(tol_str):
    """±0.02 | +0.02/-0.01 | +0.02 | -0.02 | 0.02 -> (plus, minus)"""
    if not tol_str:
        return 0.0, 0.0
    s = tol_str.strip().replace("±", "+")
    plus = minus = None
    if "/" in s:
        p1, p2 = [p.strip() for p in s.split("/", 1)]
        if p1.startswith("+"): plus = to_float(p1[1:], 0.0)
        if p1.startswith("-"): minus = abs(to_float(p1[1:], 0.0))
        if p2.startswith("+"): plus = to_float(p2[1:], plus or 0.0)
        if p2.startswith("-"): minus = abs(to_float(p2[1:], minus or 0.0))
    else:
        if s.startswith("+"):
            plus = to_float(s[1:], 0.0); minus = plus
        elif s.startswith("-"):
            minus = abs(to_float(s[1:], 0.0)); plus = minus
        else:
            v = to_float(s, 0.0); plus = minus = v
    return plus or 0.0, minus or 0.0

def judge(actual, nominal, tol_plus, tol_minus):
    if nominal is None:
        return None, None, None
    lower = nominal - (tol_minus or 0.0)
    upper = nominal + (tol_plus or 0.0)
    if actual is None:
        return None, lower, upper
    return (lower <= actual <= upper), lower, upper

def judge_extra_checks(spec_list, actual_list):
    """spec_list: [{name, nominal, tol_plus, tol_minus}] ; actual_list: [{name, actual}]"""
    actual_map = {(a.get("name") or "").strip(): a.get("actual") for a in (actual_list or [])}
    results, judgements = [], []
    for sp in spec_list or []:
        name = (sp.get("name") or "").strip()
        nominal = to_float(sp.get("nominal"))
        tol_plus = to_float(sp.get("tol_plus"), 0.0)
        tol_minus = to_float(sp.get("tol_minus"), 0.0)
        actual = to_float(actual_map.get(name))
        v, low, up = judge(actual, nominal, tol_plus, tol_minus)
        results.append({
            "name": name, "nominal": nominal,
            "tol_plus": tol_plus, "tol_minus": tol_minus,
            "actual": actual, "pass": v
        })
        if v is not None:
            judgements.append(v)
    overall = (all(judgements) if judgements else None)
    return results, overall

# ---------------------- Routes ----------------------
@app.route("/")
def index():
    if current_user():
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        con = get_db()
        cur = con.cursor()
        cur.execute("SELECT * FROM users WHERE username = ?", (username,))
        user = cur.fetchone()
        con.close()
        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            return redirect(url_for("dashboard"))
        flash("Sai tài khoản hoặc mật khẩu.", "danger")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/dashboard")
@login_required
def dashboard():
    con = get_db()
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM measurements")
    total = cur.fetchone()["cnt"]
    cur.execute("SELECT AVG(value) AS avg_val FROM measurements")
    avg_val = cur.fetchone()["avg_val"]
    cur.execute("""
        SELECT id, item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
               actual_id, actual_od1, actual_od2, measured_by, area, note, verdict_overall, created_at
        FROM measurements
        ORDER BY created_at DESC LIMIT 5
    """)
    recent = cur.fetchall()

    # OK/NG
    cur.execute("""
        SELECT
          SUM(CASE WHEN verdict_overall = 1 THEN 1 ELSE 0 END) AS ok_cnt,
          SUM(CASE WHEN verdict_overall = 0 THEN 1 ELSE 0 END) AS ng_cnt
        FROM measurements
        WHERE verdict_overall IS NOT NULL
    """)
    row = cur.fetchone()
    ok_cnt = row["ok_cnt"] or 0
    ng_cnt = row["ng_cnt"] or 0

    # By hour today (dùng substr/replace do created_at là ISO có T và offset)
    by_hour = {f"{h:02d}": 0 for h in range(24)}
    cur.execute("""
        SELECT strftime('%H', replace(substr(created_at, 1, 19), 'T', ' ')) AS hh, COUNT(*) AS c
        FROM measurements
        WHERE substr(created_at, 1, 10) = date('now')
        GROUP BY hh ORDER BY hh
    """)
    for r in cur.fetchall():
        if r["hh"] is not None:
            by_hour[r["hh"]] = r["c"]
    hours_labels = list(by_hour.keys())
    hours_values = list(by_hour.values())

    # 14 days (UTC)
    cur.execute("""
        SELECT substr(created_at,1,10) AS d, COUNT(*) AS c
        FROM measurements
        WHERE substr(created_at,1,10) >= date('now','-13 days')
        GROUP BY d ORDER BY d
    """)
    got = {r["d"]: r["c"] for r in cur.fetchall()}
    today = datetime.utcnow().date()
    days_labels, days_values = [], []
    for i in range(13, -1, -1):
        d = today - timedelta(days=i)
        ds = d.isoformat()
        days_labels.append(ds)
        days_values.append(got.get(ds, 0))

    con.close()
    return render_template(
        "dashboard.html",
        total=total, avg_val=avg_val, recent=recent,
        ok_cnt=ok_cnt, ng_cnt=ng_cnt,
        hours_labels=hours_labels, hours_values=hours_values,
        days_labels=days_labels, days_values=days_values
    )

@app.route("/measurements")
@login_required
def list_measurements():
    q = request.args.get("q", "").strip()
    con = get_db()
    cur = con.cursor()
    if q:
        cur.execute("""
            SELECT * FROM measurements
            WHERE (title LIKE ? OR item_code LIKE ?)
            ORDER BY created_at DESC
        """, (f"%{q}%", f"%{q}%"))
    else:
        cur.execute("SELECT * FROM measurements ORDER BY created_at DESC")
    rows = cur.fetchall()
    con.close()
    return render_template("measurements.html", rows=rows, q=q)

@app.route("/measurements/history")
@login_required
def history():
    start = request.args.get("start")
    end   = request.args.get("end")
    con = get_db()
    cur = con.cursor()
    query = "SELECT * FROM measurements WHERE 1=1"
    params = []
    if start:
        query += " AND created_at >= ?"; params.append(start)
    if end:
        query += " AND created_at <= ?"; params.append(end)
    query += " ORDER BY created_at DESC"
    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    con.close()
    return render_template("history.html", rows=rows, start=start, end=end)

# ---------- ĐO HÀNG: thiết lập specs + phán định tự động ----------
@app.route("/measurements/inspect", methods=["GET", "POST"])
@login_required
def inspect_measure():
    item_code   = (request.values.get("item_code") or "").strip()
    action_init = request.form.get("init_specs") == "on"

    con = get_db()
    cur = con.cursor()
    rows, latest = [], None
    missing_fields = []

    # 1) Có mã hàng -> lấy lịch sử
    if item_code:
        cur.execute("SELECT * FROM measurements WHERE item_code = ? ORDER BY created_at DESC", (item_code,))
        rows = cur.fetchall()
        latest = rows[0] if rows else None

        # 1b) Thêm/cập nhật hạng mục bổ sung (cập nhật baseline)
        if request.method == "POST" and request.form.get("add_extra") == "on" and latest:
            try:
                ej = json.loads(latest["extra_checks"] or "null")
                cur_spec = (ej.get("spec") if isinstance(ej, dict) else ej) or []
            except Exception:
                cur_spec = []

            ex_name = (request.form.get("add_extra_name") or "").strip()
            ex_nom  = to_float(request.form.get("add_extra_nominal"))
            ex_tol  = (request.form.get("add_extra_tol") or "").strip()
            if ex_name:
                p, m = parse_tol(ex_tol)
                found = False
                for sp in cur_spec:
                    if (sp.get("name") or "").strip().lower() == ex_name.lower():
                        sp["nominal"]   = ex_nom
                        sp["tol_plus"]  = p
                        sp["tol_minus"] = m
                        found = True
                        break
                if not found:
                    cur_spec.append({"name": ex_name, "nominal": ex_nom, "tol_plus": p, "tol_minus": m})

                cur.execute("""
                    INSERT INTO measurements
                    (title, value, created_at, created_by,
                     item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
                     extra_checks, actual_id, actual_od1, actual_od2,
                     verdict_id, verdict_od1, verdict_od2, verdict_overall)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    "Cập nhật specs",
                    latest["id_size"] or 0.0,
                    datetime.now(timezone.utc).isoformat(),
                    session.get("user_id"),
                    item_code,
                    latest["id_size"], latest["id_tol"],
                    latest["od1_size"], latest["od1_tol"],
                    latest["od2_size"], latest["od2_tol"],
                    json.dumps({"spec": cur_spec, "history": []}, ensure_ascii=False),
                    None, None, None,
                    None, None, None, None
                ))
                con.commit()

                cur.execute("SELECT * FROM measurements WHERE item_code = ? ORDER BY created_at DESC", (item_code,))
                rows = cur.fetchall()
                latest = rows[0] if rows else None
                flash(f"Đã thêm/cập nhật hạng mục '{ex_name}' vào specs của {item_code}.", "success")

    # 2) POST tạo specs lần đầu
    if request.method == "POST" and action_init and item_code and not latest:
        id_size  = to_float(request.form.get("id_size"))
        id_tol   = (request.form.get("id_tol") or "").strip()
        od1_size = to_float(request.form.get("od1_size"))
        od1_tol  = (request.form.get("od1_tol") or "").strip()
        od2_size = to_float(request.form.get("od2_size"))
        od2_tol  = (request.form.get("od2_tol") or "").strip()
        spec_json_raw = request.form.get("extra_checks_spec", "[]")
        try:
            spec_spec = json.loads(spec_json_raw)
        except Exception:
            spec_spec = []

        cur.execute("""
            INSERT INTO measurements
            (title, value, created_at, created_by,
             item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
             extra_checks, actual_id, actual_od1, actual_od2,
             verdict_id, verdict_od1, verdict_od2, verdict_overall,
             measured_by, area, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "Thiết lập specs",
            id_size or 0.0,
            datetime.now(timezone.utc).isoformat(),
            session.get("user_id"),
            item_code,
            id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
            json.dumps({"spec": spec_spec, "history": []}, ensure_ascii=False),
            None, None, None,
            None, None, None, None,
            None, None, None  # measured_by, area, note
        ))
        con.commit()

        cur.execute("SELECT * FROM measurements WHERE item_code = ? ORDER BY created_at DESC", (item_code,))
        rows = cur.fetchall()
        latest = rows[0] if rows else None
        flash("Đã thiết lập specs ban đầu cho mã hàng.", "success")

    # 3) Phán định + tự lưu khi đủ số liệu
    verdict = {"id": None, "od1": None, "od2": None, "overall": None,
               "ranges": {"id": (None, None), "od1": (None, None), "od2": (None, None)}}

    actual_id  = to_float(request.values.get("actual_id"))
    actual_od1 = to_float(request.values.get("actual_od1"))
    actual_od2 = to_float(request.values.get("actual_od2"))
    measured_by  = (request.values.get("measured_by") or session.get("username") or "").strip()
    measure_area = (request.values.get("measure_area") or "").strip()
    note         = (request.values.get("note") or "").strip()

    extra_checks_actual_raw = request.values.get("extra_checks_actual", "[]")
    try:
        extra_checks_actual = json.loads(extra_checks_actual_raw)
    except Exception:
        extra_checks_actual = []

    if item_code and latest:
        id_plus,  id_minus  = parse_tol(latest["id_tol"])  if latest["id_tol"]  is not None else (0.0, 0.0)
        od1_plus, od1_minus = parse_tol(latest["od1_tol"]) if latest["od1_tol"] is not None else (0.0, 0.0)
        od2_plus, od2_minus = parse_tol(latest["od2_tol"]) if latest["od2_tol"] is not None else (0.0, 0.0)

        v_id,  id_low,  id_up  = judge(actual_id,  latest["id_size"],  id_plus,  id_minus)
        v_od1, od1_low, od1_up = judge(actual_od1, latest["od1_size"], od1_plus, od1_minus)
        v_od2, od2_low, od2_up = judge(actual_od2, latest["od2_size"], od2_plus, od2_minus)

        verdict["id"], verdict["od1"], verdict["od2"] = v_id, v_od1, v_od2
        verdict["ranges"]["id"], verdict["ranges"]["od1"], verdict["ranges"]["od2"] = (id_low, id_up), (od1_low, od1_up), (od2_low, od2_up)

        # Chuẩn hoá spec cho extra
        extra_spec = []
        try:
            ej = json.loads(latest["extra_checks"] or "null")
            if isinstance(ej, dict) and "spec" in ej: extra_spec = ej.get("spec") or []
            elif isinstance(ej, list):               extra_spec = ej
        except Exception:
            extra_spec = []

        norm_spec = []
        for sp in extra_spec:
            name = (sp.get("name") or "").strip()
            nominal = to_float(sp.get("nominal"))
            tp, tm = sp.get("tol_plus"), sp.get("tol_minus")
            if isinstance(tp, str) or isinstance(tm, str):
                if isinstance(tp, str) and tp and not tm:
                    p, m = parse_tol(tp); tp, tm = p, m
                elif isinstance(tm, str) and tm and not tp:
                    p, m = parse_tol(tm); tp, tm = p, m
                else:
                    tp, tm = to_float(tp, 0.0), to_float(tm, 0.0)
            norm_spec.append({"name": name, "nominal": nominal,
                              "tol_plus": to_float(tp, 0.0), "tol_minus": to_float(tm, 0.0)})

        extra_with_verdict, extra_overall = judge_extra_checks(norm_spec, extra_checks_actual)

        # ---- BẮT BUỘC NHẬP ĐỦ TẤT CẢ ACTUAL MỚI PHÁN ĐỊNH ----
        missing = []

        # 3 kích thước chính
        need_id  = latest["id_size"]  is not None
        need_od1 = latest["od1_size"] is not None
        need_od2 = latest["od2_size"] is not None

        if need_id  and actual_id  is None: missing.append("Actual ID")
        if need_od1 and actual_od1 is None: missing.append("Actual OD1")
        if need_od2 and actual_od2 is None: missing.append("Actual OD2")

        # Hạng mục bổ sung
        actual_map = { (a.get("name") or "").strip(): to_float(a.get("actual")) for a in (extra_checks_actual or []) }
        for sp in norm_spec:
            nm = (sp.get("name") or "").strip()
            nominal = to_float(sp.get("nominal"))
            if nominal is not None and to_float(actual_map.get(nm)) is None:
                missing.append(f"Actual {nm}")

        all_present = (len(missing) == 0)

        # ✅ Nếu đủ dữ liệu: phán định tổng thể
        if all_present:
            checks_main = [v for v in (v_id, v_od1, v_od2) if v is not None]
            for it in (extra_with_verdict or []):
                if it.get("pass") is not None:
                    checks_main.append(bool(it.get("pass")))
            verdict["overall"] = (all(checks_main) if checks_main else None)
        else:
            verdict["overall"] = None  # chưa đủ dữ liệu => chưa phán định

        missing_fields = missing

        # Tự lưu khi đủ dữ liệu
        if request.method in ("GET", "POST") and all_present:
            cur.execute("""
                INSERT INTO measurements
                (title, value, created_at, created_by,
                 item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
                 extra_checks, actual_id, actual_od1, actual_od2,
                 verdict_id, verdict_od1, verdict_od2, verdict_overall,
                 measured_by, area, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                f"KQ đo {item_code}",
                latest["id_size"] or 0.0,
                datetime.now(timezone.utc).isoformat(),
                session.get("user_id"),
                item_code,
                latest["id_size"], latest["id_tol"],
                latest["od1_size"], latest["od1_tol"],
                latest["od2_size"], latest["od2_tol"],
                json.dumps({
                    "spec": norm_spec,
                    "actuals": extra_checks_actual,
                    "verdict_items": extra_with_verdict
                }, ensure_ascii=False),
                actual_id, actual_od1, actual_od2,
                int(v_id)  if v_id  is not None else None,
                int(v_od1) if v_od1 is not None else None,
                int(v_od2) if v_od2 is not None else None,
                int(verdict["overall"]) if verdict["overall"] is not None else None,
                measured_by or None, measure_area or None, note or None
            ))
            con.commit()
            cur.execute("SELECT * FROM measurements WHERE item_code = ? ORDER BY created_at DESC", (item_code,))
            rows = cur.fetchall()
            latest = rows[0] if rows else None

    con.close()
    return render_template(
        "inspect.html",
        item_code=item_code, rows=rows, latest=latest,
        actual_id=actual_id, actual_od1=actual_od1, actual_od2=actual_od2,
        verdict=verdict,
        missing_fields=missing_fields
    )

@app.route("/measurements/delete_extra/<name>", methods=["POST"])
@login_required
def delete_extra_check(name):
    item_code = request.args.get("item_code")
    if not item_code or not name:
        return "Missing item_code or name", 400
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT * FROM measurements WHERE item_code = ? ORDER BY created_at DESC", (item_code,))
    latest = cur.fetchone()
    if not latest:
        con.close()
        return "Not found", 404

    try:
        ej = json.loads(latest["extra_checks"] or "null")
        cur_spec = (ej.get("spec") if isinstance(ej, dict) else ej) or []
    except Exception:
        cur_spec = []

    new_spec = [sp for sp in cur_spec if (sp.get("name") or "").strip().lower() != name.lower()]

    cur.execute("""
        INSERT INTO measurements
        (title, value, created_at, created_by,
         item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
         extra_checks, actual_id, actual_od1, actual_od2,
         verdict_id, verdict_od1, verdict_od2, verdict_overall)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        f"Xóa hạng mục {name}",
        latest["id_size"] or 0.0,
        datetime.now(timezone.utc).isoformat(),
        session.get("user_id"),
        item_code,
        latest["id_size"], latest["id_tol"],
        latest["od1_size"], latest["od1_tol"],
        latest["od2_size"], latest["od2_tol"],
        json.dumps({"spec": new_spec, "history": []}, ensure_ascii=False),
        None, None, None,
        None, None, None, None
    ))
    con.commit()
    con.close()
    flash(f"Đã xóa hạng mục '{name}' khỏi {item_code}.", "success")
    return ("", 204)

@app.route("/export")
@login_required
def export_csv():
    con = get_db()
    cur = con.cursor()
    cur.execute("""
        SELECT id, item_code,
               id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
               actual_id, actual_od1, actual_od2,
               verdict_id, verdict_od1, verdict_od2, verdict_overall,
               created_at, extra_checks
        FROM measurements
        ORDER BY created_at DESC
    """)
    rows = cur.fetchall()
    con.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id", "item_code",
        "id_size", "id_tol",
        "od1_size", "od1_tol",
        "od2_size", "od2_tol",
        "actual_id", "actual_od1", "actual_od2",
        "verdict_id", "verdict_od1", "verdict_od2", "verdict_overall",
        "created_at", "extra_checks(JSON)"
    ])
    for r in rows:
        writer.writerow([
            r["id"], r["item_code"],
            r["id_size"], r["id_tol"],
            r["od1_size"], r["od1_tol"],
            r["od2_size"], r["od2_tol"],
            r["actual_id"], r["actual_od1"], r["actual_od2"],
            r["verdict_id"], r["verdict_od1"], r["verdict_od2"], r["verdict_overall"],
            r["created_at"], r["extra_checks"]
        ])
    mem = io.BytesIO(output.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, as_attachment=True, download_name="measurements.csv", mimetype="text/csv")

# --------- TẠO MỚI BÀI ĐO (form riêng cho specs) ---------
@app.route("/measurements/new", methods=["GET", "POST"])
@login_required
def new_measurement():
    if request.method == "POST":
        item_code = (request.form.get("item_code") or "").strip()
        id_size   = to_float(request.form.get("id_size"))
        id_tol    = (request.form.get("id_tol") or "").strip()
        od1_size  = to_float(request.form.get("od1_size"))
        od1_tol   = (request.form.get("od1_tol") or "").strip()
        od2_size  = to_float(request.form.get("od2_size"))
        od2_tol   = (request.form.get("od2_tol") or "").strip()

        spec_raw = request.form.get("extra_checks_spec", "[]")
        try:
            spec_spec = json.loads(spec_raw)
        except Exception:
            spec_spec = []

        con = get_db()
        cur = con.cursor()
        cur.execute("""
            INSERT INTO measurements
            (title, value, created_at, created_by,
             item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
             extra_checks, actual_id, actual_od1, actual_od2,
             verdict_id, verdict_od1, verdict_od2, verdict_overall)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "Thiết lập specs",
            id_size or 0.0,
            datetime.now(timezone.utc).isoformat(),
            session.get("user_id"),
            item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
            json.dumps({"spec": spec_spec, "history": []}, ensure_ascii=False),
            None, None, None,
            None, None, None, None
        ))
        con.commit()
        con.close()
        flash("Đã tạo bài đo (specs) cho mã hàng.", "success")
        return redirect(url_for("inspect_measure", item_code=item_code))

    return render_template("create_measurement.html")

@app.route("/measurements/<int:mid>/delete", methods=["POST"])
@login_required
def delete_measurement(mid):
    password = request.form.get("password", "")
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT password_hash FROM users WHERE id = ?", (session.get("user_id"),))
    u = cur.fetchone()
    if not u or not check_password_hash(u["password_hash"], password):
        con.close()
        flash("Mật khẩu không đúng. Không xoá.", "danger")
        return redirect(url_for("list_measurements"))

    cur.execute("DELETE FROM measurements WHERE id = ?", (mid,))
    con.commit()
    con.close()
    flash(f"Đã xoá bài đo #{mid}.", "success")
    return redirect(url_for("list_measurements"))

# --------------- Run ---------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
