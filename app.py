from flask import Flask, render_template, request, redirect, url_for, session, send_file, flash
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import os, csv, io, json
from datetime import datetime, timedelta, timezone
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json  # <== thêm dòng này
from psycopg_pool import ConnectionPool 

# ===================== Config =====================
app = Flask(__name__)
app.config["SECRET_KEY"] = "change-this-secret-in-production"

# Lấy URL Postgres từ biến môi trường
DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql://... ?sslmode=require
if not DATABASE_URL:
    raise RuntimeError("Missing env DATABASE_URL")

# Timezone VN
TZ_VN = timezone(timedelta(hours=7))

# Kết nối Pool
_pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,          # giữ sẵn 1 kết nối ấm
    max_size=5,          # đủ cho tải nhẹ-trung bình
    max_idle=30,         # đóng nếu idle > 30s
    kwargs={"row_factory": dict_row, "connect_timeout": 5},
)

# Tuỳ chọn: mở pool ngay (giúp “làm ấm” container)
_pool.open()

# ===================== DB Helpers =====================
def get_db():
    """Lấy connection từ pool (dùng context manager)."""
    return _pool.connection()



def init_db():
    """Tạo schema nếu chưa có + seed user admin."""
    ddl = """
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS measurements (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by INTEGER REFERENCES users(id),
  item_code TEXT,
  id_size DOUBLE PRECISION, id_tol TEXT,
  od1_size DOUBLE PRECISION, od1_tol TEXT,
  od2_size DOUBLE PRECISION, od2_tol TEXT,
  measured_by TEXT, area TEXT, note TEXT,
  extra_checks JSONB,
  actual_id DOUBLE PRECISION, actual_od1 DOUBLE PRECISION, actual_od2 DOUBLE PRECISION,
  verdict_id BOOLEAN, verdict_od1 BOOLEAN, verdict_od2 BOOLEAN, verdict_overall BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_measurements_item_code ON measurements(item_code);
CREATE INDEX IF NOT EXISTS idx_measurements_created_at_vn_date
  ON measurements ( ((created_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date) );

-- NEW: bảng hành động khắc phục
CREATE TABLE IF NOT EXISTS corrective_actions (
  id SERIAL PRIMARY KEY,
  measurement_id INTEGER NOT NULL REFERENCES measurements(id) ON DELETE CASCADE,
  seq_no INTEGER NOT NULL,
  action TEXT NOT NULL,
  owner TEXT,
  due_date DATE,
  status TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ca_measurement ON corrective_actions(measurement_id);
"""
    with get_db() as con, con.cursor() as cur:
        cur.execute(ddl)
        cur.execute("SELECT id FROM users WHERE username = %s", ("admin",))
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                ("admin", generate_password_hash("admin123")),
            )


init_db()


# ===================== Template Filters =====================
@app.template_filter("fmt_dt")
def fmt_dt(value):
    """Hiển thị datetime theo Asia/Ho_Chi_Minh."""
    if not value:
        return ""
    try:
        if isinstance(value, str):
            # hỗ trợ cả chuỗi ISO (fallback)
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value.astimezone(TZ_VN).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


# ---- Các filter xử lý JSON extra_checks: chấp nhận dict hoặc str ----
def _ec_to_obj(extra_checks):
    if extra_checks is None:
        return None
    if isinstance(extra_checks, (dict, list)):
        return extra_checks
    try:
        return json.loads(extra_checks)
    except Exception:
        return None


@app.template_filter("extract_extra_summary")
def extract_extra_summary(extra_checks_str):
    """Hiển thị các hạng mục bổ sung, KHÔNG hiển thị PASS/FAIL riêng."""
    data = _ec_to_obj(extra_checks_str)
    if data is None:
        return ""
    spec_list = []
    if isinstance(data, dict) and "spec" in data:
        spec_list = data.get("spec") or []
    elif isinstance(data, list):
        spec_list = data

    actual_map = {}
    if isinstance(data, dict) and "actuals" in data:
        actual_map = { (a.get("name") or "").strip(): a.get("actual") for a in (data.get("actuals") or []) }

    html = '<div class="extras">'
    for sp in spec_list:
        name = (sp.get("name") or "").strip()
        nom = sp.get("nominal", "")
        tp = sp.get("tol_plus", "")
        tm = sp.get("tol_minus", "")
        actual = actual_map.get(name)
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
    data = _ec_to_obj(extra_checks_str)

    def to_float(v, d=None):
        try:
            return float(v)
        except Exception:
            return d

    def judge(actual, nominal, tol_plus, tol_minus):
        if nominal is None:
            return None
        low = nominal - (tol_minus or 0.0)
        up  = nominal + (tol_plus  or 0.0)
        if actual is None:
            return None
        return (low <= actual <= up)

    out = []
    if isinstance(data, dict):
        vitems = data.get("verdict_items")
        if isinstance(vitems, list):
            for it in vitems:
                out.append({
                    "name": (it.get("name") or "").strip(),
                    "actual": it.get("actual"),
                    "pass": it.get("pass"),
                })
            return out

        spec = data.get("spec") or []
        actuals = data.get("actuals") or []
        amap = {(a.get("name") or "").strip(): a.get("actual") for a in actuals}
        for sp in spec:
            name = (sp.get("name") or "").strip()
            nominal = to_float(sp.get("nominal"))
            tp = to_float(sp.get("tol_plus"), 0.0)
            tm = to_float(sp.get("tol_minus"), 0.0)
            actual = to_float(amap.get(name))
            v = judge(actual, nominal, tp, tm)
            out.append({"name": name, "actual": actual, "pass": v})
        return out

    if isinstance(data, list):
        for sp in data:
            out.append({"name": (sp.get("name") or "").strip(), "actual": None, "pass": None})
    return out


@app.template_filter("render_extras")
def render_extras(extra_checks_str: str) -> str:
    """Trả về HTML cho cột Extras: mỗi hạng mục là chip + PASS/FAIL."""
    import html as _html

    obj = _ec_to_obj(extra_checks_str)
    items = []
    if isinstance(obj, dict):
        vitems = obj.get("verdict_items")
        if isinstance(vitems, list) and vitems:
            items = vitems
        else:
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
        return _html.escape(s)

    chips = []
    for it in items:
        name     = it.get("name") or ""
        nominal  = it.get("nominal")
        tp       = it.get("tol_plus")
        tm       = it.get("tol_minus")
        actual   = it.get("actual")
        p        = it.get("pass")  # True/False/None

        verdict_cls = "chip--none"
        verdict_txt = ""
        if p is True:
            verdict_cls = "chip--pass"; verdict_txt = "PASS"
        elif p is False:
            verdict_cls = "chip--fail"; verdict_txt = "FAIL"

        chips.append(
            f'''
            <div class="chip {verdict_cls}">
              <span class="chip__name">{_html.escape(str(name))}</span>
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
    """Trả về list [{"name": "...", "actual": ...}] từ cột extra_checks (dict/str)."""
    data = _ec_to_obj(extra_checks_str)
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


# ===================== Utilities (business) =====================
def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    with get_db() as con, con.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE id = %s", (uid,))
        return cur.fetchone()


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


def _parse_dt_local(s):
    """Parse 'YYYY-MM-DDTHH:MM' từ input datetime-local (không timezone) -> Asia/Ho_Chi_Minh."""
    if not s:
        return None
    try:
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=TZ_VN)
        return d
    except Exception:
        return None


# ===================== Routes =====================
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
        with get_db() as con, con.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE username = %s", (username,))
            user = cur.fetchone()
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
    with get_db() as con, con.cursor() as cur:
        # ====== Thống kê cơ bản ======
        cur.execute("SELECT COUNT(*) AS cnt FROM measurements")
        total = cur.fetchone()["cnt"]

        # (giữ lại nếu còn dùng chỗ khác)
        cur.execute("SELECT AVG(value) AS avg_val FROM measurements")
        avg_val = cur.fetchone()["avg_val"]

        # 5 bản ghi gần nhất
        cur.execute("""
            SELECT id, item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
                   actual_id, actual_od1, actual_od2, measured_by, area, note,
                   verdict_overall, created_at
            FROM measurements
            ORDER BY created_at DESC
            LIMIT 5
        """)
        recent = cur.fetchall()

        # OK / NG tổng
        cur.execute("""
            SELECT
              COALESCE(SUM(CASE WHEN verdict_overall = TRUE  THEN 1 ELSE 0 END),0) AS ok_cnt,
              COALESCE(SUM(CASE WHEN verdict_overall = FALSE THEN 1 ELSE 0 END),0) AS ng_cnt
            FROM measurements
            WHERE verdict_overall IS NOT NULL
        """)
        row = cur.fetchone()
        ok_cnt = row["ok_cnt"] or 0
        ng_cnt = row["ng_cnt"] or 0

        # Theo giờ (hôm nay - TZ VN)
        by_hour = {f"{h:02d}": 0 for h in range(24)}
        cur.execute("""
            SELECT TO_CHAR(created_at AT TIME ZONE 'Asia/Ho_Chi_Minh', 'HH24') AS hh, COUNT(*) AS c
            FROM measurements
            WHERE (created_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date = CURRENT_DATE
            GROUP BY hh ORDER BY hh
        """)
        for r in cur.fetchall():
            if r["hh"] is not None:
                by_hour[r["hh"]] = r["c"]
        hours_labels = list(by_hour.keys())
        hours_values = list(by_hour.values())

        # 14 ngày gần nhất (TZ VN)
        cur.execute("""
            SELECT (created_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS d, COUNT(*) AS c
            FROM measurements
            WHERE (created_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date >= CURRENT_DATE - INTERVAL '13 days'
            GROUP BY d ORDER BY d
        """)
        got = {r["d"].isoformat(): r["c"] for r in cur.fetchall()}
        today = datetime.now(TZ_VN).date()
        days_labels, days_values = [], []
        for i in range(13, -1, -1):
            d = today - timedelta(days=i)
            ds = d.isoformat()
            days_labels.append(ds)
            days_values.append(got.get(ds, 0))

        # ====== Khu vực lỗi cao nhất (rate %) ======
        cur.execute("""
            WITH base AS (
              SELECT area,
                     SUM(CASE WHEN verdict_overall = FALSE THEN 1 ELSE 0 END) AS ng,
                     SUM(CASE WHEN verdict_overall IS NOT NULL THEN 1 ELSE 0 END) AS total
              FROM measurements
              GROUP BY area
            )
            SELECT area,
                   ng,
                   total,
                   CASE WHEN total > 0 THEN 100.0 * ng / total ELSE 0 END AS rate
            FROM base
            WHERE area IS NOT NULL AND total >= 3
            ORDER BY rate DESC, total DESC
            LIMIT 1
        """)
        r = cur.fetchone()
        worst_area_name = (r and r["area"]) or None
        worst_area_rate = (r and r["rate"]) or 0.0

        # ====== Kích thước lỗi cao nhất (ID/OD1/OD2) theo rate % ======
        cur.execute("""
            SELECT
              SUM(CASE WHEN verdict_id  = FALSE THEN 1 ELSE 0 END) AS fail_id,
              SUM(CASE WHEN verdict_id  IS NOT NULL THEN 1 ELSE 0 END) AS tot_id,
              SUM(CASE WHEN verdict_od1 = FALSE THEN 1 ELSE 0 END) AS fail_od1,
              SUM(CASE WHEN verdict_od1 IS NOT NULL THEN 1 ELSE 0 END) AS tot_od1,
              SUM(CASE WHEN verdict_od2 = FALSE THEN 1 ELSE 0 END) AS fail_od2,
              SUM(CASE WHEN verdict_od2 IS NOT NULL THEN 1 ELSE 0 END) AS tot_od2
            FROM measurements
        """)
        r = cur.fetchone()
        dims = []
        for key, label in (("id", "ID"), ("od1", "OD1"), ("od2", "OD2")):
            fail = r[f"fail_{key}"] or 0
            tot  = r[f"tot_{key}"] or 0
            rate = (100.0 * fail / tot) if tot else 0.0
            dims.append((label, rate))
        worst_dim_name, worst_dim_rate = max(dims, key=lambda x: x[1]) if dims else (None, 0.0)

        # ====== Top 5 lỗi phổ biến (gộp ID/OD1/OD2 + Extras) ======
        # 1) lỗi ở 3 kích thước chính
        cur.execute("""
            SELECT 'ID'  AS name, COUNT(*) AS c FROM measurements WHERE verdict_id  = FALSE
            UNION ALL
            SELECT 'OD1' AS name, COUNT(*)       FROM measurements WHERE verdict_od1 = FALSE
            UNION ALL
            SELECT 'OD2' AS name, COUNT(*)       FROM measurements WHERE verdict_od2 = FALSE
        """)
        counts = {}
        for r in cur.fetchall():
            counts[r["name"]] = counts.get(r["name"], 0) + (r["c"] or 0)

        # 2) lỗi ở hạng mục bổ sung (extra_checks.verdict_items[].pass = false)
        cur.execute("""
            SELECT TRIM(BOTH FROM x->>'name') AS name, COUNT(*) AS c
            FROM measurements m
            JOIN LATERAL jsonb_array_elements(m.extra_checks->'verdict_items') x ON TRUE
            WHERE (x->>'pass')::boolean = FALSE
            GROUP BY 1
            ORDER BY c DESC
            LIMIT 10
        """)
        for r in cur.fetchall():
            nm = r["name"] or ""
            if not nm:
                continue
            counts[nm] = counts.get(nm, 0) + (r["c"] or 0)

        # Lấy top 5
        top_errors = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:5]
        top_err_labels = [k for k, _ in top_errors]
        top_err_counts = [v for _, v in top_errors]

        # ====== Top 5 mã hàng lỗi cao (tỷ lệ %) ======
        cur.execute("""
            WITH b AS (
              SELECT item_code,
                     SUM(CASE WHEN verdict_overall = FALSE THEN 1 ELSE 0 END) AS ng,
                     SUM(CASE WHEN verdict_overall IS NOT NULL THEN 1 ELSE 0 END) AS total
              FROM measurements
              GROUP BY item_code
            )
            SELECT item_code,
                   CASE WHEN total > 0 THEN 100.0 * ng / total ELSE 0 END AS rate
            FROM b
            WHERE item_code IS NOT NULL AND total >= 3
            ORDER BY rate DESC, total DESC
            LIMIT 5
        """)
        rows = cur.fetchall()
        top_sku_labels = [r["item_code"] for r in rows]
        top_sku_rates  = [r["rate"] for r in rows]

        # ====== Top 5 khu vực lỗi cao (tỷ lệ %) ======
        cur.execute("""
            WITH b AS (
              SELECT area,
                     SUM(CASE WHEN verdict_overall = FALSE THEN 1 ELSE 0 END) AS ng,
                     SUM(CASE WHEN verdict_overall IS NOT NULL THEN 1 ELSE 0 END) AS total
              FROM measurements
              GROUP BY area
            )
            SELECT area,
                   CASE WHEN total > 0 THEN 100.0 * ng / total ELSE 0 END AS rate
            FROM b
            WHERE area IS NOT NULL AND total >= 3
            ORDER BY rate DESC, total DESC
            LIMIT 5
        """)
        rows = cur.fetchall()
        top_area_labels = [r["area"] for r in rows]
        top_area_rates  = [r["rate"] for r in rows]

    return render_template(
        "dashboard.html",
        # cũ
        total=total, avg_val=avg_val, recent=recent,
        ok_cnt=ok_cnt, ng_cnt=ng_cnt,
        hours_labels=hours_labels, hours_values=hours_values,
        days_labels=days_labels, days_values=days_values,
        # mới - thẻ thống kê
        worst_area_name=worst_area_name, worst_area_rate=worst_area_rate,
        worst_dim_name=worst_dim_name,   worst_dim_rate=worst_dim_rate,
        # mới - 3 biểu đồ top
        top_err_labels=top_err_labels, top_err_counts=top_err_counts,
        top_sku_labels=top_sku_labels, top_sku_rates=top_sku_rates,
        top_area_labels=top_area_labels, top_area_rates=top_area_rates,
    )



@app.route("/measurements")
@login_required
def list_measurements():
    q = request.args.get("q", "").strip()
    with get_db() as con, con.cursor() as cur:
        if q:
            cur.execute("""
                SELECT * FROM measurements
                WHERE (title ILIKE %s OR item_code ILIKE %s)
                ORDER BY created_at DESC
            """, (f"%{q}%", f"%{q}%"))
        else:
            cur.execute("SELECT * FROM measurements ORDER BY created_at DESC")
        rows = cur.fetchall()

        # --- Kéo kèm các hành động cho các measurement đang hiển thị ---
        actions_by_mid = {}
        if rows:
            ids = [r["id"] for r in rows]
            cur.execute("""
                SELECT id, measurement_id, seq_no, action, owner, due_date, status, created_at
                FROM corrective_actions
                WHERE measurement_id = ANY(%s)
                ORDER BY measurement_id, seq_no, id
            """, (ids,))
            for a in cur.fetchall():
                actions_by_mid.setdefault(a["measurement_id"], []).append(a)

        # ✅ Bước 1: gắn hành động mới nhất (nếu có) vào từng row
        for r in rows:
            acts = actions_by_mid.get(r["id"]) or []
            r["action"] = acts[-1] if acts else None

    return render_template("measurements.html", rows=rows, q=q, actions_by_mid=actions_by_mid)



@app.route("/measurements/history")
@login_required
def history():
    start = request.args.get("start")
    end   = request.args.get("end")
    start_dt = _parse_dt_local(start)
    end_dt   = _parse_dt_local(end)

    query = "SELECT * FROM measurements WHERE 1=1"
    params = []
    if start_dt:
        query += " AND created_at >= %s"; params.append(start_dt)
    if end_dt:
        query += " AND created_at <= %s"; params.append(end_dt)
    query += " ORDER BY created_at DESC"

    with get_db() as con, con.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
    return render_template("history.html", rows=rows, start=start, end=end)


# ---------- ĐO HÀNG: thiết lập specs + phán định tự động ----------
@app.route("/measurements/inspect", methods=["GET", "POST"])
@login_required
def inspect_measure():
    item_code   = (request.values.get("item_code") or "").strip()
    action_init = request.form.get("init_specs") == "on"

    rows, latest = [], None
    missing_fields = []

    with get_db() as con, con.cursor() as cur:
        # 1) Có mã hàng -> lấy lịch sử
        if item_code:
            cur.execute("SELECT * FROM measurements WHERE item_code = %s ORDER BY created_at DESC", (item_code,))
            rows = cur.fetchall()
            latest = rows[0] if rows else None

            # 1b) Thêm/cập nhật hạng mục bổ sung (cập nhật baseline)
            if request.method == "POST" and request.form.get("add_extra") == "on" and latest:
                ej = _ec_to_obj(latest["extra_checks"])
                cur_spec = (ej.get("spec") if isinstance(ej, dict) else ej) or []

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
                        VALUES (%s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s)
                    """, (
                        "Cập nhật specs",
                        latest["id_size"] or 0.0,
                        datetime.now(TZ_VN),
                        session.get("user_id"),
                        item_code,
                        latest["id_size"], latest["id_tol"],
                        latest["od1_size"], latest["od1_tol"],
                        latest["od2_size"], latest["od2_tol"],
                        Json({"spec": cur_spec, "history": []}),
                        None, None, None,
                        None, None, None, None
                    ))
                    cur.execute("SELECT * FROM measurements WHERE item_code = %s ORDER BY created_at DESC", (item_code,))
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

            with get_db() as con, con.cursor() as cur:
                cur.execute("""
                    INSERT INTO measurements
                    (title, value, created_at, created_by,
                     item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
                     extra_checks, actual_id, actual_od1, actual_od2,
                     verdict_id, verdict_od1, verdict_od2, verdict_overall,
                     measured_by, area, note)
                    VALUES (%s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s)
                """, (
                    "Thiết lập specs",
                    id_size or 0.0,
                    datetime.now(TZ_VN),
                    session.get("user_id"),
                    item_code,
                    id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
                    Json({"spec": spec_spec, "history": []}),
                    None, None, None,
                    None, None, None, None,
                    None, None, None  # measured_by, area, note
                ))
            with get_db() as con, con.cursor() as cur:
                cur.execute("SELECT * FROM measurements WHERE item_code = %s ORDER BY created_at DESC", (item_code,))
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
            ej = _ec_to_obj(latest["extra_checks"])
            if isinstance(ej, dict) and "spec" in ej:
                extra_spec = ej.get("spec") or []
            elif isinstance(ej, list):
                extra_spec = ej

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
            need_id  = latest["id_size"]  is not None
            need_od1 = latest["od1_size"] is not None
            need_od2 = latest["od2_size"] is not None

            if need_id  and actual_id  is None: missing.append("Actual ID")
            if need_od1 and actual_od1 is None: missing.append("Actual OD1")
            if need_od2 and actual_od2 is None: missing.append("Actual OD2")

            actual_map = { (a.get("name") or "").strip(): to_float(a.get("actual")) for a in (extra_checks_actual or []) }
            for sp in norm_spec:
                nm = (sp.get("name") or "").strip()
                nominal = to_float(sp.get("nominal"))
                if nominal is not None and to_float(actual_map.get(nm)) is None:
                    missing.append(f"Actual {nm}")

            all_present = (len(missing) == 0)

            # ✅ Phán định tổng thể khi đủ dữ liệu
            if all_present:
                checks_main = [v for v in (v_id, v_od1, v_od2) if v is not None]
                for it in (extra_with_verdict or []):
                    if it.get("pass") is not None:
                        checks_main.append(bool(it.get("pass")))
                verdict["overall"] = (all(checks_main) if checks_main else None)
            else:
                verdict["overall"] = None

            missing_fields = missing

            # Tự lưu khi đủ dữ liệu
            if request.method in ("GET", "POST") and all_present:
                verdict_id_val  = (bool(v_id)  if v_id  is not None else None)
                verdict_od1_val = (bool(v_od1) if v_od1 is not None else None)
                verdict_od2_val = (bool(v_od2) if v_od2 is not None else None)
                overall_val     = (bool(verdict["overall"]) if verdict["overall"] is not None else None)

                cur.execute("""
                    INSERT INTO measurements
                    (title, value, created_at, created_by,
                     item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
                     extra_checks, actual_id, actual_od1, actual_od2,
                     verdict_id, verdict_od1, verdict_od2, verdict_overall,
                     measured_by, area, note)
                    VALUES (%s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s)
                """, (
                    f"KQ đo {item_code}",
                    latest["id_size"] or 0.0,
                    datetime.now(TZ_VN),
                    session.get("user_id"),
                    item_code,
                    latest["id_size"], latest["id_tol"],
                    latest["od1_size"], latest["od1_tol"],
                    latest["od2_size"], latest["od2_tol"],
                   Json({
    "spec": norm_spec,
    "actuals": extra_checks_actual,
    "verdict_items": extra_with_verdict
}),
actual_id, actual_od1, actual_od2,
                    verdict_id_val, verdict_od1_val, verdict_od2_val, overall_val,
                    measured_by or None, measure_area or None, note or None
                ))
                cur.execute("SELECT * FROM measurements WHERE item_code = %s ORDER BY created_at DESC", (item_code,))
                rows = cur.fetchall()
                latest = rows[0] if rows else None

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

    with get_db() as con, con.cursor() as cur:
        cur.execute("SELECT * FROM measurements WHERE item_code = %s ORDER BY created_at DESC", (item_code,))
        latest = cur.fetchone()
        if not latest:
            return "Not found", 404

        ej = _ec_to_obj(latest["extra_checks"])
        cur_spec = (ej.get("spec") if isinstance(ej, dict) else ej) or []
        new_spec = [sp for sp in cur_spec if (sp.get("name") or "").strip().lower() != name.lower()]

        cur.execute("""
            INSERT INTO measurements
            (title, value, created_at, created_by,
             item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
             extra_checks, actual_id, actual_od1, actual_od2,
             verdict_id, verdict_od1, verdict_od2, verdict_overall)
            VALUES (%s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s)
        """, (
            f"Xóa hạng mục {name}",
            latest["id_size"] or 0.0,
            datetime.now(TZ_VN),
            session.get("user_id"),
            item_code,
            latest["id_size"], latest["id_tol"],
            latest["od1_size"], latest["od1_tol"],
            latest["od2_size"], latest["od2_tol"],
            Json({"spec": new_spec, "history": []}),
            None, None, None,
            None, None, None, None
        ))
    flash(f"Đã xóa hạng mục '{name}' khỏi {item_code}.", "success")
    return ("", 204)


@app.route("/export")
@login_required
def export_csv():
    with get_db() as con, con.cursor() as cur:
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
            r["created_at"],
            json.dumps(r["extra_checks"], ensure_ascii=False) if r["extra_checks"] is not None else None
        ])
    mem = io.BytesIO(output.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, as_attachment=True, download_name="measurements.csv", mimetype="text/csv")


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

        with get_db() as con, con.cursor() as cur:
            cur.execute("""
                INSERT INTO measurements
                (title, value, created_at, created_by,
                 item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
                 extra_checks, actual_id, actual_od1, actual_od2,
                 verdict_id, verdict_od1, verdict_od2, verdict_overall)
                VALUES (%s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s)
            """, (
                "Thiết lập specs",
                id_size or 0.0,
                datetime.now(TZ_VN),
                session.get("user_id"),
                item_code, id_size, id_tol, od1_size, od1_tol, od2_size, od2_tol,
                Json({"spec": spec_spec, "history": []}),
                None, None, None,
                None, None, None, None
            ))
        flash("Đã tạo bài đo (specs) cho mã hàng.", "success")
        return redirect(url_for("inspect_measure", item_code=item_code))

    return render_template("create_measurement.html")


@app.route("/measurements/<int:mid>/delete", methods=["POST"])
@login_required
def delete_measurement(mid):
    password = request.form.get("password", "")
    with get_db() as con, con.cursor() as cur:
        cur.execute("SELECT password_hash FROM users WHERE id = %s", (session.get("user_id"),))
        u = cur.fetchone()
        if not u or not check_password_hash(u["password_hash"], password):
            flash("Mật khẩu không đúng. Không xoá.", "danger")
            return redirect(url_for("list_measurements"))

        cur.execute("DELETE FROM measurements WHERE id = %s", (mid,))
    flash(f"Đã xoá bài đo #{mid}.", "success")
    return redirect(url_for("list_measurements"))


# ---------- CẬP NHẬT HÀNH ĐỘNG (CORRECTIVE ACTION) ----------
@app.post("/measurements/<int:mid>/actions")
@login_required
def upsert_action(mid: int):
    """Thêm hoặc cập nhật hành động khắc phục cho một bài đo."""
    seq_no = request.form.get("seq_no", type=int)
    action_txt = (request.form.get("action") or "").strip()
    owner = (request.form.get("owner") or "").strip() or None
    due_date = request.form.get("due_date") or None
    status = (request.form.get("status") or "").strip() or None

    if not action_txt:
        flash("Vui lòng nhập mô tả hành động.", "warning")
        return redirect(url_for("list_measurements"))

    if not seq_no or seq_no < 1:
        seq_no = 1

    with get_db() as con, con.cursor() as cur:
        # Kiểm tra tồn tại bài đo
        cur.execute("SELECT 1 FROM measurements WHERE id = %s", (mid,))
        if cur.fetchone() is None:
            flash(f"Không tìm thấy bài đo #{mid}.", "danger")
            return redirect(url_for("list_measurements"))

        # Thêm bản ghi vào bảng corrective_actions
        cur.execute("""
            INSERT INTO corrective_actions
                (measurement_id, seq_no, action, owner, due_date, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (mid, seq_no, action_txt, owner, due_date, status))

    flash("Đã lưu hành động khắc phục.", "success")
    return redirect(url_for("list_measurements") + f"#m-{mid}")

# ===================== Run =====================
if __name__ == "__main__":
    # Local run
    app.run(host="0.0.0.0", port=5000, debug=True)
