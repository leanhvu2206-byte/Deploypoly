# MeasureApp (Flask)

Ứng dụng web Python (Flask) demo với đăng nhập và các chức năng:
- Dashboard
- Tất cả bài đo
- Tạo mới bài đo
- Lịch sử bài đo (lọc theo thời gian)
- Xuất file CSV
- Đăng xuất

## Chạy nhanh

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py  # hoặc: flask --app app run --debug
```

- Tài khoản mặc định: `admin` / `admin123`
- Cấu hình SECRET_KEY trong `app.py` cho môi trường production.

## Cấu trúc thư mục

```
measure_app/
├─ app.py
├─ app.db (tự tạo khi chạy lần đầu)
├─ requirements.txt
├─ templates/
│  ├─ base.html
│  ├─ login.html
│  ├─ dashboard.html
│  ├─ measurements.html
│  ├─ create_measurement.html
│  └─ history.html
└─ static/
   └─ style.css
```

## Ghi chú
- DB: SQLite đơn giản, file `app.db` nằm cùng thư mục.
- Auth: session + mật khẩu băm (Werkzeug).
- UI: Bootstrap 5 từ CDN, dễ tùy biến.
- Debug: `debug=True` trong `app.py` khi dev.