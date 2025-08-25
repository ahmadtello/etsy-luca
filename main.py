import os, time, json, sqlite3, threading
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import quote


import requests
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from starlette.middleware.sessions import SessionMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exception_handlers import http_exception_handler as default_http_exception_handler

# ------------------ Load .env (optional) ------------------
load_dotenv()
APP_SECRET      = os.getenv("APP_SECRET", "change-me-please")
ADMIN_PASSWORD  = os.getenv("ADMIN_PASSWORD", "admin")   # set strong in prod

# Behavior defaults (can be overridden later via Settings UI)
DEFAULT_POLL_MINUTES      = int(os.getenv("POLL_INTERVAL_MINUTES", "5"))
DEFAULT_LOOKBACK_DAYS     = int(os.getenv("ORDERS_LOOKBACK_DAYS", "60"))
DEFAULT_KDV_RATE_FALLBACK = float(os.getenv("DEFAULT_KDV_RATE", "0"))
DEFAULT_CURRENCY          = os.getenv("CURRENCY_FALLBACK", "TRY")

# ------------------ FastAPI ------------------
app = FastAPI(title="Etsy → Luca e-Arşiv")
app.add_middleware(SessionMiddleware, secret_key=APP_SECRET)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# ------------------ Redirect 401s to /login ------------------
@app.exception_handler(StarletteHTTPException)
async def auth_redirect_on_401(request: Request, exc: StarletteHTTPException):
    # If not authorized, send to login and preserve where the user wanted to go
    if exc.status_code == 401:
        next_url = request.url.path
        if request.url.query:
            next_url += f"?{request.url.query}"
        return RedirectResponse(url=f"/login?next={quote(next_url)}", status_code=303)
    # other HTTP errors: default handling
    return await default_http_exception_handler(request, exc)

# ------------------ DB ------------------
DB_PATH = os.getenv("DB_PATH", "app.sqlite3")
DB_LOCK = threading.Lock()

def db_init():
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS orders (
            receipt_id TEXT PRIMARY KEY,
            buyer_name TEXT,
            total REAL,
            currency TEXT,
            status TEXT,
            updated_epoch INTEGER,
            raw_json TEXT,
            created_at TEXT,
            updated_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS invoices (
            receipt_id TEXT PRIMARY KEY,
            ettn TEXT,
            invoice_number TEXT,
            url TEXT,
            status TEXT,  -- NONE | CREATED | SENT | ERROR
            error TEXT,
            created_at TEXT,
            updated_at TEXT
        )""")
        c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('last_sync_epoch','0')")
        defaults = {
            "poll_minutes": str(DEFAULT_POLL_MINUTES),
            "lookback_days": str(DEFAULT_LOOKBACK_DAYS),
            "default_kdv_rate": str(DEFAULT_KDV_RATE_FALLBACK),
            "currency_fallback": DEFAULT_CURRENCY,
            "auto_invoice": "false",
            "etsy_client_id": os.getenv("ETSY_CLIENT_ID",""),
            "etsy_refresh_token": os.getenv("ETSY_REFRESH_TOKEN",""),
            "etsy_shop_id": os.getenv("ETSY_SHOP_ID",""),
            "luca_identity": os.getenv("LUCA_IDENTITY",""),
            "luca_password": os.getenv("LUCA_PASSWORD",""),
            "luca_company_id": os.getenv("LUCA_COMPANY_ID",""),
            "luca_base": os.getenv("LUCA_BASE","https://einvoiceapiturmob.luca.com.tr"),
        }
        for k,v in defaults.items():
            c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k,v))
        con.commit()

def db_get_setting(key: str, default: str = "") -> str:
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        c.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = c.fetchone()
        return row[0] if row else default

def db_set_setting(key: str, value: str):
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        c.execute("""
            INSERT INTO settings(key,value) VALUES(?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key, value))
        con.commit()

def settings_all() -> Dict[str,str]:
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        c.execute("SELECT key,value FROM settings")
        rows = c.fetchall()
    return {k:v for (k,v) in rows}

# ------------------ Auth helpers ------------------
def authed(request: Request) -> bool:
    return bool(request.session.get("auth"))

def require_auth(request: Request):
    if not authed(request):
        raise HTTPException(status_code=401, detail="Unauthorized")

# ------------------ Utils ------------------
def money_to_float(m) -> float:
    if m is None:
        return 0.0
    if isinstance(m, (int, float)):
        return float(m)
    amount = float(m.get("amount", 0))
    divisor = float(m.get("divisor", 100)) or 100.0
    return round(amount / divisor, 2)

def now_iso():
    return datetime.now().isoformat(timespec="seconds")

# ------------------ Etsy API ------------------
def etsy_refresh_access_token(st: Dict[str,str]) -> str:
    r = requests.post("https://api.etsy.com/v3/public/oauth/token", data={
        "grant_type": "refresh_token",
        "client_id": st["etsy_client_id"],
        "refresh_token": st["etsy_refresh_token"]
    }, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def etsy_receipts(st: Dict[str,str], access_token: str, since_epoch: int) -> List[Dict[str,Any]]:
    url = f"https://openapi.etsy.com/v3/application/shops/{st['etsy_shop_id']}/receipts"
    headers = {"Authorization": f"Bearer {access_token}", "x-api-key": st["etsy_client_id"]}
    params = {"limit": 100, "min_last_modified": since_epoch, "status": "paid", "offset": 0}
    out = []
    while True:
        rr = requests.get(url, headers=headers, params=params, timeout=30)
        rr.raise_for_status()
        data = rr.json()
        rows = data.get("results", [])
        out.extend(rows)
        if len(rows) < params["limit"]:
            break
        params["offset"] += params["limit"]
        if params["offset"] > 2000:
            break
    return out

def etsy_transactions(st: Dict[str,str], access_token: str, receipt_id: str) -> List[Dict[str,Any]]:
    url = f"https://openapi.etsy.com/v3/application/shops/{st['etsy_shop_id']}/receipts/{receipt_id}/transactions"
    headers = {"Authorization": f"Bearer {access_token}", "x-api-key": st["etsy_client_id"]}
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code == 200:
        return r.json().get("results", [])
    return []

# ------------------ Luca API ------------------
def luca_login(st: Dict[str,str]) -> str:
    r = requests.post(f"{st['luca_base']}/api/Account/Login",
                      json={"IdentificationNumber": st["luca_identity"], "Password": st["luca_password"]},
                      timeout=30)
    r.raise_for_status()
    return r.json()["Token"]

def luca_save_archive(st: Dict[str,str], token: str, payload: Dict[str,Any]) -> Dict[str,Any]:
    h = {"Authorization": f"Bearer {token}"}
    r = requests.post(f"{st['luca_base']}/api/Invoice/SaveArchive", headers=h, json=payload, timeout=45)
    r.raise_for_status()
    return r.json()

def luca_send_archive(st: Dict[str,str], token: str, ettn: str) -> Dict[str,Any]:
    h = {"Authorization": f"Bearer {token}"}
    r = requests.post(f"{st['luca_base']}/api/Invoice/SendStagingArchive", headers=h,
                      json={"CompanyId": float(st["luca_company_id"]), "ETTN": ettn}, timeout=45)
    r.raise_for_status()
    return r.json()

def luca_get_external_url(st: Dict[str,str], token: str, ettn: str) -> str:
    h = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{st['luca_base']}/api/Invoice/GetDocumentExternalUrl", headers=h,
                     params={"companyId": st["luca_company_id"], "ettn": ettn}, timeout=30)
    r.raise_for_status()
    try:
        data = r.json()
        return data.get("Url") or data.get("url") or json.dumps(data)
    except Exception:
        return r.text

def luca_get_pdf(st: Dict[str,str], token: str, ettn: str) -> Optional[bytes]:
    h = {"Authorization": f"Bearer {token}"}
    for path in ("/api/Invoice/GetDocumentPdf", "/api/Invoice/GetInvoicePdf"):
        try:
            r = requests.get(f"{st['luca_base']}{path}", headers=h,
                             params={"companyId": st["luca_company_id"], "ettn": ettn}, timeout=45)
            if r.status_code == 200 and r.headers.get("content-type","").lower().startswith("application/pdf"):
                return r.content
        except Exception:
            pass
    return None

# ------------------ Mapping ------------------
def build_luca_payload(st: Dict[str,str], receipt: Dict[str,Any], txs: List[Dict[str,Any]]) -> Dict[str,Any]:
    kdv = float(st.get("default_kdv_rate") or "0")
    currency = (receipt.get("grandtotal") or {}).get("currency_code") \
               or (receipt.get("total_price") or {}).get("currency_code") \
               or st.get("currency_fallback") or DEFAULT_CURRENCY

    buyer_name = (receipt.get("name") or "Etsy Buyer").strip()
    email = receipt.get("buyer_email") or ""
    phone = receipt.get("phone") or ""
    addr = {
        "BoulevardAveneuStreetName": (receipt.get("first_line") or "")[:250],
        "TownName": (receipt.get("city") or "")[:60],
        "CityName": ((receipt.get("state") or receipt.get("city") or ""))[:60],
        "PostalCode": receipt.get("zip") or "",
        "CountryName": receipt.get("country_name") or receipt.get("country_iso") or "",
        "TelephoneNumber": phone,
        "EMail": email
    }

    lines = []
    total_net = 0.0
    if txs:
        for t in txs:
            qty = float(t.get("quantity", 1))
            unit = money_to_float(t.get("price") or t.get("amount_paid") or receipt.get("total_price"))
            line_net = round(qty * unit, 2)
            total_net += line_net
            lines.append({
                "ProductName": t.get("title") or f"Etsy Item #{t.get('transaction_id')}",
                "Quantity": qty,
                "UnitPrice": unit,
                "VatRate": kdv,
                "VatAmount": round(line_net * kdv / 100.0, 2),
                "LineExtensionAmount": line_net
            })
    else:
        grand = money_to_float(receipt.get("grandtotal") or receipt.get("total_price"))
        total_net = round(grand, 2)
        lines = [{
            "ProductName": f"Etsy Order #{receipt.get('receipt_id')}",
            "Quantity": 1,
            "UnitPrice": total_net,
            "VatRate": kdv,
            "VatAmount": round(total_net * kdv / 100.0, 2),
            "LineExtensionAmount": total_net
        }]

    total_vat   = round(total_net * kdv / 100.0, 2)
    total_gross = round(total_net + total_vat, 2)

    ts  = int(receipt.get("updated_timestamp") or receipt.get("created_timestamp") or time.time())
    dt  = datetime.utcfromtimestamp(ts)
    day = dt.strftime("%Y-%m-%d")
    tme = dt.strftime("%H:%M:%S")

    return {
        "RecipientType": "NONE",
        "InvoiceNumber": "",
        "IdFaturaExternal": str(receipt.get("receipt_id")),
        "CompanyId": float(st["luca_company_id"]),
        "ScenarioType": "None",
        "InvoiceDate": day,
        "InvoiceTime": tme,
        "InvoiceType": 1,
        "OrderDate": day,
        "OrderNumber": str(receipt.get("receipt_id")),
        "Receiver": {
            "ReceiverName": buyer_name,
            "ReceiverTaxCode": "",
            "RecipientType": "NONE",
            "Address": addr
        },
        "Products": lines,
        "CurrencyCode": currency,
        "TotalLineExtensionAmount": total_net,
        "TotalVATAmount": total_vat,
        "TotalTaxInclusiveAmount": total_gross,
        "TotalPayableAmount": total_gross,
        "SendMailAutomatically": True,
        "WebSellingInfo": {
            "WebAddress": f"https://www.etsy.com/shop/{receipt.get('seller_user_id','')}",
            "PaymentMediatorName": "Etsy",
            "PaymentType": "NONE",
            "OtherPaymentType": "Etsy Payments",
            "PaymentDate": day,
            "SendingDate": day
        }
    }

# ------------------ DB helpers ------------------
def db_upsert_order(rcpt: Dict[str,Any]):
    rid   = str(rcpt.get("receipt_id"))
    buyer = (rcpt.get("name") or "Etsy Buyer").strip()
    currency = (rcpt.get("grandtotal") or {}).get("currency_code") \
               or (rcpt.get("total_price") or {}).get("currency_code") \
               or db_get_setting("currency_fallback", DEFAULT_CURRENCY)
    total = money_to_float(rcpt.get("grandtotal") or rcpt.get("total_price"))
    status  = (rcpt.get("status") or "").upper()
    updated = int(rcpt.get("updated_timestamp") or rcpt.get("last_modified_tsz") or rcpt.get("created_timestamp") or time.time())
    now = now_iso()
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        c.execute("""
            INSERT INTO orders(receipt_id,buyer_name,total,currency,status,updated_epoch,raw_json,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(receipt_id) DO UPDATE SET
                buyer_name=excluded.buyer_name,
                total=excluded.total,
                currency=excluded.currency,
                status=excluded.status,
                updated_epoch=excluded.updated_epoch,
                raw_json=excluded.raw_json,
                updated_at=excluded.updated_at
        """, (rid,buyer,total,currency,status,updated,json.dumps(rcpt),now,now))
        con.commit()

def db_get_order(receipt_id: str) -> Optional[Dict[str,Any]]:
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        c.execute("SELECT receipt_id,buyer_name,total,currency,status,updated_epoch,raw_json FROM orders WHERE receipt_id=?", (receipt_id,))
        row = c.fetchone()
        if not row: return None
        keys = ["receipt_id","buyer_name","total","currency","status","updated_epoch","raw_json"]
        obj = dict(zip(keys, row))
        obj["raw_json"] = json.loads(obj["raw_json"]) if obj["raw_json"] else {}
        return obj

def db_recent_orders(limit:int=500) -> List[Dict[str,Any]]:
    lookback_days = int(db_get_setting("lookback_days", str(DEFAULT_LOOKBACK_DAYS)))
    cutoff = int((datetime.utcnow() - timedelta(days=lookback_days)).timestamp())
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        c.execute("""
            SELECT receipt_id,buyer_name,total,currency,status,updated_epoch
            FROM orders
            WHERE updated_epoch >= ?
            ORDER BY updated_epoch DESC
            LIMIT ?""", (cutoff, limit))
        rows = c.fetchall()
        keys = ["receipt_id","buyer_name","total","currency","status","updated_epoch"]
        return [dict(zip(keys, r)) for r in rows]

def db_upsert_invoice(receipt_id:str, **fields):
    now = now_iso()
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        c.execute("SELECT receipt_id FROM invoices WHERE receipt_id=?", (receipt_id,))
        exists = c.fetchone() is not None
        if exists:
            c.execute("""
                UPDATE invoices
                SET ettn=?, invoice_number=?, url=?, status=?, error=?, updated_at=?
                WHERE receipt_id=?
            """, (fields.get("ettn"), fields.get("invoice_number"), fields.get("url"),
                  fields.get("status"), fields.get("error"), now, receipt_id))
        else:
            c.execute("""
                INSERT INTO invoices(receipt_id,ettn,invoice_number,url,status,error,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?)
            """, (receipt_id, fields.get("ettn"), fields.get("invoice_number"),
                  fields.get("url"), fields.get("status") or "NONE", fields.get("error"), now, now))
        con.commit()

def db_get_invoice(receipt_id:str) -> Optional[Dict[str,Any]]:
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        c.execute("SELECT receipt_id,ettn,invoice_number,url,status,error FROM invoices WHERE receipt_id=?", (receipt_id,))
        row = c.fetchone()
        if not row: return None
        keys = ["receipt_id","ettn","invoice_number","url","status","error"]
        return dict(zip(keys, row))

def db_recent_invoices_map() -> Dict[str,Dict[str,Any]]:
    with sqlite3.connect(DB_PATH) as con:
        c = con.cursor()
        c.execute("SELECT receipt_id,ettn,invoice_number,url,status,error FROM invoices")
        rows = c.fetchall()
        keys = ["receipt_id","ettn","invoice_number","url","status","error"]
        return { r[0]: dict(zip(keys, r)) for r in rows }

# ---- Filtering & pagination helpers ----
def _attach_invoices(orders: List[Dict[str,Any]], inv_map: Dict[str,Dict[str,Any]]):
    for o in orders:
        inv = inv_map.get(o["receipt_id"])
        o["invoice"] = inv or {"status":"NONE","ettn":None,"invoice_number":None,"url":None,"error":None}
    return orders

def _filter_orders(orders: List[Dict[str,Any]], q: str, istatus: str, ostatus: str):
    q = (q or "").strip().lower()
    out = []
    for o in orders:
        ok = True
        if q:
            ok = (q in (o.get("buyer_name") or "").lower()) or (q in str(o.get("receipt_id")))
        if ok and istatus:
            ok = (o["invoice"]["status"] == istatus.upper())
        if ok and ostatus:
            ok = (o.get("status","").upper() == ostatus.upper())
        if ok:
            out.append(o)
    return out

def _sort_orders(orders: List[Dict[str,Any]], sort_key: str):
    if sort_key == "updated_asc":
        return sorted(orders, key=lambda x: x.get("updated_epoch",0))
    if sort_key == "total_desc":
        return sorted(orders, key=lambda x: x.get("total",0.0), reverse=True)
    if sort_key == "total_asc":
        return sorted(orders, key=lambda x: x.get("total",0.0))
    # default newest first
    return sorted(orders, key=lambda x: x.get("updated_epoch",0), reverse=True)

# ------------------ Sync core ------------------
def sync_orders_and_maybe_invoice() -> Tuple[int,int,int]:
    """Returns: (orders_synced, invoices_created, errors)"""
    st = settings_all()
    with DB_LOCK:
        last_sync = int(db_get_setting("last_sync_epoch", "0"))
        if last_sync == 0:
            last_sync = int(time.time()) - 24*3600

    access = etsy_refresh_access_token(st)
    receipts = etsy_receipts(st, access, last_sync)
    if not receipts:
        with DB_LOCK:
            db_set_setting("last_sync_epoch", str(last_sync + 1))
        return (0,0,0)

    max_updated = last_sync
    created = 0
    errs = 0
    auto_invoice = (st.get("auto_invoice","false").lower() == "true")
    token = luca_login(st) if auto_invoice else None

    for rc in receipts:
        rid = str(rc.get("receipt_id"))
        updated = int(rc.get("updated_timestamp") or rc.get("last_modified_tsz") or rc.get("created_timestamp") or time.time())
        if updated > max_updated: max_updated = updated
        try:
            db_upsert_order(rc)
            if auto_invoice:
                txs = etsy_transactions(st, access, rid)
                payload = build_luca_payload(st, rc, txs)
                saved = luca_save_archive(st, token, payload)
                ettn = saved.get("Ettn") or saved.get("ETTN") or ""
                invno = saved.get("InvoiceNumber") or ""
                db_upsert_invoice(rid, status="CREATED", ettn=ettn, invoice_number=invno, url=None, error=None)
                created += 1
                if ettn:
                    _ = luca_send_archive(st, token, ettn)
                    url = luca_get_external_url(st, token, ettn)
                    db_upsert_invoice(rid, status="SENT", ettn=ettn, invoice_number=invno, url=url, error=None)
        except Exception as e:
            errs += 1
            db_upsert_invoice(rid, status="ERROR", error=str(e))

    with DB_LOCK:
        db_set_setting("last_sync_epoch", str(max_updated + 1))
    return (len(receipts), created, errs)

# ------------------ Scheduler ------------------
scheduler = BackgroundScheduler(daemon=True)

def schedule_poll():
    st = settings_all()
    minutes = max(1, int(st.get("poll_minutes", str(DEFAULT_POLL_MINUTES)) or DEFAULT_POLL_MINUTES))
    job = scheduler.get_job("poll_etsy")
    if job:
        job.reschedule(trigger="interval", minutes=minutes)
    else:
        scheduler.add_job(sync_orders_and_maybe_invoice, "interval", minutes=minutes,
                          id="poll_etsy", replace_existing=True)

@app.on_event("startup")
def on_start():
    db_init()
    schedule_poll()
    scheduler.start()

@app.on_event("shutdown")
def on_shutdown():
    scheduler.shutdown(wait=False)

# ------------------ Routes ------------------
@app.get("/healthz")
def healthz():
    return {"ok": True}

# ---- Auth ----
@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    nxt = request.query_params.get("next")
    return templates.TemplateResponse("login.html", {"request": request, "error": None, "next": nxt})

@app.post("/login")
def login_post(request: Request, password: str = Form(...), next: Optional[str] = Form(None)):
    if password == ADMIN_PASSWORD:
        request.session["auth"] = True
        return RedirectResponse(next or "/", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Wrong password", "next": next})

@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)

# ---- Dashboard ----
@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    require_auth(request)

    # Query params
    q        = request.query_params.get("q", "") or ""
    istatus  = request.query_params.get("istatus", "") or ""
    ostatus  = request.query_params.get("ostatus", "") or ""
    sort     = request.query_params.get("sort", "updated_desc")
    page     = int(request.query_params.get("page", "1") or "1")
    per_page = int(request.query_params.get("per_page", "20") or "20")
    page = max(1, page); per_page = max(1, min(per_page, 200))

    with DB_LOCK:
        orders  = db_recent_orders(5000)   # wide window, fine for SMB volumes
        inv_map = db_recent_invoices_map()
        last_sync = db_get_setting("last_sync_epoch", "0")
        st = settings_all()

    _attach_invoices(orders, inv_map)
    filtered = _filter_orders(orders, q, istatus, ostatus)
    ordered  = _sort_orders(filtered, sort)

    total = len(ordered)
    start = (page - 1) * per_page
    end   = start + per_page
    page_orders = ordered[start:end]

    # Build a querystring without 'page' for pager links
    qp = request.query_params.multi_items()
    qp_no_page = "&".join([f"{k}={v}" for (k,v) in qp if k != "page"])

    return templates.TemplateResponse("index.html", {
        "request": request,
        "orders": orders,  # not used in template, kept for completeness
        "page_orders": page_orders,
        "total": total,
        "page": page,
        "per_page": per_page,
        "start_index": start,
        "end_index": end,
        "query_no_page": qp_no_page,
        "last_sync": last_sync,
        "poll": st.get("poll_minutes"),
        "auto_invoice": (st.get("auto_invoice","false") == "true"),
    })

@app.post("/sync-now")
def sync_now(request: Request):
    require_auth(request)
    sync_orders_and_maybe_invoice()
    return RedirectResponse("/", status_code=303)

# ---- Actions: invoice ----
@app.post("/invoice/create/{receipt_id}")
def create_invoice(request: Request, receipt_id: str):
    require_auth(request)
    st = settings_all()
    order = db_get_order(receipt_id)
    if not order:
        raise HTTPException(404, "Order not found in local cache. Click Sync first.")
    token = luca_login(st)
    access = etsy_refresh_access_token(st)
    txs = etsy_transactions(st, access, receipt_id)
    payload = build_luca_payload(st, order["raw_json"], txs)
    saved = luca_save_archive(st, token, payload)
    ettn = saved.get("Ettn") or saved.get("ETTN") or ""
    invno = saved.get("InvoiceNumber") or ""
    db_upsert_invoice(receipt_id, status="CREATED", ettn=ettn, invoice_number=invno, url=None, error=None)
    return RedirectResponse("/", status_code=303)

@app.post("/invoice/send/{receipt_id}")
def send_invoice(request: Request, receipt_id: str):
    require_auth(request)
    st = settings_all()
    inv = db_get_invoice(receipt_id)
    if not inv or not inv.get("ettn"):
        raise HTTPException(400, "No invoice ETTN found. Create invoice first.")
    token = luca_login(st)
    _ = luca_send_archive(st, token, inv["ettn"])
    url = luca_get_external_url(st, token, inv["ettn"])
    db_upsert_invoice(receipt_id, status="SENT", url=url)
    return RedirectResponse("/", status_code=303)

@app.get("/invoice/view/{receipt_id}")
def view_invoice(request: Request, receipt_id: str):
    require_auth(request)
    st = settings_all()
    inv = db_get_invoice(receipt_id)
    if not inv or not inv.get("ettn"):
        raise HTTPException(404, "Invoice not created yet.")
    token = luca_login(st)
    url = inv.get("url") or luca_get_external_url(st, token, inv["ettn"])
    if url and url != inv.get("url"):
        db_upsert_invoice(receipt_id, url=url)
    return RedirectResponse(url, status_code=302)

@app.post("/invoice/bulk/create")
def bulk_create_invoices(request: Request, ids: str = Form("")):
    require_auth(request)
    st = settings_all()
    access = etsy_refresh_access_token(st)
    token  = luca_login(st)

    ids_list = [x.strip() for x in (ids or "").split(",") if x.strip()]
    created = 0; errs = 0
    for rid in ids_list:
        try:
            order = db_get_order(rid)
            if not order:
                errs += 1; continue
            txs = etsy_transactions(st, access, rid)
            payload = build_luca_payload(st, order["raw_json"], txs)
            saved = luca_save_archive(st, token, payload)
            ettn  = saved.get("Ettn") or saved.get("ETTN") or ""
            invno = saved.get("InvoiceNumber") or ""
            db_upsert_invoice(rid, status="CREATED", ettn=ettn, invoice_number=invno, url=None, error=None)
            created += 1
        except Exception as e:
            errs += 1
            db_upsert_invoice(rid, status="ERROR", error=str(e))
    return RedirectResponse(f"/?msg=Created+{created}+invoice(s)&err={errs}+error(s)", status_code=303)

@app.post("/invoice/bulk/send")
def bulk_send_invoices(request: Request, ids: str = Form("")):
    require_auth(request)
    st = settings_all()
    token  = luca_login(st)

    ids_list = [x.strip() for x in (ids or "").split(",") if x.strip()]
    sent = 0; errs = 0
    for rid in ids_list:
        try:
            inv = db_get_invoice(rid)
            if not inv or not inv.get("ettn"):
                errs += 1; continue
            _ = luca_send_archive(st, token, inv["ettn"])
            url = luca_get_external_url(st, token, inv["ettn"])
            db_upsert_invoice(rid, status="SENT", url=url)
            sent += 1
        except Exception as e:
            errs += 1
            db_upsert_invoice(rid, status="ERROR", error=str(e))
    return RedirectResponse(f"/?msg=Sent+{sent}+invoice(s)&err={errs}+error(s)", status_code=303)

@app.get("/invoice/pdf/{receipt_id}")
def pdf_invoice(request: Request, receipt_id: str):
    require_auth(request)
    st = settings_all()
    inv = db_get_invoice(receipt_id)
    if not inv or not inv.get("ettn"):
        raise HTTPException(404, "Invoice not created yet.")
    token = luca_login(st)
    pdf_bytes = luca_get_pdf(st, token, inv["ettn"])
    if not pdf_bytes:
        url = luca_get_external_url(st, token, inv["ettn"])
        return RedirectResponse(url, status_code=302)
    fname = f"invoice-{receipt_id}.pdf"
    return StreamingResponse(iter([pdf_bytes]),
                             media_type="application/pdf",
                             headers={"Content-Disposition": f"attachment; filename={fname}"})

# ---- Settings ----
@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, msg: Optional[str] = None, err: Optional[str] = None):
    require_auth(request)
    st = settings_all()
    return templates.TemplateResponse("settings.html",
        {"request": request, "st": st, "msg": msg, "err": err})

@app.post("/settings")
def settings_save(
    request: Request,
    etsy_client_id: str = Form(""),
    etsy_refresh_token: str = Form(""),
    etsy_shop_id: str = Form(""),
    luca_identity: str = Form(""),
    luca_password: str = Form(""),
    luca_company_id: str = Form(""),
    luca_base: str = Form("https://einvoiceapiturmob.luca.com.tr"),
    poll_minutes: str = Form("5"),
    lookback_days: str = Form("60"),
    default_kdv_rate: str = Form("0"),
    currency_fallback: str = Form("TRY"),
    auto_invoice: str = Form("off"),
):
    require_auth(request)
    kv = locals().copy()
    kv.pop("request", None)
    for k,v in kv.items():
        if k == "auto_invoice":
            db_set_setting(k, "true" if (v == "on" or v == "true") else "false")
        else:
            db_set_setting(k, str(v).strip())
    # reschedule poller if interval changed
    schedule_poll()
    return RedirectResponse("/settings?msg=Saved", status_code=303)

@app.post("/settings/test/etsy")
def settings_test_etsy(request: Request):
    require_auth(request)
    try:
        st = settings_all()
        tok = etsy_refresh_access_token(st)
        # tiny check: try a very small receipts call with min_last_modified = now-1d
        since = int(time.time()) - 24*3600
        _ = etsy_receipts(st, tok, since)
        return RedirectResponse("/settings?msg=Etsy+OK", status_code=303)
    except Exception as e:
        return RedirectResponse(f"/settings?err=Etsy+Error:+{str(e)}", status_code=303)

@app.post("/settings/test/luca")
def settings_test_luca(request: Request):
    require_auth(request)
    try:
        st = settings_all()
        tok = luca_login(st)
        # no-op call: just token acquisition is enough to verify
        _ = tok[:8]
        return RedirectResponse("/settings?msg=Luca+OK", status_code=303)
    except Exception as e:
        return RedirectResponse(f"/settings?err=Luca+Error:+{str(e)}", status_code=303)
