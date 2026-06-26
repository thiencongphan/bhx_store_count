#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cao toan bo danh sach cua hang Bach Hoa Xanh qua API gateway.
Ban danh cho GitHub Actions: doc token nhay cam tu bien moi truong (Secrets).

Endpoint:
  https://api.bachhoaxanh.com/gw/Location/V2/GetStoresByLocation
"""

import csv
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import requests

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
BASE_URL = "https://api.bachhoaxanh.com/gw/Location/V2/GetStoresByLocation"

# Token nhay cam doc tu bien moi truong (GitHub Secrets).
# Neu chay local, co the dat truc tiep gia tri mac dinh o phia sau "or".
BHX_AUTHORIZATION = os.environ.get("BHX_AUTHORIZATION", "Bearer 4129A95533B98728D6218C622CFC48A4")
BHX_DEVICEID = os.environ.get("BHX_DEVICEID", "0411f9fe-f4c6-452d-9768-cb07805193be")
BHX_XAPIKEY = os.environ.get("BHX_XAPIKEY", "bhx-api-core-2022")

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.6,en;q=0.5",
    "Origin": "https://www.bachhoaxanh.com",
    "Referer": "https://www.bachhoaxanh.com/he-thong-cua-hang",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
    ),
    "authorization": BHX_AUTHORIZATION,
    "deviceid": BHX_DEVICEID,
    "platform": "webnew",
    "referer-url": "https://www.bachhoaxanh.com/he-thong-cua-hang",
    "reversehost": "http://bhxapi.live",
    "xapikey": BHX_XAPIKEY,
}

# ---- 34 tinh/thanh (sau sap nhap 2025) -------------------------------------
# id 1034-1042: PHONG DOAN theo thu tu - tinh 24/6/2026 BHX chua mo cua hang.
# Cac tinh nay co the tra ve 0 (chua co cua hang) hoac id chua chinh xac.
PROVINCES = {
    "Ha Noi": 1000,
    "Quang Ninh": 1009,
    "Bac Ninh": 1010,
    "Hai Phong": 1012,
    "Hung Yen": 1013,
    "Ninh Binh": 1014,
    "Thanh Hoa": 1015,
    "Nghe An": 1016,
    "Ha Tinh": 1017,
    "Quang Tri": 1018,
    "Hue": 1019,
    "Da Nang": 1020,
    "Quang Ngai": 1021,
    "Gia Lai": 1022,
    "Khanh Hoa": 1023,
    "Dak Lak": 1024,
    "Lam Dong": 1025,
    "Dong Nai": 1026,
    "TP Ho Chi Minh": 1027,
    "Tay Ninh": 1028,
    "Dong Thap": 1029,
    "Vinh Long": 1030,
    "An Giang": 1031,
    "Can Tho": 1032,
    "Ca Mau": 1033,
    # --- Nhom phong doan (chua co cua hang tinh 24/6/2026) ---
    "Lai Chau": 1034,
    "Dien Bien": 1035,
    "Son La": 1036,
    "Lang Son": 1037,
    "Cao Bang": 1038,
    "Tuyen Quang": 1039,
    "Lao Cai": 1040,
    "Thai Nguyen": 1041,
    "Phu Tho": 1042,
}

# Da co du id cho 34 tinh -> tat quet dai. Bat lai (True) neu muon do them id la.
SCAN_MISSING = False
SCAN_START = 1001
SCAN_END = 1050

PAGE_SIZE = 50          # server BHX gioi han toi da 50/trang
OUTPUT_DIR = Path("./bhx_output")
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BACKOFF = 5
SLEEP_BETWEEN = 0.4

# ----------------------------------------------------------------------------
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "scraper.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("bhx")

session = requests.Session()
session.headers.update(HEADERS)

ID_TO_NAME = {v: k for k, v in PROVINCES.items() if v is not None}


def request_json(params):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 401:
                log.error("401 Unauthorized - token co the da het han. Cap nhat Secrets.")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            log.warning("Loi (lan %d/%d) params=%s: %s", attempt, MAX_RETRIES, params, e)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
    raise RuntimeError("That bai sau %d lan: %s" % (MAX_RETRIES, params)) from last_err


def normalize(store, province_name=""):
    return {
        "province_name": province_name,
        "store_id": store.get("storeId"),
        "store_location": store.get("storeLocation"),
        "store_address": store.get("storeAddress"),
        "open_hour": store.get("openHour"),
        "province_id": store.get("provinceId"),
        "district_id": store.get("districtId"),
        "ward_id": store.get("wardId"),
        "lat": store.get("lat"),
        "lng": store.get("lng"),
        "is_store_virtual": store.get("isStoreVirtual"),
        "raw": json.dumps(store, ensure_ascii=False),
    }


def scrape_province(province_id, province_name=""):
    rows = []
    page = 0
    total = None
    MAX_PAGES = 200   # chot an toan tranh lap vo han (200*50 = 10000 store/tinh)
    while page < MAX_PAGES:
        params = {"provinceId": province_id, "wardId": 0,
                  "pageSize": PAGE_SIZE, "pageIndex": page}
        data = request_json(params)
        if data.get("code") != 0:
            return rows, -1
        d = data.get("data") or {}
        stores = d.get("stores") or []
        if total is None:
            total = d.get("total", 0)
        if not stores:
            break
        for s in stores:
            rows.append(normalize(s, province_name))
        # Dung khi da lay du total. KHONG dung theo PAGE_SIZE vi server
        # co the gioi han so luong tra ve moi trang (vd 50) < pageSize yeu cau.
        if total and len(rows) >= total:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN)
    return rows, (total if total is not None else 0)


def scrape_all():
    all_rows = []
    seen = set()
    found = []

    known_ids = sorted(v for v in PROVINCES.values() if v is not None)
    ids_to_scan = list(known_ids)
    if SCAN_MISSING:
        extra = [i for i in range(SCAN_START, SCAN_END + 1) if i not in set(known_ids)]
        ids_to_scan = sorted(set(ids_to_scan + extra))

    for pid in ids_to_scan:
        name = ID_TO_NAME.get(pid, "(id %d - chua biet ten)" % pid)
        try:
            rows, total = scrape_province(pid, name)
        except Exception as e:
            log.error("Loi tinh %s (%s): %s", pid, name, e)
            continue

        if rows:
            added = 0
            for r in rows:
                sid = r["store_id"]
                if sid in seen:
                    continue
                seen.add(sid)
                all_rows.append(r)
                added += 1
            found.append((pid, name, len(rows)))
            log.info("id %s [%s]: %d cua hang (them moi %d)", pid, name, len(rows), added)
        else:
            if pid in known_ids:
                log.info("id %s [%s]: rong", pid, name)
        time.sleep(SLEEP_BETWEEN)

    log.info("=== Tinh co cua hang:")
    for pid, name, n in found:
        log.info("    %s\t%s\t%d", pid, name, n)
    log.info("=== TONG: %d cua hang (unique) tu %d tinh", len(all_rows), len(found))
    return all_rows


def save(rows):
    if not rows:
        log.warning("Khong co du lieu de luu.")
        return
    stamp = datetime.now().strftime("%Y%m%d")
    fields = [k for k in rows[0].keys() if k != "raw"] + ["raw"]

    with (OUTPUT_DIR / ("bhx_stores_%s.json" % stamp)).open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    for path in (OUTPUT_DIR / ("bhx_stores_%s.csv" % stamp), OUTPUT_DIR / "bhx_stores_latest.csv"):
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)

    log.info("Da luu vao %s", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    log.info("=== Bat dau cao BHX ===")
    save(scrape_all())
    log.info("=== Hoan tat ===")
