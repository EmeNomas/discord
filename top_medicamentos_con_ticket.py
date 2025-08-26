#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime as dt
import time
import re
import requests
import pandas as pd

API_BASE = "https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json"
TICKET = "1A53E862-03F9-4F2C-9E15-32046FC3C18F"

def dmy(date: dt.date) -> str:
    return date.strftime("%d%m%Y")

def fetch_ocs_for_day(date: dt.date, estado: str = "aceptada", sleep_s: float = 0.25):
    params = {"fecha": dmy(date), "ticket": TICKET, "estado": estado}
    r = requests.get(API_BASE, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    codigos = []
    for el in data.get("Listado", []):
        code = el.get("Codigo") or el.get("codigo")
        if code:
            codigos.append(code)
    time.sleep(sleep_s)
    return codigos

def fetch_oc_detail(codigo: str, sleep_s: float = 0.25):
    params = {"codigo": codigo, "ticket": TICKET}
    r = requests.get(API_BASE, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and "Listado" in data and isinstance(data["Listado"], list) and data["Listado"]:
        data = data["Listado"][0]
    time.sleep(sleep_s)
    return data

def is_medicamento(item: dict) -> bool:
    cod = str(item.get("CodigoCategoria") or "")
    return cod.startswith("51")

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).upper()

def build_med_key(item: dict) -> str:
    text = " ".join([str(item.get(k, "")) for k in [
        "EspecificacionProveedor", "EspecificacionComprador", "NombreProducto", "Producto"
    ]])
    t = normalize_text(text)
    m = re.search(r"([A-ZÁÉÍÓÚÑ/+\- ]{3,}?)\s+(\d{1,4}(?:[.,]\d{1,3})?)\s*(MG|MCG|G|ML|UI|U|%)\b", t)
    if m:
        generic = " ".join(normalize_text(m.group(1)).split()[:4])
        fuerza = m.group(2)
        unidad = m.group(3)
        return f"{generic} {fuerza} {unidad}"
    return t or "DESCONOCIDO"

def main():
    desde = dt.date(2024, 7, 1)
    hasta = dt.date(2024, 7, 31)
    all_items = []
    day = desde
    while day <= hasta:
        try:
            codigos = fetch_ocs_for_day(day)
            for code in codigos:
                try:
                    oc = fetch_oc_detail(code)
                    items = oc.get("Items") or []
                    for it in items:
                        if not is_medicamento(it):
                            continue
                        cantidad = it.get("Cantidad") or 0
                        all_items.append({
                            "codigo_oc": code,
                            "fecha_oc": oc.get("Fecha", ""),
                            "organismo": oc.get("NombreOrganismo", ""),
                            "proveedor": oc.get("NombreProveedor", ""),
                            "med_key": build_med_key(it),
                            "cantidad": float(str(cantidad).replace(",", ".")) if cantidad else 0.0,
                            "unidad": it.get("Unidad", ""),
                            "precio_neto": it.get("PrecioNeto", "")
                        })
                except Exception as e:
                    print(f"Error OC {code}: {e}")
        except Exception as e:
            print(f"Error día {day}: {e}")
        day += dt.timedelta(days=1)

    if not all_items:
        print("No se encontraron medicamentos en el rango")
        return

    df = pd.DataFrame(all_items)
    resumen = df.groupby("med_key")["cantidad"].sum().reset_index().sort_values("cantidad", ascending=False)
    resumen.to_csv("resumen_top_medicamentos.csv", index=False, encoding="utf-8-sig")
    df.to_csv("items_medicamentos_raw.csv", index=False, encoding="utf-8-sig")
    print(resumen.head(20).to_string(index=False))

if __name__ == "__main__":
    main()
