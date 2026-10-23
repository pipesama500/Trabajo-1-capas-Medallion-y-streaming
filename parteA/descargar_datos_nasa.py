# =====================================================================
# TRABAJO 1 - PARTE A: descarga de datos horarios de NASA POWER
# 15 municipios de Antioquia, 2015-2024 -> ~1.315.080 filas
# Pensado para Google Colab (al final descarga el CSV).
# =====================================================================
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

PUNTOS = {
    "Medellin": (6.25, -75.56), "Rionegro": (6.15, -75.37),
    "Santa Fe de Antioquia": (6.56, -75.83), "Turbo": (8.09, -76.73),
    "Apartado": (7.88, -76.63), "Caucasia": (7.99, -75.19),
    "Yarumal": (6.96, -75.42), "Puerto Berrio": (6.49, -74.40),
    "Andes": (5.66, -75.88), "Sonson": (5.71, -75.31),
    "Segovia": (7.08, -74.70), "Urrao": (6.32, -76.13),
    "Necocli": (8.42, -76.78), "Amalfi": (6.91, -75.08),
    "El Bagre": (7.60, -74.80),
}
ANIOS = range(2015, 2025)
PARAMS = "ALLSKY_SFC_SW_DWN,CLRSKY_SFC_SW_DWN,T2M,RH2M,WS10M"
URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"


def descargar(municipio, lat, lon, anio):
    params = {
        "parameters": PARAMS, "community": "RE",
        "latitude": lat, "longitude": lon,
        "start": f"{anio}0101", "end": f"{anio}1231",
        "format": "JSON", "time-standard": "LST",
    }
    for intento in range(4):
        try:
            r = requests.get(URL, params=params, timeout=180)
            r.raise_for_status()
            datos = r.json()["properties"]["parameter"]
            df = pd.DataFrame(datos)
            df.index.name = "fecha_hora"
            df = df.reset_index()
            df.insert(0, "longitud", lon)
            df.insert(0, "latitud", lat)
            df.insert(0, "municipio", municipio)
            return df
        except Exception as e:
            print(f"Reintento {intento + 1} {municipio} {anio}: {e}")
            time.sleep(10 * (intento + 1))
    raise RuntimeError(f"Falló {municipio} {anio}")


tareas = [(m, la, lo, a) for m, (la, lo) in PUNTOS.items() for a in ANIOS]
partes = []
with ThreadPoolExecutor(max_workers=5) as ex:
    futuros = [ex.submit(descargar, *t) for t in tareas]
    for i, f in enumerate(as_completed(futuros), 1):
        partes.append(f.result())
        print(f"{i}/{len(tareas)} listas")

df = pd.concat(partes, ignore_index=True)
df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], format="%Y%m%d%H").dt.strftime("%Y-%m-%d %H:00")
df = df.rename(columns={
    "ALLSKY_SFC_SW_DWN": "radiacion_wh_m2",
    "CLRSKY_SFC_SW_DWN": "radiacion_cielo_despejado_wh_m2",
    "T2M": "temperatura_c",
    "RH2M": "humedad_relativa_pct",
    "WS10M": "velocidad_viento_m_s",
})
df = df.sort_values(["municipio", "fecha_hora"])
df.to_csv("nasa_power_antioquia_horario.csv", index=False)

print("Filas:", len(df))
print(df.head())

try:
    from google.colab import files
    files.download("nasa_power_antioquia_horario.csv")
except ImportError:
    pass
