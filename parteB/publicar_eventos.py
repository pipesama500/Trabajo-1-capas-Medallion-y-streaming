import csv
import json
import math
import random
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import boto3

REGION = "us-east-2"
TOPICO = "trabajo1/radiacion"
N_EVENTOS = 10000
HILOS = 20
SEMILLA = 42

MUNICIPIOS = ["Medellin", "Rionegro", "Santa Fe de Antioquia", "Turbo", "Apartado",
              "Caucasia", "Yarumal", "Puerto Berrio", "Andes", "Sonson",
              "Segovia", "Urrao", "Necocli", "Amalfi", "El Bagre"]
CAMPOS_MEDIDA = ["municipio", "fecha_hora", "radiacion_wh_m2", "temperatura_c",
                 "humedad_relativa_pct", "velocidad_viento_m_s"]

random.seed(SEMILLA)

iot = boto3.client("iot", region_name=REGION)
endpoint = iot.describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
iot_data = boto3.client("iot-data", region_name=REGION, endpoint_url=f"https://{endpoint}")


def lectura_valida(i):
    fecha = datetime(2026, 10, 1) + timedelta(hours=i // len(MUNICIPIOS))
    h = fecha.hour
    if 6 <= h <= 18:
        radiacion = 1000 * math.sin(math.pi * (h - 6) / 12) * random.uniform(0.3, 1.0)
    else:
        radiacion = 0.0
    return {
        "event_id": str(uuid.uuid4()),
        "municipio": MUNICIPIOS[i % len(MUNICIPIOS)],
        "fecha_hora": fecha.strftime("%Y-%m-%d %H:00"),
        "radiacion_wh_m2": round(radiacion, 2),
        "temperatura_c": round(random.uniform(15, 32), 2),
        "humedad_relativa_pct": round(random.uniform(50, 98), 2),
        "velocidad_viento_m_s": round(random.uniform(0, 6), 2),
    }


def construir_evento(i):
    """Devuelve (event_id, tipo_esperado, evento_dict o None, payload_roto o None)."""
    ev = lectura_valida(i)
    dado = random.random()

    if dado < 0.020:
        ev.pop(random.choice(CAMPOS_MEDIDA))
        return ev["event_id"], "Falta campo", ev, None
    if dado < 0.030:
        ev[random.choice(["radiacion_wh_m2", "temperatura_c"])] = "abc"
        return ev["event_id"], "Tipo inválido", ev, None
    if dado < 0.040:
        if random.random() < 0.5:
            ev["humedad_relativa_pct"] = 150.0
        else:
            ev["radiacion_wh_m2"] = 5000.0
        return ev["event_id"], "Fuera de rango", ev, None
    if dado < 0.045:
        return ev["event_id"], "JSON inválido", ev, "ROTO"
    return ev["event_id"], "Válido", ev, None


def publicar(evento_id, tipo, ev, roto):
    ev["sent_at"] = int(time.time() * 1000)
    payload = json.dumps(ev)
    if roto:
        payload = payload[: len(payload) // 2]  # JSON cortado a la mitad
    for intento in range(5):
        try:
            iot_data.publish(topic=TOPICO, qos=1, payload=payload.encode("utf-8"))
            return evento_id, tipo, ev["sent_at"]
        except Exception as e:
            time.sleep(0.5 * (intento + 1))
            error = e
    raise RuntimeError(f"No se pudo publicar {evento_id}: {error}")


def main():
    eventos = [construir_evento(i) for i in range(N_EVENTOS)]
    print(f"Publicando {N_EVENTOS} eventos en '{TOPICO}' ({endpoint})...")

    inicio = time.time()
    resultados = []
    with ThreadPoolExecutor(max_workers=HILOS) as ex:
        futuros = [ex.submit(publicar, *e) for e in eventos]
        for n, f in enumerate(as_completed(futuros), 1):
            resultados.append(f.result())
            if n % 1000 == 0:
                print(f"  {n} publicados")
    duracion = time.time() - inicio

    with open("publicados.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_id", "tipo_esperado", "sent_at"])
        w.writerows(resultados)

    conteo = Counter(t for _, t, _ in resultados)
    print(f"\nPublicados: {len(resultados)} en {duracion:.1f} s")
    for tipo, cantidad in conteo.most_common():
        print(f"  {tipo}: {cantidad}")
    print("Detalle guardado en publicados.csv")


if __name__ == "__main__":
    main()
