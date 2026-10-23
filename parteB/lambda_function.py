import json
import time
from decimal import Decimal

import boto3

dynamodb = boto3.resource("dynamodb")
tabla_ok = dynamodb.Table("eventos_radiacion")
tabla_descartados = dynamodb.Table("eventos_descartados")

# Esquema del evento: campo -> (tipo, mínimo, máximo)
CAMPOS_NUMERICOS = {
    "radiacion_wh_m2":      (0, 1400),
    "temperatura_c":        (-10, 50),
    "humedad_relativa_pct": (0, 100),
    "velocidad_viento_m_s": (0, 60),
}
CAMPOS_TEXTO = ["event_id", "municipio", "fecha_hora"]
CAMPO_ENVIO = "sent_at"  # milisegundos desde epoch, puesto por el publicador


def es_numero(valor):
    return isinstance(valor, (int, float)) and not isinstance(valor, bool)


def validar(evento):
    """Devuelve None si el evento es válido, o el motivo del descarte."""
    if not isinstance(evento, dict):
        return "JSON no es un objeto"

    faltantes = [c for c in CAMPOS_TEXTO + list(CAMPOS_NUMERICOS) + [CAMPO_ENVIO]
                 if c not in evento or evento[c] is None or evento[c] == ""]
    if faltantes:
        return "Falta campo: " + ",".join(faltantes)

    for c in CAMPOS_TEXTO:
        if not isinstance(evento[c], str):
            return "Tipo inválido: " + c
    for c in list(CAMPOS_NUMERICOS) + [CAMPO_ENVIO]:
        if not es_numero(evento[c]):
            return "Tipo inválido: " + c

    for c, (minimo, maximo) in CAMPOS_NUMERICOS.items():
        if evento[c] < minimo or evento[c] > maximo:
            return "Fuera de rango: " + c

    return None


def a_decimal(valor):
    return Decimal(str(valor))


def lambda_handler(event, context):
    guardados = 0
    descartados = 0

    with tabla_ok.batch_writer(overwrite_by_pkeys=["event_id"]) as escritor_ok, \
         tabla_descartados.batch_writer(overwrite_by_pkeys=["event_id"]) as escritor_desc:

        for registro in event["Records"]:
            llegada_ms = int(time.time() * 1000)
            cuerpo = registro.get("body", "")

            try:
                evento = json.loads(cuerpo)
                motivo = validar(evento)
            except (json.JSONDecodeError, TypeError):
                evento = None
                motivo = "JSON inválido"

            if motivo is None:
                escritor_ok.put_item(Item={
                    "event_id":             evento["event_id"],
                    "municipio":            evento["municipio"],
                    "fecha_hora":           evento["fecha_hora"],
                    "radiacion_wh_m2":      a_decimal(evento["radiacion_wh_m2"]),
                    "temperatura_c":        a_decimal(evento["temperatura_c"]),
                    "humedad_relativa_pct": a_decimal(evento["humedad_relativa_pct"]),
                    "velocidad_viento_m_s": a_decimal(evento["velocidad_viento_m_s"]),
                    "sent_at":              int(evento["sent_at"]),
                    "llegada_ms":           llegada_ms,
                    "latencia_ms":          llegada_ms - int(evento["sent_at"]),
                })
                guardados += 1
            else:
                # Si el evento no trae event_id válido, se usa el id del mensaje de SQS
                event_id = None
                if isinstance(evento, dict) and isinstance(evento.get("event_id"), str) and evento["event_id"]:
                    event_id = evento["event_id"]
                if event_id is None:
                    event_id = "sqs-" + registro["messageId"]

                item = {
                    "event_id":   event_id,
                    "motivo":     motivo.split(":")[0],
                    "detalle":    motivo,
                    "cuerpo":     cuerpo[:1000],
                    "llegada_ms": llegada_ms,
                }
                if isinstance(evento, dict) and es_numero(evento.get(CAMPO_ENVIO)):
                    item["latencia_ms"] = llegada_ms - int(evento[CAMPO_ENVIO])
                escritor_desc.put_item(Item=item)
                descartados += 1

    print(json.dumps({"guardados": guardados, "descartados": descartados}))
    return {"guardados": guardados, "descartados": descartados}
