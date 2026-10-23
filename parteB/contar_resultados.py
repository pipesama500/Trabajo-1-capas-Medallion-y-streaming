import csv
from collections import Counter

import boto3

REGION = "us-east-2"
dynamodb = boto3.resource("dynamodb", region_name=REGION)


def escanear(nombre_tabla, campos):
    tabla = dynamodb.Table(nombre_tabla)
    items = []
    params = {"ProjectionExpression": ", ".join(f"#c{i}" for i in range(len(campos))),
              "ExpressionAttributeNames": {f"#c{i}": c for i, c in enumerate(campos)}}
    while True:
        r = tabla.scan(**params)
        items.extend(r["Items"])
        if "LastEvaluatedKey" not in r:
            break
        params["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    return items


with open("publicados.csv", encoding="utf-8") as f:
    publicados = list(csv.DictReader(f))

guardados = escanear("eventos_radiacion", ["event_id", "latencia_ms"])
descartados = escanear("eventos_descartados", ["event_id", "motivo"])

n_pub, n_ok, n_desc = len(publicados), len(guardados), len(descartados)

print("=========== CUADRE DE EVENTOS ===========")
print(f"Publicados en IoT Core:        {n_pub}")
print(f"Guardados en DynamoDB:         {n_ok}")
print(f"Descartados:                   {n_desc}")
print(f"Guardados + descartados:       {n_ok + n_desc}")
print(f"¿Cuadra?                       {'SÍ' if n_pub == n_ok + n_desc else 'NO'}")

print("\nDescartados por motivo:")
for motivo, cantidad in Counter(d["motivo"] for d in descartados).most_common():
    print(f"  {motivo}: {cantidad}")

print("\nEsperado según el publicador:")
for tipo, cantidad in Counter(p["tipo_esperado"] for p in publicados).most_common():
    print(f"  {tipo}: {cantidad}")

latencias = [int(g["latencia_ms"]) for g in guardados]
if latencias:
    print("\n=========== RETRASO ENVÍO -> DYNAMODB ===========")
    print(f"Promedio: {sum(latencias) / len(latencias):,.0f} ms")
    print(f"Máximo:   {max(latencias):,.0f} ms")
    print(f"Mínimo:   {min(latencias):,.0f} ms")

if n_pub != n_ok + n_desc:
    print("\nSi no cuadra, espera 1-2 minutos (puede haber mensajes en la cola) y vuelve a correr.")
