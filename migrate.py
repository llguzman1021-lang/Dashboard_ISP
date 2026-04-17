import os
import gspread
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Conexión a Google Sheets
gc = gspread.service_account_from_dict(eval(os.environ['GSHEET_CRED_JSON']))
spreadsheet = gc.open("Dashboard_ISP")

# Conexión a Neon
conn = psycopg2.connect(os.environ['NEON_DSN'])
conn.autocommit = False

def parse_date(s):
    try:
        return datetime.strptime(s, "%d/%m/%Y").date()
    except:
        return None

def parse_time(s):
    if s in ["N/A", "", None]:
        return None
    try:
        return datetime.strptime(s, "%H:%M:%S").time()
    except:
        return None

def normalize_row(r, ws_title, row_idx):
    return {
        "worksheet_name": ws_title,
        "gsheet_id": row_idx,

        "zona": r.get("Zona"),
        "servicio": r.get("Servicio"),
        "categoria": r.get("Categoria"),
        "equipo_afectado": r.get("Equipo_Afectado"),

        "fecha_inicio": parse_date(r.get("Fecha_Inicio")),
        "hora_inicio": parse_time(r.get("Hora_Inicio")),
        "fecha_fin": parse_date(r.get("Fecha_Fin")),
        "hora_fin": parse_time(r.get("Hora_Fin")),

        "clientes_afectados": int(r.get("Clientes_Afectados") or 0),
        "causa_raiz": r.get("Causa_Raiz"),
        "descripcion": r.get("Descripcion"),
        "duracion_horas": float(r.get("Duracion_Horas") or 0),
        "conocimiento_tiempos": r.get("Conocimiento_Tiempos")
    }

def batch_insert(rows):
    sql = """
    INSERT INTO incidents (
      worksheet_name, gsheet_id, zona, servicio, categoria, equipo_afectado,
      fecha_inicio, hora_inicio, fecha_fin, hora_fin,
      clientes_afectados, causa_raiz, descripcion,
      duracion_horas, conocimiento_tiempos
    )
    VALUES %s
    ON CONFLICT (worksheet_name, gsheet_id) DO UPDATE SET
      zona = EXCLUDED.zona,
      servicio = EXCLUDED.servicio,
      categoria = EXCLUDED.categoria,
      equipo_afectado = EXCLUDED.equipo_afectado,
      fecha_inicio = EXCLUDED.fecha_inicio,
      hora_inicio = EXCLUDED.hora_inicio,
      fecha_fin = EXCLUDED.fecha_fin,
      hora_fin = EXCLUDED.hora_fin,
      clientes_afectados = EXCLUDED.clientes_afectados,
      causa_raiz = EXCLUDED.causa_raiz,
      descripcion = EXCLUDED.descripcion,
      duracion_horas = EXCLUDED.duracion_horas,
      conocimiento_tiempos = EXCLUDED.conocimiento_tiempos,
      fecha_actualizacion = now();
    """

    vals = [
        (
            r["worksheet_name"], r["gsheet_id"], r["zona"], r["servicio"], r["categoria"],
            r["equipo_afectado"], r["fecha_inicio"], r["hora_inicio"], r["fecha_fin"],
            r["hora_fin"], r["clientes_afectados"], r["causa_raiz"], r["descripcion"],
            r["duracion_horas"], r["conocimiento_tiempos"]
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, vals, page_size=500)
    conn.commit()

def migrate():
    buffer = []
    for ws in spreadsheet.worksheets():
        records = ws.get_all_records()
        row_idx = 2
        for r in records:
            buffer.append(normalize_row(r, ws.title, row_idx))
            row_idx += 1

            if len(buffer) >= 500:
                batch_insert(buffer)
                buffer = []

    if buffer:
        batch_insert(buffer)

if __name__ == "__main__":
    migrate()
    conn.close()
