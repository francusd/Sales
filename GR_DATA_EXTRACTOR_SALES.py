import pyodbc
import pandas as pd
import datetime as dt
import configparser
import time
from pathlib import Path

start = time.perf_counter()

try:
    QUERY = """
        select         
           CAST([AVC].[FECHA] AS DATE) as [FECHA] ,
            [AVL].[CODALMACEN],            
            [A].[CODDEPARTAMENTO],
            [A].[DEPARTAMENTO],
            [A].[CODSECCION],
            [A].[SECCION],
            [A].[CODFAMILIA],
            [A].[FAMILIA],
            [A].[CODSUBFAMILIA],
            [A].[SUBFAMILIA],
            Sum([AVL].[UNIDADESTOTAL]) as [UNIDADES],
            Sum([AVL].[UNIDADESTOTAL] * 
                ([AVL].[PRECIO] 
                - ([AVL].[PRECIO] * ([AVL].[DTO] / 100)) 
                - ([AVL].[PRECIO] * ([AVC].[DTOCOMERCIAL] / 100)))
            ) as [VENTA],
            Sum([AVL].[UNIDADESTOTAL] * [AVL].[COSTE]) as [COSTO]
        from
            [BD1].[dbo].[ALBVENTACAB] [AVC] With(NoLock)
        left join [BD1].[dbo].[ALBVENTALIN] [AVL] With(NoLock)
            on [AVC].[NUMSERIE] = [AVL].[NUMSERIE]
            and [AVC].[NUMALBARAN] = [AVL].[NUMALBARAN]
            and [AVC].[N] = [AVL].[N]
        left join [BD1].[dbo].[Z_ICG_ARTICULOSMAESTROS] [A] With(NoLock)
            on [A].[CODARTICULO] = [AVL].[CODARTICULO]
        where
            [AVC].[FECHA] >= '20240101'
            AND [AVC].[FECHA]< '20250101'
            and [AVC].[TIQUET] = 'T'
            and [AVC].[TIPODOC] = 13
        group by
           CAST([AVC].[FECHA] AS DATE),
            [A].[CODDEPARTAMENTO],
            [A].[DEPARTAMENTO],
            [A].[CODSECCION],
            [A].[SECCION],
            [A].[CODFAMILIA],
            [A].[FAMILIA],
            [A].[CODSUBFAMILIA],
            [A].[SUBFAMILIA],
            [AVL].[CODALMACEN]
        having
            Sum([AVL].[UNIDADESTOTAL]) <> 0
    """

    # =========================
    # CONFIG
    # =========================
    config = configparser.ConfigParser()
    config.read(r"C:\DevProjects\Codes\config.ini")
    icg_sales = config["ventas_icg"]

    connection_string = (
        f"DRIVER={icg_sales['DRIVER']};"
        f"SERVER={icg_sales['server']};"
        f"DATABASE={icg_sales['database']};"
        f"UID={icg_sales['username']};"
        f"PWD={icg_sales['password']}"
    )

    # =========================
    # DB CONNECTION
    # =========================
    conn = pyodbc.connect(connection_string)
    df = pd.read_sql(QUERY, conn)

    # =========================
    # DATE HANDLING
    # =========================
    df["FECHA"] = pd.to_datetime(df["FECHA"])
    df["YEAR_MONTH"] = df["FECHA"].dt.to_period("M")

    # =========================
    # OUTPUT FOLDER
    # =========================
    output_dir = Path(r"C:\DevProjects\DATASETS\VENTAS_POR_MES")
    output_dir.mkdir(parents=True, exist_ok=True)

    # =================================
    # EXPORT PARQUET FILE PER MONTH
    # =================================
    bloques=200_000 
    for i, chunk in enumerate(
        pd.read_sql(QUERY, conn, chunksize=bloques), start=1
    ):

        # ---- DATE HANDLING ----
        chunk["FECHA"] = pd.to_datetime(chunk["FECHA"])
        chunk["YEAR_MONTH"] = chunk["FECHA"].dt.to_period("M")

        # ---- EXPORT PER MONTH ----
        for period, df_month in chunk.groupby("YEAR_MONTH"):

            file_path = output_dir / f"VENTAS_ICG_{period}.parquet"

            #df_month = df_month.drop(columns=["YEAR_MONTH"])

            if file_path.exists():
                df_month.to_parquet(
                    file_path,
                    engine="pyarrow",
                    compression="snappy",
                    append=True
                )
            else:
                df_month.to_parquet(
                    file_path,
                    engine="pyarrow",
                    compression="snappy"
                )

        print(f"✔ Chunk {i} procesado ({len(chunk):,} filas)")
        print(f"📊 Archivo generado: {file_path}")

except pyodbc.Error as e:
    print("Database error:", e)

finally:
    if "conn" in locals():
        conn.close()

end = time.perf_counter()
print(f"⏱ Ejecutado en {end - start:.2f} segundos ({(end - start)/60:.2f} minutos)")
