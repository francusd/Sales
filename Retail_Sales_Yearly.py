import pyodbc
import pandas as pd
import datetime as dt 
import configparser


try:
 QUERY = """
           select            
            [AVC].[FECHA],
            YEAR([AVC].[FECHA])ANIO_FECHA,
            MONTH([AVC].[FECHA])MES_FECHA,
            CONCAT( YEAR([AVC].[FECHA]), MONTH([AVC].[FECHA]))AS PERIODO,
            [A].[CODDEPARTAMENTO],
            [A].[DEPARTAMENTO],
            [A].[CODSECCION],
            [A].[SECCION],
            [A].[CODFAMILIA],
            [A].[FAMILIA],
            [A].[CODSUBFAMILIA],
            [A].[SUBFAMILIA],
            [AVL].[CODALMACEN],
            Sum([AVL].[UNIDADESTOTAL]) as [UNIDADES],
            Sum([AVL].[UNIDADESTOTAL] * ([AVL].[PRECIO] - ([AVL].[PRECIO] * ([AVL].[DTO] / 100)) - ([AVL].[PRECIO] * ([AVC].[DTOCOMERCIAL] / 100)))) as [VENTA],
            Sum([AVL].[UNIDADESTOTAL] * [AVL].[COSTE]) as [COSTO],
            Sum([AVL].[UNIDADESTOTAL] * ([AVL].[PRECIO] - ([AVL].[PRECIO] * ([AVL].[DTO] / 100)) - ([AVL].[PRECIO] * ([AVC].[DTOCOMERCIAL] / 100)))) - Sum([AVL].[UNIDADESTOTAL] * [AVL].[COSTE]) as [BENEFICIO],
            (Sum([AVL].[UNIDADESTOTAL] * ([AVL].[PRECIO] - ([AVL].[PRECIO] * ([AVL].[DTO] / 100)) - ([AVL].[PRECIO] * ([AVC].[DTOCOMERCIAL] / 100)))) - Sum([AVL].[UNIDADESTOTAL] * [AVL].[COSTE])) / Sum([AVL].[UNIDADESTOTAL] * ([AVL].[PRECIO] - ([AVL].[PRECIO] * ([AVL].[DTO] / 100)) - ([AVL].[PRECIO] * ([AVC].[DTOCOMERCIAL] / 100)))) as [MARGEN]
        from
            [BD1].[dbo].[ALBVENTACAB] [AVC] With(NoLock)
        left join [BD1].[dbo].[ALBVENTALIN] [AVL] With(NoLock) on
            [AVC].[NUMSERIE] = [AVL].[NUMSERIE]
            and [AVC].[NUMALBARAN] = [AVL].[NUMALBARAN]
            and [AVC].[N] = [AVL].[N]
        left join [BD1].[dbo].[Z_ICG_ARTICULOSMAESTROS] [A] With(NoLock) on
            [A].[CODARTICULO] = [AVL].[CODARTICULO]
        where
            YEAR([AVC].[FECHA])='2025'
            --MONTH([AVC].[FECHA])
            and [AVC].[TIQUET] = 'T'
            and [AVC].[TIPODOC] = 13
        group by
            [AVC].[FECHA],
            YEAR([AVC].[FECHA]),
            MONTH([AVC].[FECHA]),
            CONCAT( YEAR([AVC].[FECHA]), MONTH([AVC].[FECHA])),
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
            Sum([AVL].[TOTAL]) <> 0
    """
 
 config = configparser.ConfigParser()
 config.read('C:\DevProjects\Codes\config.ini')
 icg_sales=config["ventas_icg"]

 #connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
 
 connection_string = f"DRIVER={icg_sales['DRIVER']};SERVER={icg_sales['server']};DATABASE={icg_sales['database']};UID={icg_sales['username']};PWD={icg_sales['password']}"
 conn = pyodbc.connect(connection_string)
 chunks = pd.read_sql(QUERY, conn)
 chunks.to_parquet(
            r'C:\DevProjects\DATASETS\Retail_Yearly_Sales_2025.parquet',
            engine="pyarrow",
            compression="snappy",
            index=False
    )

except pyodbc.Error as e:
    print("Database error:", e)
    #conn.close()
except pyodbc.DataError as e:
    print("Database error:", e)
    #conn.close()
except pyodbc.DatabaseError as e:
    print("Database error:", e)
    #conn.close()
finally:
    if 'conn' in locals():
        conn.close()
