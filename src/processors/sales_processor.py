import pandas as pd
import pytz

def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes in-course tables, removes and renames columns, adds restaurant_key.
    (Función auxiliar 'privada' para este módulo)
    """
    #df = df[(df['attributes.saleState'] != 'IN-COURSE') & (df['attributes.saleState'] != 'PENDING')]
    
    columns_to_delete = [
        'type', 
        'attributes.customerName',
        'attributes.anonymousCustomer', 'attributes.anonymousCustomer.name',
        'attributes.expectedPayments',
        'relationships.customer.data', 'relationships.items.data',
        'relationships.payments.data', 
        'relationships.table.data', 'relationships.table.data.type',
        'relationships.waiter.data', 'relationships.waiter.data.type', 
        'relationships.saleIdentifier.data', 'relationships.table.data',
        'relationships.customer.data.type', 'attributes.customerName'
    ]
    df = df.drop(columns=columns_to_delete, errors='ignore')

    new_column_names = {
        'id': 'sales_key',
        'attributes.comment': 'comments',
        'attributes.people': 'party_size',
        'attributes.total': 'total_sale',
        'attributes.saleType': 'sale_type',
        'attributes.saleState': 'sale_state',
        'relationships.discounts.data': 'discounts_data',
        'relationships.tips.data': 'tips_data',
        'relationships.shippingCosts.data': 'shipping_costs_data',
        'relationships.table.data.id': 'table_key',
        'relationships.waiter.data.id': 'employee_key',
        'relationships.customer.data.id': 'customer_key'
    }
    df = df.rename(columns=new_column_names)

    df['restaurant_key'] = 1
    return df

def _process_date(df: pd.DataFrame, source_column: str, prefix: str) -> pd.DataFrame:
    """
    Converts a datetime column to local timezone and extracts date/time keys.
    (Función auxiliar 'privada' para este módulo)
    """
    if source_column in df.columns:
        df[source_column] = pd.to_datetime(df[source_column], errors='coerce', utc=True)
        argentina_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        df[source_column] = df[source_column].dt.tz_convert(argentina_timezone)

        df[f'{prefix}_date_key'] = df[source_column].dt.strftime('%Y%m%d').astype('Int64')
        df[f'{prefix}_time_key'] = (df[source_column].dt.hour * 60 + df[source_column].dt.minute).astype('Int64')

        df = df.drop(columns=[source_column])
    
    return df

def enforce_fact_sales_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica el esquema de tipos de datos correcto a un DataFrame de ventas.

    Maneja correctamente los valores nulos para enteros y convierte
    la columna de fecha a un tipo de dato de fecha nativo.

    Args:
        df: El DataFrame de pandas de entrada.

    Returns:
        Un nuevo DataFrame con los tipos de datos corregidos.
    """
    # Usamos tipos que soportan nulos como 'Int64' (con I mayúscula).
    schema_types = {
        'sales_key': 'int64',
        'comments': 'string',
        'party_size': 'Int64',
        'total_sale': 'float64',
        'sale_type': 'string',
        'sale_state': 'string',
        'discounts_data': 'string',
        'tips_data': 'string',
        'shipping_costs_data': 'string',
        'table_key': 'Int64',
        'employee_key': 'Int64',
        'customer_key': 'Int64',
        'restaurant_key': 'Int64',
        'start_date_key': 'Int64',
        'start_time_key': 'Int64',
        'closed_date_key': 'Int64',
        'closed_time_key': 'Int64',
    }
    
    # Itera sobre el diccionario y aplica los tipos de forma segura
    for col, dtype in schema_types.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except (ValueError, TypeError) as e:
                print(f"No se pudo convertir la columna '{col}' a {dtype}. Error: {e}")

    # Maneja la columna de fecha de forma especial para asegurar la conversión
    if 'date' in df.columns:
        # errors='coerce' convierte fechas inválidas en NaT (Not a Time)
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date

    return df

def process(df: pd.DataFrame) -> pd.DataFrame:
    """
    Punto de entrada principal para procesar el DataFrame de fact_sales.
    Esta es la función que será llamada por el orquestador (main.py).
    
    Args:
        df (pd.DataFrame): El DataFrame crudo leído desde GCS.

    Returns:
        pd.DataFrame: El DataFrame limpio y procesado.
    """
    df_clean = _clean_data(df)
    df_with_dates = _process_date(df_clean, 'attributes.createdAt', 'start')
    df_final = _process_date(df_with_dates, 'attributes.closedAt', 'closed')
    df_final = enforce_fact_sales_schema(df_final)
    
    return df_final