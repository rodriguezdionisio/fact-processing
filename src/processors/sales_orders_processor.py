import pandas as pd
import pytz

def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes and renames columns, adds unit_price.

    """
    
    columns_to_delete = [
        'type', 'relationships.product.data.type', 
        'relationships.priceList.data', 'relationships.priceList.data.type',
        'relationships.sale.data.type'
    ]
    df = df.drop(columns=columns_to_delete, errors='ignore')

    new_column_names = {
        'id': 'order_key',
        'attributes.canceled': 'canceled',
        'attributes.cancellationComment': 'cancellation_comment',
        'attributes.comment': 'comments',
        'attributes.createdAt': 'created_at',
        'attributes.price': 'total_price',
        'attributes.quantity': 'quantity_ordered',
        'attributes.status': 'status',
        'attributes.paid': 'paid',
        'relationships.product.data.id': 'product_key',
        'relationships.subitems.data': 'subitems_data',
        'relationships.priceList.data.id': 'price_list_key',
        'relationships.sale.data.id': 'sales_key'
    }

    df = df.rename(columns=new_column_names)

    df['unit_price'] = df['total_price'] / df['quantity_ordered']

    return df

def _process_date(df: pd.DataFrame, source_column: str, prefix: str) -> pd.DataFrame:
    """
    Converts a datetime column to local timezone and extracts date/time keys.

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
        'order_key': 'int64',
        'canceled': 'string',
        'cancellation_comment': 'string',
        'comments': 'string',
        'total_price': 'float64',
        'quantity_ordered': 'Int64',
        'status': 'string',
        'paid': 'string',
        'product_key': 'Int64',
        'subitems_data': 'string',
        'sales_key': 'Int64',
        'unit_price': 'float64',
        'created_date_key': 'Int64',
        'created_time_key': 'Int64',
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
    df_final = _process_date(df_clean, 'created_at', 'created')
    
    return df_final