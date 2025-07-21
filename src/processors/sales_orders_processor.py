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