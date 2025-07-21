import pandas as pd
import pytz

def _clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes in-course tables, removes and renames columns, adds restaurant_key.
    (Función auxiliar 'privada' para este módulo)
    """
    df = df[(df['attributes.saleState'] != 'IN-COURSE') & (df['attributes.saleState'] != 'PENDING')]
    
    columns_to_delete = [
        'type', 'attributes.anonymousCustomer_name', 'attributes.expectedPayments',
        'relationships.customer.data', 'relationships.items.data',
        'relationships.payments.data', 'relationships.table.data.type',
        'relationships.waiter.data.type', 'relationships.saleIdentifier.data',
        'relationships.waiter.data', 'relationships.table.data',
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
    
    # Aquí puedes agregar cualquier otro paso de transformación específico para 'sales'
    
    return df_final