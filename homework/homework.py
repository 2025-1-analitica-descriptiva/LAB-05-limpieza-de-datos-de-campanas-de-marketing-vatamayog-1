"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

# Importar las librerías
import os
import zipfile
import pandas as pd
from io import TextIOWrapper

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    # Rutas
    input_dir = 'files/input'
    output_dir = 'files/output'
    os.makedirs(output_dir, exist_ok=True)

    # Listas para insertar los datos
    client_rows = []
    campaing_rows = []
    economic_rows = []

    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }



    for file in os.listdir(input_dir):
        if file.endswith('.zip'):
            with zipfile.ZipFile(os.path.join(input_dir, file)) as archive:
                for csv_file in archive.namelist():
                    with archive.open(csv_file) as f:
                        df = pd.read_csv(f, encoding='utf-8')

                        # Client
                        client = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
                        client['job'] = client['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
                        client['education'] = client['education'].str.replace('.', '_', regex=False)
                        client['education'] = client['education'].replace('unknown', pd.NA)
                        client['credit_default'] = (client['credit_default'] == 'yes').astype(int)
                        client['mortgage'] = (client['mortgage'] == 'yes').astype(int)
                        client_rows.append(client)

                        #Campaign
                        campaign = df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts',
                                       'previous_outcome', 'campaign_outcome', 'day', 'month']].copy()
                        campaign['previous_outcome'] = (campaign['previous_outcome'] == 'success').astype(int)
                        campaign['campaign_outcome'] = (campaign['campaign_outcome'] == 'yes').astype(int)
                        campaign['last_contact_date'] = (
                            '2022-' +
                            campaign['month'].str.lower().map(month_map) + '-' +
                            campaign['day'].astype(str).str.zfill(2)
                        )
                        campaign = campaign[['client_id', 'number_contacts', 'contact_duration',
                                             'previous_campaign_contacts', 'previous_outcome',
                                             'campaign_outcome', 'last_contact_date']]
                        campaing_rows.append(campaign)

                        # ECONOMICS
                        economics = df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
                        economic_rows.append(economics)

    return (
            pd.concat(client_rows).drop_duplicates().to_csv(os.path.join(output_dir, 'client.csv'), index=False),
            pd.concat(campaing_rows).drop_duplicates().to_csv(os.path.join(output_dir, 'campaign.csv'), index=False),
            pd.concat(economic_rows).drop_duplicates().to_csv(os.path.join(output_dir, 'economics.csv'), index=False)
    )





if __name__ == "__main__":
    clean_campaign_data()