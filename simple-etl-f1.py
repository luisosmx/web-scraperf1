import functions_framework


import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas_gbq
from google.cloud import bigquery


# GET request to URL:
url = "https://www.formula1.com/en/results.html/2022/drivers.html"
response = requests.get(url)

def data_extraction(url: str) -> pd.DataFrame:

  response = requests.get(url)

  # Analizar el contenido HTML usando BeautifulSoup
  soup = BeautifulSoup(response.content, "html.parser")
  # Encontrar la tabla de resultados para los conductores
  table = soup.find("table", attrs={"class": "resultsarchive-table"})

  # Extraer los datos de la tabla en un DataFrame de pandas
  df = pd.read_html(str(table))[0]
  return df

def data_tranformation(df: pd.DataFrame) -> pd.DataFrame:
  # Seleccionar sólo las columnas necesarias
  df = df[["Pos", "Driver", "Nationality", "Car", "PTS"]]

  # Crear una nueva columna "Acronym" con los últimos 3 caracteres de la columna "Driver"
  df["Acronym"] = df["Driver"].str.slice(-3)

  # Quitar los últimos 3 caracteres de la columna "Driver"
  df["Driver"] = df["Driver"].str.slice(stop=-3)


  now = datetime.now()

  epoch_time = int(now.timestamp())
    
  df["Date"] = epoch_time

  new_name = {"Pos": "Position","Car": "Team", "PTS": "Points"}
  df = df.rename(columns=new_name)

  # Convertir la columna "PTS" a un tipo de datos de cadena
  df["Points"] = df["Points"].astype(str)
  df["Date"] = df["Date"].astype(str)
  df["Position"] = df["Position"].astype(str)
  return df


def load_data(df: pd.DataFrame) -> str:
  try:

    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "airflow-gke-381100.data_f1.results_drivers"


    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("Position", bigquery.enums.SqlTypeNames.STRING)
            
        ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    return "OK"
  except Exception as e:
    print("########################")
    print(str(e))
    print("########################")
    return "Error"


@functions_framework.http
def main(request):
  raw_data = data_extraction(url)
  transformed_data = data_tranformation(raw_data)
  result = load_data(transformed_data)
  print(result)
  return result
