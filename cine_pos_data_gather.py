import boto3
import json
from botocore.exceptions import ClientError
import pandas as pd

s3 = boto3.client('s3')

def lambda_handler(event, context):
    source_bucket = 'cinexideralposjoan'
    target_bucket = 'cinexideralposjoan-analisis'

    consolidated_analysis = []

    try:
        # Lista de objetos del bucket fuente
        response = s3.list_objects_v2(Bucket=source_bucket)

        # Iterar sobre cada archivo JSON en el bucket
        for obj in response.get('Contents', []):
            key = obj['Key']

            if key.endswith('.json'): 
                file_obj = s3.get_object(Bucket=source_bucket, Key=key)
                file_content = file_obj['Body'].read().decode('utf-8')

                try:
                    json_data = json.loads(file_content)

                    # Analizar las características del JSON
                    analysis = analyze_json(key, json_data)

                    # Añadir el análisis a la lista consolidada
                    consolidated_analysis.append(analysis)
                    print(consolidated_analysis)

                except json.JSONDecodeError:
                    print(f"El archivo {key} no es un JSON válido.")

        # Crear un archivo consolidado y guardarlo en el bucket de destino
        output_key = 'consolidated_analysis.json'
        save_analysis_to_s3(target_bucket, output_key, consolidated_analysis)

        return {
            'statusCode': 200,
            'body': f'Análisis consolidado guardado en {target_bucket}/{output_key}'
        }

    except ClientError as e:
        print(f"Error al procesar los archivos: {e}")
        return {
            'statusCode': 500,
            'body': f'Error al procesar los archivos: {e}'
        }

def analyze_json(key, json_data):
    df = pd.json_normalize(json_data)
    rows, cols = df.shape

    null_columns = df.columns[df.isnull().any()].tolist()

    # Guardar los resultados en un diccionario
    result_dict = {
        "ID" : key.split('-')[2].split('.')[0],
        "Num_rows": rows,
        "Num_columns": cols,
        "Null_columns": null_columns
    }

    return result_dict

def save_analysis_to_s3(bucket, key, data):
    """
    Guarda el análisis consolidado en el bucket destino.
    """
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=4),
        ContentType='application/json'
    )

