import json
import boto3
import pandas as pd
from io import StringIO
import os
s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = event["bucket"]
    object_key = event["object"]
    target_bucket = event["target_bucket"]

    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')

        data = json.loads(content)

        movies = data['movies']
        df = pd.DataFrame([{
            "Title": movie['titleText']['text'],
            "ID": movie['id'],
            "Image_URL": movie['primaryImage']['url'] if 'primaryImage' in movie else None,
            "Release_Date": f"{movie['releaseDate']['day']}-{movie['releaseDate']['month']}-{movie['releaseDate']['year']}" if 'releaseDate' in movie else None,
            "Country": movie['releaseDate']['country']['text'] if 'releaseDate' in movie and 'country' in movie['releaseDate'] else None,
            "Plot": movie['plot']['plotText']['plainText'] if 'plot' in movie else None
        } for movie in movies])


        num_filas, num_columnas = df.shape

        columnas_nulas = df.columns[df.isnull().any()].tolist()

        response = {
            "Rows": str(num_filas),
            "Columns": str(num_columnas),
            "Nan_columns": str(columnas_nulas)
        }

        
        s3.put_object(Bucket=target_bucket, Key=object_key, Body=json.dumps(response))


        return {
            'statusCode': 200,
            'body': json.dumps(response)
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }
