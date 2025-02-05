# AWS Lambda - Análisis y Procesamiento de Archivos JSON en S3

## Descripción del Proyecto
Este repositorio contiene dos funciones **Lambda** diseñadas para procesar y analizar archivos JSON almacenados en **Amazon S3**. Estas funciones extraen información relevante de los archivos y generan reportes consolidados en un bucket de destino.

---

## Tecnologías Utilizadas
- **AWS Lambda** (para el procesamiento de archivos)
- **Amazon S3** (almacenamiento de archivos JSON y de los análisis generados)
- **Boto3** (para la manipulación de objetos en S3)
- **Pandas** (para análisis de datos en los JSON)

---

## Descripción de las Lambdas

### 1️⃣ **Lambda "lambda_function.py" - Análisis de Archivos JSON de Películas**
Esta función Lambda recibe un archivo JSON con información de películas, lo analiza y extrae detalles clave como:
- **Título**
- **ID de la película**
- **Imagen principal**
- **Fecha de lanzamiento y país**
- **Resumen de la trama**

Posteriormente, se genera un reporte con el número de filas, columnas y columnas con valores nulos. Este análisis se guarda en un bucket de destino en S3.

### 2️⃣ **Lambda "cine_pos_data_gather.py" - Consolidación de Análisis de JSON**
Esta función Lambda lista y procesa múltiples archivos JSON dentro de un bucket de origen en S3. Para cada archivo:
- Extrae información clave de su estructura.
- Cuenta filas y columnas.
- Identifica columnas con valores nulos.
- Consolida los resultados en un archivo JSON único.

Finalmente, el análisis consolidado se almacena en un bucket de destino en S3.

---
