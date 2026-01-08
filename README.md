# Spark ETL – DVD Rental Analytics Pipeline

## English Version

This project implements a complete ETL pipeline using **Apache Spark** to process data from a transactional PostgreSQL database and transform it into an analytical model ready for consumption by Business Intelligence tools such as Power BI.

The main goal is to simulate a real-world Data Engineering environment, applying best practices in data architecture, data quality, dimensional modeling, and layered data processing.

## Technologies Used

The pipeline is built primarily with **Python and PySpark**, using **Apache Spark 3.x** as the distributed processing engine.
The source data comes from a **PostgreSQL** database (DVD Rental) accessed via JDBC.

Data storage is handled using **Parquet** format, organized by data layers (`bronze`, `silver`, `gold`).
The project runs locally on **WSL2 (Linux)**, is version-controlled with **Git**, and the final datasets are consumed using **Power BI**.

## Project Architecture

The project follows a **Medallion Architecture** pattern, clearly separating responsibilities across layers:

* **Bronze**: raw data ingestion
* **Silver**: data cleaning, normalization, and quality control
* **Gold**: business-ready analytical datasets

This structure improves traceability, scalability, and maintainability.

## Bronze Layer

The Bronze layer is responsible for the **full extraction** of data from PostgreSQL.
No business logic or complex validations are applied at this stage.

Data is stored exactly as it arrives from the source, preserving the original schema and enabling auditing or full reprocessing if needed.

Output example:

```
data/dev/bronze/film
```

## Silver Layer

The Silver layer transforms raw data into clean, reliable, and consistent datasets.

At this stage, the pipeline performs:

* Data type normalization
* String cleaning
* Duplicate removal
* Business rule validations
* Invalid value handling
* Data quality checks
* Addition of technical and audit metadata

This layer represents the **trusted source** for analytical processing.

Output example:

```
data/dev/silver/film
```

## Gold Layer – Star Schema

The Gold layer implements a **star schema** optimized for analytical queries.

It includes:

* Dimension tables (such as `dim_film`, `dim_rating`)
* A fact table (`fact_film`)

The model is designed for efficient querying and seamless integration with BI tools.

Output example:

```
data/dev/gold/star
```

## Gold Layer – Analytics and Marts

On top of the star schema, **analytical marts** are created to serve specific business use cases.

An example included in this project:

* `film_overview`: an aggregated dataset by rating, ready for reporting and dashboards.

These marts are designed to be consumed directly by analysts without additional transformations.

Output example:

```
data/dev/gold/analytics/marts
```

## Pipeline Orchestration

The pipeline is executed sequentially using Bash scripts:

1. Bronze ingestion
2. Silver transformations
3. Gold layer generation (dimensions, fact table, and marts)

This setup simulates a production-ready workflow and can be easily migrated to orchestration tools such as Apache Airflow or Azure Data Factory.

## Power BI Integration

The Gold datasets are directly connected to **Power BI**, where relationships between dimensions and fact tables are modeled.

This allows:

* Dashboard creation
* KPI analysis
* Realistic BI consumption scenarios

Power BI is used strictly as a consumption layer, with all data logic handled in Spark.

## Project Purpose

This project demonstrates:

* End-to-end ETL pipeline design
* Apache Spark and PySpark proficiency
* Data quality enforcement
* Dimensional modeling
* BI-ready dataset creation
* Practical Data Engineering workflows

It is intended as both a learning project and a portfolio-ready example of real-world Data Engineering practices.

---

## Versión en Español

Este proyecto implementa un pipeline ETL completo utilizando **Apache Spark** para procesar datos provenientes de una base transaccional PostgreSQL y transformarlos en un modelo analítico listo para ser consumido por herramientas de Business Intelligence como Power BI.

El objetivo principal es simular un entorno real de Data Engineering, aplicando buenas prácticas de arquitectura de datos, control de calidad, modelado dimensional y procesamiento por capas.

## Tecnologías Utilizadas

El pipeline está desarrollado principalmente con **Python y PySpark**, utilizando **Apache Spark 3.x** como motor de procesamiento distribuido.
Los datos de origen provienen de una base **PostgreSQL** (DVD Rental) accedida mediante JDBC.

El almacenamiento de datos se realiza en formato **Parquet**, organizado por capas (`bronze`, `silver`, `gold`).
El proyecto se ejecuta en entorno local sobre **WSL2 (Linux)**, se versiona con **Git** y los datasets finales se consumen desde **Power BI**.

## Arquitectura del Proyecto

El proyecto sigue el patrón de **Arquitectura Medallón**, separando claramente las responsabilidades de cada capa:

* **Bronze**: ingesta cruda de datos
* **Silver**: limpieza, normalización y control de calidad
* **Gold**: datasets analíticos listos para negocio

Esta estructura mejora la trazabilidad, escalabilidad y mantenibilidad del pipeline.

## Capa Bronze

La capa Bronze se encarga de la **extracción completa** de los datos desde PostgreSQL.
En esta etapa no se aplican reglas de negocio ni validaciones complejas.

Los datos se almacenan tal como llegan desde el sistema origen, preservando el esquema original y permitiendo auditoría o reprocesamiento completo.

Salida de ejemplo:

```
data/dev/bronze/film
```

## Capa Silver

La capa Silver transforma los datos crudos en datasets limpios, confiables y consistentes.

En esta etapa se realizan:

* Normalización de tipos de datos
* Limpieza de strings
* Eliminación de duplicados
* Validaciones de reglas de negocio
* Manejo de valores inválidos
* Controles de calidad
* Agregado de metadata técnica y de auditoría

Esta capa representa la **fuente confiable** para análisis posteriores.

Salida de ejemplo:

```
data/dev/silver/film
```

## Capa Gold – Modelo Estrella

La capa Gold implementa un **modelo dimensional en estrella**, optimizado para consultas analíticas.

Incluye:

* Tablas de dimensiones (como `dim_film`, `dim_rating`)
* Tabla de hechos (`fact_film`)

El modelo está diseñado para una integración eficiente con herramientas de BI.

Salida de ejemplo:

```
data/dev/gold/star
```

## Capa Gold – Analytics y Marts

Sobre el modelo estrella se construyen **marts analíticos**, orientados a casos de negocio específicos.

Un ejemplo incluido en el proyecto:

* `film_overview`: dataset agregado por rating, listo para reporting y dashboards.

Estos marts están pensados para ser consumidos directamente sin transformaciones adicionales.

Salida de ejemplo:

```
data/dev/gold/analytics/marts
```

## Orquestación del Pipeline

El pipeline se ejecuta de forma secuencial mediante scripts Bash:

1. Ingesta Bronze
2. Transformaciones Silver
3. Generación de la capa Gold (dimensiones, fact y marts)

Esta estructura simula un flujo productivo real y puede migrarse fácilmente a herramientas de orquestación como Apache Airflow o Azure Data Factory.

## Integración con Power BI

Los datasets de la capa Gold se conectan directamente a **Power BI**, donde se modelan las relaciones entre dimensiones y tablas de hechos.

Esto permite:

* Crear dashboards
* Analizar KPIs
* Simular escenarios reales de consumo BI

Power BI se utiliza únicamente como capa de consumo, manteniendo toda la lógica de datos en Spark.

## Objetivo del Proyecto

Este proyecto demuestra:

* Diseño de pipelines ETL de punta a punta
* Dominio de Apache Spark y PySpark
* Control de calidad de datos
* Modelado dimensional
* Preparación de datos para BI
* Flujos reales de Data Engineering