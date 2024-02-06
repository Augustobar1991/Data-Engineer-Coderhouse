# Proyecto de Obtención del Clima alrededor del mundo

Este proyecto utiliza Python y Amazon Redshift para obtener y procesar los diferentes climas en el mundo. Proporciona una solución automatizada que te permite obtener de manera regular y confiable los datos actuales del clima alrededor del mundo.

## Descripción

El objetivo principal de este proyecto es brindar una herramienta que facilite el seguimiento y análisis del clima alrededor del mundo. Los datos se almacenan en una base de datos en Amazon Redshift.

## Tecnologías utilizadas

Este proyecto utiliza las siguientes tecnologías:

- [Python](https://www.python.org/): Lenguaje de programación utilizado para implementar las diferentes funcionalidades del proyecto.
- [Amazon Redshift](https://aws.amazon.com/redshift/): Servicio de almacenamiento y análisis de datos en la nube utilizado para almacenar y consultar el clima alrededor del mundo.

## Bibliotecas utilizadas

Este proyecto hace uso de las siguientes bibliotecas de Python:

- `requests`: Utilizada para realizar solicitudes de datos a fuentes externas.
- `psycopg2`: Utilizada para la conexión y manipulación de la base de datos en Amazon Redshift.
- `pandas`: Utilizada para el procesamiento y análisis de datos.
- `smtplib`: Utilizada para el envío de correos electrónicos.

## Funcionalidades principales

- **Obtención de datos**: El proyecto realiza solicitudes a fuentes confiables del clima, obteniendo los datos más recientes.
- **Almacenamiento en Amazon Redshift**: los datos del clima se almacenan en una base de datos en Amazon Redshift, lo que permite un acceso rápido y eficiente a los datos.
