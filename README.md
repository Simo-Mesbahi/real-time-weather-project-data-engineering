FROM apache/spark:3.3.1

# Installer les dépendances Python
RUN pip install requests pyspark
