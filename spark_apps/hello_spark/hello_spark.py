from sparksession import spark

# Datos de ejemplo
data = [
    (1, "Ana", 25),
    (2, "Luis", 30),
    (3, "Carlos", 28)
]

columns = ["id", "nombre", "edad"]

# Crear DataFrame
df = spark.createDataFrame(data, columns)

# Información de la aplicación
print(f"Aplicación: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")

# Mostrar resultados
df.show()

# Finalizar sesión
spark.stop()