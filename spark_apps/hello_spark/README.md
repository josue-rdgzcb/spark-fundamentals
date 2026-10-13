# Hello Spark

## Descripción

`hello_spark` es la **aplicación inicial de validación** del directorio `spark_apps/`. Su objetivo es comprobar que el entorno local de Apache Spark con PySpark está correctamente configurado y que es posible ejecutar una aplicación desde **Git Bash en Windows** mediante `spark-submit`.

La aplicación crea una `SparkSession`, construye un DataFrame pequeño en memoria y muestra su contenido. **No está diseñada para procesar datos productivos ni para evaluar rendimiento.**

## Objetivos de validación

Al ejecutarla correctamente, se comprueba que:

- Java está disponible para iniciar los componentes de Spark.
- El entorno virtual de Python y PySpark están disponibles.
- El script `run.sh` puede lanzar `spark-submit` desde Git Bash.
- La configuración de `app_config.py` se utiliza al crear la `SparkSession`.
- Spark puede crear un DataFrame y ejecutar una acción (`show()`) en modo local.
- La aplicación puede finalizar y cerrar la sesión de Spark.

## Estructura

```text
hello_spark/
├── README.md
├── app_config.py
├── sparksession.py
├── hello_spark.py
└── run.sh
```

| Archivo | Responsabilidad |
| --- | --- |
| `app_config.py` | Define el nombre de la aplicación (`APP_NAME`). |
| `sparksession.py` | Crea la `SparkSession` usando la configuración de la aplicación. |
| `hello_spark.py` | Crea un DataFrame de prueba, muestra información de la sesión y ejecuta `show()`. |
| `run.sh` | Configura el entorno local y ejecuta `hello_spark.py` con `spark-submit`. |

## Requisitos previos

Antes de ejecutar la aplicación, sigue la guía general de [`spark_apps/README.md`](../README.md) para preparar el entorno. La configuración de referencia utiliza:

- Windows 11 y Git Bash.
- Python 3.11 y un entorno virtual `.venv` en la raíz del repositorio.
- Java 8.
- PySpark 3.5.7.

**Importante:** revisa `JAVA_HOME` en `run.sh` y ajústalo a la instalación de Java de tu equipo. El script configura el entorno para su propia ejecución; no es necesario activar `.venv` manualmente antes de ejecutarlo.

## Ejecución

Abre Git Bash en la **raíz del repositorio** y ejecuta:

```bash
bash spark_apps/hello_spark/run_app.sh
```

El script utiliza `spark-submit` con `--master local[2]`, por lo que la aplicación se ejecuta en la computadora local con dos hilos de trabajo. No requiere un clúster YARN.

El nombre de la aplicación se define en `app_config.py` y se aplica desde `sparksession.py` mediante `SparkSession.builder.appName(...)`.

## Resultado esperado

La aplicación debe iniciar Spark, mostrar el mensaje de inicio de la apliación, y presentar un DataFrame similar al siguiente:

```text
==================================
Iniciando Aplicación
==================================

+---+------+----+
| id|nombre|edad|
+---+------+----+
|  1|   Ana|  25|
|  2|  Luis|  30|
|  3|Carlos|  28|
+---+------+----+
```

Al finalizar correctamente, el script muestra:

```text
==================================
Aplicación finalizada
==================================
```

El orden y formato de los mensajes de Spark pueden variar según el entorno.

## Interpretación de la validación

Si se muestra el DataFrame y el proceso termina sin errores, se ha validado el **flujo básico de ejecución local**: Git Bash → `run.sh` → `spark-submit` → `hello_spark.py` → `SparkSession` → DataFrame.

Esto **no garantiza** que funcionen todas las funcionalidades de Spark, el acceso a sistemas externos, la ejecución distribuida en YARN o las configuraciones de producción.

### Mensajes frecuentes en Windows

- **`Did not find winutils.exe`**: advertencia asociada a componentes de Hadoop en Windows. Puede aparecer sin impedir esta prueba básica.
- **`Unable to load native-hadoop library`**: Spark puede continuar utilizando implementaciones Java cuando corresponda.
- **`BlockManager` / `MemoryStore`**: mensajes normales de inicialización del almacenamiento interno de Spark.

Si la aplicación falla, revisa el error específico y consulta la sección de problemas frecuentes del README general de `spark_apps/`.

## Alcance

`hello_spark` se mantiene como una **prueba mínima de configuración**. Las aplicaciones posteriores incorporarán transformaciones, lecturas de datos, configuraciones de recursos y otros componentes de Spark de manera independiente.