# Spark Apps

Esta sección del repositorio contiene aplicaciones de **Apache Spark desarrolladas con PySpark**, ejecutadas mediante scripts de Bash. El objetivo es practicar la estructura, configuración y ejecución de aplicaciones Spark independientes.

Las instrucciones siguientes están orientadas a **Windows con Git Bash** y a un entorno de ejecución **local**. Cada aplicación tiene su propio directorio y puede documentar sus particularidades en un `README.md` independiente.

## ESTRUCTURA

```text
spark_apps/
├── README.md
└── hello_spark/
    ├── README.md
    ├── app_config.py
    ├── sparksession.py
    ├── hello_spark.py
    └── run_app.sh
```

En cada aplicación:

| Archivo | Responsabilidad |
|---|---|
| `app_config.py` | Define el nombre de la aplicación (`APP_NAME`) y, cuando sea necesario, otros parámetros propios de ella. |
| `sparksession.py` | Crea la `SparkSession` y establece el nombre mediante `APP_NAME`. |
| `<nombre_app>.py` | Contiene la lógica de procesamiento y utiliza la sesión creada en `sparksession.py`. |
| `run_app.sh` | Prepara el entorno y ejecuta el archivo principal con `spark-submit` en modo local. |
| `README.md` | Documenta los detalles particulares de esa aplicación. |

La carpeta `hello_spark/` contiene la **primera aplicación de validación**: permite comprobar que Java, Python, PySpark y el mecanismo de ejecución funcionan correctamente. 

## REQUISITOS

| Componente | Versión de referencia | Uso |
|---|---|---|
| Windows | 11 | Sistema operativo del laboratorio |
| Git Bash | Git for Windows | Terminal para ejecutar scripts `.sh` |
| Python | 3.11.9 | Intérprete de PySpark |
| Java | 8 | Máquina virtual de Java requerida por Spark |
| PySpark | 3.5.7 | API Python y distribución de Spark instalada mediante `pip` |

Estas versiones corresponden al entorno validado. Otras combinaciones deben comprobarse por compatibilidad. Para estas prácticas locales **no se requiere WSL, Hadoop ni una instalación independiente de Apache Spark**.

## PREPARACION INICIAL DESDE GIT BASH

Abre Git Bash en el directorio raíz del repositorio clonado. Todos los comandos siguientes se ejecutan desde allí; no se necesita una ubicación específica en el equipo.

### 1. Comprobar Python

Verifica que Python esté instalado y disponible desde **Git Bash**:

```bash
python --version
```

El comando debe mostrar la versión de Python instalada, por ejemplo:

```text
Python 3.11.9
```

Para este entorno se recomienda utilizar **Python 3.11**, compatible con la versión de PySpark utilizada en el proyecto.

Si Python no está instalado, realiza la instalación siguiendo los procedimientos permitidos en tu equipo.

### 2. Comprobar Java

Apache Spark requiere Java para ejecutar sus componentes internos. Verifica que esté instalado y disponible desde **Git Bash**:

```bash
java -version
```

Para este entorno se utiliza **Java 8**, compatible con PySpark 3.5.7.

#### Identificar la ruta de instalación de Java

Antes de configurar `JAVA_HOME`, identifica la ubicación del entorno de ejecución Java mediante el siguiente comando:

```bash
java -XshowSettings:properties -version 2>&1 | grep 'java.home ='
```

Este comando consulta las propiedades de Java y muestra el directorio del entorno de ejecución utilizado.

**Ejemplo de salida:**

```text
java.home = C:\Program Files\Java\jre1.8.0_503
```

Utiliza la ruta obtenida para configurar `JAVA_HOME` dentro del script de ejecución `.sh`, sustituyendo el siguiente valor por la ubicación correspondiente a tu equipo:

```bash
export JAVA_HOME="C:/Program Files/Java/jre1.8.0_503"
export PATH="$JAVA_HOME/bin:$PATH"
```

**Nota:** la ruta de instalación puede variar entre equipos. Si Java no está instalado o el comando no devuelve una ubicación válida, será necesario verificar su instalación antes de continuar.

### 3. Crear el entorno virtual

Desde la raíz del repositorio:

```bash
python -m venv .venv
source .venv/Scripts/activate
```

### 4. Instalar y verificar PySpark

```bash
python -m pip install --upgrade pip
python -m pip install pyspark==3.5.7
python -c "import pyspark; print(pyspark.__version__)"
```

Al terminar la preparación, puedes salir del entorno virtual:

```bash
deactivate
```

El directorio `.venv` permanece en la raíz del repositorio. No es necesario activarlo manualmente antes de ejecutar una aplicación, ya que su `run_app.sh` lo hace automáticamente.

## ESTRUCTURA DEL CODIGO EN CADA APP

### `app_config.py`: configuración de la aplicación

Aquí se define el nombre de la aplicación, que será utilizado al crear la sesión Spark:

```python
APP_NAME = "NombreAplicacion"
```
También pueden añadirse parámetros propios del procesamiento, cuando se necesiten. 

### `sparksession.py`: creación de la sesión

```python
from pyspark.sql import SparkSession
from app_config import APP_NAME

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .getOrCreate()
)
```

La sesión se crea al importar el módulo.

### `<nombre_app>.py`: procesamiento

El archivo principal importa la sesión y contiene la lógica de la aplicación:

```python
from sparksession import spark

# Lógica de procesamiento de la aplicación
print(f"Aplicación: {spark.sparkContext.appName}")

# Cerrar la sesión al finalizar
spark.stop()
```

Este es únicamente un patrón general. Cada aplicación documentará su procesamiento en su propio README.

## CONFIGURACION DE LOS SCRIPTS DE EJECUCIÓN (.SH)

Cada aplicación dentro de `spark_apps/` dispone de un script `run_app.sh`, encargado de preparar el entorno y ejecutar la aplicación mediante `spark-submit`.

El script está diseñado para utilizarse desde **Git Bash en Windows**, sin necesidad de configurar manualmente las variables de entorno antes de cada ejecución.

Su estructura se divide en cuatro secciones principales:

### 1. Directorios

Esta sección identifica automáticamente la ubicación de la aplicación, la raíz del repositorio y el entorno virtual de Python.

```bash
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$APP_DIR/../.." && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
```

- `APP_DIR`: directorio donde se encuentra el script de ejecución.
- `PROJECT_DIR`: directorio raíz del repositorio, ubicado dos niveles por encima del script.
- `VENV_DIR`: ubicación del entorno virtual `.venv`, compartido por las aplicaciones del repositorio.

El uso de rutas relativas permite ejecutar las aplicaciones independientemente de dónde se haya clonado el repositorio, siempre que se conserve su estructura de directorios.

### 2. Configuración del entorno

Esta sección configura las dependencias necesarias para ejecutar PySpark.

**Java**

```bash
export JAVA_HOME="C:/Program Files/Java/jre1.8.0_503"
export PATH="$JAVA_HOME/bin:$PATH"
```

- `JAVA_HOME`: indica la ubicación de la instalación de Java utilizada por Spark.
- `PATH`: incorpora el directorio de ejecutables de Java para que pueda localizarse desde la terminal.

**Importante:** cada usuario debe ajustar `JAVA_HOME` según su instalación de Java.

**Entorno virtual de Python**

```bash
source "$VENV_DIR/Scripts/activate"
```

Activa el entorno virtual `.venv`, donde se encuentran instalados Python y las dependencias del proyecto, incluido PySpark.

**Configuración de PySpark**

```bash
export PYSPARK_PYTHON="$(cygpath -w "$VENV_DIR/Scripts/python.exe")"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
```

- `PYSPARK_PYTHON`: define el intérprete de Python que utilizarán los procesos Python de PySpark.
- `PYSPARK_DRIVER_PYTHON`: define el intérprete de Python utilizado por el proceso Python del Driver cuando corresponda.

La función `cygpath -w` transforma rutas de Git Bash a rutas de Windows para los ejecutables nativos.

**Ubicación de Spark**

```bash
export SPARK_HOME="$(cygpath -w "$VENV_DIR/Lib/site-packages/pyspark")"
```

`SPARK_HOME` identifica el directorio de instalación de Spark. En este proyecto, Spark se encuentra dentro del paquete PySpark instalado mediante `pip` en el entorno virtual.


### 3. Configuración de la aplicación

Esta sección define los parámetros específicos de la aplicación y los archivos necesarios para ejecutarla.

```bash
MASTER="local[2]"

SPARK_SUBMIT="$VENV_DIR/Scripts/spark-submit.cmd"
APP_FILE="$(cygpath -w "$APP_DIR/<nombre_app>.py")" # Sustituir <nombre_app> por el archivo Python principal

```

- `MASTER`: determina dónde se ejecutará Spark. `local[2]` utiliza dos hilos de ejecución en la computadora local.
- `SPARK_SUBMIT`: ubicación del lanzador de Spark instalado en el entorno virtual.
- `APP_FILE`: ruta del archivo Python que contiene la aplicación.

### 4. Ejecución

Esta sección inicia la aplicación utilizando `spark-submit`.

```bash
echo "=================================="
echo "Iniciando aplicación Spark"
echo "Master: $MASTER"
echo "=================================="

export MSYS2_ARG_CONV_EXCL="*"

"$SPARK_SUBMIT" \
    --master "$MASTER" \
    "$APP_FILE"

echo "=================================="
echo "Aplicación finalizada"
echo "=================================="
```

- `set -e`: hace que el script se detenga si un comando devuelve un estado de error; por ello, el mensaje final solo aparece cuando `spark-submit` termina correctamente.
- `MSYS2_ARG_CONV_EXCL="*"`: Evita que Git Bash convierta automáticamente los argumentos y rutas enviados al ejecutable de Windows. Esto permite conservar los parámetros esperados por el lanzador `spark-submit.cmd`.
- `--master`: especifica el entorno de ejecución de Spark.
- `APP_FILE`: identifica el archivo Python que Spark debe ejecutar.

## EJECUTAR UNA APLICACION

Desde la **raíz del repositorio**:

```bash
bash spark_apps/<nombre_directorio>/run_app.sh
```

Sustituye `<nombre_directorio>` por la carpeta que contiene la aplicación. Por ejemplo, para validar la configuración inicial:

```bash
bash spark_apps/hello_spark/run_app.sh
```

> `local[2]` ejecuta Spark en una sola computadora; **no representa un clúster YARN**. Las configuraciones para clústeres deben documentarse por separado.

## Problemas frecuentes

| Situación | Qué revisar |
|---|---|
| `python: command not found` | Instalación de Python y disponibilidad en Git Bash. |
| Error de Java o JVM | Versión de Java, `JAVA_HOME` y `PATH`. |
| `ModuleNotFoundError: pyspark` | Instalación de PySpark dentro de `.venv`. |
| Errores de rutas Bash/Windows | Uso de `cygpath` y ejecución del lanzador `.cmd`. |
| Advertencias de `winutils.exe` o bibliotecas nativas Hadoop | Pueden aparecer en Windows sin impedir pruebas locales básicas; si una operación falla, revisa el error concreto. |
| Mensajes de `BlockManager` | Son parte normal de la inicialización y administración de bloques de Spark. |
| Nombre inesperado de la aplicación | Revisa `APP_NAME` en `app_config.py` y su uso en `sparksession.py`. |

## Convenciones para nuevas aplicaciones

Cada aplicación debe residir en su propia carpeta dentro de `spark_apps/` y mantener la separación entre `app_config.py`, `sparksession.py`, el archivo principal `<nombre_app>.py`, `run.sh` y su README.

Al reutilizar la plantilla de `run_app.sh`, ajusta **`JAVA_HOME`** a tu equipo y **`APP_FILE`** al archivo principal correspondiente. Modifica `MASTER` únicamente si necesitas otro nivel de paralelismo local. Evita rutas absolutas de perfiles de usuario.

Este README documenta exclusivamente los requisitos y convenciones compartidos de `spark_apps/`; la lógica de cada aplicación se describe en su propio README.
