#!/usr/bin/env bash
set -e

# ==========================================
# 1. DIRECTORIOS
# ==========================================

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$APP_DIR/../.." && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"

# ==========================================
# 2. CONFIGURACIÓN DEL ENTORNO
# ==========================================

# Java 8
export JAVA_HOME="C:/Program Files/Java/jre1.8.0_503"
export PATH="$JAVA_HOME/bin:$PATH"

# Activar entorno virtual
source "$VENV_DIR/Scripts/activate"

# Python de PySpark
export PYSPARK_PYTHON="$(cygpath -w "$VENV_DIR/Scripts/python.exe")"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"

# Spark instalado mediante pip
export SPARK_HOME="$(cygpath -w "$VENV_DIR/Lib/site-packages/pyspark")"

# ==========================================
# 3. CONFIGURACIÓN DE EJECUCIÓN
# ==========================================

MASTER="local[2]"

SPARK_SUBMIT="$VENV_DIR/Scripts/spark-submit.cmd"
APP_FILE="$(cygpath -w "$APP_DIR/hello_spark.py")"

# ==========================================
# 4. EJECUCIÓN
# ==========================================

echo "=================================="
echo "Iniciando Aplicación"
echo "=================================="

# Evitar conversiones automáticas de Git Bash
export MSYS2_ARG_CONV_EXCL="*"

"$SPARK_SUBMIT" \
    --master "$MASTER" \
    "$APP_FILE"

echo "=================================="
echo "Aplicación finalizada"
echo "=================================="