
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependencias del sistema necesarias para compilar algunas libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Actualizar pip, setuptools y wheel ANTES de instalar paquetes
RUN pip install --upgrade pip setuptools wheel

# Instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Verificar que el módulo telegram quedó instalado
RUN python -c "from telegram import Update; print('✅ telegram OK')"

# Copiar código fuente
COPY main.py .

EXPOSE 8080

CMD ["python", "main.py"]
