FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    ca-certificates \
    gnupg \
    apt-transport-https \
    libc-ares2 \
    libcrypto++8 \
    libmediainfo0v5 \
    libzen0v5 \
    libtinyxml2-9 \
    libpcre2-8-0 \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    wget -O /tmp/megacmd.deb https://mega.nz/linux/repo/Debian_12/amd64/megacmd-Debian_12_amd64.deb; \
    apt-get update; \
    apt-get install -y /tmp/megacmd.deb || apt-get install -f -y; \
    rm -f /tmp/megacmd.deb; \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

COPY . /app

CMD ["python", "bot.py"]
