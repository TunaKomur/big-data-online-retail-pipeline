#!/bin/bash
# JupyterLab'i proje kök dizininden başlatır
# Kullanım: ./scripts/start_jupyter.sh

set -e

# Proje kök dizinine git
cd "$(dirname "$0")/.."

# Sanal ortam aktif mi kontrol et
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "⚠️  Sanal ortam aktif değil. Önce şunu çalıştır:"
    echo "    source .venv/bin/activate"
    exit 1
fi

# JAR'lar var mı kontrol et
if [ ! -d "jars" ] || [ -z "$(ls -A jars/*.jar 2>/dev/null)" ]; then
    echo "⚠️  jars/ klasörü boş veya yok. Önce çalıştır:"
    echo "    ./scripts/download_jars.sh"
    exit 1
fi

echo "🚀 JupyterLab başlatılıyor..."
jupyter lab --notebook-dir=. --no-browser