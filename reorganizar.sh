#!/bin/bash

echo "== Reorganizando projeto =="

mkdir -p backend frontend/css frontend/js audio scripts

# Backend
[ -f server.js ] && mv server.js backend/
[ -f broadcaster.js ] && mv broadcaster.js backend/
[ -f streamer.js ] && mv streamer.js backend/

# Frontend
[ -f index.html ] && mv index.html frontend/
[ -f style.css ] && mv style.css frontend/css/
[ -f app.js ] && mv app.js frontend/js/

# Script
[ -f install.sh ] && mv install.sh scripts/

# Áudios
find . -maxdepth 1 -iname "*.mp3" -exec mv {} audio/ \;

# Criar arquivos faltantes
touch backend/server.js
touch backend/broadcaster.js
touch backend/streamer.js
touch frontend/index.html
touch frontend/css/style.css
touch frontend/js/app.js
touch scripts/install.sh
touch .env
touch .env.example
touch README.md

# package.json
if [ ! -f package.json ]; then
cat > package.json <<EOF
{
  "name": "radio-amigos-seu-ze",
  "version": "1.0.0",
  "main": "backend/server.js",
  "scripts": {
    "start": "node backend/server.js"
  }
}
EOF
fi

echo
echo "==============================="
echo "Estrutura reorganizada!"
echo "==============================="

find . -maxdepth 3
