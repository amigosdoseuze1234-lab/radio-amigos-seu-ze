#!/bin/bash
set -e

echo "🎙️ RÁDIO AMIGOS DO SEU ZÉ — Instalação"

if ! command -v node &> /dev/null; then
  echo "❌ Node.js não encontrado. Instale em https://nodejs.org"
  exit 1
fi

echo "📦 Instalando dependências..."
npm install

if [ ! -f .env ]; then
  cp .env.example .env
  echo "✅ Arquivo .env criado"
fi

mkdir -p logs audio

echo "✅ Instalação concluída!"
echo "Execute: npm start"
