# Arquitetura - Rádio Amigos do Seu Zé

## Componentes

- **Backend**: Node.js + Express + WebSocket
- **Frontend**: HTML5 + CSS3 + JavaScript (PWA)
- **Streaming**: Icecast + FFmpeg
- **Hospedagem**: Render.com (Free Tier)

## Fluxo

1. Mixxx transmite via Icecast
2. Backend serve a página e API
3. Frontend conecta via WebSocket para metadata
4. Player HTML5 toca o stream
