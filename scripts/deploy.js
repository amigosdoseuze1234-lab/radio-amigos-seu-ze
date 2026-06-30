#!/usr/bin/env node
/**
 * Script de Deploy - Rádio Amigos do Seu Zé
 */
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const args = process.argv.slice(2);
const command = args[0] || 'help';

function log(msg) {
  console.log(`[DEPLOY] ${msg}`);
}

function checkCommand(cmd) {
  try {
    execSync(`which ${cmd}`, { stdio: 'ignore' });
    return true;
  } catch {
    return false;
  }
}

switch (command) {
  case 'pm2':
    if (!checkCommand('pm2')) {
      log('❌ PM2 não instalado. Instale com: npm install -g pm2');
      process.exit(1);
    }

    const pm2Config = {
      apps: [
        {
          name: 'radio-server',
          script: './backend/server.js',
          instances: 1,
          exec_mode: 'fork',
          env: { NODE_ENV: 'production', PORT: 3000 },
          log_file: './logs/server.log',
          error_file: './logs/server-error.log',
          out_file: './logs/server-out.log',
          merge_logs: true,
          max_restarts: 10,
          min_uptime: '10s',
          watch: false,
          autorestart: true
        },
        {
          name: 'radio-broadcaster',
          script: './backend/broadcaster.js',
          instances: 1,
          exec_mode: 'fork',
          env: { NODE_ENV: 'production' },
          log_file: './logs/broadcaster.log',
          error_file: './logs/broadcaster-error.log',
          out_file: './logs/broadcaster-out.log',
          merge_logs: true,
          max_restarts: 10,
          min_uptime: '10s',
          watch: false,
          autorestart: true
        }
      ]
    };

    fs.writeFileSync('ecosystem.config.js', `module.exports = ${JSON.stringify(pm2Config, null, 2)};`);

    log('🚀 Iniciando com PM2...');
    execSync('pm2 start ecosystem.config.js', { stdio: 'inherit' });
    execSync('pm2 save', { stdio: 'inherit' });
    log('✅ Deploy com PM2 concluído!');
    break;

  case 'docker':
    if (!checkCommand('docker')) {
      log('❌ Docker não instalado');
      process.exit(1);
    }
    log('🐳 Iniciando com Docker...');
    execSync('docker-compose up -d --build', { stdio: 'inherit' });
    log('✅ Deploy com Docker concluído!');
    break;

  default:
    log('🎧 Rádio Amigos do Seu Zé - Deploy Script');
    log('Uso: node scripts/deploy.js <comando>');
    log('Comandos: pm2, docker');
}
