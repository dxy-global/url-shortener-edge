const express = require('express');
const redis = require('redis');
const axios = require('axios');
const geoip = require('geoip-lite');
const cron = require('node-cron');
require('dotenv').config();

const app = express();
const CORE_SERVICE_URL = process.env.CORE_SERVICE_URL || 'http://localhost:3000';
const EDGE_HOSTNAME = process.env.EDGE_HOSTNAME || 'localhost';

const redisClient = redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379'
});

redisClient.on('error', (err) => console.error('Redis Client Error', err));

// Redirection Logic
app.get('/:short_path', async (req, res) => {
    const { short_path } = req.params;
    try {
        const original_url = await redisClient.hGet(`paths:${EDGE_HOSTNAME}`, short_path);
        
        if (original_url) {
            const ip = req.ip || req.connection.remoteAddress;
            const geo = geoip.lookup(ip);
            const log = {
                hostname: EDGE_HOSTNAME,
                short_path,
                ip_address: ip,
                user_agent: req.headers['user-agent'],
                country: geo ? geo.country : 'Unknown',
                timestamp: new Date().toISOString()
            };
            
            await redisClient.lPush('access_logs_buffer', JSON.stringify(log));
            return res.redirect(original_url);
        } else {
            return res.status(404).send('URL not found');
        }
    } catch (err) {
        console.error(err);
        res.status(500).send('Internal Server Error');
    }
});

async function syncPaths() {
    try {
        const response = await axios.get(`${CORE_SERVICE_URL}/internal/sync/paths`);
        const paths = response.data;
        const relevantPaths = paths.filter(p => p.hostname === EDGE_HOSTNAME);
        
        if (relevantPaths.length > 0) {
            const hashData = {};
            relevantPaths.forEach(p => { hashData[p.short_path] = p.original_url; });
            await redisClient.del(`paths:${EDGE_HOSTNAME}`);
            await redisClient.hSet(`paths:${EDGE_HOSTNAME}`, hashData);
            console.log(`[Sync] Updated ${relevantPaths.length} paths.`);
        }
    } catch (err) {
        console.error('[Sync] Path sync failed:', err.message);
    }
}

async function syncLogs() {
    try {
        const logs = await redisClient.lRange('access_logs_buffer', 0, -1);
        if (logs.length === 0) return;

        const parsedLogs = logs.map(l => JSON.parse(l));
        const response = await axios.post(`${CORE_SERVICE_URL}/internal/sync/logs`, parsedLogs);
        
        if (response.data.success) {
            await redisClient.lTrim('access_logs_buffer', logs.length, -1);
            console.log(`[Sync] Flushed ${parsedLogs.length} logs.`);
        }
    } catch (err) {
        console.error('[Sync] Log flush failed:', err.message);
    }
}

cron.schedule('* * * * *', () => {
    syncPaths();
    syncLogs();
});

async function start() {
    console.log('Checking dependencies...');
    
    // Check Redis
    try {
        await redisClient.connect();
        console.log('✅ Redis connection: SUCCESS');
    } catch (err) {
        console.error('❌ Redis connection: FAILED');
        console.error(err.message);
        process.exit(1);
    }

    // Check Core Service
    try {
        await axios.get(`${CORE_SERVICE_URL}/internal/sync/paths`);
        console.log('✅ Core Service connection: SUCCESS');
    } catch (err) {
        console.warn('⚠️ Core Service connection: FAILED (Will retry during sync)');
    }

    const PORT = process.env.EDGE_PORT || 4000;
    app.listen(PORT, () => {
        console.log(`🚀 Edge Service running on port ${PORT}`);
        syncPaths();
    });
}

start();
