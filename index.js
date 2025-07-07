import pg from 'pg';
import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';

dotenv.config();

const { Pool } = pg;

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
});

const cache = {
    data: null,
    timestamp: null,
    cacheTime: 5 * 60 * 1000 
};

const testConnection = async () => {
    const client = await pool.connect();
    try {
        await client.query('SELECT NOW()');
        console.log('Соединение с PostgreSQL установлено');
    } catch (error) {
        console.error('Ошибка при подключении к PostgreSQL:', error);
    } finally {
        client.release();
    }
};

const authenticateApiKey = async (req, res, next) => {
    const apiKey = req.headers['x-api-key'];
    
    if (!apiKey) {
        return res.status(401).json({ error: 'API key is required' });
    }
    
    try {
        const result = await pool.query('SELECT apikey FROM apikeys WHERE apikey = $1', [apiKey]);
        
        if (result.rows.length === 0) {
            return res.status(401).json({ error: 'Invalid API key' });
        }
        
        next();
    } catch (error) {
        console.error(`API key validation error: ${error.message}`);
        res.status(500).json({ error: 'Authentication error' });
    }
};

const s3Client = new S3Client({
    region: 'auto',
    endpoint: process.env.R2_ENDPOINT,
    credentials: {
      accessKeyId: process.env.R2_ACCESS_KEY_ID,
      secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
    },
});

/**
 * @typedef {Object} ProfilePicture
 * @property {string} url - URL of the profile picture
 * @property {string} artistName - Name of the artist
 */

/**
 * Fetch user profile pictures from S3/R2 storage
 * @param {string} discordId - Discord ID of the user
 * @returns {Promise<Array<ProfilePicture>>} Array of profile pictures
 */

const initServer = () => {
    
    const PORT = process.env.PORT || 3000;
    const app = express();

    app.use(cors({
        origin: '*',
        methods: ['GET', 'POST', 'PUT', 'DELETE']
    }));
    app.use(express.json());

app.get('/api/twitter/roles', authenticateApiKey, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT DISTINCT xhandle 
            FROM maptable 
            WHERE on_server = true 
              AND xhandle IS NOT NULL 
              AND (
                roles ILIKE '%PROVED UR LUV%' OR
                roles ILIKE '%PROVEDURLUV(KOL)%'
              )
          `);
        res.json(result.rows.map(row => row.xhandle));
    } catch (error) {
        console.error('Error fetching roles:', error);
        res.status(500).json({ error: 'Failed to fetch roles' });
    }
});

app.get('/api/discord', authenticateApiKey, async (req, res) => {
        try {
            const now = Date.now();
            if (cache.data && cache.timestamp && (now - cache.timestamp < cache.cacheTime)) {
                console.log('Returning cached data');
                return res.json(cache.data);
            }

            const result = await pool.query(`
                SELECT DISTINCT ON (discordid)
                    discordid, 
                    discordusername, 
                    roles,
                    pfp
                FROM maptable
                WHERE on_server = true
                ORDER BY discordid
            `);
            
            const users = result.rows.map(user => ({
                discordid: user.discordid,
                username: user.discordusername,
                roles: user.roles ? user.roles.split(', ') : [],
                pfp: user.pfp
            }));
            
            cache.data = users;
            cache.timestamp = now;
            
            res.json(users);
        } catch (error) {
            console.error(`Error fetching all users: ${error.message}`);
            res.status(500).json({ error: 'Failed to fetch all users' });
        }
    });

app.get('/api/twitter/core', authenticateApiKey, async (req, res) => {
        try {
            const minMessages = parseInt(req.query.msg, 10) || 500;

            // Получаем дату 7 дней назад
            const sevenDaysAgo = new Date();
            sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
            const today = new Date();

            // Находим пользователей с достаточным количеством сообщений за последние 7 дней
            const usersRes = await pool.query(`
                SELECT userid, SUM(messages) AS total_messages
                FROM messages_data
                WHERE date BETWEEN $1 AND $2
                GROUP BY userid
                HAVING SUM(messages) > $3
            `, [sevenDaysAgo.toISOString().split('T')[0], today.toISOString().split('T')[0], minMessages]);

            if (usersRes.rows.length === 0) {
                return res.json([]);
            }

            const validUserIds = usersRes.rows.map(r => r.userid);

            // Получаем Twitter handles для этих пользователей
            const xhandlesRes = await pool.query(
                `SELECT xhandle FROM maptable WHERE discordid = ANY($1) AND xhandle IS NOT NULL`,
                [validUserIds]
            );
            const coreSet = new Set(xhandlesRes.rows.map(r => r.xhandle).filter(Boolean));

            res.json(Array.from(coreSet));
        } catch (error) {
            console.error('Error in /api/twitter/core:', error);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

app.get('/api/core/discord', authenticateApiKey, async (req, res) => {
        try {
            const minMessages = parseInt(req.query.msg, 10) || 500;

            // Получаем дату 7 дней назад
            const sevenDaysAgo = new Date();
            sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
            const today = new Date();

            // Находим пользователей с достаточным количеством сообщений за последние 7 дней
            const usersRes = await pool.query(`
                SELECT userid, SUM(messages) AS total_messages
                FROM messages_data
                WHERE date BETWEEN $1 AND $2
                GROUP BY userid
                HAVING SUM(messages) > $3
            `, [sevenDaysAgo.toISOString().split('T')[0], today.toISOString().split('T')[0], minMessages]);

            const validUserIds = usersRes.rows.map(r => r.userid);

            res.json(validUserIds);
        } catch (error) {
            console.error('Error in /api/core/discord:', error);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

app.get('/api/discord/:idusername', authenticateApiKey, async (req, res) => {
        try {
            const idusername = req.params.idusername;
            
            const result = await pool.query(`
                SELECT DISTINCT ON (discordid)
                    discordid, 
                    discordusername, 
                    roles,
                    pfp
                FROM maptable
                WHERE on_server = true
                AND (discordid = $1 OR discordusername = $1)
                ORDER BY discordid
                LIMIT 1
            `, [idusername]);
            
            if (result.rows.length === 0) {
                return res.status(404).json({ error: 'User not found' });
            }
            
            const user = result.rows[0];
            const userData = {
                discordid: user.discordid,
                username: user.discordusername,
                roles: user.roles ? user.roles.split(', ') : [],
                pfp: user.pfp
            };
            
            res.json(userData);
        } catch (error) {
            console.error(`Error fetching user by ID/username: ${error.message}`);
            res.status(500).json({ error: 'Failed to fetch user' });
        }
    });

    app.listen(PORT, () => {
        console.log(`Server is running on port ${PORT}`);
        testConnection();
    });

    return app;
};

const app = initServer();

export default app;