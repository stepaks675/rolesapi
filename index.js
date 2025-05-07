import pg from 'pg';
import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';

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

const initServer = () => {
    const PORT = process.env.PORT || 3000;
    const app = express();

    app.use(cors({
        origin: '*',
        methods: ['GET', 'POST', 'PUT', 'DELETE']
    }));
    app.use(express.json());

    app.get('/api/discord', async (req, res) => {
        try {
            const now = Date.now();
            if (cache.data && cache.timestamp && (now - cache.timestamp < cache.cacheTime)) {
                console.log('Returning cached data');
                return res.json(cache.data);
            }

            const result = await pool.query(`
                SELECT DISTINCT ON (user_id)
                    user_id, 
                    username, 
                    roles
                FROM channel_activity
                WHERE on_server = true
                ORDER BY user_id, last_message DESC
            `);
            
            const users = result.rows.map(user => ({
                discordid: user.user_id,
                username: user.username,
                roles: user.roles
            }));
            
            cache.data = users;
            cache.timestamp = now;
            
            res.json(users);
        } catch (error) {
            console.error(`Error fetching all users: ${error.message}`);
            res.status(500).json({ error: 'Failed to fetch all users' });
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
