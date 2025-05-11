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
async function fetchUserProfilePictures(discordId) {
  try {
    const command = new ListObjectsV2Command({
      Bucket: process.env.R2_BUCKET_NAME,
      Prefix: `pfps/${discordId}-`, // List objects starting with pfps/{discordId}-
    });

    const response = await s3Client.send(command);

    const pfps = (response.Contents || [])
      .map(obj => {
        if (!obj.Key) return null;

        const filename = obj.Key.split('/').pop(); // Get the filename e.g., {discordId}-{artistName}.{ext}
        if (!filename) return null;

        const parts = filename.match(new RegExp(`^${discordId}-(.+?)\\.(.+)$`));
        if (!parts || parts.length < 2) {
          console.warn(`Skipping file with unexpected format: ${obj.Key}`);
          return null; 
        }
        
        const artistNameRaw = parts[1]; // Extract artistName
        const artistName = artistNameRaw.replace(/_/g, ' '); // Replace underscores with spaces

        return {
          url: `${process.env.R2_PUBLIC_URL}/${obj.Key}`,
          artistName: artistName || 'Unknown', // Use 'Unknown' if artistName extraction fails
        };
      })
      .filter(pfp => pfp !== null); // Filter out null values

    return pfps;
  } catch (error) {
    console.error('Error fetching profile pictures:', error);
    return [];
  }
}

const initServer = () => {
    
    const PORT = process.env.PORT || 3000;
    const app = express();

    app.use(cors({
        origin: '*',
        methods: ['GET', 'POST', 'PUT', 'DELETE']
    }));
    app.use(express.json());

    app.get('/api/discord', authenticateApiKey, async (req, res) => {
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
                roles: user.roles ? user.roles.split(', ') : []
            }));
            
            cache.data = users;
            cache.timestamp = now;
            
            res.json(users);
        } catch (error) {
            console.error(`Error fetching all users: ${error.message}`);
            res.status(500).json({ error: 'Failed to fetch all users' });
        }
    });

    app.get('/api/discord/pfps/:discordId', authenticateApiKey, async (req, res) => {
        try {
            const { discordId } = req.params;
            
            const profilePictures = await fetchUserProfilePictures(discordId);
            
            if (profilePictures.length === 0) {
                return res.status(404).json({ error: 'No profile pictures found for this user' });
            }
            
            res.json(profilePictures);
        } catch (error) {
            console.error(`Error fetching profile pictures: ${error.message}`);
            res.status(500).json({ error: 'Failed to fetch profile pictures' });
        }
    });

    app.get('/api/discord/:idusername', authenticateApiKey, async (req, res) => {
        try {
            const idusername = req.params.idusername;
            
            const result = await pool.query(`
                SELECT DISTINCT ON (user_id)
                    user_id, 
                    username, 
                    roles
                FROM channel_activity
                WHERE on_server = true
                AND (user_id = $1 OR username = $1)
                ORDER BY user_id, last_message DESC
                LIMIT 1
            `, [idusername]);
            
            if (result.rows.length === 0) {
                return res.status(404).json({ error: 'User not found' });
            }
            
            const user = result.rows[0];
            const userData = {
                discordid: user.user_id,
                username: user.username,
                roles: user.roles ? user.roles.split(', ') : []
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