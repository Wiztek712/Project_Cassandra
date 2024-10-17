const cassandra = require('cassandra-driver');
const { v4: uuidv4 } = require('uuid');

function createClient(){
    const client = new cassandra.Client({
        contactPoints: ['127.0.0.1'],
        localDataCenter: 'datacenter1'
    });
    return client;
}

// Fonction pour créer le keyspace
async function createKeyspace(client) {
    const query = `
        CREATE KEYSPACE IF NOT EXISTS weather_data_ks 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
    `;
    try {
        await client.execute(query);
        console.log('Keyspace "weather_data_ks" created or already exists.');
    } catch (error) {
        console.error('Error creating keyspace:', error);
    }
}

// Fonction pour créer la table
async function createTable(client) {
    const query = `
        CREATE TABLE IF NOT EXISTS weather_data_ks.weather_station_data (
            station_name TEXT,
            station_id UUID,
            timestamp timestamp,
            latitude float,
            longitude float,
            temperature float,
            humidity float,
            PRIMARY KEY (station_id, timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC);
    `;
    try {
        await client.execute(query);
        console.log('Table "weather_station_data" created or already exists.');
    } catch (error) {
        console.error('Error creating table:', error);
    }
}

// Fonction pour insérer une mesure
async function insertMeasurement(client, station_name, stationId,timestamp, latitude, longitude, temperature, humidity) {
    const query = `INSERT INTO weather_data_ks.weather_station_data (station_name, station_id, timestamp, latitude, longitude, temperature, humidity)
                   VALUES (?, ?, ?, ?, ?, ?, ?)`;
    try {
        await client.execute(query, [station_name, stationId, timestamp, latitude, longitude, temperature, humidity], { prepare: true });
        console.log('Measurement inserted successfully');
    } catch (error) {
        console.error('Error inserting measurement:', error);
    }
}

// Fonction pour récupérer les mesures d'une station météo donnée
async function getMeasurementsByStation(client, stationId) {
    const query = `SELECT * FROM weather_data_ks.weather_station_data WHERE station_id = ?`;
    try {
        const result = await client.execute(query, [stationId], { prepare: true });
        console.log('Measurements:', result.rows);
    } catch (error) {
        console.error('Error retrieving measurements:', error);
    }
}

// Fonction pour récupérer les mesures d'une station météo pour une plage de temps donnée
async function getMeasurementsByStationAndTimeRange(client, stationId, startTime, endTime) {
    const query = `SELECT * FROM weather_data_ks.weather_station_data 
                   WHERE station_id = ? 
                   AND timestamp >= ? 
                   AND timestamp <= ?`;
    try {
        const result = await client.execute(query, [stationId, startTime, endTime], { prepare: true });
        console.log('Measurements:', result.rows);
    } catch (error) {
        console.error('Error retrieving measurements:', error);
    }
}

async function displayIds(client) {
    const distinctQuery = 'SELECT DISTINCT station_id FROM weather_data_ks.weather_station_data';
    const nameQuery = 'SELECT station_name FROM weather_data_ks.weather_station_data WHERE station_id = ? LIMIT 1';

    try {
        // Récupérer tous les station_id distincts
        const distinctResult = await client.execute(distinctQuery);
        
        console.log('ID des stations:');
        
        // Pour chaque station_id, récupérer le station_name correspondant
        for (const row of distinctResult.rows) {
            const stationId = row.station_id;
            const nameResult = await client.execute(nameQuery, [stationId]);
            
            if (nameResult.rows.length > 0) {
                const stationName = nameResult.rows[0].station_name;
                console.log(`ID: ${stationId}, Nom: ${stationName}`);
            }
        }
    } catch (err) {
        console.error('Erreur lors de l\'exécution des requêtes:', err);
    }
}

async function alreadyExist(station_name, client){
    const query = `SELECT station_id, latitude, longitude FROM weather_data_ks.weather_station_data WHERE station_name = ? ALLOW FILTERING`;
    
    const result = await client.execute(query, [station_name], { prepare: true });
    
    if (result.rowLength > 0) {
        // Station exists, return the data
        const station = result.first();
        return {
            station_id: station.station_id,
            latitude: station.latitude,
            longitude: station.longitude
        };
    } else {
        // Station does not exist, create a new one
        
        const stationId = uuidv4();  // Generate a new UUID for the station
        const lat = (Math.random() * 180) - 90;
        const lon = (Math.random() * 360) - 180;

        return {
            station_id: stationId,
            latitude: lat,
            longitude: lon
        };
    }
}

module.exports = {getMeasurementsByStation, getMeasurementsByStationAndTimeRange, createKeyspace, createTable, insertMeasurement, displayIds, createClient, alreadyExist};