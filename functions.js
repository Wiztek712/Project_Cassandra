const cassandra = require('cassandra-driver');

function createClient(){
    const client = new cassandra.Client({
        contactPoints: ['127.0.0.1'],
        localDataCenter: 'datacenter1'
    });
}

// Fonction pour créer le keyspace
async function createKeyspace() {
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
async function createTable() {
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
async function insertMeasurement(station_name, stationId, latitude, longitude, temperature, humidity) {
    const query = `INSERT INTO weather_data_ks.weather_station_data (station_name, station_id, timestamp, latitude, longitude, temperature, humidity)
                   VALUES (?, ?, toTimestamp(now()), ?, ?, ?, ?)`;
    try {
        await client.execute(query, [station_name, stationId, latitude, longitude, temperature, humidity], { prepare: true });
        console.log('Measurement inserted successfully');
    } catch (error) {
        console.error('Error inserting measurement:', error);
    }
}

// Fonction pour récupérer les mesures d'une station météo donnée
async function getMeasurementsByStation(stationId) {
    const query = `SELECT * FROM weather_data_ks.weather_station_data WHERE station_id = ?`;
    try {
        const result = await client.execute(query, [stationId], { prepare: true });
        console.log('Measurements:', result.rows);
    } catch (error) {
        console.error('Error retrieving measurements:', error);
    }
}

// Fonction pour récupérer les mesures d'une station météo pour une plage de temps donnée
async function getMeasurementsByStationAndTimeRange(stationId, startTime, endTime) {
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

async function displayIds() {
    const query = 'SELECT station_id, station_name FROM stations';

    client.execute(query)
    .then(result => {
        // Récupérer et afficher les résultats
        result.rows.forEach(row => {
        console.log(`Station ID: ${row.station_id}, Station Name: ${row.station_name}`);
        });
    });
}

module.export = {getMeasurementsByStation, getMeasurementsByStationAndTimeRange, createKeyspace, createTable, insertMeasurement, displayIds, createClient};