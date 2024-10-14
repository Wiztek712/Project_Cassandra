const cassandra = require('cassandra-driver');

// Configuration de la connexion
const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'], // Remplacez par l'adresse IP de votre cluster Cassandra
  localDataCenter: 'datacenter1' // Remplacez par le nom de votre datacenter
});

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
async function insertMeasurement(stationId, latitude, longitude, temperature, humidity) {
    const query = `INSERT INTO weather_data_ks.weather_station_data (station_id, timestamp, latitude, longitude, temperature, humidity)
                   VALUES (?, toTimestamp(now()), ?, ?, ?, ?)`;
    try {
        await client.execute(query, [stationId, latitude, longitude, temperature, humidity], { prepare: true });
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

// Fonction principale pour exécuter toutes les étapes
async function main() {
    try {
        // Création du keyspace et de la table
        await createKeyspace();
        await createTable();

        // Exemple d'utilisation
        const stationId = cassandra.types.Uuid.random();
        await insertMeasurement(stationId, 48.8566, 2.3522, 20.5, 60);

        // Récupérer les mesures pour une station
        await getMeasurementsByStation(stationId);

        // Récupérer les mesures pour une station sur une plage de temps
        const startTime = '2024-10-10 00:00:00';
        const endTime = '2024-10-14 23:59:59';
        await getMeasurementsByStationAndTimeRange(stationId, startTime, endTime);

    } catch (error) {
        console.error('Error in main function:', error);
    } finally {
        await client.shutdown(); // Ferme la connexion à Cassandra
    }
}

// Appel de la fonction principale
main();
