// import {createKeyspace, createTable, insertMeasurement, createClient} from "./functions.js";
const {createKeyspace, createTable, insertMeasurement, createClient} = require("./functions")
const cassandra = require('cassandra-driver');

function getRandomTimestamp() {
    const start = new Date(2023, 0, 1).getTime();
    const end = new Date().getTime();
    const timestamp = new Date(start + Math.random() * (end - start)); // Random time between start and now
    return timestamp;
}

// Fonction principale pour exécuter toutes les étapes
async function initialize() {
    const client = createClient();
    try {
        // Création du keyspace et de la table
        await createKeyspace(client);
        await createTable(client);

        // Jeu de données test
        const measurements = [
            { station: "Aussois", stationId: "f47ac10b-58cc-4372-a567-0e02b2c3d479", lat: 45.2272, lon: 6.7458, temperature: 12.5, humidity: 65 },
            { station: "Chamonix", stationId: "550e8400-e29b-41d4-a716-446655440000", lat: 45.9237, lon: 6.8694, temperature: 8.3, humidity: 72 },
            { station: "Val Thorens", stationId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", lat: 45.2983, lon: 6.5800, temperature: 2.1, humidity: 80 },
            { station: "Tignes", stationId: "6ba7b811-9dad-11d1-80b4-00c04fd430c8", lat: 45.4683, lon: 6.9056, temperature: 5.7, humidity: 68 },
            { station: "Les Arcs", stationId: "6ba7b812-9dad-11d1-80b4-00c04fd430c8", lat: 45.5723, lon: 6.7969, temperature: 7.2, humidity: 70 },
            { station: "Aussois", stationId: "f47ac10b-58cc-4372-a567-0e02b2c3d479", lat: 45.2272, lon: 6.7458, temperature: 13.8, humidity: 62 },
            { station: "Chamonix", stationId: "550e8400-e29b-41d4-a716-446655440000", lat: 45.9237, lon: 6.8694, temperature: 9.5, humidity: 75 },
            { station: "Val Thorens", stationId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", lat: 45.2983, lon: 6.5800, temperature: 1.3, humidity: 85 },
            { station: "Tignes", stationId: "6ba7b811-9dad-11d1-80b4-00c04fd430c8", lat: 45.4683, lon: 6.9056, temperature: 4.9, humidity: 71 },
            { station: "Les Arcs", stationId: "6ba7b812-9dad-11d1-80b4-00c04fd430c8", lat: 45.5723, lon: 6.7969, temperature: 6.8, humidity: 73 },
            { station: "Aussois", stationId: "f47ac10b-58cc-4372-a567-0e02b2c3d479", lat: 45.2272, lon: 6.7458, temperature: 11.2, humidity: 68 },
            { station: "Chamonix", stationId: "550e8400-e29b-41d4-a716-446655440000", lat: 45.9237, lon: 6.8694, temperature: 7.7, humidity: 78 },
            { station: "Val Thorens", stationId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", lat: 45.2983, lon: 6.5800, temperature: 0.5, humidity: 88 },
            { station: "Tignes", stationId: "6ba7b811-9dad-11d1-80b4-00c04fd430c8", lat: 45.4683, lon: 6.9056, temperature: 3.6, humidity: 74 },
            { station: "Les Arcs", stationId: "6ba7b812-9dad-11d1-80b4-00c04fd430c8", lat: 45.5723, lon: 6.7969, temperature: 5.4, humidity: 76 },
            { station: "Aussois", stationId: "f47ac10b-58cc-4372-a567-0e02b2c3d479", lat: 45.2272, lon: 6.7458, temperature: 14.6, humidity: 60 },
            { station: "Chamonix", stationId: "550e8400-e29b-41d4-a716-446655440000", lat: 45.9237, lon: 6.8694, temperature: 10.1, humidity: 70 },
            { station: "Val Thorens", stationId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", lat: 45.2983, lon: 6.5800, temperature: 2.8, humidity: 82 },
            { station: "Tignes", stationId: "6ba7b811-9dad-11d1-80b4-00c04fd430c8", lat: 45.4683, lon: 6.9056, temperature: 6.2, humidity: 67 },
            { station: "Les Arcs", stationId: "6ba7b812-9dad-11d1-80b4-00c04fd430c8", lat: 45.5723, lon: 6.7969, temperature: 7.9, humidity: 69 },
            { station: "Aussois", stationId: "f47ac10b-58cc-4372-a567-0e02b2c3d479", lat: 45.2272, lon: 6.7458, temperature: 13.1, humidity: 64 },
            { station: "Chamonix", stationId: "550e8400-e29b-41d4-a716-446655440000", lat: 45.9237, lon: 6.8694, temperature: 8.9, humidity: 73 },
            { station: "Val Thorens", stationId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", lat: 45.2983, lon: 6.5800, temperature: 1.7, humidity: 86 },
            { station: "Tignes", stationId: "6ba7b811-9dad-11d1-80b4-00c04fd430c8", lat: 45.4683, lon: 6.9056, temperature: 5.3, humidity: 70 },
            { station: "Les Arcs", stationId: "6ba7b812-9dad-11d1-80b4-00c04fd430c8", lat: 45.5723, lon: 6.7969, temperature: 7.5, humidity: 72 },
            { station: "Aussois", stationId: "f47ac10b-58cc-4372-a567-0e02b2c3d479", lat: 45.2272, lon: 6.7458, temperature: 12.8, humidity: 66 },
            { station: "Chamonix", stationId: "550e8400-e29b-41d4-a716-446655440000", lat: 45.9237, lon: 6.8694, temperature: 8.6, humidity: 74 },
            { station: "Val Thorens", stationId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", lat: 45.2983, lon: 6.5800, temperature: 2.4, humidity: 83 },
            { station: "Tignes", stationId: "6ba7b811-9dad-11d1-80b4-00c04fd430c8", lat: 45.4683, lon: 6.9056, temperature: 6.0, humidity: 69 },
            { station: "Les Arcs", stationId: "6ba7b812-9dad-11d1-80b4-00c04fd430c8", lat: 45.5723, lon: 6.7969, temperature: 7.7, humidity: 71 },
            { station: "Aussois", stationId: "f47ac10b-58cc-4372-a567-0e02b2c3d479", lat: 45.2272, lon: 6.7458, temperature: 14.2, humidity: 61 },
            { station: "Chamonix", stationId: "550e8400-e29b-41d4-a716-446655440000", lat: 45.9237, lon: 6.8694, temperature: 9.8, humidity: 71 },
            { station: "Val Thorens", stationId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", lat: 45.2983, lon: 6.5800, temperature: 3.2, humidity: 81 },
            { station: "Tignes", stationId: "6ba7b811-9dad-11d1-80b4-00c04fd430c8", lat: 45.4683, lon: 6.9056, temperature: 6.6, humidity: 66 },
            { station: "Les Arcs", stationId: "6ba7b812-9dad-11d1-80b4-00c04fd430c8", lat: 45.5723, lon: 6.7969, temperature: 8.3, humidity: 68 },
            { station: "Aussois", stationId: "f47ac10b-58cc-4372-a567-0e02b2c3d479", lat: 45.2272, lon: 6.7458, temperature: 13.5, humidity: 63 },
            { station: "Chamonix", stationId: "550e8400-e29b-41d4-a716-446655440000", lat: 45.9237, lon: 6.8694, temperature: 9.2, humidity: 72 },
            { station: "Val Thorens", stationId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", lat: 45.2983, lon: 6.5800, temperature: 2.0, humidity: 84 },
            { station: "Tignes", stationId: "6ba7b811-9dad-11d1-80b4-00c04fd430c8", lat: 45.4683, lon: 6.9056, temperature: 5.6, humidity: 69 },
            { station: "Les Arcs", stationId: "6ba7b812-9dad-11d1-80b4-00c04fd430c8", lat: 45.5723, lon: 6.7969, temperature: 7.1, humidity: 71 },
            { station: "Aussois", stationId: "f47ac10b-58cc-4372-a567-0e02b2c3d479", lat: 45.2272, lon: 6.7458, temperature: 14.9, humidity: 59 },
            { station: "Chamonix", stationId: "550e8400-e29b-41d4-a716-446655440000", lat: 45.9237, lon: 6.8694, temperature: 10.4, humidity: 69 },
            { station: "Val Thorens", stationId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", lat: 45.2983, lon: 6.5800, temperature: 3.6, humidity: 80 },
            { station: "Tignes", stationId: "6ba7b811-9dad-11d1-80b4-00c04fd430c8", lat: 45.4683, lon: 6.9056, temperature: 7.0, humidity: 65 },
            { station: "Les Arcs", stationId: "6ba7b812-9dad-11d1-80b4-00c04fd430c8", lat: 45.5723, lon: 6.7969, temperature: 8.7, humidity: 67 },
            { station: "Aussois", stationId: "f47ac10b-58cc-4372-a567-0e02b2c3d479", lat: 45.2272, lon: 6.7458, temperature: 13.9, humidity: 62 },
            { station: "Chamonix", stationId: "550e8400-e29b-41d4-a716-446655440000", lat: 45.9237, lon: 6.8694, temperature: 9.6, humidity: 71 }
        ];

        // Insertion des données
        for (const measurement of measurements) {
            const timestamp = getRandomTimestamp();
            await insertMeasurement(
                client,
                measurement.station,
                measurement.stationId,
                timestamp,
                measurement.lat,
                measurement.lon,
                measurement.temperature,
                measurement.humidity
            );
        }

    } catch (error) {
        console.error('Error during initialization:', error);
    } finally {
        await client.shutdown(); // Ferme la connexion à Cassandra
    }
}

initialize();