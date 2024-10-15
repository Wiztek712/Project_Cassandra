// import {createKeyspace, createTable, insertMeasurement, createClient} from "./functions.js";
const {createKeyspace, createTable, insertMeasurement, createClient} = require("./functions")

const client = createClient();

// Fonction principale pour exécuter toutes les étapes
async function initialize() {
    try {
        // Création du keyspace et de la table
        await createKeyspace();
        await createTable();

        // Exemple d'utilisation
        const stationId = cassandra.types.Uuid.random();
        await insertMeasurement("Aussois", stationId, 48.8566, 2.3522, 20.5, 60);


    } catch (error) {
        console.error('Error during initialization:', error);
    } finally {
        await client.shutdown(); // Ferme la connexion à Cassandra
    }
}

initialize();