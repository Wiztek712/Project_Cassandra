const { getMeasurementsByStation, getMeasurementsByStationAndTimeRange, displayIds, createClient, alreadyExist, insertMeasurement } = require ("./functions.js");
const readline = require('readline');

async function main(){
    const client = createClient();
    try {
        console.log("Connexion réussie à la base de données Cassandra!");

        let continueloop = true;

        while(continueloop){
            let choice = await Welcome();
            switch(choice){
                case "1":
                    await retrieveMeasurementsByStation(client,);
                    break;

                case "2":
                    await retrieveMeasurementsByStationAndTimeRange(client);
                    break;

                case "3":
                    await retrieveStationIds(client);
                    break;

                case "4":
                await insertNewMeasurement(client);
                break;

                case "exit":
                    continueloop = false;
                    console.log("Fermeture du programme...");
                    break;

                default:
                    console.log("Choix non reconnu");
            }
        }

    } catch (error) {
        console.error("Erreur lors de l'exécution : ", error);
    } finally {
        await client.shutdown();
    }
}

// Fonction utilitaire pour poser une question à l'utilisateur
function askQuestion(query) {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    return new Promise(resolve => rl.question(query, answer => {
        rl.close();
        resolve(answer);
    }));
}

async function Welcome(){
    console.log("Selectionnez une action :");
    console.log("1. Données pour une station");
    console.log("2. Données pour une station sur une plage de temps");
    console.log("3. Afficher les IDs des stations");
    console.log("4. Insérer une mesure");
    console.log('Tapez "exit" pour sortir');

    const answer = await askQuestion("Entrez le numéro de l'action souhaitée : ");
    return answer;
}


async function retrieveMeasurementsByStation(client){
    console.log("Données pour une station");
    const stationId = await askQuestion("Entrez l'ID de la station souhaitée : ");

    await getMeasurementsByStation(client, stationId);
}

async function retrieveMeasurementsByStationAndTimeRange(client){
    console.log("Données pour une station");
    const stationId = await askQuestion("Entrez l'ID de la station souhaitée : ");
    const startTime = await askQuestion("Entrez l'heure de début au format AAAA-MM-JJ : ");
    const endTime = await askQuestion("Entrez l'heure de fin au format AAAA-MM-JJ : ");
    await getMeasurementsByStationAndTimeRange(client, stationId, startTime, endTime);
}

async function retrieveStationIds(client) {
    console.log("Id des stations");
    await displayIds(client);
    await askQuestion("Appuyer sur entrée pour revenir au menu");
}

async function insertNewMeasurement(client){
    console.log("Ajout d'une nouvelle mesure");
    const station = await askQuestion("Entrez le nom de la station");
    const temperature = await askQuestion("Entrez la température");
    const humidity = await askQuestion("Entrez l'humidité'");
    const timestamp = await askQuestion("Entrez la date au format AAAA-MM-JJ");

    // Search in database if the station already exists, so as to retrieve lat, lon and id or randomly generate they.
    const info = await alreadyExist(station, client);

    await insertMeasurement(client, station, info.station_id, timestamp, info.latitude, info.longitude, temperature, humidity);
}

main();