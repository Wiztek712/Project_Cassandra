const { getMeasurementsByStation, getMeasurementsByStationAndTimeRange, displayIds } = require ("./functions.js");

// // Récupérer les mesures pour une station
// const stationId = cassandra.types.Uuid.random();
// await getMeasurementsByStation(stationId);

// // Récupérer les mesures pour une station sur une plage de temps
// const startTime = '2024-10-10 00:00:00';
// const endTime = '2024-10-15 23:59:59';
// await getMeasurementsByStationAndTimeRange(stationId, startTime, endTime);

async function main(){
    try {
        console.log("Connexion réussie à la base de données Cassandra!");

        let continueloop = true;

        while(continueloop){
            let choice = await Welcome();
            switch(choice){
                case "1":
                    await retrieveMeasurementsByStation(session);
                    break;

                case "2":
                    await retrieveMeasurementsByStationAndTimeRange(session);
                    break;

                case "3":
                    await retrieveStationIds(session);
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
    console.log('Tapez "exit" pour sortir');

    const answer = await askQuestion("Entrez le numéro de l'action souhaitée : ");
    return answer;
}


async function retrieveMeasurementsByStation(){
    console.log("Données pour une station");
    const stationId = await askQuestion("Entrez l'ID de la station souhaitée : ");

    await getMeasurementsByStation(stationId);
}

async function retrieveMeasurementsByStationAndTimeRange(){
    console.log("Données pour une station");
    const stationId = await askQuestion("Entrez l'ID de la station souhaitée : ");
    const startTime = await askQuestion("Entrez l'heure de début au format AAAA-MM-JJ : ");
    const endTime = await askQuestion("Entrez l'heure de fin au format AAAA-MM-JJ : ");
    await getMeasurementsByStationAndTimeRange(stationId, startTime, endTime);
}

async function retrieveStationIds() {
    console.log("Id des stations");
    await displayIds();
    const endTime = await askQuestion("Appuyer sur entrée pour revenir au menu");
}

main();