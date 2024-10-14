const cassandra = require('cassandra-driver');

// Create a client instance
const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'], // IP address(es) of your Cassandra nodes
  localDataCenter: 'datacenter1', // Data center name (used in a multi-DC setup)
  keyspace: 'system'  // The keyspace you want to connect to
});

// Connect to the cluster
client.connect()
  .then(() => console.log('Connected to Cassandra'))
  .catch(err => console.error('Connection error:', err));

client.shutdown()
.then(() => console.log('Connection closed'))
.catch(err => console.error('Error closing connection:', err));

