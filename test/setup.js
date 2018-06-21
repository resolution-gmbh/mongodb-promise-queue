const mongodb = require('mongodb');

const connectionString = 'mongodb://localhost:27017/mongodb-queue';

module.exports = async function () {
    const client = await mongodb.MongoClient.connect(connectionString);
    // Setting db name = null ensures the library uses the name given in the URI
    const db = client.db(null);
    // let's empty out some collections to make sure there are no messages
    const collections = [
        'default',
        'delay',
        'multi',
        'visibility',
        'clean',
        'ping',
        'stats',
        'init',
        'indexes',
        'many',
        'nack',
        'queue',
        'dead-queue',
        'queue-2',
        'dead-queue-2'
    ];
    for (const collection of collections) {
        await db.collection(collection).remove();
    }
    return {client, db};
};
