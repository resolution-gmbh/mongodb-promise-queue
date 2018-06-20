const setup = require('./setup.js');
const MongoDbQueue = require('../');
const {assert} = require('chai');

describe('indexes', function () {
    let client, db, queue;

    before(async function () {
        ({client, db} = await setup());
        queue = new MongoDbQueue(db, 'indexes');
    });

    it('checks if indexes are created without error', async function () {
        const indexNames = await queue.createIndexes();
        assert.isArray(indexNames);
        assert.equal(indexNames.length, 2);
        assert.isString(indexNames[0]);
        assert.isString(indexNames[1]);
    });

    after(async function () {
        await client.close();
    });

});