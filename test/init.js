const setup = require('./setup.js');
const MongoDbQueue = require('../');
const {assert} = require('chai');

describe('init', function () {
    let client, db;

    before(async function () {
        ({client, db} = await setup());
    });

    it('checks if init variables are checked', async function () {
        try {
            new MongoDbQueue(null, 'init');
            assert.fail('Queue was created without db');
        } catch (e) {
            if (e.message === 'assert.fail()') {
                throw e;
            }
            // else ok
        }

        try {
            new MongoDbQueue(db, null);
            assert.fail('Queue was created without name');
        } catch (e) {
            if (e.message === 'assert.fail()') {
                throw e;
            }
            // else ok
        }
    });

    after(async function () {
        await client.close();
    });

});
