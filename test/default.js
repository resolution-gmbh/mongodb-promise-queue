const setup = require('./setup.js');
const MongoDbQueue = require('../');
const {assert} = require('chai');

describe('default', function () {
    let client, db, queue;

    before(async function () {
        ({client, db} = await setup());
        queue = new MongoDbQueue(db, 'default');
    });

    it('checks default functionality', async function () {
        assert.isOk(await queue.add('Hello, World!'));
        const msg = await queue.get();
        assert.isString(msg.id);
        assert.isString(msg.ack);
        assert.isNumber(msg.tries);
        assert.equal(msg.tries, 1);
        assert.equal(msg.payload, 'Hello, World!');

        assert.isOk(await queue.ack(msg.ack));

        // Try to ack twice
        try {
            await queue.ack(msg.ack);
            assert.fail('Message was successfully ACKed twice');
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
