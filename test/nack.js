const setup = require('./setup.js');
const MongoDbQueue = require('..');
const {assert} = require('chai');
const delay = require('timeout-as-promise');

describe('nack', function () {
    let client, db, queue;

    before(async function () {
        ({client, db} = await setup());
        queue = new MongoDbQueue(db, 'nack');
    });

    it('checks nack functionality', async function () {
        assert.isOk(await queue.add('Hello, World!'));
        let msg = await queue.get();
        assert.isOk(msg.id);

        // nack msg -> put it back into queue
        assert.isOk(await queue.nack(msg.ack));

        // ACKing it should not work now
        try {
            await queue.ack(msg.ack);
            assert.fail('Message was successfully ACKed after being NACKed');
        } catch (e) {
            if (e.message === 'assert.fail()') {
                throw e;
            }
            // else ok
        }
        // NACKing it should not work now
        try {
            await queue.nack(msg.ack);
            assert.fail('Message was successfully NACKed after being NACKed');
        } catch (e) {
            if (e.message === 'assert.fail()') {
                throw e;
            }
            // else ok
        }

        // But fetching it again should work now
        msg = await queue.get();
        assert.isOk(msg.id);

        // now ack it
        assert.isOk(await queue.ack(msg.ack));

    });

    it('checks nack with delay functionality', async function () {
        this.timeout(4000);
        assert.isOk(await queue.add('Hello, World!'));
        let msg = await queue.get();
        assert.isOk(msg.id);

        // nack msg -> put it back into queue
        assert.isOk(await queue.nack(msg.ack, {delay: 2}));

        // ACKing it should not work now
        try {
            await queue.ack(msg.ack);
            assert.fail('Message was successfully ACKed after being NACKed');
        } catch (e) {
            if (e.message === 'assert.fail()') {
                throw e;
            }
            // else ok
        }
        // NACKing it should not work now
        try {
            await queue.nack(msg.ack);
            assert.fail('Message was successfully NACKed after being NACKed');
        } catch (e) {
            if (e.message === 'assert.fail()') {
                throw e;
            }
            // else ok
        }
        // getting should not work now
        assert.isNotOk(await queue.get(msg.ack));
        await delay(2000);

        // Now, fetching it again should work
        msg = await queue.get();
        assert.isOk(msg.id);

        // now ack it
        assert.isOk(await queue.ack(msg.ack));

    });


    after(async function () {
        await client.close();
    });

});
