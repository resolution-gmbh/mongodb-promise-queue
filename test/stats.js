const setup = require('./setup.js');
const MongoDbQueue = require('../');
const {assert} = require('chai');
const delay = require('timeout-as-promise');

describe('stats', function () {
    let client, db;

    before(async function () {
        ({client, db} = await setup());
    });

    it('checks stats for a single message added, received and acked are correct', async function () {
        const queue = new MongoDbQueue(db, 'stats');

        // Initial state
        assert.equal(await queue.total(), 0);
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.inFlight(), 0);
        assert.equal(await queue.done(), 0);

        // Adds a message
        assert.isOk(await queue.add('Hello, World!'));
        assert.equal(await queue.total(), 1);
        assert.equal(await queue.size(), 1);
        assert.equal(await queue.inFlight(), 0);
        assert.equal(await queue.done(), 0);

        // Fetch it so it's now in flight
        const msg = await queue.get();
        assert.isOk(msg.id);
        assert.equal(await queue.total(), 1);
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.inFlight(), 1);
        assert.equal(await queue.done(), 0);

        // Ack it so it's done
        assert.isOk(await queue.ack(msg.ack));
        assert.equal(await queue.total(), 1);
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.inFlight(), 0);
        assert.equal(await queue.done(), 1);

        // And clear it so it's not even done any more
        await queue.clean();
        assert.equal(await queue.total(), 0);
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.inFlight(), 0);
        assert.equal(await queue.done(), 0);
    });

    it('checks stats for a single message added, received, timed out, received, pinged and acked are correct', async function () {
        this.timeout(6000);
        const queue = new MongoDbQueue(db, 'stats', {visibility: 2});
        let msg;

        // Initial state
        assert.equal(await queue.total(), 0);
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.inFlight(), 0);
        assert.equal(await queue.done(), 0);

        // Adds a message
        assert.isOk(await queue.add('Hello, World!'));
        assert.equal(await queue.total(), 1);
        assert.equal(await queue.size(), 1);
        assert.equal(await queue.inFlight(), 0);
        assert.equal(await queue.done(), 0);

        // Fetch it so it's now in flight
        msg = await queue.get();
        assert.isOk(msg.id);
        assert.equal(await queue.total(), 1);
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.inFlight(), 1);
        assert.equal(await queue.done(), 0);

        // Let it time out
        await delay(2500);
        assert.equal(await queue.total(), 1);
        assert.equal(await queue.size(), 1);
        assert.equal(await queue.inFlight(), 0);
        assert.equal(await queue.done(), 0);

        // Fetch it again
        msg = await queue.get();
        assert.isOk(msg.id);
        assert.equal(await queue.total(), 1);
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.inFlight(), 1);
        assert.equal(await queue.done(), 0);

        // wait a bit then ping it
        await delay(1000);
        assert.isOk(await queue.ping(msg.ack));
        assert.equal(await queue.total(), 1);
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.inFlight(), 1);
        assert.equal(await queue.done(), 0);

        // wait again, should still be in flight
        await delay(1000);
        assert.equal(await queue.total(), 1);
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.inFlight(), 1);
        assert.equal(await queue.done(), 0);

        // Ack it so it's done
        assert.isOk(await queue.ack(msg.ack));
        assert.equal(await queue.total(), 1);
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.inFlight(), 0);
        assert.equal(await queue.done(), 1);

        // And clear it so it's not even done any more
        await queue.clean();
        assert.equal(await queue.total(), 0);
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.inFlight(), 0);
        assert.equal(await queue.done(), 0);
    });

    after(async function () {
        await client.close();
    });

});

