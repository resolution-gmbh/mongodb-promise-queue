const setup = require('./setup.js');
const MongoDbQueue = require('../');
const {assert} = require('chai');
const delay = require('timeout-as-promise');

describe('dead queue', function () {
    let client, db;

    before(async function () {
        ({client, db} = await setup());
    });

    it('checks that a single message going over 5 tries appears on dead-queue', async function() {
        this.timeout(10000);
        let deadQueue = new MongoDbQueue(db, 'dead-queue');
        let queue = new MongoDbQueue(db, 'queue', { visibility : 1, deadQueue : deadQueue });
        let origId, msg;

        origId = await queue.add('Hello, World!');
        assert.isOk(origId);

        // First expiration
        msg = await queue.get();
        assert.isOk(msg);
        assert.equal(msg.id, origId);
        await delay(1500);

        // Second expiration
        msg = await queue.get();
        assert.isOk(msg);
        assert.equal(msg.id, origId);
        await delay(1500);

        // Third expiration
        msg = await queue.get();
        assert.isOk(msg);
        assert.equal(msg.id, origId);
        await delay(1500);

        // Fourth expiration
        msg = await queue.get();
        assert.isOk(msg);
        assert.equal(msg.id, origId);
        await delay(1500);

        // Fifth expiration
        msg = await queue.get();
        assert.isOk(msg);
        assert.equal(msg.id, origId);
        await delay(1500);

        // Message should now be on the dead queue
        msg = await queue.get();
        assert.isNotOk(msg);

        // ... where we should be able to get it
        msg = await deadQueue.get();
        assert.isOk(msg);
        assert.equal(msg.payload.id, origId);
        assert.equal(msg.payload.payload, 'Hello, World!');
        assert.equal(msg.payload.tries, 6);
    });

    it('checks two messages, with first going over 3 tries', async function() {
        this.timeout(6000);
        let deadQueue = new MongoDbQueue(db, 'dead-queue-2');
        let queue = new MongoDbQueue(db, 'queue-2', { visibility : 1, deadQueue : deadQueue, maxRetries : 3 });
        let msg;
        let origId, origId2;

        origId = await queue.add('Hello, World!');
        origId2 = await queue.add('Part II');

        // First expiration, returning first message
        msg = await queue.get();
        assert.isOk(msg);
        assert.equal(msg.id, origId);
        await delay(1500);

        // Second expiration
        msg = await queue.get();
        assert.isOk(msg);
        assert.equal(msg.id, origId);
        await delay(1500);

        // Third expiration
        msg = await queue.get();
        assert.isOk(msg);
        assert.equal(msg.id, origId);
        await delay(1500);

        // Should get second message now
        msg = await queue.get();
        assert.isOk(msg);
        assert.equal(msg.id, origId2);

        // Should get first message from deadQueue now
        msg = await deadQueue.get();
        assert.isOk(msg);
        assert.equal(msg.payload.id, origId);
        assert.equal(msg.payload.payload, 'Hello, World!');
        assert.equal(msg.payload.tries, 4);
    });

    after(async function () {
        await client.close();
    });

});