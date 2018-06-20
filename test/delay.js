const setup = require('./setup.js');
const MongoDbQueue = require('..');
const {assert} = require('chai');
const delay = require('timeout-as-promise');

describe('delay', function () {
    let client, db;

    before(async function () {
        ({client, db} = await setup());
    });

    it('checks if messages in a delayed queue are returned after the delay', async function () {
        this.timeout(4000);
        const queue = new MongoDbQueue(db, 'delay', {delay: 2});
        let msg;

        const origId = await queue.add('Hello, World!');
        assert.isOk(origId);

        // This message should not be there yet
        msg = await queue.get();
        assert.isNotOk(msg);
        await delay(2500);

        // Now it should be there
        msg = await queue.get();
        assert.isOk(msg);
        assert.equal(msg.id, origId);
        await queue.ack(msg.ack);

        // No more messages, but also no errors
        msg = await queue.get();
        assert.isNotOk(msg);
    });

    it('checks if a per-message delay overrides the default delay', async function () {
        this.timeout(4000);
        const queue = new MongoDbQueue(db, 'delay');
        let msg;

        const origId = await queue.add('I am delayed by 2 seconds', { delay : 2 });
        assert.isOk(origId);

        // This message should not be there yet
        msg = await queue.get();
        assert.isNotOk(msg);
        await delay(2500);

        // Now it should be there
        msg = await queue.get();
        assert.isOk(msg);
        assert.equal(msg.id, origId);
        await queue.ack(msg.ack);

        // No more messages, but also no errors
        msg = await queue.get();
        assert.isNotOk(msg);
    });

    after(async function () {
        await client.close();
    });

});