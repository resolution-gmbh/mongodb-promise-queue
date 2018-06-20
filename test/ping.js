const setup = require('./setup.js');
const MongoDbQueue = require('../');
const {assert} = require('chai');
const delay = require('timeout-as-promise');

describe('ping', function () {
    let client, db, queue;

    before(async function () {
        ({client, db} = await setup());
        queue = new MongoDbQueue(db, 'ping', {visibility: 2});
    });

    it('checks if a retrieved message with a ping can still be acked', async function () {
        this.timeout(3000);
        assert.isOk(await queue.add('Hello, World!'));

        // Should get message back
        const msg = await queue.get();
        assert.isOk(msg.id);
        await delay(1000);

        // Ping it
        assert.isOk(await queue.ping(msg.ack));
        await delay(1000);

        // ACK it
        assert.isOk(await queue.ack(msg.ack));

        // Queue should now be empty
        assert.isNotOk(await queue.get());
    });

    it('makes sure an acked message can\'t be pinged again', async function () {
        assert.isOk(await queue.add('Hello, World!'));

        // Get it back
        const msg = await queue.get();
        assert.isOk(msg.id);

        // ACK it
        assert.isOk(await queue.ack(msg.ack));

        // Should not be possible
        try {
            await queue.ping(msg.ack);
            assert.fail('Successfully acked an already acked message');
        } catch (e) {
            if (e.message === 'assert.fail()') {
                throw e;
            }
            // else ok
        }
    });

    it('makes sure ping options override queue options', async function () {
        this.timeout(7000);
        assert.isOk(await queue.add('Hello, World!'));

        // Get it back
        let msg = await queue.get();
        assert.isOk(msg.id);
        await delay(1000);

        // ping it with a longer visibility
        assert.isOk(await queue.ping(msg.ack, {visibility: 4}));
        // wait longer than queue visibility, but shorter than msg visibility
        await delay(3000);

        // Should not get a message
        assert.isNotOk(await queue.get());
        await delay(1500);

        // Should be available now
        msg = await queue.get();
        assert.isOk(msg);

        // And done.
        assert.isOk(await queue.ack(msg.ack));
    });

    after(async function () {
        await client.close();
    });

});
