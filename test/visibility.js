const setup = require('./setup.js');
const MongoDbQueue = require('../');
const {assert} = require('chai');
const delay = require('timeout-as-promise');

describe('visibility', function () {
    let client, db, queue;

    before(async function () {
        ({client, db} = await setup());
        queue = new MongoDbQueue(db, 'visibility', {visibility: 1});
    });

    it('checks is message is back in queue after visibility runs out', async function () {
        assert.isOk(await queue.add('Hello, World!'));
        assert.isOk(await queue.get());

        // wait a bit so the message goes back into the queue
        await delay(1500);

        // Fetch it again
        const msg = await queue.get();
        assert.isOk(msg.id);

        // ACK it
        await queue.ack(msg.ack);

        assert.isNotOk(await queue.get());
    });

    it('checks that a late ack doesn\'t remove the msg', async function () {
        assert.isOk(await queue.add('Hello, World!'));
        let msg = await queue.get();
        const oldAck = msg.ack;
        assert.isOk(msg.ack);

        // let it time out
        await delay(1500);
        try {
            await queue.ack(oldAck);
            assert.fail('Successfully acked timed out message');
        } catch (e) {
            if (e.message === 'assert.fail()') {
                throw e;
            }
            // else ok
        }

        // fetch again, ack should now be different
        msg = await queue.get();
        assert.notEqual(msg.ack, oldAck);

        // and finalize
        await queue.ack(msg.ack);
        assert.isNotOk(await queue.get());
    });

    it('checks if message visibility overrides queue visibility', async function () {
        this.timeout(5000);
        assert.isOk(await queue.add('Hello, World!'));
        let msg = await queue.get({visibility: 3});
        assert.isOk(msg.id);

        // Wait for the regular visibility to run out
        await delay(2000);

        // This should not return anything
        msg = await queue.get();
        assert.isNotOk(msg);

        // wait a bit so the message goes back into the queue
        await delay(2000);

        // Now it should be there again
        msg = await queue.get();
        assert.isOk(msg.id);

        // ACK it
        await queue.ack(msg.ack);

        assert.isNotOk(await queue.get());
    });


    after(async function () {
        await client.close();
    });

});
