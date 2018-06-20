const setup = require('./setup.js');
const MongoDbQueue = require('..');
const {assert} = require('chai');

const TOTAL = 250;

describe('many', function () {
    let client, db, queue;

    before(async function () {
        ({client, db} = await setup());
        queue = new MongoDbQueue(db, 'many');
    });

    it('checks if many messages can be inserted at once and gotten back', async function () {
        const messagesToQueue = [];
        for (let i = 0; i < TOTAL; i++) {
            messagesToQueue.push(`no=${i}`);
        }

        const messageIds = await queue.add(messagesToQueue);
        assert.equal(messageIds.length, TOTAL);
        const messages = [];
        let message;
        while ((message = await queue.get())) {
            messages.push(message);
        }

        // Should have received all messages now
        assert.equal(messages.length, TOTAL);

        // ACK them
        for (message of messages) {
            await queue.ack(message.ack);
        }
    });

    it('checks if many messages can be inserted one after another and gotten back', async function () {
        const messageIds = [];
        for (let i = 0; i < TOTAL; i++) {
            messageIds.push(await queue.add(`no=${i}`));
        }
        assert.equal(messageIds.length, TOTAL);

        const messages = [];
        let message;
        while ((message = await queue.get())) {
            messages.push(message);
        }

        // Should have received all messages now
        assert.equal(messages.length, TOTAL);

        // ACK them
        for (message of messages) {
            await queue.ack(message.ack);
        }
    });

    it('should not be possible to add zero messages', async function () {
        try {
            await queue.add([]);
            assert.fail('Successfully added zero messages');
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