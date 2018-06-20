const setup = require('./setup.js');
const MongoDbQueue = require('..');
const {assert} = require('chai');

describe('clean', function () {
    let client, db, queue;

    before(async function () {
        ({client, db} = await setup());
        queue = new MongoDbQueue(db, 'clean', {visibility: 3});
    });

    it('checks clean does not change an empty queue', async function () {
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.total(), 0);
        await queue.clean();
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.total(), 0);
    });

    it('check only ACKed messages are deleted', async function () {
        assert.isOk(await queue.add('Hello, World!'));
        await queue.clean();
        assert.equal(await queue.size(), 1);
        assert.equal(await queue.total(), 1);

        const msg = await queue.get();
        assert.isOk(msg.id);
        assert.equal(msg.payload, 'Hello, World!');
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.total(), 1);

        await queue.clean();
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.total(), 1);

        assert.isOk(await queue.ack(msg.ack));
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.total(), 1);

        await queue.clean();
        assert.equal(await queue.size(), 0);
        assert.equal(await queue.total(), 0);
    });


    after(async function () {
        await client.close();
    });

});