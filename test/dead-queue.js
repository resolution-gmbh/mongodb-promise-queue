const setup = require('./setup.js');
const MongoDbQueue = require('../');
const {assert} = require('chai');
const delay = require('timeout-as-promise');

describe('', function () {
    let client, db;

    before(async function () {
        ({client, db} = await setup());
    });

    it('creates a queue with dead queue', function () {
        new MongoDbQueue(db, 'queue', { visibility : 3, deadQueue : 'dead-queue' });
    });

    it('checks that a single message going over 5 tries appears on dead-queue', async function() {
        let deadQueue = new MongoDbQueue(db, 'dead-queue');
        let queue = new MongoDbQueue(db, 'queue', { visibility : 1, deadQueue : deadQueue });
        let origId, msg;

        origId = await queue.add('Hello, World!');
        assert.isOk(origId);

        // First expiration
        msg = await queue.get();
        assert.isOk(msg);
        await delay(1500);

        // Second expiration
        msg = await queue.get();
        assert.isOk(msg);
        await delay(1500);

        // Third expiration
        msg = await queue.get();
        assert.isOk(msg);
        await delay(1500);

        // Fourth expiration
        msg = await queue.get();
        assert.isOk(msg);
        await delay(1500);

        // Fifth expiration
        msg = await queue.get();
        assert.isOk(msg);
        await delay(1500);

        // Message should now be on the dead queue
        msg = await queue.get();
        assert.isNotOk(msg);

        msg = await deadQueue.get();
        assert.isOk(msg);
        assert.equal(msg.payload.id, origId);
        assert.equal(msg.payload.payload, 'Hello, World!');
        assert.equal(msg.payload.tries, 6);

    });

    test('two messages, with first going over 3 tries', function(t) {
        let deadQueue = mongoDbQueue(db, 'dead-queue-2');
        let queue = mongoDbQueue(db, 'queue-2', { visibility : 1, deadQueue : deadQueue, maxRetries : 3 });
        let msg;
        let origId, origId2;

        async.series(
            [
                function(next) {
                    queue.add('Hello, World!', function(err, id) {
                        t.ok(!err, 'There is no error when adding a message.');
                        t.ok(id, 'Received an id for this message');
                        origId = id;
                        next();
                    });
                },
                function(next) {
                    queue.add('Part II', function(err, id) {
                        t.ok(!err, 'There is no error when adding another message.');
                        t.ok(id, 'Received an id for this message');
                        origId2 = id;
                        next();
                    });
                },
                function(next) {
                    queue.get(function(err, thisMsg) {
                        t.equal(thisMsg.id, origId, 'We return the first message on first go');
                        setTimeout(function() {
                            t.pass('First expiration');
                            next();
                        }, 2 * 1000);
                    });
                },
                function(next) {
                    queue.get(function(err, thisMsg) {
                        t.equal(thisMsg.id, origId, 'We return the first message on second go');
                        setTimeout(function() {
                            t.pass('Second expiration');
                            next();
                        }, 2 * 1000);
                    });
                },
                function(next) {
                    queue.get(function(err, thisMsg) {
                        t.equal(thisMsg.id, origId, 'We return the first message on third go');
                        setTimeout(function() {
                            t.pass('Third expiration');
                            next();
                        }, 2 * 1000);
                    });
                },
                function(next) {
                    // This is the 4th time, so we SHOULD have moved it to the dead queue
                    // pior to it being returned.
                    queue.get(function(err, msg) {
                        t.ok(!err, 'No error when getting the 2nd message');
                        t.equal(msg.id, origId2, 'Got the ID of the 2nd message');
                        t.equal(msg.payload, 'Part II', 'Got the same payload as the 2nd message');
                        next();
                    });
                },
                function(next) {
                    deadQueue.get(function(err, msg) {
                        t.ok(!err, 'No error when getting from the deadQueue');
                        t.ok(msg.id, 'Got a message id from the deadQueue');
                        t.equal(msg.payload.id, origId, 'Got the same message id as the original message');
                        t.equal(msg.payload.payload, 'Hello, World!', 'Got the same as the original message');
                        t.equal(msg.payload.tries, 4, 'Got the tries as 4');
                        next();
                    });
                },
            ],
            function(err) {
                t.ok(!err, 'No error during single round-trip test');
                t.end();
            }
        );
    });

    after(async function () {
        await client.close();
    });

});