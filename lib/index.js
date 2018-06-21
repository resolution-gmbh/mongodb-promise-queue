/**
 *
 * index.js - Use your existing MongoDB as a local queue with promise.
 *
 * Copyright (c) 2014 Andrew Chilton
 * Copyright (c) 2018 Frédéric Mascaro
 * Copyright (c) 2018 Tobias Theobald, resolution Reichert Network Solutions GmbH
 *
 * License: MIT
 *
 **/


const crypto = require('crypto');


// ========================================================================================


// some helper functions
function id() {
    return crypto.randomBytes(16).toString('hex');
}

// ----------------------------------------------------------------------

function now() {
    return new Date();
}

// ----------------------------------------------------------------------

function nowPlusSecs(secs) {
    return new Date(Date.now() + secs * 1000);
}

// ========================================================================================

// the Queue object itself
class MongoDbQueue {
    constructor(mongoDb, name, opts = {}) {
        if (!mongoDb) {
            throw new Error('mongodb-queue: provide a mongodb.MongoClient.db');
        }
        if (!name) {
            throw new Error('mongodb-queue: provide a queue name');
        }

        this.name = name;
        this.col = mongoDb.collection(name);
        this.visibility = opts.visibility || 30;
        this.delay = opts.delay || 0;

        if (opts.deadQueue) {
            this.deadQueue = opts.deadQueue;
            this.maxRetries = opts.maxRetries || 5;
        }
    }

    // ----------------------------------------------------------------------

    async createIndexes() {
        return [
            await this.col.createIndex({deleted: 1, visible: 1}),
            await this.col.createIndex({ack: 1}, {unique: true, sparse: true}),
        ];
    }

    // ----------------------------------------------------------------------

    async add(payload, opts = {}) {
        const delay = opts.delay || this.delay;
        const visible = delay ? nowPlusSecs(delay) : now();

        if (payload instanceof Array) {
            // Insert many
            if (payload.length === 0) {
                const errMsg = 'Queue.add(): Array payload length must be greater than 0';
                throw new Error(errMsg);
            }
            const messages = payload.map((payload) => {
                return {
                    visible: visible,
                    payload: payload,
                };
            });
            const result = await this.col.insertMany(messages);

            // These need to be converted because they're in a weird format.
            const insertedIds = [];
            for (const key of Object.keys(result.insertedIds)) {
                const numericKey = +key;
                insertedIds[numericKey] = `${result.insertedIds[key]}`;
            }

            return insertedIds;
        } else {
            // insert one
            const result = await this.col.insertOne({
                visible: visible,
                payload: payload,
            });
            return result.insertedId;
        }
    }

    // ----------------------------------------------------------------------

    async get(opts = {}) {
        const visibility = opts.visibility || this.visibility;

        const query = {
            deleted: null,
            visible: {$lte: now()},
        };

        const sort = {
            _id: 1
        };

        const update = {
            $inc: {tries: 1},
            $set: {
                ack: id(),
                visible: nowPlusSecs(visibility),
            }
        };

        const result = await this.col.findOneAndUpdate(query, update, {sort: sort, returnOriginal: false});
        let msg = result.value;

        if (!msg) return;

        // convert to an external representation
        msg = {
            // convert '_id' to an 'id' string
            id: `${msg._id}`,
            ack: msg.ack,
            payload: msg.payload,
            tries: msg.tries,
        };

        // if we have a deadQueue, then check the tries, else don't
        if (this.deadQueue) {
            // check the tries
            if (msg.tries > this.maxRetries) {
                // So:
                // 1) add this message to the deadQueue
                // 2) ack this message from the regular queue
                // 3) call ourself to return a new message (if exists)
                await this.deadQueue.add(msg);
                await this.ack(msg.ack);
                return await this.get(opts);
            }
        }

        return msg;
    }

    // ----------------------------------------------------------------------

    async ping(ack, opts = {}) {
        const visibility = opts.visibility || this.visibility;

        const query = {
            ack: ack,
            visible: {$gt: now()},
            deleted: null,
        };

        const update = {
            $set: {
                visible: nowPlusSecs(visibility)
            }
        };

        const msg = await this.col.findOneAndUpdate(query, update, {returnOriginal: false});
        if (!msg.value) {
            throw new Error('Queue.ping(): Unidentified ack  : ' + ack);
        }

        return `${msg.value._id}`;
    }

    // ----------------------------------------------------------------------

    async ack(ack) {
        const query = {
            ack: ack,
            visible: {$gt: now()},
            deleted: null,
        };

        const update = {
            $set: {
                deleted: now(),
            }
        };

        const msg = await this.col.findOneAndUpdate(query, update, {returnOriginal: false});
        if (!msg.value) {
            throw new Error('Queue.ack(): Unidentified ack : ' + ack);
        }

        return `${msg.value._id}`;
    }
    // ----------------------------------------------------------------------

    async nack(ack) {
        const query = {
            ack: ack,
            visible: {$gt: now()},
            deleted: null,
        };

        const update = {
            $set: {
                visible: now(),
            }
        };

        const msg = await this.col.findOneAndUpdate(query, update, {returnOriginal: false});
        if (!msg.value) {
            throw new Error('Queue.nack(): Unidentified ack : ' + ack);
        }

        return `${msg.value._id}`;
    }

    // ----------------------------------------------------------------------

    async clean() {
        const query = {
            deleted: {$exists: true},
        };

        return await this.col.deleteMany(query);
    }

    // ----------------------------------------------------------------------

    async total() {
        return await this.col.count();
    }

    // ----------------------------------------------------------------------

    async size() {
        const query = {
            deleted: null,
            visible: {$lte: now()},
        };

        return await this.col.count(query);
    }

    // ----------------------------------------------------------------------

    async inFlight() {
        const query = {
            ack: {$exists: true},
            visible: {$gt: now()},
            deleted: null,
        };

        return await this.col.count(query);
    }

    // ----------------------------------------------------------------------

    async done() {
        const query = {
            deleted: {$exists: true},
        };

        return await this.col.count(query);
    }
}

// ----------------------------------------------------------------------

module.exports = MongoDbQueue;
