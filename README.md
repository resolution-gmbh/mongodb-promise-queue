# mongodb-promise-queue #

[![NPM](https://nodei.co/npm/mongodb-promise-queue.png?mini=true)](https://nodei.co/npm/mongodb-promise-queue/)

A really light-weight way to create queues with a nice API and *promise* if you're already
using MongoDB.

Based of the code of [mongodb-queue](https://github.com/chilts/mongodb-queue), a great project with nodejs callback, and not promise.

### Requirements ###
* Node.js version 8 or higher (heavily using async/await)
* MonogDB Client Lib Version ^3.0.0 (the syntax for gaining db objects has changed in v3)

## Synopsis ##

Create a connection to your MongoDB database, and use it to create a queue object:

```js
const mongodb = require('mongodb')
const MongoDbQueue = require('mongodb-promise-queue')

const con = 'mongodb://localhost:27017/test'

mongodb.MongoClient.connect(con, function(err, client) {
    const db = client.db(null)
    let queue = new MongoDbQueue(db, 'my-queue')
})
```

Add a message to a queue:

```js
queue.add('Hello, World!')
.then(id => {
    // Message with payload 'Hello, World!' added.
    // 'id' is returned, useful for logging.
})
```

Get a message from the queue:

```js
queue.get()
.then(message => {
    console.log('message.id     =' + message.id)
    console.log('message.ack    =' + message.ack)
    console.log('message.payload=' + message.payload) // 'Hello, World!'
    console.log('message.tries  =' + message.tries)
})
```

Ping a message to keep it's visibility open for long-running tasks

```js
queue.ping(msg.ack)
.then(id => {
    // Visibility window now increased for this message id.
    // 'id' is returned, useful for logging.
})
```

Ack a message (and remove it from the queue):

```js
queue.ack(msg.ack)
.then(id => {
    // This msg removed from queue for this ack.
    // The 'id' of the message is returned, useful for logging.
})
```

By default, all old messages - even processed ones - are left in MongoDB. This is so that
you can go and analyse them if you want. However, you can call the following function
to remove processed messages:

```js
queue.clean()
.then(() => {
    // All processed (ie. acked) messages have been deleted
})
```

And if you haven't already, you should call this to make sure indexes have
been added in MongoDB. Of course, if you've called this once (in some kind
one-off script) you don't need to call it in your program. Of course, check
the changelock to see if you need to update them with new releases:

```js
queue.createIndexes()
.then(indexName => {
    // The indexes needed have been added to MongoDB.
})
```

## Creating a Queue ##

To create a queue, call the exported function with the `MongoClient`, the name
and a set of opts. The MongoDB collection used is the same name as the name
passed in:

```
const MongoDbQueue = require('mongodb-queue')

// an instance of a queue
const queue1 = new MongoDbQueue(db, 'a-queue')
// another queue which uses the same collection as above
const queue2 = new MongoDbQueue(db, 'a-queue')
```

Using `queue1` and `queue2` here won't interfere with each other and will play along nicely, but that's not
a good idea code-wise - just use the same object. This example is for illustrative purposes only.

Note: Don't use the same queue name twice with different options, otherwise behaviour is undefined and again
it's not something you should do.

To pass in options for the queue:

```
const resizeQueue = new MongoDbQueue(db, 'resize-queue', { visibility : 30, delay : 15 })
```

This example shows a queue with a message visibility of 30s and a delay to each message of 15s.

## Options ##

### name ###

This is the name of the MongoDB Collection you wish to use to store the messages.
Each queue you create will be it's own collection.

e.g.

```
const resizeImageQueue = new MongoDbQueue(db, 'resize-image-queue')
const notifyOwnerQueue = new MongoDbQueue(db, 'notify-owner-queue')
```

This will create two collections in MongoDB called `resize-image-queue` and `notify-owner-queue`.

### visibility - Message Visibility Window ###

Default: `30`

By default, if you don't ack a message within the first 30s after receiving it,
it is placed back in the queue so it can be fetched again. This is called the
visibility window.

You may set this visibility window on a per queue basis. For example, to set the
visibility to 15 seconds:

```
const queue = new MongoDbQueue(db, 'queue', { visibility : 15 })
```

All messages in this queue now have a visibility window of 15s, instead of the
default 30s.

### delay - Delay Messages on Queue ###

Default: `0`

When a message is added to a queue or nacked, it is immediately available for retrieval.
However, there are times when you might like to delay messages coming off a queue.
ie. if you set delay to be `10`, then every message will only be available for
retrieval 10s after being added.

To delay all messages by 10 seconds, try this:

```
const queue = new MongoDbQueue(db, 'queue', { delay : 10 })
```

This is now the default for every message added to the queue.

### deadQueue - Dead Message Queue ###

Default: none

Messages that have been retried over `maxRetries` will be pushed to this queue so you can
automatically see problem messages.

Pass in a queue (that you created) onto which these messages will be pushed:

```js
const deadQueue = new MongoDbQueue(db, 'dead-queue')
const queue = new MongoDbQueue(db, 'queue', { deadQueue : deadQueue })
```

If you pop a message off the `queue` over `maxRetries` times and have still not acked it,
it will be pushed onto the `deadQueue` for you. This happens when you `.get()` (not when
you miss acking a message in it's visibility window). By doing it when you call `.get()`,
the unprocessed message will be received, pushed to the `deadQueue`, acked off the normal
queue and `.get()` will check for new messages prior to returning you one (or none).

### maxRetries - Maximum Retries per Message ###

Default: 5

This option only comes into effect if you pass in a `deadQueue` as shown above. What this
means is that if an item is popped off the queue `maxRetries` times (e.g. 5) and not acked,
it will be moved to this `deadQueue` the next time it is tried to pop off. You can poll your
`deadQueue` for dead messages much like you can poll your regular queues.

The payload of the messages in the dead queue are the entire messages returned when `.get()`ing
them from the original queue.

e.g.

Given this message:

```
msg = {
  id: '533b1eb64ee78a57664cc76c',
  ack: 'c8a3cc585cbaaacf549d746d7db72f69',
  payload: 'Hello, World!',
  tries: 1
}
```

If it is not acked within the `maxRetries` times, then when you receive this same message
from the `deadQueue`, it may look like this:

```
msg = {
  id: '533b1ecf3ca3a76b667671ef',
  ack: '73872b204e3f7be84050a1ce82c5c9c0',
  payload: {
    id: '533b1eb64ee78a57664cc76c',
    ack: 'c8a3cc585cbaaacf549d746d7db72f69',
    payload: 'Hello, World!',
    tries: 5
  },
  tries: 1
}
```

Notice that the payload from the `deadQueue` is exactly the same as the original message
when it was on the original queue (except with the number of tries set to 5).

## Operations ##

### .add() ###

You can add a string to the queue:

```js
queue.add('Hello, World!')
.then(id => {
    // Message with payload 'Hello, World!' added.
    // 'id' is returned, useful for logging.
})
```

Or add an object of your choosing:

```js
queue.add({ err: 'E_BORKED', msg: 'Broken' })
.then(id => {
    // Message with payload { err: 'E_BORKED', msg: 'Broken' } added.
    // 'id' is returned, useful for logging.
})
```

Or add multiple messages:

```js
queue.add(['msg1', 'msg2', 'msg3'])
.then(ids => {
    // Messages with payloads 'msg1', 'msg2' & 'msg3' added.
    // All 'id's are returned as an array, useful for logging.
})
```

You can delay individual messages from being visible by passing the `delay` option:

```js
queue.add('Later', { delay: 120 })
.then(id => {
    // Message with payload 'Later' added.
    // 'id' is returned, useful for logging.
    // This message won't be available for getting for 2 mins.
})
```

### .get() ###

Retrieve a message from the queue:

```js
queue.get()
.then(message => {
    // You can now process the message
    // IMPORTANT: The callback will not wait for an message if the queue is empty.  The message will be undefined if the queue is empty.
})
```

You can choose the visibility of an individual retrieved message by passing the `visibility` option:

```js
queue.get({ visibility: 10 })
.then(message => {
    // You can now process the message for 10s before it goes back into the queue if not ack'd instead of the duration that is set on the queue in general
})
```

Message will have the following structure:

```js
{
  id: '533b1eb64ee78a57664cc76c', // ID of the message
  ack: 'c8a3cc585cbaaacf549d746d7db72f69', // ID for ack and ping operations
  payload: 'Hello, World!', // Payload passed when the message was addded
  tries: 1 // Number of times this message has been retrieved from queue without being ack'd
}
```

### .ack() ###

After you have received an item from a queue and processed it, you can delete it
by calling `.ack()` with the unique `ackId` returned:

```js
queue.get()
.then(message => {
    return queue.ack(message.ack).then(id => {
        // this message has now been removed from the queue
    })
})
```

### .ping() ###

After you have received an item from a queue and you are taking a while
to process it, you can `.ping()` the message to tell the queue that you are
still alive and continuing to process the message:

```js
queue.get()
.then(message => {
    return queue.ping(message.ack).then(id => {
        // this message has had it's visibility window extended
    })
})
```

You can also choose the visibility time that gets added by the ping operation by passing the `visibility` option:

```js
queue.get()
.then(message => {
    return queue.ping(msg.ack, { visibility: 10 }).then(id => {
        // this message has had it's visibility window extended by 10s instead of the visibilty set on the queue in general
    })
})
```

### .nack() ###

After you have received an item from a queue and realized you can't process it, you can make it available again in the
queue by calling `.nack()` with the unique `ackId` returned:

```js
queue.get()
.then(message => {
    return queue.nack(message.ack).then(id => {
        // this message has now been put back in the queue
    })
})
```

This has the same effect as letting the message time out, except that the message is available for processing again
immediately unless a delay is defined on this message or the queue. Note that if you run .get() immediately after
nack-ing a message, you'll likely immediately get it again unless another process snatched it up again in that short
time.

Therefore, if there are certain types of messages you can't handle, it's recommended to use separate queues instead of
nack-ing messages that are not interesting to you.

**Do** use it, for example, if, while processing the job, your program realizes it needs to shutdown.

An alternative is to define a delay after which the message will be available again:

```js
queue.get()
.then(message => {
    return queue.nack(message.ack, { delay: 5 }).then(id => {
        // this message has now been put back in the queue, but will only be retrievable after 5 seconds.
    })
})
```

Then you don't have to worry about getting the message back too soon, but this will also delay processing of the
message.

### .total() ###

Returns the total number of messages that has ever been in the queue, including
all current messages:

```js
queue.total()
.then(count => {
    console.log('This queue has seen %d messages', count)
})
```

### .size() ###

Returns the total number of messages that are waiting in the queue.

```js
queue.size()
.then(count => {
    console.log('This queue has %d current messages', count)
})
```

### .inFlight() ###

Returns the total number of messages that are currently in flight. ie. that
have been received but not yet acked:

```js
queue.inFlight()
.then(count => {
    console.log('A total of %d messages are currently being processed', count)
})
```

### .done() ###

Returns the total number of messages that have been processed correctly in the
queue:

```js
queue.done()
.then(count => {
    console.log('This queue has processed %d messages', count)
})
```

### .clean() ###

Deletes all processed mesages from the queue. Of course, you can leave these hanging around
if you wish, but delete them if you no longer need them. Perhaps do this using `setInterval`
for a regular cleaning:

```js
queue.clean()
.then(() => {
    console.log('The processed messages have been deleted from the queue')
})
```

### Notes about Numbers ###

If you add up `.size() + .inFlight() + .done()` then you should get `.total()`
but this will only be approximate since these are different operations hitting the database
at slightly different times. Hence, a message or two might be counted twice or not at all
depending on message turnover at any one time. You should not rely on these numbers for
anything but are included as approximations at any point in time.

## Use of MongoDB ##

Whilst using MongoDB recently and having a need for lightweight queues, I realised
that the atomic operations that MongoDB provides are ideal for this kind of job.

Since everything it atomic, it is impossible to lose messages in or around your
application. I guess MongoDB could lose them but it's a safer bet it won't compared
to your own application.

As an example of the atomic nature being used, messages stay in the same collection
and are never moved around or deleted, just a couple of fields are set, incremented
or deleted. We always use MongoDB's excellent `collection.findAndModify()` so that
each message is updated atomically inside MongoDB and we never have to fetch something,
change it and store it back.

## Note on MongoDB Version ##

When using MongoDB v2.6 and the v1.3.23 version of the mongodb driver from npm, I was getting
a weird error similar to "key $exists must not start with '$'". Yes, very strange. Anyway, the fix
is to install a later version of the driver. I have tried this with v1.4.9 and it seems ok.

## Releases ##

*See the previous release of the original project: https://github.com/chilts/mongodb-queue*

### 1.0.0 (2018-06-16) ###

* [NEW] Migration to promise.

## Author ##

Original project written by [Andrew Chilton](http://chilts.org/) -
[Twitter](https://twitter.com/andychilton).

Current project migrated by [Frédéric Mascaro](http://www.izi-done.com/)

## License ##

MIT - https://github.com/omninnov/mongodb-promise-queue/blob/master/LICENSE

(Ends)
