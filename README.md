[![Build Status](https://travis-ci.org/ornl-sava/redis-pubsub-stream.png?branch=master)](https://travis-ci.org/ornl-sava/redis-pubsub-stream)


# Send a stream of items to Redis as PubSub events

This module will take in objects or strings as a [stream](http://nodejs.org/docs/latest/api/stream.html), and send it to [Redis](http://redis.io/) as a [pubsub](http://redis.io/topics/pubsub) message.

## Install

npm install redis-pubsub-stream

## Configuration

The RedisPubsubStream constructor should be passed an opts object as its argument, similar to the following:

    var opts = { 
          channel:"Test" 
        , serverAddress: "localhost" 
        , serverPort:6379 
        , redisOpts: {} 
      }

Where `channel` is the name of the channel to publish messages, `serverAddress` is the address of Redis server, and `port` is the port on which the Redis server is listening.

The `redisOpts` field contains any options to pass to the Redis constructor, which are listed in the [documentation for the node_redis module](https://github.com/mranney/node_redis#rediscreateclientport-host-options)

## Usage

For each incoming message, this module will output a corresponding pubsub message, on the specified channel, sent to the specified Redis instance.

This example (based on one of the test cases) reads in a json file with an array of items, parses them, and sends each items to the pubsub stream. 

    var RedisPubsubStream = require('redis-pubsub-stream')
      , fs = require('fs')
      , path = require('path')

    var inFile = path.join('test', 'input', 'simpleData.json')
      , opts = { 
          channel:"simplePubsubTest" 
        , serverAddress: "localhost" 
        , serverPort:6379 
        , redisOpts: {}
      }

    var pubsubStream = new RedisPubsubStream(opts)

    fs.readFile(inFile, function (err, data) {
      if (err) throw err
      data = JSON.parse(data)
      for(var i=0; i<data.length; i++){
        pubsubStream.write(data[i]);
      }
    })

Rather than sending items with .write(), a more typical example may simply pipe several streams together. The following example reads from a file, `data.txt`, parses the text using [regexStream](https://github.com/ornl-situ/regex-stream), and sends the parsed output to a Redis pubsub channel:

    var RedisPubsubStream = require('redis-pubsub-stream')
      , RegexStream = require('regex-stream')
      , util = require('util')

    var input = require('fs').createReadStream('./data.txt', {encoding:'utf-8'})
      , parser = {
          "regex": "^([\\S]+) ([\\S]+) ([\\S]+)"
        , "labels": ["A label", "B label", "C label"]
        , "delimiter": "\r\n|\n"
      }
      , regexStream = new RegexStream(parser)

    var opts = { channel:"simplePubsubTest" 
        , serverAddress: "localhost" 
        , serverPort:6379 
        , redisOpts: {} }

    var pubsubStream = new RedisPubsubStream(opts)

    // pipe data from input file to the regexStream parser to redis pubsub
    input.pipe(regexStream)
    regexStream.pipe(pubsubStream)

See the test cases for some usage examples.

## Development

If you are going to do development, you may want to use the [git pre-commit hook](http://git-scm.com/book/en/Customizing-Git-Git-Hooks), which will check the `redis-pubsub-stream.js` file using [jshint](https://github.com/jshint/jshint) script (if you have it installed) and run the [mocha](visionmedia.github.com/mocha/) tests (mocha is in the node_modules directory). If either of these fail, the commit wont work. To use the hook, from project directory, run:

    ln -s ../../pre-commit.sh .git/hooks/pre-commit

# License

redis-pubsub-stream is freely distributable under the terms of the MIT License.

Copyright (c) UT-Battelle, LLC (the "Original Author")

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS, THE U.S. GOVERNMENT, OR UT-BATTELLE BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
