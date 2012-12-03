/*
  Node.js module to send a stream of items to a Redis pubsub channel
*/

'use strict';

var Stream = require('stream').Stream
  , util = require('util')
  , redis = require('redis')

var verbose = false //TODO meh.

module.exports = RedisPubsubStream

//RedisPubsubStream constructor available options: 
//  opts.channel        //name of the channel to publish messages
//  opts.serverAddress  //address of redis server
//  opts.serverPort     //port of redis server
//  opts.redisOpts      //see https://github.com/mranney/node_redis#rediscreateclientport-host-options for options.
function RedisPubsubStream (opts) {
  this.writable = true
  this.readable = true

  this._paused = this._ended = this._destroyed = false

  this._buffer = ''

  Stream.call(this)

  if (!opts)
    opts = {}
  if (!opts.serverPort)
    opts.serverPort = 6379
  if(!opts.serverAddress)
    opts.serverAddress = "localhost"
  if(opts.channel)
    this.channel = opts.channel
  else
    this.channel = "Default"
  var redisOpts = {}
  if(opts.redisOpts) redisOpts = opts.redisOpts

  this.redisClient = redis.createClient(opts.serverPort, opts.serverAddress, redisOpts)

  return this
}

util.inherits(RedisPubsubStream, Stream)

/**
 *
 * Parse a chunk and emit the parsed data. Implements writable stream method [stream.write(string)](http://nodejs.org/docs/latest/api/stream.html#stream_stream_write_string_encoding)
 * Assumes UTF-8
 * 
 * @param {String} data to write to stream (assumes UTF-8)
 * @return {boolean} true if written, false if it will be sent later
 *
 */
RedisPubsubStream.prototype.write = function (record) {
  // cannot write to a stream after it has ended
  if ( this._ended ) 
    throw new Error('RedisPubsubStream: write after end')

  if ( ! this.writable ) 
    throw new Error('RedisPubsubStream: not a writable stream')
  
  if ( this._paused ) 
    return false

  if(verbose){ 
    console.log('publish to redis channel: ' + this.channel + ', message: ' + util.inspect(record))
  }
  //TODO callback need to do anything?
  this.redisClient.publish(this.channel, JSON.stringify(record), function (err, res){  })
  
  return true  
}

/*
 *
 * Write optional parameter and terminate the stream, allowing queued write data to be sent before closing the stream. Implements writable stream method [stream.end(string)](http://nodejs.org/docs/latest/api/stream.html#stream_stream_end)
 *
 * @param {String} data The data to write to stream (assumes UTF-8)
 *
 */
RedisPubsubStream.prototype.end = function (str) {
  if ( this._ended ) return
  
  if ( ! this.writable ) return
  
  if ( arguments.length )
    this.write(str)
  
  this._ended = true
  this.readable = false
  this.writable = false

  this.emit('end')
  this.emit('close')
}

/*
 *
 * Pause the stream. Implements readable stream method [stream.pause()](http://nodejs.org/docs/latest/api/stream.html#stream_stream_pause)
 *
 */
RedisPubsubStream.prototype.pause = function () {
  if ( this._paused ) return
  
  this._paused = true
  this.emit('pause')
}

/*
 *
 * Resume stream after a pause, emitting a drain. Implements readable stream method [stream.resume()](http://nodejs.org/docs/latest/api/stream.html#stream_stream_resume)
 *
 */
RedisPubsubStream.prototype.resume = function () {
  if ( this._paused ) {
    this._paused = false
    this.emit('drain')
  }
}


/*
 *
 * Destroy the stream. Stream is no longer writable nor readable. Implements writable stream method [stream.destroy()](http://nodejs.org/docs/latest/api/stream.html#stream_stream_destroy_1)
 *
 */
RedisPubsubStream.prototype.destroy = function () {
  if ( this._destroyed ) return
  
  this._destroyed = true
  this._ended = true

  this.readable = false
  this.writable = false

  this.emit('end')
  this.emit('close')
}

RedisPubsubStream.prototype.flush = function () {
  this.emit('flush')
}
