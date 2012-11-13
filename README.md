[![Build Status](https://travis-ci.org/ornl-visual-analytics/redis-pubsub-stream.png?branch=master)](https://travis-ci.org/ornl-visual-analytics/redis-pubsub-stream)


# Parse strings into JSON using regular expressions

This module will take in objects or strings as a [stream](http://nodejs.org/docs/latest/api/stream.html), and send it to redis as a pubsub message.


## Install

npm install redis-pubsub-stream


## configuration


## Usage


## Development

If you are going to do development, you may want to use the [git pre-commit hook](http://git-scm.com/book/en/Customizing-Git-Git-Hooks), which will check the `regex-stream.js` file using [jshint](https://github.com/jshint/jshint) script (if you have it installed) and run the [mocha](visionmedia.github.com/mocha/) tests (mocha is in the git repo). If either of these fail, the commit wont work. To use the hook, from project directory, run:

    ln -s ../../pre-commit.sh .git/hooks/pre-commit

    

# License

regex-stream is freely distributable under the terms of the MIT License.

Copyright (c) John R. Goodall (the "Original Author")

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS, THE U.S. GOVERNMENT, OR UT-BATTELLE BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
