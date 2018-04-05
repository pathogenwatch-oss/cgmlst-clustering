const fs = require('fs');
const BSON = require('bson');
const path = require('path');
const es = require('event-stream');
const BsonStream = require('bson-stream');

const bson = new BSON();

const LIMIT = 5;
const FILENAME = 'docs.bson';

function dumpBson(p, data) {
  // return
  f = fs.createWriteStream(p)
  for (const d of data) {
    f.write(bson.serialize(d))
  }
  f.end()
}

function limit(input) {
  const docs = [];

  const bs = new BsonStream()
  bs.on('data', d => {
    docs.push(d)

    if (docs.length === LIMIT) {
      bs.end();
    }
  })

  bs.on('end', () => {
    dumpBson(FILENAME, docs);
  })

  input.pipe(bs)
}

limit(process.stdin);