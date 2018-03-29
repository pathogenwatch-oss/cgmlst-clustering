const fs = require('fs');
const BSON = require('bson');
const path = require('path');

const bson = new BSON();

// function reformat(p) {
//   const es = require('event-stream');
//   const BsonStream = require('bson-stream');
//   fileIds = new Set()

//   const bs = new BsonStream()
//   bs.on('data', d => {
//     fileIds.add(d.fileId)
//   })
  
//   bs.on('end', () => {
//     process.stderr.write(`${fileIds.size} fileIDs\n`)
//     process.stdout.write(
//       bson.serialize({genomes: [ ...fileIds ].map(f => ({ fileId: f}))})
//     )
//     fs.createReadStream(p)
//     .pipe(new BsonStream())
//     .pipe(
//       es.map((data, done) => {
//         const doc = { ...data };
//         doc._id = new BSON.ObjectID(data._id);
//         delete doc.version;
//         delete doc.matches;
//         matches = [];
//         for (m in data.matches) {
//           matches.push({gene: m, id: data.matches[m]});
//         }
//         doc.analysis = {
//           cgmlst: {
//             __v: data.version,
//             matches
//           }
//         };
//         done(null, bson.serialize(doc));
//       })
//     ).pipe(process.stdout);
//   })

//   fs.createReadStream(p)
//     .pipe(bs)
// }

// reformat("all_staph.bson.bak")

function dumpBson(p, data) {
  // return
  f = fs.createWriteStream(p)
  for (let i =  0; i < data.length; i++) {
    f.write(
      bson.serialize(data[i])
    )
  }
  f.end()
}

dumpBson("TestParseGenomeDoc.bson", [
  {
    genomes: [
      { "fileId": "abc" },
      { "fileId": "def" },
      { "fileId": "ghi" }
    ]
  },
  {
    genomes: [
      { "fileId": "abc" },
      { "fileId": "abc" },
      { "fileId": "ghi" }
    ]
  },
  {
    genomes: [
      { "wrong": "abc" },
    ]
  },
  {
    wrong: [
      { "fileId": "abc" },
    ]
  }
])

dumpBson("TestUpdateScores.bson", [
  {
		"fileId": "abc",
		"scores": {
			"bcd": 1,
			"cde": 2,
		},
	}
])

dumpBson("TestUpdateProfiles.bson", [
  {
    "_id":        new BSON.ObjectID(),
    "fileId":     "abc",
    "organismId": "1280",
    "public":     true,
    "analysis": {
      "cgmlst": {
        "__v":    "0",
        "matches": [
          {"gene": "foo", "id": 1},
          {"gene": "bar", "id": "xyz"},
        ],
      }
    }
  }
])

dumpBson("TestParse.bson", [
  {
    genomes: [
      { "fileId": "abc" },
      { "fileId": "def" },
      { "fileId": "ghi" },
      { "fileId": "jkl" }
    ]
  },
  {
    fileId: "abc",
    scores: {
      "def": 1,
      "ghi": 2,
      "jkl": 3
    }
  },
  {
    fileId: "def",
    scores: {
      "ghi": 4,
      "jkl": 5
    }
  },
  {
    _id:        new BSON.ObjectID(),
    fileId:     "ghi",
    organismId: "1280",
    public:     true,
    analysis: {
      cgmlst: {
        __v:    "0",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: "xyz" }
        ]
      }
    }
  },
  {
    _id:        new BSON.ObjectID(),
    fileId:     "jkl",
    organismId: "1280",
    public:     true,
    analysis: {
      cgmlst: {
        __v:    "0",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: 2 }
        ]
      }
    }
  }
])

deepStruct = []
for (i = 0; i < 10000; i++) {
  doc = {
    A: {
      B: []
    }
  }
  for (j = 0; j < 2000; j++){
    doc.A.B.push({C: j, i})
  }
  deepStruct.push(doc)
  if (i % 1000 == 0) 
    process.stderr.write(`Added doc ${i}\n`)
}
dumpBson("deepStruct.bson", deepStruct)