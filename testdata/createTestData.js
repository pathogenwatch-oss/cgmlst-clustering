const fs = require('fs');
const BSON = require('bson');
const path = require('path');
const assert = require('assert');
const zlib = require('zlib')
const readline = require('readline');
const hasha = require('hasha');

const bson = new BSON();

function getRandom(seed) {
  // Modified from https://stackoverflow.com/a/19303725
  // by Antti Sykäri
  function _random() {
    var x = Math.sin(seed++) * 10000;
    return x - Math.floor(x);
  }
  return _random
}

random = getRandom(1)

function novelAllele() {
  return random().toString(16).slice(2)
}

function knownAllele() {
  // Returns random numbers between 0 and 100
  // biased towards 0
  return 100 - Math.floor((random())**0.2 * 100)
}

function objectId(i) {
  return `000000000000000000000000${i}`.slice(-24)
}

function mutate(seed) {
  mutationRate = random() < 0.1 ? 0.2 : 0.02
  novelAlleleRate = mutationRate * 0.02 // What is the chance it's 'novel'
  missingRate = mutationRate * 0.02 // What is the chance it's missing
  mutation = []
  missing = 0
  for (let i = 0; i < seed.length; i++) {
    r = random()
    if (r < mutationRate) {
      mutation.push(knownAllele())
    } else if (r < mutationRate + novelAlleleRate) {
      mutation.push(novelAllele())
    } else if (r < mutationRate + novelAlleleRate + missingRate) {
      mutation.push(null)
    } else {
      mutation.push(seed[i])
    }
  }
  return mutation
}

function fakeAnalysisDocs(nProfiles) {
  console.log(`Creating ${nProfiles} fake profile documents`)

  nGenes = 2000
  seed =  []
  for (let i = 0; i < nGenes; i++) {
    seed.push(1)
  }

  mutations = [mutate(seed)]
  while (mutations.length < nProfiles) {
    extraMutations = Math.min(mutations.length, nProfiles - mutations.length)
    for (let i = 0; i < extraMutations; i++) {
      mutation = mutate(mutations[i])
      mutations.push(mutation)
      if (mutations.length % 1000 == 0) {
        console.log(`Calculated ${mutations.length} fake mutations`)
      }
    }
  }

  publicProportion = 0.8
  for (i = 0; i < mutations.length; i++) {
    m = mutations[i]
    id = objectId(i)
    doc = {
      _id: new BSON.ObjectID(id),
      task: "cgmlst",
      version: "20180806174658-v1.6.10",
      _public: random() < publicProportion,
      results: {
        st: id,
        matches: [],
      }
    }
    for (g = 0; g < nGenes; g++) {
      if (m[g] != null) {
        doc.results.matches.push({"gene": `gene${g}`, "id": m[g]})
      }
    }
    mutations[i] = doc
    if (i == 5000) {
      nMatches = Object.values(doc.results.matches).length
      console.log(`doc ${objectId(i)} has ${nMatches} matches`)
    }
    if ((i+1) % 1000 == 0) {
      console.log(`Created ${i+1} fake docs`)
    }
  }

  return mutations
}

function dumpBson(p, data, append=false) {
  console.log(`Adding ${data.length} documents to ${p}`)
  fn = fs.openSync(p, append ? 'a' : 'w')
  for (let i =  0; i < data.length; i++) {
    fs.writeSync(fn, bson.serialize(data[i]))
  }
}

async function appendScores(data, scoresFile) {
  var onDone
  out = new Promise(resolve => {
    onDone = resolve
  })
  appended = 0

  gunzip = zlib.createGunzip()
  lines = readline.createInterface({
    input: fs.createReadStream(scoresFile).pipe(gunzip)
  })

  lines.on('line', line => {
    data.push(JSON.parse(line))
    appended++
    if (appended%1000 == 0) {
      console.log(`Parsed ${appended} scores from ${scoresFile}`)
    }
  })
  lines.on('close', () => {
    console.log(`Parsed ${appended} scores from ${scoresFile}`)
    onDone()
  })

  return out
}

function smallDataset() {
  // This is a handcoded dataset which looks like this:
    // Loci
      // A 1 1 1 1 1
      // B 2 1 1 1 1
      // C 2 2 2 2 2
      // D 2 - 1 2 2
      // E 3 3 3 3 3

    // Distance matrix
      //   A B C D E
      // A 0 1 5 3 5
      // B   0 4 2 5
      // C     0 1 5
      // D       0 4
      // E         0

  let request = { STs: ["A", "B", "C", "D", "E"], maxThreshold: 10 }
  let cache = {
    pi: [1, 2, 2],
    lambda: [1, 4, 2147483647],
    STs: ["A", "B", "C"],
    edges: {
      "0": [],
      "1": [[0, 1]],
      "2": [],
      "3": [],
      "4": [[1, 2]],
      "5": [[0, 2]],
      "6": [],
      "7": [],
      "8": [],
      "9": [],
      "10": [],
    },
    threshold: 10
  }
  const docs = []
  docs.push({
    _id: new BSON.ObjectID(objectId(1)),
    task: "cgmlst",
    version: "v1",
    _public: false,
    results: {
      st: "A",
      matches: [
        { gene: 'gene1', id: 1 },
        { gene: 'gene2', id: 1 },
        { gene: 'gene3', id: 1 },
        { gene: 'gene4', id: 1 },
        { gene: 'gene5', id: 1 }
      ],
    }
  })
  docs.push({
    _id: new BSON.ObjectID(objectId(2)),
    task: "cgmlst",
    version: "v1",
    _public: false,
    results: {
      st: "B",
      matches: [
        { gene: 'gene1', id: 2 },
        { gene: 'gene2', id: 1 },
        { gene: 'gene3', id: 1 },
        { gene: 'gene4', id: 1 },
        { gene: 'gene5', id: 1 }
      ],
    }
  })
  docs.push({
    _id: new BSON.ObjectID(objectId(3)),
    task: "cgmlst",
    version: "v1",
    _public: false,
    results: {
      st: "C",
      matches: [
        { gene: 'gene1', id: 2 },
        { gene: 'gene2', id: 2 },
        { gene: 'gene3', id: 2 },
        { gene: 'gene4', id: 2 },
        { gene: 'gene5', id: 2 }
      ],
    }
  })
  docs.push({
    _id: new BSON.ObjectID(objectId(4)),
    task: "cgmlst",
    version: "v1",
    _public: false,
    results: {
      st: "D",
      matches: [
        { gene: 'gene1', id: 2 },
        // { gene: 'gene2', id: 1 },
        { gene: 'gene3', id: 1 },
        { gene: 'gene4', id: 2 },
        { gene: 'gene5', id: 2 }
      ],
    }
  })
  docs.push({
    _id: new BSON.ObjectID(objectId(5)),
    task: "cgmlst",
    version: "v1",
    _public: false,
    results: {
      st: "E",
      matches: [
        { gene: 'gene1', id: 3 },
        { gene: 'gene2', id: 3 },
        { gene: 'gene3', id: 3 },
        { gene: 'gene4', id: 3 },
        { gene: 'gene5', id: 3 }
      ],
    }
  })
  dumpBson("SmallDatasetWithoutCache.bson", [request, ...docs])
  dumpBson("SmallDatasetWithCache.bson", [request, cache, ...docs])

  // Change the order of the STs in the cache (but not the "request")
  //   A B D C E
  // A 0 1 3 5 5
  // B   0 2 4 5
  // D     0 1 4
  // C       0 5
  // E         0

  cache = {
    pi: [1, 2, 2],
    lambda: [1, 2, 2147483647],
    STs: ["A", "B", "D"],
    edges: {
      "0": [],
      "1": [[0, 1]],
      "2": [[1, 2]],
      "3": [[0, 2]],
      "4": [],
      "5": [],
      "6": [],
      "7": [],
      "8": [],
      "9": [],
      "10": [],
    },
    threshold: 10
  }
  dumpBson("SmallDatasetWithReorderedCache.bson", [request, cache, ...docs])

  request = { STs: ["A", "C", "D", "E"], maxThreshold: 10 }
  cache = {
    pi: [1, 2, 2],
    lambda: [1, 4, 2147483647],
    STs: ["A", "B", "C"],
    edges: {
      "0": [],
      "1": [[0, 1]],
      "2": [],
      "3": [],
      "4": [[1, 2]],
      "5": [[0, 2]],
      "6": [],
      "7": [],
      "8": [],
      "9": [],
      "10": [],
    },
    threshold: 10
  }
  dumpBson("SmallDatasetWithUnusedCache.bson", [request, cache, docs[0], ...docs.slice(2)])
}

function bigDataset() {
  random = getRandom(1)
  nProfiles = 7000
  request = { STs: [], maxThreshold: 50 }
  for (let i = 0; i < nProfiles; i++) {
    id = objectId(i)
    request.STs.push(id)
  }
  cache = JSON.parse(fs.readFileSync("FakePublicCache.json"))
  assert.equal(cache.STs.length, 5573)
  assert.equal(cache.pi.length, 5573)
  assert.equal(cache.lambda.length, 5573)
  fakeData = [request, cache]

  dumpBson("FakeProfiles.bson", fakeData)
  profiles = fakeAnalysisDocs(nProfiles)
  assert.equal(random(), 0.19474356789214653)
  // If this assertion passes, the test data should be consistent
  dumpBson("FakeProfiles.bson", profiles, true)

  dumpBson("FakeProfilesWithoutCache.bson", [request])
  dumpBson("FakeProfilesWithoutCache.bson", profiles, true)

  // Just the public profiles
  request = { STs: [], maxThreshold: 50 }
  fakeData = [request]
  for (let i = 0; i < profiles.length; i++) {
    profile = profiles[i]
    if (profile._public) {
      id = objectId(i)
      request.STs.push(id)
      fakeData.push(profile)
    }
  }
  dumpBson("FakePublicProfiles.bson", fakeData)
}

function testParseRequestDoc() {
  dumpBson("TestParseRequestDoc.bson", [
    {
      STs: [ "abc", "def", "ghi" ],
      maxThreshold: 50
    },
    {
      STs: [ "abc", "abc", "ghi" ],
      maxThreshold: 50
    },
    {
      genomes: [
        { "wrong": "abc" },
      ],
      maxThreshold: 50
    },
    {
      STs: [ "abc", "def", "ghi" ]
    },
  ])
}

function testParseCache() {
  dumpBson("TestParseCache.bson", [
    {
      threshold: 5,
      STs: ["a", "b", "c", "d"],
      pi: [2, 3, 3, 3],
      lambda: [1, 1, 2, 2147483647],
      edges: {
        0: [],
        1: [[0, 2], [1, 3]],
        2: [[2, 3]],
        3: [],
        4: [],
        5: [[0, 1]]
      }
    }
  ])
}

function testUpdateProfiles() {
  dumpBson("TestUpdateProfiles.bson", [
    {
      "_id":        new BSON.ObjectID(),
      "fileId":     "whoCares",
      "task": "cgmlst",
      "results": {
        "st": "abc",
        "matches": [
          {"gene": "foo", "id": 1},
          {"gene": "bar", "id": "xyz"},
        ],
      }
    }
  ])
}

function testParse() {
  dumpBson("TestParse.bson", [
    {
      STs: ["a", "e", "b", "c", "d"],
      maxThreshold: 5
    },
    {
      threshold: 5,
      STs: ["a", "b", "c", "d"],
      pi: [2, 3, 3, 3],
      lambda: [1, 1, 2, 2147483647],
      edges: {
        1: [[0, 2], [1, 3]]
      }
    },
    {
      threshold: 5,
      edges: {
        2: [[2, 3]],
        0: [],
        4: [],
      }
    },
    {
      _id:        new BSON.ObjectID(3),
      fileId:     "xxx",
      results: {
        st: "a",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: "xyz" }
        ]
      }
    },
    {
      threshold: 5,
      edges: {
        3: [],
        5: [[0, 1]]
      }
    },
    {
      _id:        new BSON.ObjectID(4),
      fileId:     "yyy",
      results: {
        st: "e",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: 2 }
        ]
      }
    }
  ])
}

function testParseNoCache() {
  dumpBson("TestParseNoCache.bson", [
    {
      STs: ["a", "e", "b", "c", "d"],
      maxThreshold: 5
    },
    {
      _id:        new BSON.ObjectID(3),
      fileId:     "xxx",
      results: {
        st: "a",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: "xyz" }
        ]
      }
    },
    {
      _id:        new BSON.ObjectID(4),
      fileId:     "yyy",
      results: {
        st: "e",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: 2 }
        ]
      }
    }
  ])
}

function testParsePartialCache() {
  dumpBson("TestParsePartialCache.bson", [
    {
      STs: ["a", "e", "b", "c", "d"],
      maxThreshold: 5
    },
    {
      threshold: 5,
      STs: ["a", "b", "c", "d"],
      pi: [2, 3, 3, 3],
      lambda: [1, 1, 2, 2147483647],
      edges: {
        1: [[0, 2], [1, 3]]
      }
    },
    {
      threshold: 5,
      edges: {
        4: [],
      }
    },
    {
      _id:        new BSON.ObjectID(3),
      fileId:     "xxx",
      results: {
        st: "a",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: "xyz" }
        ]
      }
    },
    {
      threshold: 5,
      edges: {
        3: [],
        5: [[0, 1]]
      }
    },
    {
      _id:        new BSON.ObjectID(4),
      fileId:     "yyy",
      results: {
        st: "e",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: 2 }
        ]
      }
    }
  ])
}

function testDuplicatePi() {
  dumpBson("TestDuplicatePi.bson", [
    {
      pi: [1, 2, 3]
    },
    {
      pi: []
    },
    {
      pi: [4, 5]
    }
  ])
}


function testRequestIsSubset() {
  // What if someone deleted just one of their genomes
  dumpBson("TestRequestIsSubset.bson", [
    {
      STs: ["a", "b"],
      maxThreshold: 4
    },
    {
      threshold: 5,
      STs: ["a", "b", "c"],
      pi: [1, 2, 2],
      lambda: [1, 2, 2147483647],
      edges: {
        1: [[0, 1]],
        2: [[1, 2]],
      }
    },
    {
      threshold: 5,
      edges: {
        0: [],
        4: [],
        5: [],
      }
    },
    {
      threshold: 5,
      edges: {
        3: [[0, 2]],
      }
    },
    {
      _id:        new BSON.ObjectID(3),
      fileId:     "xxx",
      results: {
        st: "a",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: "xyz" }
        ]
      }
    },
    {
      _id:        new BSON.ObjectID(4),
      fileId:     "yyy",
      results: {
        st: "b",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: 2 }
        ]
      }
    }
  ])
}

function testRequestHasHigherThreshold() {
  dumpBson("TestRequestHasHigherThreshold.bson", [
    {
      STs: ["a", "b", "d"],
      maxThreshold: 5
    },
    {
      threshold: 4,
      STs: ["a", "b", "c"],
      pi: [1, 2, 2],
      lambda: [1, 2, 2147483647],
      edges: {
        1: [[0, 1]],
        2: [[1, 2]],
      }
    },
    {
      threshold: 4,
      edges: {
        0: [],
        4: [],
      }
    },
    {
      threshold: 4,
      edges: {
        3: [[0, 2]],
      }
    },
    {
      _id:        new BSON.ObjectID(3),
      fileId:     "xxx",
      results: {
        st: "a",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: "xyz" }
        ]
      }
    },
    {
      _id:        new BSON.ObjectID(4),
      fileId:     "yyy",
      results: {
        st: "b",
        matches: [
          { gene: "foo", id: 1 },
          { gene: "bar", id: 2 }
        ]
      }
    },
    {
      _id:        new BSON.ObjectID(4),
      fileId:     "yyy",
      results: {
        st: "d",
        matches: [
          { gene: "bar", id: 2 }
        ]
      }
    }
  ])
}

function main() {
  smallDataset()
  bigDataset()
  testParseRequestDoc()
  testParseCache()
  testUpdateProfiles()
  testParse()
  testParseNoCache()
  testParsePartialCache()
  testDuplicatePi()
  testRequestIsSubset()
  testRequestHasHigherThreshold()
}

main()
