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
  for (let i = 0; i < nGenes; i++) {
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

function fakeProfiles(nProfiles) {
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
    profile = {
      _id: new BSON.ObjectID(id),
      public: random() < publicProportion,
      analysis: {
        cgmlst: {
          st: id,
          matches: [],
        }
      }
    }
    for (g = 0; g < nGenes; g++) {
      if (m[g] != null) {
        profile.analysis.cgmlst.matches.push({"gene": `gene${g}`, "id": m[g]})
      }
    }
    mutations[i] = profile
    if (i == 5000) {
      nMatches = Object.values(profile.analysis.cgmlst.matches).length
      console.log(`Profile ${objectId(i)} has ${nMatches} matches`)
    }
    if ((i+1) % 1000 == 0) {
      console.log(`Created ${i+1} fake profiles`)
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

async function main() {
  // Make some fake profiles
  nProfiles = 7000
  genomes = { genomes: [], thresholds: [5, 50, 200, 500]}
  for (let i = 0; i < nProfiles; i++) {
    id = objectId(i)
    genomes.genomes.push({
      "_id": new BSON.ObjectID(id),
      "st": id
    })
  }
  fakeData = [genomes]
  await appendScores(fakeData, "FakePublicScores.json.gz")
  assert.equal(fakeData.length, 5573)

  dumpBson("FakeProfiles.bson", fakeData)
  // I was running out of RAM
  for (let i = 0; i < fakeData.length; i++) {
    fakeData[i] = null
  }
  delete fakeData

  profiles = fakeProfiles(nProfiles)
  assert.equal(random(), 0.19474356789214653)
  // If this assertion passes, the test data should be consistent
  // MD5 (FakeProfiles.bson) = 71fbd0dab8be0afddadc3506d49caf70
  // MD5 (FakePublicProfiles.bson) = a0484af3ecc1d8c3d0321786c125cf08
  dumpBson("FakeProfiles.bson", profiles, true)

  // Just the public profiles
  genomes = { genomes: [], thresholds: [5, 50, 200, 500]}
  fakeData = [genomes]
  for (let i = 0; i < profiles.length; i++) {
    profile = profiles[i]
    if (profile.public) {
      id = objectId(i)
      genomes.genomes.push({
        "_id": new BSON.ObjectID(id),
        "st": id
      })
      fakeData.push(profile)
    }
  }
  dumpBson("FakePublicProfiles.bson", fakeData)

  dumpBson("TestParseGenomeDoc.bson", [
    {
      genomes: [
        { "_id": new BSON.ObjectID(1), "st": "abc" },
        { "_id": new BSON.ObjectID(2), "st": "def" },
        { "_id": new BSON.ObjectID(3), "st": "ghi" }
      ],
      thresholds: [5, 50]
    },
    {
      genomes: [
        { "_id": new BSON.ObjectID(4), "st": "abc" },
        { "_id": new BSON.ObjectID(5), "st": "abc" },
        { "_id": new BSON.ObjectID(6), "st": "ghi" }
      ],
      thresholds: [5, 50]
    },
    {
      genomes: [
        { "wrong": "abc" },
      ],
      thresholds: [5, 50]
    },
    {
      wrong: [
        { "_id": new BSON.ObjectID(7), "st": "abc" },
      ],
      thresholds: [5, 50]
    },
    {
      genomes: [
        { "_id": new BSON.ObjectID(1), "st": "abc" },
        { "_id": new BSON.ObjectID(2), "st": "def" },
        { "_id": new BSON.ObjectID(3), "st": "ghi" }
      ]
    },
    {
      genomes: [
        { "_id": new BSON.ObjectID(1), "st": "abc" },
        { "_id": new BSON.ObjectID(2), "st": "def" },
        { "_id": new BSON.ObjectID(3), "st": "ghi" }
      ],
      thresholds: []
    }
  ])

  dumpBson("TestUpdateScores.bson", [
    {
      "st": "abc",
      "alleleDifferences": {
        "bcd": 1,
        "cde": 2,
      },
    }
  ])

  dumpBson("TestUpdateProfiles.bson", [
    {
      "_id":        new BSON.ObjectID(),
      "fileId":     "whoCares",
      "organismId": "1280",
      "public":     true,
      "analysis": {
        "cgmlst": {
          "__v":    "0",
          "st": "abc",
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
        { _id: new BSON.ObjectID(1), st: "abc" },
        { _id: new BSON.ObjectID(2), st: "def" },
        { _id: new BSON.ObjectID(3), st: "ghi" },
        { _id: new BSON.ObjectID(4), st: "jkl" }
      ],
      thresholds: [5, 50, 200, 500]
    },
    {
      st: "abc",
      alleleDifferences: {
        "def": 1,
        "ghi": 2,
        "jkl": 3
      }
    },
    {
      st: "def",
      alleleDifferences: {
        "ghi": 4,
        "jkl": 5
      }
    },
    {
      _id:        new BSON.ObjectID(3),
      fileId:     "xxx",
      organismId: "1280",
      public:     true,
      analysis: {
        cgmlst: {
          __v:    "0",
          st: "ghi",
          matches: [
            { gene: "foo", id: 1 },
            { gene: "bar", id: "xyz" }
          ]
        }
      }
    },
    {
      _id:        new BSON.ObjectID(4),
      fileId:     "yyy",
      organismId: "1280",
      public:     true,
      analysis: {
        cgmlst: {
          __v:    "0",
          st: "jkl",
          matches: [
            { gene: "foo", id: 1 },
            { gene: "bar", id: 2 }
          ]
        }
      }
    }
  ])

  doc = {
    id: new BSON.ObjectID(0),
    st: hasha((0).toString(), { algorithm: "sha1" }),
    alleleDifferences: {},
  }

  for (let i = 1; i <= 1000; i++) {
    st = hasha(i.toString(), { algorithm: "sha1" })
    doc.alleleDifferences[st] = i
  }

  dumpBson("scoresDoc.bson", [doc])
}

main().then(() => console.log("Done")).catch(err => console.log(err))
