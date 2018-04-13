const fs = require('fs');
const BSON = require('bson');
const path = require('path');

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
      "_id":       new BSON.ObjectID(id),
      "fileId":     id,
      "organismId": "fake",
      "public":     random() < publicProportion,
      "analysis": {
        "cgmlst": {
          "__v":    "0",
          "matches": [],
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

function dumpBson(p, data) {
  console.log(`Adding ${data.length} documents to ${p}`)
  f = fs.createWriteStream(p)
  for (let i =  0; i < data.length; i++) {
    f.write(
      bson.serialize(data[i])
    )
  }
  f.end()
}

// Make some fake profiles
profiles = fakeProfiles(7000)
genomes = { genomes: []}
for (let i = 0; i < profiles.length; i++) {
  genomes.genomes.push({ "fileId": profiles[i].fileId })
}
fakeData = [genomes].concat(profiles)
dumpBson("FakeProfiles.bson", fakeData)

// Just the public profiles
genomes = { genomes: []}
fakeData = [genomes]
for (let i = 0; i < profiles.length; i++) {
  profile = profiles[i]
  if (profile.public) {
    genomes.genomes.push({ "fileId": profile.fileId })
    fakeData.push(profile)
  }
}
dumpBson("FakePublicProfiles.bson", fakeData)

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
		"alleleDifferences": {
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
    alleleDifferences: {
      "def": 1,
      "ghi": 2,
      "jkl": 3
    }
  },
  {
    fileId: "def",
    alleleDifferences: {
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
