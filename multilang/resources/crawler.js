storm = require('./storm')
var BasicBolt = storm.BasicBolt
var request = require('request')

class CrawlerBolt extends BasicBolt {
  constructor() {
    super()
    this.headers = {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14'
    }
    this.maxThreads = 10
    this.threads = 0
    this.queue = []
  }

  process(tup, done) {
    var self = this

    if (tup.values[2] == null) {
      self.emit({tuple: ['poll'], stream: 'requestStream', anchorTupleId: tup.id}, (taskIds) => {})
      done();
    } else if (self.threads < self.maxThreads) {
      self.crawl(self, tup, done);
    } else {
      self.queue.push([tup, done]);
    }
  }

  crawl(self, tup, done) {
    self.threads++
    var values = tup.values
    var url = values[2]
    let option = {
      url: url,
      headers: self.headers,
      proxy: 'http://192.168.128.40:5432',
    }

    request(option, (err, res, body) => {
      if (!err && res.statusCode == 200) {
        values[3] = body
        self.emit({tuple: values, stream: 'documentStream', anchorTupleId: tup.id}, (taskIds) => {})
      } else {
        self.emit({tuple: values, stream: 'updateStream', anchorTupleId: tup.id}, (taskIds) => {
          // self.log(`Error:${url} is missing\n${err}`)
        })
      }
      self.threads--
      if (self.queue.length > 0) {
        let [t, d] = self.queue.shift()
        self.crawl(self, t, d)
      } else {
        self.emit({tuple: ['poll'], stream: 'requestStream', anchorTupleId: tup.id}, (taskIds) => {})
      }
      done();
    })
  }
}

new CrawlerBolt().run()
