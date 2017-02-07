storm = require('./storm')
var BasicBolt = storm.BasicBolt
var request = require('request')
var util = require('util')

class CrawlerBolt extends BasicBolt {
  constructor() {
    super()
    this.headers = {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14'
    }
    this.max_threads = 30
    this.threads = 0
    this.queue = []
    this.crawling = {}
    this.threads = 0
    this.blackList = [
      1499803714,
      74553050,
      -2106875771,
      -1138457050,
    ]
  }

  process(tup, done) {
    let sid = tup.values[1]
    if (this.blackList.indexOf(sid) > 0) {
      done() // skip black list
    } else if (sid in this.crawling && this.threads < this.max_threads) {
      this.queue.push([tup, done])
      if (this.queue.length > 1) this.poll(this)
    } else if (this.threads >= this.max_threads) {
      this.queue.push([tup, done])
    } else {
      this.crawling[sid] = true
      this.crawl(this, tup, done)
    }
  }

  poll(self) {
    if (self.queue.length > 0) {
      let index = self.queue.findIndex((e) => {
        return !(e[0].values[1] in self.crawling)
      })
      if (index >= 0 && self.threads < self.max_threads) {
        let [[tup, done]] = this.queue.splice(index)
        self.crawling[tup.values[1]] = true
        self.crawl(self, tup, done)
      }
    }
  }

  crawl(self, tup, done) {
    var values = tup.values
    var url = values[2]
    let option = {
      url: url,
      headers: self.headers,
      agentOptions: {
        securityOptions: 'SSL_OP_NO_SSLv3'
      },
      proxy: 'http://localhost:5432'
    }

    self.threads++
    request(option, (err, res, body) => {
      self.threads--
      self.log("Running threads: " + self.threads);
      if (!err && res.statusCode == 200) {
        values[3] = body
        self.emit({tuple: values, stream: 'documentStream', anchorTupleId: tup.id}, (taskIds) => {})
      } else {
        self.emit({tuple: values, stream: 'updateStream', anchorTupleId: tup.id}, (taskIds) => {})
      }
      setTimeout(() => {
        delete self.crawling[values[1]]
        if (self.threads < self.max_threads) self.poll(self)
      }, 30000)
      self.poll(self)
      done()
    })
  }
}

new CrawlerBolt().run()
