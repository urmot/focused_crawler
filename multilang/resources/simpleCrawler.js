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
    this.threads = 0;
    this.blackList = [
      1499803714,
      74553050,
      -2106875771,
      -1138457050,
    ]
  }

  process(tup, done) {
    let sid = tup.values[1]
    if (this.blackList.indexOf(sid) > 0) done() // skip black list
    else this.crawl(this, tup, done)
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
      // proxy: 'http://localhost:5432'
    }

    request(option, (err, res, body) => {
      if (!err && res.statusCode == 200) {
        values[3] = body
        try {
          self.emit({tuple: values, stream: 'documentStream', anchorTupleId: tup.id}, (taskIds) => {})
        } catch(e) {
          self.log(e);
        }
      } else {
        self.emit({tuple: values, stream: 'updateStream', anchorTupleId: tup.id}, (taskIds) => {})
      }
      done()
    })
  }
}

new CrawlerBolt().run()
