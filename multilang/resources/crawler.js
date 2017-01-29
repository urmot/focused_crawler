storm = require('./storm')
var BasicBolt = storm.BasicBolt
var request = require('request')

class CrawlerBolt extends BasicBolt {
  constructor() {
    super()
    this.headers = {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14'
    }
  }

  process(tup, done) {
    this.crawl(this, tup, done)
  }

  crawl(self, tup, done) {
    var values = tup.values
    var url = values[2]
    let option = {
      url: url,
      headers: self.headers,
      proxy: 'http://192.168.128.40:5432'
    }

    request(option, (err, res, body) => {
      if (!err && res.statusCode == 200) {
        values[3] = body
        self.emit({tuple: values, stream: 'documentStream', anchorTupleId: tup.id}, (taskIds) => {})
      } else {
        self.emit({tuple: values, stream: 'updateStream', anchorTupleId: tup.id}, (taskIds) => {
 //         self.log(`Error:${url} is missing\n${err}`)
        })
      }
      done();
    })
  }
}

new CrawlerBolt().run()
