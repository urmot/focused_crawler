storm = require('./storm')
var BasicBolt = storm.BasicBolt
var path = require('path')
var fs = require('fs-extra')

class WriteFileBolt extends BasicBolt {
  constructor() {
    super()
    this.basedir = '/tmp/storm/documents'
  }

  process(tup, done) {
    var self = this
    let p = path.join(self.basedir, path.dirname(tup.values[2]))
    fs.mkdirs(p, (err) => {
      fs.writeFile(path.join(self.basedir, tup.values[2]), tup.values[3])
      done();
    })
  }
}

new WriteFileBolt().run()
