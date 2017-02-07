storm = require('./storm')
var BasicBolt = storm.BasicBolt
var url = require('url')
var path = require('path')
var fs = require('fs-extra')

class WriteFileBolt extends BasicBolt {
  constructor() {
    super()
    this.basedir = '/Users/mura/Documents/playground/storm/focused_crawler/cocproxy/files'
  }

  process(tup, done) {
    var self = this
    var u = url.parse(tup.values[2])
    if (u.path == '/') {
      let p = path.join(self.basedir, 'root', u.host)
      fs.writeFile(p, tup.values[3], done)
    } else {
      var p = path.join(self.basedir, u.host, path.dirname(u.path))
      fs.mkdirs(p, (err) => {
        fs.writeFile(path.join(p, path.basename(u.path)), tup.values[3], done)
      })
    }
  }
}

new WriteFileBolt().run()
