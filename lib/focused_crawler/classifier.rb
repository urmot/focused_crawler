
module FocusedCrawler
  class Classifier
    def initialize(target, io)
      @target = target
      @reader, @writer = io
      run
    end

    def run
      fork do
        queue = [] << @reader.read
        loop do
          thread = Thread.start { loop { queue << @reader.read } }
          doc = queue.pop
          next if doc.nil?
          document = Document.new(doc)
          write @writer, [similarity(document), document.links]
          thread.exit.join
        end
      end
    end

    def similarity(document)
      cos_dist(
        document.tf_idf.normalize,
        @target.adjust(document.index),
        normalized: true
      )
    end

    def cos_dist(v1, v2, opt = {})
      return -1 if v1.size != v2.size
      return v1.dot(v2) if opt[:normalized]
      v1.normalize.dot(v2.normalize)
    end
  end
end
