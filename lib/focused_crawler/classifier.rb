module FocusedCrawler
  class Classifier
    attr_accessor :queue

    def initialize(target, writer)
      @target = target
      @writer = writer
      @queue = Queue.new
    end

    def run
      Thread.start do
        loop do
          doc = queue.pop
          break if doc == :stop
          write [doc.url, doc.links, similarity(doc)]
        end
        @writer.close
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

    private

    def write(object)
      @writer.puts object.to_json
    end
  end
end
