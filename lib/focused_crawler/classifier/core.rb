require 'focused_crawler/classifier/tf_idf'
require 'focused_crawler/classifier/cosine_dist'
require 'focused_crawler/classifier/parser'
require 'focused_crawler/classifier/document'

module FocusedCrawler
  module Classifier
    class Core
      def initialize(target, io)
        @tf_idf = TFIDF.new
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
        tf_idf = @tf_idf[document]
        CosineDist[tf_idf[:tf_idf], @target.adjust(result[:terms])]
      end
    end
  end
end
