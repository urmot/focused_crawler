require 'json'
require 'matrix'

module FocusedCrawler
  class IDF
    @index = []
    @lexicon = []
    @idf_base_docs = 33_600
    @default_idf = Math.log(@idf_base_docs)

    class << self
      def [](tid)
        pos = @index.bsearch_index {|i| tid - i }
        return default_idf if pos.nil?
        @lexicon[pos]
      end

      def default_idf
        @default_idf
      end

      def index
        return @index unless @index.empty?
        @index, @lexicon = load_lexicon
        @index
      end

      def lexicon
        return @lexicon unless @lexicon.empty?
        @index, @lexicon = load_lexicon
        @lexicon
      end

      def load_lexicon
        open lexicon_path do |f|
          idf = f.lazy.map {|line| JSON.parse line }.to_h
          [idf.keys, idf.values]
        end
      end

      def lexicon_path
        'data/idf.json'
      end
    end
  end
end
