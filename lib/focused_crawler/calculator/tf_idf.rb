require 'json'
require 'matrix'
require 'digest/murmurhash'

module FocusedCrawler
  module Calculator
    class TFIDF
      def initialize
        @parser = Parser.new
        @number_of_docs = 33_600
        @index = []
        @lexicon = []
        before_action
      end

      def tf_idf(document)
        terms = @parser.parse(document)
        tfs(terms).map2(idfs(terms[:terms])) do |tf, idf|
          tf * idf
        end
      end

      def tfs(terms)
        Vector.elements(terms[:counts], false) / terms[:sum]
      end

      def idfs(terms)
        terms.map! do |term|
          idf(term)
        end
        Vector.elements(terms, false)
      end

      def idf(term)
        tid = tid(term)
        pos = @index.bsearch_index {|i| tid - i }
        return default_idf if pos.nil?
        @lexicon[pos]
      end

      def default_idf
        @default_idf ||= Math.log(@number_of_docs)
      end

      def tid(term)
        Digest::MurmurHash1.rawdigest term
      end

      private

      def before_action
        open lexicon_path do |f|
          f.each do |line|
            row = JSON.parse line
            @index << row.first
            @lexicon << row.last
          end
        end
      end

      def lexicon_path
        File.absolute_path 'data/idf.json'
      end
    end
  end
end
