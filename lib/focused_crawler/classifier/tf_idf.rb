require 'json'
require 'matrix'
require 'digest/murmurhash'

module FocusedCrawler
  module Classifier
    class TFIDF
      def initialize
        @index = []
        @lexicon = []
        @idf_base_docs = 33_600
        before_action
      end

      def [](document)
        vtf(document).map2(vidf(document)) do |tf, idf|
          tf * idf
        end
      end

      def vtf(document)
        Vector.elements(document.count_terms.values, false) / document.length
      end

      def vidf(document)
        idfs = document.count_terms.keys.map! { |term| idf(term) }
        Vector.elements(idfs, false)
      end

      def idf(term)
        tid = tid(term)
        pos = @index.bsearch_index {|i| tid - i }
        return default_idf if pos.nil?
        @lexicon[pos]
      end

      def tid(term)
        Digest::MurmurHash1.rawdigest term
      end

      def default_idf
        @default_idf ||= Math.log(@idf_base_docs)
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
