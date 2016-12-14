require 'json'
require 'fast_stemmer'
require 'digest/murmurhash'

module FocusedCrawler
  class Parser
    @stopwords ||= open('data/stopwords.json') {|file| JSON.load file }

    class << self
      def parse(document)
        filtering terms(document)
      end

      def count(terms)
        terms.each_with_object(Hash.new(0.0)) do |term, index|
          index[tid(term)] += 1
        end
      end

      def terms(document)
        document.css('body').inner_text.scan(/[\p{Alpha}\-']+/).map!(&:downcase)
      end

      def filtering(terms)
        terms.reject! {|term| term.size > 32 }
        stemming stopwords_filtering(terms)
      end

      def stopwords_filtering(terms)
        terms - @stopwords
      end

      def stemming(terms)
        terms.map!(&:stem)
      end

      def tid(term)
        Digest::MurmurHash1.rawdigest term
      end
    end
  end
end
