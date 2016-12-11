require 'nokogiri'
require 'json'
require 'fast_stemmer'

module FocusedCrawler
  module Classifier
    class Parser
      class << self
        def parse(document)
          filtering terms(document)
        end

        def count(terms)
          terms.each_with_object(Hash.new(0.0)) do |token, index|
            index[token] += 1
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
          terms - stopwords
        end

        def stemming(terms)
          terms.map!(&:stem)
        end

        def stopwords
          @@stopwords ||= File.open('data/stopwords.json') do |file|
            JSON.load file
          end
        end
      end
    end
  end
end
