require 'nokogiri'
require 'json'
require 'lingua/stemmer'

module FocusedCrawler
  class Parser
    def initialize
      @stemmer = Lingua::Stemmer.new
    end

    def parse(doc)
      count filtering(split(doc))
    end

    def count(tokens)
      number_of_terms = 0
      term_count_index = tokens.each_with_object(Hash.new(0.0)) do |token, index|
        index[token] += 1
        number_of_terms += 1
      end
      {
        terms:  term_count_index.keys,
        counts: term_count_index.values,
        sum:    number_of_terms
      }
    end

    def split(doc)
      Nokogiri.HTML(doc).css('body').inner_text.scan(/[\p{Alpha}\-']+/).map!(&:downcase)
    end

    def filtering(terms)
      terms.reject! {|term| term.size > 32 }
      stemming stopwords_filtering(terms)
    end

    def stopwords_filtering(terms)
      terms - stopwords
    end

    def stemming(terms)
      terms.map! {|term| @stemmer.stem term }
    end

    def stopwords
      @stopwords ||= File.open('data/stopwords.json') {|file| JSON.load file }
    end
  end
end
