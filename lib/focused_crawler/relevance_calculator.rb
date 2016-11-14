require 'natto'

module FocusedCrawler
  class RelevanceCalculator
    def initialize
      @mecab = Natto::MeCab.new
    end

    def calculate(document)
      @document = document
      cosine_similarity
    end

    def cosine_similarity
    end

    def term_frequency
      @tf ||= counted_words.each_with_object words.count do |(key, value), n|
        hash[key] = value / n
      end
    end
    alias tf term_frequency

    def words
      @words ||= @mecab.enum_parse(document).map(&:surface)
    end

    def counted_words
      @counted_words ||= words.each_with_object Hash.new(0) do |word, hash|
        hash[word] += 1
      end
    end
  end
end
