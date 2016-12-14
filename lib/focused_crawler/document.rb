require 'matrix'

module FocusedCrawler
  class Document
    attr_reader :url, :document

    def initialize(url, document)
      @url = url
      @document = Nokogiri::HTML(document)
    end

    def terms
      @terms ||= Parser.parse(document)
    end

    def size
      terms.size
    end
    alias length size

    def count_terms
      @count_terms ||= Parser.count terms
    end

    def tf_idf
      Vector.elements(count_terms.map {|tid, count|
        count / size * idf(tid)
      }, false)
    end

    def idf(tid)
      IDF[tid]
    end

    def tf_idf_index
      count_terms.keys
    end
    alias index tf_idf_index

    def links
      document.css('a').map {|a| a[:href] }
    end
  end
end
