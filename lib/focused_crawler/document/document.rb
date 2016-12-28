require 'matrix'
require 'nokogiri'

module FocusedCrawler
  class Document
    attr_accessor :relevance

    def initialize(arg)
      @url = arg[:url]
      @document = Nokogiri::HTML.parse arg[:doc]
    end

    def url
      { url: @url, oid: Parser.hash(@url), sid: Parser.hash(server) }
    end

    def server
      @url.match(%r{\Ahttps?://([^/]+)/?})[1]
    end

    def terms
      @terms ||= Parser.parse @document
    end

    def length
      terms.length
    end

    def count_terms
      @count_terms ||= Parser.count terms
    end

    def tf_idf
      idf = IDF.new
      Vector.elements count_terms.map {|tid, count| count / length * idf[tid] }
    ensure
      idf.delete
    end

    def tf_idf_index
      count_terms.keys
    end
    alias index tf_idf_index

    def links
      @links ||= @document.css('a').flat_map do |a|
        next [] if a[:href].nil? || a[:href].length > 500
        next [] unless server = a[:href].match(%r{\Ahttps?://([^/]+)/?})
        {
          url: a[:href],
          oid: Parser.hash(a[:href]),
          sid: Parser.hash(server[1])
        }
      end.sort_by {|link| link[:oid] }
    end
  end
end
