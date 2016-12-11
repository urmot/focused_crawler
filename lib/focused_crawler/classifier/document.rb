module FocusedCrawler
  module Classifier
    class Document
      attr_reader :document

      def initialize(document)
        @document = Nokogiri::HTML(document)
      end

      def terms
        @terms ||= Parser.parse(document)
      end

      def size
        @size ||= terms.size
      end
      alias length size

      def count_terms
        @term_count ||= Parser.count terms
      end

      def links
        @link ||= @document.css('a').map {|a| a[:href] }
      end
    end
  end
end
