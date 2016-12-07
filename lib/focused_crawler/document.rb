module FocusedCrawler
  class Document
    attr_reader :document, :path

    def initialize(path)
      @path = path
      @document = Nokogiri::HTML.parse(File.read(path))
    end

    def expand_anchor_texts
      document
    end
  end
end
