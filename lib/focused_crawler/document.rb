module FocusedCrawler
  class Document
    attr_reader :document, :path

    def initialize(path)
      @path = path
      @document = Nokogiri::HTML.parse(File.read(path))
    end
  end
end
