require 'nokogiri'

module FocusedCrawler
  class Parser
    include STATE

    def run
      return unless prepared?

      busy
      parse
      wait
    end

    def parse
      documents.each do |document|
        next unless document.related?
        document.save
      end
    end

    def prepared?
      ready unless Dir.glob('pages/*').empty? || busy?
      ready?
    end

    private

    def documents
      Dir.glob('pages/*').map do |path|
        document = File.read path
        File.delete path
        nokogiri = Nokogiri::HTML.parse document
        Document.new(nokogiri)
      end
    end
  end
end
