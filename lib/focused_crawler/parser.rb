require 'nokogiri'

module FocusedCrawler
  class Parser
    include STATE

    def initialize
      @classifier = Classifier.new
      # @distiller = Distiller.new
    end

    def run
      return unless prepared?

      busy
      parse
      wait
    end

    def parse
      documents.each do |document|
        @classifier.classify document
        # @distiller.distill document
      end
    end

    def prepared?
      ready unless Dir.glob('pages/*').empty? || busy?
      ready?
    end

    private

    def documents
      Dir.glob('pages/*').map do |path|
        Document.new(path)
      end
    end

    def save(links)
      filename = Digest::SHA1.hexdigest links.first[:url]
      File.write "links/#{filename}.json", links.to_json
    end
  end
end
