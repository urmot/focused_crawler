require 'nokogiri'

module FocusedCrawler
  class Parser
    include STATE

    def initialize
      @classifier = Classifier.new
      @distiller = Distiller.new
    end

    def run
      return wait unless prepared?

      busy
      parse
    end

    def parse
      threads = []
      documents.each do |document|
        threads << @classifier.classify(document)
        threads << @distiller.distill(document)
        threads.each(&:join)
      end
    end

    def prepared?
      !Dir.glob('pages/*').empty? || busy?
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
