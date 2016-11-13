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
      pages.each do |page|
      end
    end

    def prepared?
      ready unless Dir.glob('pages/*').empty? || busy?
      ready?
    end

    private

    def pages
      Dir.glob('pages/*').map do |path|
        page = File.read path
        Nokogiri::HTML page
      end
    end
  end
end
