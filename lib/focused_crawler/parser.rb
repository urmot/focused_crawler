require 'nokogiri'

module FocusedCrawler
  class Parser
    def parse
      pages.each do |page|
      end
    end

    def ready?
      !Dir.glob('pages/*').empty?
    end

    private

    def pages
      Dir.glob('pages/*').map do |path|
        page = File.read path
        File.delete path
        Nokogiri::HTML page
      end
    end
  end
end
