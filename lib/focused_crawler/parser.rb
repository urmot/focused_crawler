require 'nokogiri'

module FocusedCrawler
  class Parser
    def parse
      pages.each do |page|
      end
    end

    def pages
      Dir.glob('pages/*').map do |path|
        page = File.read path
        Nokogiri::HTML page
      end
    end

    def ready?
      !Dir.glob('pages/*').empty?
    end
  end
end
