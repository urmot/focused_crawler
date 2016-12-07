require 'json'

module FocusedCrawler
  class Crawler
    include STATE

    def run
      return wait unless prepared?

      busy
      crawl
    end

    def crawl
      pages.each do |page|
        next if page.crawled?
        page.save
      end
    end

    def prepared?
      !Dir.glob('links/*.json').empty?
    end

    private

    def pages
      scored_links = Dir.glob('links/*').map do |path|
        json_links = File.read path
        File.delete path
        JSON.parse json_links
      end.flatten
      sort(scored_links).map {|url| Page.new(url['url']) }
    end

    def sort(scored_links)
      scored_links.uniq {|url| url['url'] }.sort_by {|url| url['score'] }
    end
  end
end
