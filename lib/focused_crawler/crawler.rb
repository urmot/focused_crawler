require 'json'

module FocusedCrawler
  class Crawler
    include STATE

    def run
      return unless prepared?

      busy
      crawl
      wait
    end

    def crawl
      pages.each do |page|
        next if page.crawled?
        page.save
      end
    end

    def prepared?
      ready unless Dir.glob('urls/*').empty? || busy?
      ready?
    end

    private

    def pages
      scored_urls = Dir.glob('urls/*').map do |path|
        json_urls = File.read path
        File.delete path
        JSON.parse json_urls
      end.flatten
      sort(scored_urls).map {|url| Page.new(url['url']) }
    end

    def sort(scored_urls)
      scored_urls.uniq {|url| url['url'] }.sort_by {|url| url['score'] }
    end
  end
end
