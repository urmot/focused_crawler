require 'json'

module FocusedCrawler
  class Crawler
    def crawl
      urls.each do |url|
        page = Page.new(url)
        next if page.crawled?
        page.save
      end
    end

    def urls
      scored_urls = Dir.glob('urls/*').map do |path|
        json_urls = File.read path
        File.delete path
        JSON.parse json_urls
      end.flatten
      sort(scored_urls).map {|url| url['url'] }
    end

    def ready?
      !Dir.glob('urls/*').empty?
    end

    private

    def sort(scored_urls)
      scored_urls.uniq {|url| url['url'] }.sort_by {|url| url['score'] }
    end
  end
end
