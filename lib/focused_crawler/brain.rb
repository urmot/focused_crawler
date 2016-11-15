module FocusedCrawler
  class Brain
    def initialize
      @crawler = Crawler.new
      @parser = Parser.new
    end

    def start
      prepare
      crawl
      parse
    end

    def prepare
      return if prepared?
      # TODO: Add prepare related words
      seed_links = { url: 'http://yahoo.co.jp', score: 0 }.to_json
      File.write 'links/seed.json', seed_links
    end

    def prepared?
      !Dir.glob('links/*.json').empty? # && related words does set?
    end

    def crawl
      Thread.new do
        loop do
          @crawler.run
          break if finish?
        end
      end
    end

    def parse
      Thread.new do
        loop do
          @parser.run
          break if finish?
        end
      end
    end

    def finish?
      @crawler.waiting? && @parser.waiting?
    end
  end
end
