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
      seed_urls = { url: 'http://yahoo.cp.jp', score: 0 }.to_json
      File.write 'urls/seed.json', seed_urls
    end

    def prepared?
      !Dir.glob('urls/*.json').empty? # && related words does set?
    end

    def crawl
      Thread.new do
        loop do
          sleep 3 unless @crawler.prepared?
          @crawler.run
          break if finish?
        end
      end
    end

    def parse
      Thread.new do
        loop do
          sleep 3 unless @parser.prepared?
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
