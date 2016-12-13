require 'open_uri_redirections'
require 'openssl'

module FocusedCrawler
  class Crawler
    def initialize(classifier, reader, opt = {})
      @classifier = classifier
      @reader = reader
      @queue = Queue.new
      @opt = {
        'User-Agent' => user_agent,
        allow_redirections: :safe,
        ssl_verify_mode: OpenSSL::SSL::VERIFY_NONE
      }.merge!(opt)
    end

    def run
      listen
      schedule
    end

    def listen
      Thread.start do
        loop do
          job = @reader.gets
          @queue.push job
          break if job == :stop
        end
        @reader.close
      end
    end

    def schedule
      Thread.start do
        loop do
          job = @queue.pop
          crawl job
          break if job == :stop
        end
      end
    end

    def crawl(url)
      Thread.start(url) do |turl|
        result = turl == :stop ? :stop : Document.new(turl, page(turl))
        @classifier.queue.push result
      end
    end

    def page(url)
      open url, @opt do |page|
        doc = page.read
        next doc if page.charset == /UTF(-)?8/i
        doc.encode! 'UTF-8', page.charset, invalid: :replace, undef: :replace
      end
    end

    def user_agent
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1)' \
      'AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14'
    end
  end
end
