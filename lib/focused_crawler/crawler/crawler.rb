require 'open_uri_redirections'
require 'openssl'

module FocusedCrawler
  class Crawler
    def initialize(classifier, opt = {})
      @classifier = classifier
      @queue = Queue.new
      @opt = {
        'User-Agent' => user_agent,
        allow_redirections: :safe,
        ssl_verify_mode: OpenSSL::SSL::VERIFY_NONE
      }.merge!(opt)
    end

    def run(url)
      Thread.start(url) do |u|
        page = crawl(u)
        Thread.exit if page.nil?
        @classifier.run Document.new(url: u, doc: page)
      end
    end

    def crawl(url)
      open url, @opt do |f|
        f.read.encode! 'UTF-8', f.charset, invalid: :replace, undef: :replace
      end
    rescue => e
      warn "#{e.message} `#{url}`"
    end

    def user_agent
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1)' \
      'AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14'
    end
  end
end
