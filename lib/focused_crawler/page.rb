require 'open-uri'
require 'digest/sha1'

module FocusedCrawler
  class Page
    attr_reader :url

    def initialize(url)
      @url = url
    end

    def save
      File.write path, page
    end

    def page
      @page ||= open(url) {|f| f.read.encode Encoding::UTF_8, f.charset }
    end

    def crawled?
      File.exist? path
    end

    def path
      return @path unless @path.nil?
      filename = Digest::SHA1.hexdigest url
      @path ||= File.join directory, filename
    end

    def directory
      File.absolute_path 'pages'
    end
  end
end
