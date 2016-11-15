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
      @path ||= File.join directory, filename
    end

    def directory
      @directory ||= File.absolute_path 'pages'
    end

    def filename
      @filename ||= Digest::SHA1.hexdigest url
    end
  end
end
