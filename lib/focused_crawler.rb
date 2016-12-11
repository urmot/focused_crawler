require 'focused_crawler/version'
require 'focused_crawler/state'
require 'focused_crawler/classifier/core'
require 'focused_crawler/crawler'
require 'focused_crawler/page'

module FocusedCrawler
  def self.run
    Brain.new.start
  end
end
