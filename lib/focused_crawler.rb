require 'focused_crawler/version'
require 'focused_crawler/state'
require 'focused_crawler/crawler'
require 'focused_crawler/page'
require 'focused_crawler/parser'
require 'focused_crawler/brain'

module FocusedCrawler
  def sef.run
    Brain.new.start
  end
end
