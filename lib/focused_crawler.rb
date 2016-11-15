require 'focused_crawler/version'
require 'focused_crawler/state'
require 'focused_crawler/brain'
require 'focused_crawler/crawler'
require 'focused_crawler/page'
require 'focused_crawler/parser'
require 'focused_crawler/document'
require 'focused_crawler/classifier'
require 'focused_crawler/distiller'

module FocusedCrawler
  def self.run
    Brain.new.start
  end
end
