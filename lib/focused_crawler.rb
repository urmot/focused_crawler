require 'focused_crawler/version'
require 'focused_crawler/classifier/classifier'
require 'focused_crawler/classifier/target'
require 'focused_crawler/classifier/idf'
require 'focused_crawler/distiller/distiller'
require 'focused_crawler/crawler/crawler'
require 'focused_crawler/crawler/frontier_manager'
require 'focused_crawler/document/parser'
require 'focused_crawler/document/document'
require 'focused_crawler/database/database'
require 'focused_crawler/database/query'

module FocusedCrawler
  def self.run
    seeds = JSON.parse open('data/seed.json').read
    crawler = Crawler.new(:_)
    db = Database.new('crawls')
    documents = seeds.map! do |url|
      doc = Document.new(url: url, doc: crawler.crawl(url))
      db.insert([:oid, :url, :relevance],
                [oid: doc.url[:oid], url: doc.url[:url], relevance: 1.0])
        .duplicate_key_update(oid: :oid).execute
      doc
    end
    target = Target.new(documents)
    classifier = Classifier.new(target)
    crawler.instance_variable_set :@classifier, classifier
    FrontierManager.new(crawler).start
  end
end
