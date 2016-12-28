module FocusedCrawler
  class Classifier
    attr_accessor :queue

    def initialize(target)
      @target = target
      @crawls = Database.new('crawls')
      @links = Database.new('links')
    end

    def run(doc)
      doc.relevance = similarity(doc)
      update doc
      insert rows(doc) unless doc.links.empty? || doc.relevance < threshold
    end

    def update(doc)
      @links.update(wgt_fwd: doc.relevance)
            .where(:oid_dst, doc.url[:oid]).execute
      @crawls.update(relevance: doc.relevance, numtries: 'numtries + 1')
             .where(:oid, doc.url[:oid]).execute
    end

    def insert(rows)
      @crawls.insert([:oid, :sid, :url, :relevance], rows[:crawls])
             .duplicate_key_update(oid: :oid).execute
      @links.insert(rows[:links].first.keys, rows[:links]).execute
    end

    def threshold
      0.2
    end

    def rows(doc)
      doc.links.each_with_object(crawls: [], links: []) do |link, hash|
        link[:wgt] = doc.relevance - Random.rand(0.01)
        hash[:crawls] << link.merge!(relevance: link[:wgt])
        hash[:links] << link(doc.url.merge!(wgt: doc.relevance), link)
      end
    end

    def link(src, dst)
      {
        oid_src: src[:oid], oid_dst: dst[:oid],
        sid_src: src[:sid], sid_dst: dst[:sid],
        wgt_rev: src[:wgt], wgt_fwd: dst[:wgt]
      }
    end

    def similarity(document)
      tf_idf = document.tf_idf
      cos_dist(
        tf_idf.normalize,
        @target.adjust(document.index),
        normalized: true
      )
    rescue
      return -1
    end

    def cos_dist(v1, v2, opt = {})
      return -1 if v1.size != v2.size
      return v1.dot(v2) if opt[:normalized]
      v1.normalize.dot(v2.normalize)
    end
  end
end
