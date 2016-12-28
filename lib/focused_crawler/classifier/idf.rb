require 'json'

module FocusedCrawler
  class IDF
    def initialize
      @db = Database.new('idf')
    end

    def [](tid)
      result = @db.select([:idf]).where(:tid, tid).execute.to_a.first
      result.nil? ? default_idf : result[:idf]
    end

    def idf_base_docs
      37_410
    end

    def default_idf
      Math.log idf_base_docs
    end

    def delete
      @db.close
    end
  end
end
