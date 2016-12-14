require 'json'
require 'sqlite3'

module FocusedCrawler
  class IDF
    class << self
      def [](tid)
        SQLite3::Database.new(lexicon_path) do |db|
          db.results_as_hash = true
          result = db.execute("select idf from idf where tid = #{tid}").first
          return default_idf if result.nil?
          return result['idf']
        end
      end

      def idf_base_docs
        37_410
      end

      def default_idf
        Math.log(idf_base_docs)
      end

      def lexicon_path
        'data/idf.db'
      end
    end
  end
end
