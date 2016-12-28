module FocusedCrawler
  class Query
    def initialize(db, opt = {})
      @db = db
      @opt = opt
      @query = ''
    end

    def <<(query)
      @query << query
      self
    end

    def execute
      @query << ';'
      return @db.close_on_exec @query if @opt[:after_close]
      @db.execute @query
    end

    def where(col, val)
      @query << " where #{col} = #{val.is_a?(String) ? @db.escape(val) : val}"
      self
    end

    def limit(num)
      @query << " limit #{num}"
      self
    end

    def order_by(columns)
      @query << " order by #{columns.map do |col, order|
        "#{col} #{order}" << (order == 'is null asc' ? ",#{col} asc" : '')
      end.join(',')}"
      self
    end

    def duplicate_key_update(changes)
      @query << 'on duplicate key update '
      @query << changes.map {|key, value| "#{key}=#{value}" }.join(',')
      self
    end
  end
end
