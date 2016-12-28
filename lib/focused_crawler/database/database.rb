require 'mysql2'

module FocusedCrawler
  class Database
    def initialize(table, opt = {})
      @table = table
      @opt = opt
      @db = connect
    end

    def insert(columns, data)
      query = "insert into #{@table} (#{columns.join(',')}) values ("
      query << data.map do |row|
        columns.map do |col|
          row[col].is_a?(String) ? escape(row[col]) : row[col]
        end.join(',')
      end.join('),(') << ')'
      Query.new(duplicate, after_close: true) << query
    end

    def update(values)
      query = "update #{@table} set "
      query << values.map do |col, val|
        "#{col}=#{val}"
      end.join(',')
      Query.new(duplicate, after_close: true) << query
    end

    def select(columns = [:*])
      query = "select #{columns.join(',')} from #{@table}"
      Query.new(self) << query
    end

    def delete
      execute "delte from #{@table}"
    end

    def execute(query)
      result = @db.query query, symbolize_keys: true
      @opt[:flags] == 65_536 ? multi_result : result
    end

    def close_on_exec(query)
      Thread.start(query) do |q|
        begin
          @db.query q
          close
        rescue => e
          warn e.inspect
        end
      end
    end

    def escape(str)
      "'#{@db.escape(str)}'"
    end

    def close
      @db.close
    end

    private

    def duplicate
      self.class.new(@table)
    end

    def multi_result
      while @db.next_result do end
    end

    def connect
      Mysql2::Client.new({
        host:     'localhost',
        username: 'root',
        database: 'focused_crawler'
      }.merge!(@opt))
    end
  end
end
