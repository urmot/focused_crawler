module FocusedCrawler
  class FrontierManager
    def initialize(crawler)
      @crawler = crawler
      @crawls = Database.new('crawls')
      @max_threads = 4
    end

    def start
      loop do
        jobs(@max_threads).map! do |job|
          next unless job[:numtries].zero?
          { oid: job[:oid], th: @crawler.run(job[:url]) }
        end.each do |job|
          error_processing job[:oid] if job[:th].value.nil?
        end
        update_serverload
      end
    end

    def update_serverload
      crawls = Database.new('crawls')
      query = "update crawls,
      (select sid, sum(numtries) count from crawls
      where numtries >= 1 group by sid) A set
      serverload = A.count where crawls.sid = A.sid;"
      crawls.close_on_exec query
    end

    def error_processing(oid)
      @crawls.update(numtries: :null).where(:oid, oid).execute
    end

    def jobs(num)
      @crawls.select.order_by(
        numtries: 'is null asc',
        relevance: :desc,
        serverload: :asc
      ).limit(num).execute.to_a
    end
  end
end
