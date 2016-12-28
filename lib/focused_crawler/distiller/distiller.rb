module FocusedCrawler
  class Distiller
    def initialize
      @hubs = Database.new('hubs', flags: 65_536) # 65,536 is multi statements.
      @auth = Database.new('auth', flags: 65_536)
    end

    def distill
      @auth.execute insert_auth
      5.times do
        @hubs.execute update_hubs
        @auth.execute update_auth(0.2)
      end
    end

    def insert_auth
      <<-EOS
      truncate table auth;
      insert into auth(oid, score) select oid, relevance from crawls
      where numtries is not null;
      EOS
    end

    def update_hubs
      <<-EOS
      truncate table hubs;
      insert into hubs(oid, score)
        select oid_src, sum(score * wgt_rev) from auth, links
        where sid_src <> sid_dst and oid = oid_dst group by oid_src;
      update hubs set score = score / (select sum from
      (select sum(score) sum from hubs) as sum);
      EOS
    end


    def update_auth(p)
      <<-EOS
      truncate table auth;
      insert into auth(oid, score)
        select oid_dst, sum(score * wgt_fwd) from hubs, links, crawls
        where sid_src <> sid_dst and hubs.oid = oid_src and oid_dst = crawls.oid
        and relevance > #{p} group by oid_dst;
      update auth set score = score / (select sum from
      (select sum(score) sum from auth) as sum);
      EOS
    end
  end
end
