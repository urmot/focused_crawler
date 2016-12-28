require 'test_helper'

class IDFTest < Minitest::Test
  def setup
    SQLite3::Database.new('test/idf.db') do |db|
      db.execute 'create table idf(tid primary key, idf)'
      db.execute 'insert into idf values (1, 0.1)'
      db.execute 'insert into idf values (2, 0.2)'
      db.execute 'insert into idf values (3, 0.3)'
    end
    path = Minitest::Mock.new
    10.times { path = path.expect :call, 'test/idf.db' }
    @path = path
  end

  def teardown
    File.delete 'test/idf.db' if File.exist? 'test/idf.db'
  end

  def test_that_it_should_return_default_idf
    n = FocusedCrawler::IDF.idf_base_docs
    default_idf = Math.log(n)
    assert_equal default_idf, FocusedCrawler::IDF.default_idf
  end

  def test_that_it_should_get_idf_of_term
    FocusedCrawler::IDF.stub :lexicon_path, @path do
      assert_equal 0.1, FocusedCrawler::IDF[1]
    end
  end

  def test_that_it_should_return_default_idf_when_term_do_not_indexed
    FocusedCrawler::IDF.stub :lexicon_path, @path do
      idf = FocusedCrawler::IDF[tid('test')]
      assert_equal FocusedCrawler::IDF.default_idf, idf
    end
  end
end
