require 'test_helper'
require 'json'

class CrawlerTest < Minitest::Test
  def setup
    @crawler = FocusedCrawler::Crawler.new
  end

  def teardown
    Dir.glob('pages/*').each do |path|
      File.delete path
    end
  end

  def test_that_it_shoud_be_falsly_when_crawler_is_not_ready
    Dir.glob('links/*.json').each {|path| File.delete path }
    assert { false == @crawler.prepared? }
  end

  def test_that_it_should_be_truly_when_crawler_is_ready
    in_test_links do
      assert { true == @crawler.prepared? }
    end
  end

  def test_that_it_should_write_pages_at_links
    in_test_links do
      FocusedCrawler::Page.stub_any_instance(:page, 'test') do
        @crawler.crawl
        pages = Dir.glob('pages/*')
        assert { 3 == pages.size }
      end
    end
  end
end
