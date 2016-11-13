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

  def test_that_it_should_get_urls_from_urls_directory
    urls_example
    assert { 3 == @crawler.pages.size }
  end

  def test_that_it_should_return_uniq_url_list
    urls_example
    urls_example
    assert { 3 == @crawler.pages.size }
  end

  def test_that_it_should_return_sorted_url_list_by_score
    urls_example
    pages = @crawler.pages
    expect = 3.times {|i| FocusedCrawler::Page.new("http://test#{i}.com") }
    assert expect, pages
  end

  def test_that_it_shoud_be_falsly_when_crawler_is_not_ready
    Dir.glob('urls/*').each {|path| File.delete path }
    assert { false == @crawler.ready? }
  end

  def test_that_it_should_be_truly_when_crawler_is_ready
    urls_example
    assert { true == @crawler.ready? }
  end

  def test_that_it_should_write_pages_at_urls
    urls_example
    FocusedCrawler::Page.stub_any_instance(:page, 'test') do
      @crawler.crawl
      pages = Dir.glob('pages/*')
      assert { 3 == pages.size }
    end
  end

  def urls_example
    content = Array.new(3) do |i|
      { url: "http://test#{i}.com", score: i }
    end.to_json
    tempfile = Tempfile.new(['', '.json'], 'urls')
    File.write tempfile.path, content
  end
end
