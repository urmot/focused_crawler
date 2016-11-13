require 'test_helper'

class PageTest < Minitest::Test
  def setup
    url = 'http://example.com'
    @page = FocusedCrawler::Page.new(url)
  end

  def teardown
    Dir.glob('pages/*').each do |path|
      File.delete path
    end
  end

  def test_that_it_should_be_truly_when_page_already_crawled
    @page.stub(:page, 'test') do
      @page.save
      assert { true == @page.crawled? }
    end
  end

  def test_that_it_should_be_falsly_when_page_still_crawled_yet
    assert { false == @page.crawled? }
  end

  def test_that_it_should_save_page_to_pages_directory
    @page.stub(:page, 'test') do
      @page.save
      assert { true == File.exist?(@page.path) }
    end
  end
end
