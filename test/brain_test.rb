require 'test_helper'

class BrainTest < Minitest::Test
  def setup
    @brain = FocusedCrawler::Brain.new
  end

  def teardown
    Dir.glob('pages/*').each {|path| File.delete path }
    Dir.glob('links/*.json').each {|path| File.delete path }
  end

  def test_that_it_should_create_seed_page_links_file_when_links_dir_is_empty
    Dir.glob('links/*.json').each {|path| File.delelte path }
    @brain.prepare
    path = 'links/seed.json'
    assert { true == File.exist?(path) }
  end

  def test_that_it_should_do_nothing_when_crawler_is_prepared
    in_test_links do
      @brain.prepare
      assert { false == File.exist?('links/seed.json') }
    end
  end

  def test_that_it_should_be_finish_when_crawler_and_parser_are_waiting
    in_test_links do
      FocusedCrawler::Page.stub_any_instance(:page, 'test') do
        @brain.start
        assert true
      end
    end
  end
end
