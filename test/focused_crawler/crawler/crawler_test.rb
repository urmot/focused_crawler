require 'test_helper'
require 'json'

class CrawlerTest < Minitest::Test
  def setup
    @classifier = Minitest::Mock.new.expect :run, nil
    @crawler = FocusedCrawler::Crawler.new(@classifier)
  end

  def test_that_it_should_push_url_send_through_reader
    thread = @crawler.listen
    @writer.puts 'http://example1.com'
    @writer.puts 'http://example2.com'
    queue = @crawler.instance_variable_get :@queue
    sleep 0.05
    assert_equal 2, queue.size
    thread.exit
  end

  def test_that_it_should_get_url_from_queue
    thread = @crawler.listen
    @writer.puts 'http://example.com1'
    @writer.puts 'http://example.com2'
    queue = @crawler.instance_variable_get :@queue
    assert_equal 'http://example.com1', queue.pop.chomp
    assert_equal 'http://example.com2', queue.pop.chomp
    thread.exit
  end

  def test_that_it_should_stop_running
    @crawler.stub :crawl, '' do
      th = @crawler.schedule
      queue = @crawler.instance_variable_get :@queue
      queue.push 'http://example.com'
      queue.push :stop
      th.join
      assert_equal false, th.status
    end
  end

  def test_that_it_should_set_a_document_to_classifier_queue
    @crawler.stub :page, '' do
      th = @crawler.schedule
      queue = @crawler.instance_variable_get :@queue
      queue.push 'http://example.com'
      th.exit
      assert_instance_of FocusedCrawler::Document, @classifier.queue.pop
    end
  end

  def test_that_it_should_set_stop_symbol_to_classifier_queue
    @crawler.stub :page, '' do
      th = @crawler.schedule
      queue = @crawler.instance_variable_get :@queue
      queue.push :stop
      th.join
      assert_equal :stop, @classifier.queue.pop
    end
  end
end
