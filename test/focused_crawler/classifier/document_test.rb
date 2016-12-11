require 'test_helper'

class DocumentTest < Minitest::Test
  def setup
    @document = FocusedCrawler::Classifier::Document.new(document)
  end

  def test_that_it_should_set_instance_of_nokogiri_element
    assert_instance_of Nokogiri::HTML::Document, @document.document
  end

  def test_that_it_should_get_terms
    terms = %w(test document will document test)
    assert_equal terms, @document.terms
  end

  def test_that_it_should_return_number_of_terms_in_document
    assert_equal 5, @document.size
  end

  def test_that_it_should_count_terms
    count_terms = { 'test' => 2.0, 'document' => 2.0, 'will' => 1.0 }
    assert_equal count_terms, @document.count_terms
  end

  def test_that_it_should_extract_links_in_document
    links = ['http://example1.com', 'http://example2.com']
    assert_equal links, @document.links
  end

  def document
    <<-EOS
    <html>
    <body>
    This is a test document.
    It will use document test.
    <a href='http://example1.com' />
    <a href='http://example2.com' />
    </body>
    </html>
    EOS
  end
end
