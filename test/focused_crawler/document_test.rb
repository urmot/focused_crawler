require 'test_helper'

class DocumentTest < Minitest::Test
  def setup
    url = 'http://example.com'
    @document = FocusedCrawler::Document.new(url, document)
  end

  def test_that_it_should_set_instance_of_nokogiri_element
    assert_instance_of Nokogiri::HTML::Document, @document.document
  end

  def test_that_it_should_set_a_instance_variable_of_url
    url = @document.instance_variable_get :@url
    assert_equal 'http://example.com', url
  end

  def test_that_it_should_get_terms
    terms = %w(test document will document test)
    assert_equal terms, @document.terms
  end

  def test_that_it_should_return_number_of_terms_in_document
    assert_equal 5, @document.size
  end

  def test_that_it_should_count_terms
    count_terms = {
      tid('test')     => 2.0,
      tid('document') => 2.0,
      tid('will')     => 1.0
    }
    assert_equal count_terms, @document.count_terms
  end

  def test_that_it_should_extract_links_in_document
    links = ['http://example1.com', 'http://example2.com']
    assert_equal links, @document.links
  end

  def test_that_it_should_return_vector_of_tf_idf_for_terms_in_document
    assert_instance_of Vector, @document.tf_idf
  end

  def test_that_it_should_return_vector_into_tf_idf_values
    idf = Minitest::Mock.new.expect(:call, 10, [tid('test')])
                        .expect(:call, 10, [tid('document')])
                        .expect(:call, 10, [tid('will')])
    FocusedCrawler::IDF.stub :[], idf do
      tf_idf = Vector[2.0 * 10 / 5, 2.0 * 10 / 5, 1.0 * 10 / 5]
      assert_equal tf_idf, @document.tf_idf
    end
  end

  def test_that_it_should_return_index_for_tf_idf_values
    assert_equal [tid('test'), tid('document'), tid('will')], @document.index
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
