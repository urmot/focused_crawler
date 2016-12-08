require 'test_helper'

class ParserTest < Minitest::Test
  def setup
    @parser = FocusedCrawler::Parser.new
  end

  def test_that_it_should_remove_stopwords
    terms = %w(I am a Yuta Muramoto)
    expect = @parser.stopwords_filtering(terms)
    assert_equal %w(Yuta Muramoto), expect
  end

  def test_that_it_should_be_stemming_for_each_term
    terms = %w(compliting exterminaly expelience)
    expect = @parser.stemming(terms)
    assert_equal %w(complit exterminali expeli), expect
  end

  def test_that_it_should_stopword_filetering_and_stemming
    terms = %w(I compliting exterminaly expelience for it)
    expect = @parser.filtering(terms)
    assert_equal %w(complit exterminali expeli), expect
  end

  def test_that_it_should_count_term_frequency_for_each_terms
    terms = %w(it is bad it is sad it is happy)
    count = @parser.count(terms)
    expect = {
      terms: %w(it is bad sad happy),
      counts: [3, 3, 1, 1, 1],
      sum: 9
    }
    assert_equal expect, count
  end

  def test_that_it_should_split_document_to_terms
    doc = %(It is a test that it should parse document to terms.)
    terms = @parser.split(doc)
    expect = doc.delete!('.').downcase!.split(' ')
    assert expect, terms
  end

  def test_that_it_should_return_array_when_size_of_argument_array_is_one
    doc = ['example']
    terms = @parser.stemming doc
    assert_instance_of Array, terms
  end
end
