require 'test_helper'
require 'nokogiri'

class ParserTest < Minitest::Test
  def setup
    @parser = FocusedCrawler::Parser
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
      tid('it')    => 3.0,
      tid('is')    => 3.0,
      tid('bad')   => 1.0,
      tid('sad')   => 1.0,
      tid('happy') => 1.0
    }
    assert_equal expect, count
  end

  def test_that_it_should_split_document_to_terms
    doc = %(It is a test that it should parse document to terms.)
    terms = @parser.terms(Nokogiri::HTML(doc))
    expect = doc.delete!('.').downcase!.split(' ')
    assert expect, terms
  end

  def test_that_it_should_return_array_when_size_of_argument_array_is_one
    doc = ['example']
    terms = @parser.stemming doc
    assert_instance_of Array, terms
  end

  def test_that_it_should_return_tid_hashed_term
    tid = @parser.tid(%w(test hogehoge fugafugafuga).sample)
    assert_equal 8, tid.size
  end
end
