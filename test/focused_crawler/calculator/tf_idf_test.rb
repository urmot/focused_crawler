require 'test_helper'

class TFIDFTest < Minitest::Test
  def test_that_it_should_create_instance_of_index
    in_test_idf do |tf_idf|
      index = tf_idf.instance_variable_get :@index
      assert_equal [1, 2, 3], index
    end
  end

  def test_that_it_should_create_instance_of_lexicon
    in_test_idf do |tf_idf|
      lexicon = tf_idf.instance_variable_get :@lexicon
      assert_equal [0.1, 0.2, 0.3], lexicon
    end
  end

  def test_that_it_should_return_tid_hashed_term
    in_test_idf do |tf_idf|
      tid = tf_idf.tid(%w(test hogehoge fugafugafuga).sample)
      assert_equal 8, tid.size
    end
  end

  def test_that_it_should_return_default_idf
    in_test_idf do |tf_idf|
      n = tf_idf.instance_variable_get :@number_of_docs
      default_idf = Math.log(n)
      assert_equal default_idf, tf_idf.default_idf
    end
  end

  def test_that_it_should_get_idf_of_term
    in_test_idf do |tf_idf|
      tf_idf.stub :tid, 1 do
        assert_equal 0.1, tf_idf.idf('test')
      end
    end
  end

  def test_that_it_should_return_default_idf_when_term_do_not_indexed
    in_test_idf do |tf_idf|
      assert_equal tf_idf.default_idf, tf_idf.idf('test')
    end
  end

  def test_that_it_should_return_vector_for_idf_of_terms
    in_test_idf do |tf_idf|
      tid = 3.times.inject(Minitest::Mock.new) do |mock, i|
        mock.expect(:call, i + 1, ["test#{i}"])
      end
      tf_idf.stub :tid, tid do
        assert_equal Vector[0.1, 0.2, 0.3], tf_idf.idfs(%w(test0 test1 test2))
      end
    end
  end

  def test_that_it_should_return_vector_for_tf_of_terms
    in_test_idf do |tf_idf|
      terms = { counts: [3.0, 1.0, 5.0], sum: 9 }
      tf = tf_idf.tfs(terms)
      assert_equal Vector[3.0 / 9, 1.0 / 9, 5.0 / 9], tf
    end
  end

  def test_that_it_should_get_tf_idf_of_terms_in_document
    doc = 'it is test document is it'
    terms = doc.split(' ').uniq!
    result = { terms: terms, counts: [2.0, 2.0, 1.0, 1.0], sum: 6 }
    parser = Minitest::Mock.new.expect :parse, result, [doc]
    in_test_idf do |tf_idf|
      tf_idf.instance_variable_set :@parser, parser
      idf = tf_idf.default_idf
      expect = Vector[2.0*idf/6,2.0*idf/6,1.0*idf/6,1.0*idf/6]
      assert_equal expect, tf_idf.tf_idf(doc)
    end
  end

  def in_test_idf
    file = Tempfile.new(%w(idf json), 'data')
    File.write file.path, "[1, 0.1]\n[2, 0.2]\n[3, 0.3]"
    FocusedCrawler::Calculator::TFIDF.stub_any_instance :lexicon_path, file.path do
      tf_idf = FocusedCrawler::Calculator::TFIDF.new
      yield tf_idf
    end
    file.close!
  end
end
