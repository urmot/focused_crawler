require 'test_helper'

class TFIDFTest < Minitest::Test
  def in_test_idf
    file = Tempfile.new(%w(idf json), 'data')
    File.write file.path, "[1, 0.1]\n[2, 0.2]\n[3, 0.3]"
    FocusedCrawler::Classifier::TFIDF.stub_any_instance :lexicon_path, file.path do
      tf_idf = FocusedCrawler::Classifier::TFIDF.new
      yield tf_idf
    end
    file.close!
  end

  def in_test_document
    document = FocusedCrawler::Classifier::Document.new('test')
    count_terms = { 'it' => 3.0, 'is' => 2.0, 'test' => 1.0, 'document' => 1.0 }
    document.stub :count_terms, count_terms do
      document.stub :length, 7 do
        yield document
      end
    end
  end

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
      n = tf_idf.instance_variable_get :@idf_base_docs
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
      in_test_document do |document|
        tid = Minitest::Mock.new.expect(:call, 1, ['it'])
                            .expect(:call, 2, ['is'])
                            .expect(:call, 3, ['test'])
                            .expect(:call, 4, ['document'])
        tf_idf.stub :tid, tid do
          assert_equal Vector[0.1, 0.2, 0.3, tf_idf.default_idf], tf_idf.vidf(document)
        end
      end
    end
  end

  def test_that_it_should_return_vector_for_tf_of_terms
    in_test_idf do |tf_idf|
      document = FocusedCrawler::Classifier::Document.new('test')
      count_terms = { term1: 3.0, term2: 1.0, term3: 5.0 }
      document.stub(:count_terms, count_terms) do
        document.stub(:length, 9) do
          tf = tf_idf.vtf(document)
          assert_equal Vector[3.0 / 9, 1.0 / 9, 5.0 / 9], tf
        end
      end
    end
  end

  def test_that_it_should_get_tf_idf_of_terms_in_document
    document = FocusedCrawler::Classifier::Document.new('test')
    count_terms = { 'it' => 2.0, 'is' => 2.0, 'test' => 1.0, 'document' => 1.0 }
    in_test_idf do |tf_idf|
      document.stub :count_terms, count_terms do
        document.stub :length, 6 do
          idf = tf_idf.default_idf
          expect = Vector[2.0*idf/6,2.0*idf/6,1.0*idf/6,1.0*idf/6]
          assert_equal expect, tf_idf[document]
        end
      end
    end
  end
end
