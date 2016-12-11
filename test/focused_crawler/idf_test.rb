require 'test_helper'

class IDFTest < Minitest::Test
  def in_test_idf
    file = Tempfile.new(%w(idf json), 'data')
    File.write file.path, "[1, 0.1]\n[2, 0.2]\n[3, 0.3]"
    lexicon_path = Minitest::Mock.new.expect :call, file.path
    FocusedCrawler::IDF.stub :lexicon_path, lexicon_path do
      idf = FocusedCrawler::IDF
      yield idf
    end
    file.close!
  end

  def test_that_it_should_create_instance_of_index
    in_test_idf do |idf|
      assert_equal [1, 2, 3], idf.index
    end
  end

  def test_that_it_should_create_instance_of_lexicon
    in_test_idf do |idf|
      assert_equal [0.1, 0.2, 0.3], idf.lexicon
    end
  end

  def test_that_it_should_return_default_idf
    in_test_idf do |idf|
      n = idf.instance_variable_get :@idf_base_docs
      default_idf = Math.log(n)
      assert_equal default_idf, idf.default_idf
    end
  end

  def test_that_it_should_get_idf_of_term
    in_test_idf do |idf|
      assert_equal 0.1, idf[1]
    end
  end

  def test_that_it_should_return_default_idf_when_term_do_not_indexed
    in_test_idf do |idf|
      assert_equal idf.default_idf, idf[tid('test')]
    end
  end
end
