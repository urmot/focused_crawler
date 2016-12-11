require 'test_helper'

class ClassifierTest < Minitest::Test
  def setup
    target = Minitest::Mock.new.expect :new, nil
    FocusedCrawler::Classifier.stub_any_instance :run, nil do
      @classifier = FocusedCrawler::Classifier.new(target, IO.pipe)
    end
  end

  def test_that_it_should_return_float_value_distination_of_v1_to_v2
    v1 = Vector[1, 2, 3]
    v2 = Vector[2, 3, 4]
    assert_instance_of Float, @classifier.cos_dist(v1, v2)
  end

  def test_that_it_should_bigger_near_one_then_far_one
    v1 = Vector[1, 2, 3]
    v2 = Vector[1, 2, 4]
    v3 = Vector[7, 7, 7]
    v1_2 = @classifier.cos_dist(v1, v2)
    v1_3 = @classifier.cos_dist(v1, v3)
    assert_equal true, v1_2 > v1_3
  end

  def test_that_it_should_return_nil_when_v1_size_is_deffrent_v2
    v1 = Vector[1, 2, 3]
    v2 = Vector[1, 3, 4, 4]
    assert_equal -1, @classifier.cos_dist(v1, v2)
  end

  def test_that_it_should_calcurate_dat_only_when_normalized_is_true
    v1 = Vector[1, 2, 3].normalize
    v2 = Vector[1, 2, 4]
    assert_equal v1.dot(v2), @classifier.cos_dist(v1, v2, normalized: true)
  end

  def test_that_it_should_return_similarity_of_document_to_target
    v1 = Vector[1, 2, 3].normalize
    v2 = Vector[1, 2, 3].normalize
    target = Minitest::Mock.new.expect :adjust, v1, [nil]
    @classifier.instance_variable_set :@target, target
    document = Minitest::Mock.new.expect :tf_idf, v2
    document = document.expect :index, nil
    assert_equal v1.dot(v2), @classifier.similarity(document)
  end
end
