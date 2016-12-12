require 'test_helper'

class ClassifierTest < Minitest::Test
  def setup
    target = Minitest::Mock.new.expect :new, nil
    writer = Tempfile.new ['writer', '.out'], 'test'
    @classifier = FocusedCrawler::Classifier.new(target, writer)
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

  def test_that_it_should_stop_when_queue_pushed_stop_symbol
    document = Minitest::Mock.new.expect(:url, 'http://example.com')
                             .expect(:links, ['http://example1.com'])
    @classifier.stub :similarity, 0 do
      @classifier.queue.push(document)
      @classifier.queue.push :stop
      assert_equal 0, @classifier.run
    end
  end

  def test_that_it_should_write_dump_object_by_writer
    document = Minitest::Mock.new.expect(:nil?, false)
                             .expect(:==, false, [:stop])
                             .expect(:url, 'http://example.com')
                             .expect(:links, ['http://example1.com'])
    @classifier.stub :similarity, 0 do
      @classifier.queue.push :stop
      @classifier.queue.push(document)
      @classifier.run
      writer = @classifier.instance_variable_get :@writer
      object = JSON.parse File.read(writer.path)
      expect = ['http://example.com', ['http://example1.com'], 0]
      assert_equal expect, object
    end
  end
end
