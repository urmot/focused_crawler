require 'test_helper'

class TargetTest < Minitest::Test
  def setup
    doc1 = Minitest::Mock.new.expect :terms, %w(it is a test)
    doc2 = Minitest::Mock.new.expect :terms, %w(that it should get terms)
    doc3 = Minitest::Mock.new.expect :terms, %w(in target documents)
    @target = FocusedCrawler::Target.new([doc1, doc2, doc3])
    @count_terms = [
      [tid('it'), 2.0], [tid('is'), 1.0], [tid('a'), 1.0], [tid('that'), 1.0],
      [tid('test'), 1.0], [tid('should'), 1.0], [tid('get'), 1.0], [tid('terms'), 1.0],
      [tid('in'), 1.0], [tid('target'), 1.0], [tid('documents'), 1.0]
    ]
  end

  def test_that_it_should_get_terms_in_target_documents
    terms = %w(it is a test that it should get terms in target documents)
    assert_equal terms, @target.terms
  end

  def test_that_it_should_count_terms_in_target_documents
    assert_equal @count_terms.to_h, @target.count_terms.to_h
  end

  def test_that_it_should_sort_by_tid
    assert_equal @count_terms.sort_by(&:first), @target.count_terms
  end

  def test_that_it_should_return_index_of_tf_idf_values
    index = @count_terms.to_h.keys.sort!
    assert index, @target.tf_idf_index
  end

  def test_that_it_should_unshift_zero_to_tf_idf_index
    assert_equal 0, @target.tf_idf_index.first
  end

  def test_that_it_should_get_tf_idf_values
    @target.stub :idf, 10 do
      once = 1.0 / 12 * 10
      twice = 2.0 / 12 * 10
      values = Array.new(11, once)
      values[7] = twice
      assert_equal values.unshift(0), @target.tf_idf_values
    end
  end

  def test_that_it_should_unshift_zero_to_tf_idf_values
    @target.stub :idf, 10 do
      assert_equal 0, @target.tf_idf_values.first
    end
  end

  def test_that_it_should_tf_idf_for_the_argument_term
    @target.stub :idf, 10 do
      @target.stub :position, 8 do
        twice = 2.0 / 12 * 10
        assert_equal twice, @target.tf_idf(tid('it'))
      end
    end
  end

  def test_that_it_should_return_position_of_tf_idf_values_for_tid
    @target.stub :idf, 10 do
      assert_equal 8, @target.position(tid('it'))
    end
  end

  def test_that_it_should_calculate_norm_of_the_tf_idf_vector
    @target.stub :idf, 10 do
      once = 1.0 / 12 * 10
      twice = 2.0 / 12 * 10
      values = Array.new(11, once)
      values[7] = twice
      norm = Vector.elements(values, false).norm
      assert_equal norm, @target.norm
    end
  end

  def test_that_it_should_adjust_a_vector_for_other_vector
    @target.stub :idf, 10 do
      assert_instance_of Vector, @target.adjust([0])
    end
  end

  def test_that_it_should_adjust_dimension_for_other_vector
    @target.stub :idf, 10 do
      other_vector = [tid('it'), tid('should'), tid('test')]
      assert_equal 3, @target.adjust(other_vector).size
    end
  end

  def test_that_it_should_nomalize_the_adjust_vector
    @target.stub :idf, 10 do
      other_vector = [tid('it'), tid('should'), tid('test')]
      vector = Vector[2.0 / 12 * 10, 1.0 / 12 * 10, 1.0 / 12 * 10] / @target.norm
      assert_equal vector, @target.adjust(other_vector)
    end
  end
end
