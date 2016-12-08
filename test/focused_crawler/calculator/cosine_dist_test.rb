require 'test_helper'

class CosineDistTest < Minitest::Test
  def setup
    @cosine_dist = FocusedCrawler::Calculator::CosineDist
  end

  def test_that_it_should_return_float_value_distination_of_v1_to_v2
    v1 = Vector[1, 2, 3]
    v2 = Vector[2, 3, 4]
    assert_instance_of Float, @cosine_dist[v1, v2]
  end

  def test_that_it_should_bigger_near_one_then_far_one
    v1 = Vector[1, 2, 3]
    v2 = Vector[1, 2, 4]
    v3 = Vector[7, 7, 7]
    v1_2 = @cosine_dist[v1, v2]
    v1_3 = @cosine_dist[v1, v3]
    puts v1_2
    puts v1_3
    assert_equal true, v1_2 > v1_3
  end

  def test_that_it_should_return_nil_when_v1_size_is_deffrent_v2
    v1 = Vector[1, 2, 3]
    v2 = Vector[1, 3, 4, 4]
    assert_nil @cosine_dist[v1, v2]
  end
end
