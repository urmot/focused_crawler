require 'test_helper'

class CosineDistTest < Minitest::Test
  def setup
    @cosine_dist = FocusedCrawler::Calculator::CosineDist.new
  end
end
