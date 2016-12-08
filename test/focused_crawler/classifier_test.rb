require 'test_helper'

class ClassifierTest < Minitest::Test
  def setup
    threshold = 0.5
    document = Minitest::Mock.new.expect(:document, 'test document')
    document = document.expect(:path, 'pages/test')
    @classifier = FocusedCrawler::Classifier.new(threshold)
    @classifier.instance_variable_set :@document, document
  end

  def test_that_it_should_be_truly_when_similarity_is_over_threshold
    @classifier.stub(:similarity, 0.6) do
      assert { true == @classifier.related? }
    end
  end

  def test_that_it_should_be_falsly_when_similarity_is_under_thereshold
    @classifier.stub(:similarity, 0.4) do
      assert { false == @classifier.related? }
    end
  end
end
