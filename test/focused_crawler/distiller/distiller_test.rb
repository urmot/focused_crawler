require 'test_helper'

class DistillerTest < Minitest::Test
  def setup
    threshold = 0.5
    @distiller = FocusedCrawler::Distiller.new(threshold)
    anchor_texts = ['test anchor text']
    @document = Minitest::Mock.new.expect(:expand_anchor_texts, anchor_texts)
  end

  def test_that_it_should_return_related_links
    @distiller.related_links(@document)
  end
end
