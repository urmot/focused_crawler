require 'test_helper'

class ParserTest < Minitest::Test
  def setup
    @parser = FocusedCrawler::Parser.new
  end

  def test_that_it_should_be_truly_when_parser_is_ready
    in_test_pages do
      assert { true == @parser.ready? }
    end
  end

  def test_that_it_should_be_falsely_when_parser_is_not_ready
    Dir.glob('pages/*').each {|path| File.delete path }
    assert { false == @parser.ready? }
  end
end
