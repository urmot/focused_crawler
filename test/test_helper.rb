$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'focused_crawler'

require 'minitest/autorun'
require 'minitest/doc_reporter'
require 'minitest/power_assert'
require 'minitest/stub_any_instance'

module Minitest
  class Test
    def in_test_links
      content = Array.new(3) do |i|
        { url: "http://test#{i}.com", score: i }
      end.to_json
      tempfile = Tempfile.new(['', '.json'], 'links')
      File.write tempfile.path, content
      yield
      tempfile.close!
    end

    def in_test_pages
      tempfiles = Array.new(3) do |i|
        tempfile = Tempfile.new('', 'pages')
        File.write tempfile.path, "test page #{i}"
        tempfile
      end
      yield
      tempfiles.each(&:close!)
    end
  end
end
