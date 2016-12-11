module FocusedCrawler
  module Classifier
    class CosineDist
      class << self
        def [](v1, v2)
          return if v1.size != v2.size
          v1.normalize.dot(v2.normalize)
        end
      end
    end
  end
end
