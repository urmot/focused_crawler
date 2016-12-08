module FocusedCrawler
  module Calculator
    class CosineDist
      def self.[](v1, v2)
        return if v1.size != v2.size
        v1.dot(v2) / (v1.norm * v2.norm)
      end
    end
  end
end
