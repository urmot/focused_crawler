module FocusedCrawler
  class Classifier
    attr_accessor :threshold

    def initialize(threshold = 0.5)
      @threshold = threshold
    end

    def classify(document)
      Thread.new do
        @document = document
        File.delete @document.path unless related?
      end
    end

    def related?
      similarity >= threshold
    end

    def similarity
    end
  end
end
