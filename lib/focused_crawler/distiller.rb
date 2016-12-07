module FocusedCrawler
  class Distiller
    attr_accessor :threshold

    def initialize(threshold = 0.5)
      # @calc = RelevanceCalculator.new
      @threshold = threshold
    end

    def distill(document)
      Thread.start do
        save related_links(document)
      end
    end

    def related_links(document)
      document.expand_anchor_texts.flat_map do |text|
        similarity = similarity text
        next [] unless related? similarity
        { url: text[:url], score: similarity }
      end
    end

    def related?(similarity)
      similarity >= threshold
    end

    def similarity(text)
      # @calc.calc text
    end

    private

    def save(links)
      filename = Digest::SHA1.hexdigest links.first[:url]
      File.write "links/#{filename}.json", links.to_json
    end
  end
end
