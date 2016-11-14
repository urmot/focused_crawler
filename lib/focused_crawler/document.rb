module FocusedCrawler
  class Document
    attr_reader :document

    def initialize(document)
      @document = document
    end

    def related?
      @related ||= Classifier.related? document
    end

    def related_links
      @related_links ||= Distiller.distill document
    end

    def save
      File.write path, related_links.to_json
    end

    def path
      return @path unless @path.nil?
      filename = Digest::SHA1.hexdigest document.title
      @path ||= File.join directory, "#{filename}.json"
    end

    def directory
      File.absolute_path 'urls'
    end
  end
end
