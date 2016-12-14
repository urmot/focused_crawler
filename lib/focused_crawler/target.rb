require 'matrix'

module FocusedCrawler
  class Target
    def initialize(documents)
      @documents = documents
    end

    def terms
      @terms ||= @documents.flat_map(&:terms)
    end

    def count_terms
      @count_terms ||= Parser.count(terms).sort_by {|tid, _| tid }
    end

    def tf_idf_index
      @tf_idf_index ||= count_terms.to_h.keys.unshift(0)
    end

    def tf_idf_values
      @tf_idf_values ||= count_terms.map do |tid, count|
        count / terms.length * idf(tid)
      end.unshift(0)
    end

    def idf(tid)
      IDF[tid]
    end

    def tf_idf(tid)
      tf_idf_values[position(tid)]
    end

    def position(tid)
      tf_idf_index.bsearch_index {|i| tid - i }.to_i
    end

    def norm
      @norm ||= Vector.elements(tf_idf_values, false).norm
    end

    def adjust(other_vector) # return a normalized vector
      Vector.elements(other_vector.map! {|tid| tf_idf(tid) }, false) / norm
    end
  end
end
