module FocusedCrawler
  module STATE
    IDLE = 0
    READY = 1
    BUSY = 2
    WAIT = 3

    attr_reader :state

    def initialize
      idle
    end

    def idling?
      state == IDLE
    end

    def ready?
      state == READY
    end

    def busy?
      state == BUSY
    end

    def waiting?
      state == WAIT
    end

    private

    def idle
      @state = IDLE
    end

    def ready
      @state = READY
    end

    def busy
      @state = BUSY
    end

    def wait
      @state = WAIT
    end
  end
end
