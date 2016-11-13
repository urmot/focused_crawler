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
      state == waiting?
    end

    private

    def idle
      @state = IDLE
    end

    def ready
      @state = READY
    end

    def busy
      warn 'warning: You change state to busy not from ready.' unless ready?
      @state = BUSY
    end

    def wait
      warn 'warning: You change state to wait not from busy.' unless busy?
      @state = WAIT
    end
  end
end