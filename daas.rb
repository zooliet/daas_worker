require 'bunny'
require 'eventmachine'
require 'streamio-ffmpeg'

module DAAS
  module Transcoding
    autoload :Producer,         "transcoding/producer"
    autoload :Consumer,         "transcoding/consumer"
  end
  
  module Replay
    autoload :Producer,         "replay/producer"
    autoload :Consumer,         "replay/consumer"
  end
end

