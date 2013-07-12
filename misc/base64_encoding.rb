
require 'base64'

src_file = ARGV[0]
dest_file = ARGV[1]

file_string = File.open(src_file) {|f| f.read }

converted = Base64.encode64(file_string)

File.open(dest_file, "w+") do |f|
  f.write(Base64.decode64(converted))
end
