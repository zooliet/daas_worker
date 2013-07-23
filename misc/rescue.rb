


def do_something
  File.open('nothing')
  # begin
    # random = rand(4)
    # i = 5/0
    # raise ZeroDivisionError, "Hello I am a random zero division error"  if random == 0
    # raise TypeError, 'You must give me truth' if random == 1
    # raise 'my own error' if random == 2
  # rescue ZeroDivisionError => e
    # p e.message
    # p e.backtrace
  # rescue TypeError => e
  #   p e.message
  #   p e.backtrace
  # rescue Exception => e  
  #   p e.message
  #   p e.backtrace  
  # else
  #   puts "NO EXCEPTION RAISED"
  # ensure
  #   puts "DO THIS IN ANY CASE"
  # end
end


begin
  do_something  
rescue Exception => e
  puts "I will handle your exception: #{e.message}"
else
  puts "END OF DO_TASK"
end


