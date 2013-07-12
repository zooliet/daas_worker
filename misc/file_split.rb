
def split_file(name, chunk_size = 1000000)
  chunks = []
  i = 0
  File.open(name, 'r') do |f|

    size = f.size        

    while size > chunk_size do
      chunks[i] = f.read(chunk_size) 
      size -= chunk_size
      i += 1
    end
    
    chunks[i] = f.read(chunk_size)         
  end
  
  chunks      
end    

