# redis-filemem-cache
Redis and File cache for storing functions indexed by datetime
   
    cache = FileMemCache(namespace="pycache1",filecache=r'z:\cache' )

    @cache.cache_it()
    def test(dateDt, a,b):
        return a+b

    test(datetime.date(2000,1,1), 10, 20)
    
    cache.list_memory()
    cache.list_files()


    
        
        
    cache.list_memory()
    cache.list_files()

    The code is influenced by vivek narayan's  https://github.com/vivekn/redis-simple-cache
    and ohanetz's https://github.com/ohanetz/redis-simple-cache-3k
