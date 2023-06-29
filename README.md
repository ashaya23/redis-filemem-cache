# redis-filemem-cache
Redis and File cache for storing functions with datetime as one of the params
   
   
   
    import datetime as dt
   
    cache = FileMemCache(namespace="pycache1",filecache=r'z:\cache' )

    @cache.cache_it()
    def test(dateDt, a,b):
        if dateDt >= dt.date(2000,3,1):
           return (a+b) * dateDt.month()
         else:
            return a*b  * dateDt.year()


    test(datetime.date(2000,1,1), 10, 20)
    test(datetime.date(2005,1,1), 10, 20)
                
    cache.list_memory()
    cache.list_files()

    The code is influenced by vivek narayan's  https://github.com/vivekn/redis-simple-cache
    and ohanetz's https://github.com/ohanetz/redis-simple-cache-3k

