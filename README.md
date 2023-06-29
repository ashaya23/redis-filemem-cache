# Redis-Filemem-Cache
Redis and File cache for storing functions with datetime as one of the params

## Features

- Caches data in both Redis for short-term and local files for persitent storage 
- Provides options to enable/disable file and memory caching independently.
- This project includes a set of atomic read and write functions that ensure data integrity and reliability when reading from and writing to files.
- Ability to list keys stored both in file and memory cache
- Ability to selectively purge certain cache entries based on different criterion 

## Installation

To use this code, you need to have Python installed along with the required dependencies. Follow these steps to install and set up the environment:

1. Clone this repository: `git clone https://github.com/ashaya23/redis-filemem-cache`
2. Change to the project directory: `cd redis-filemem-cache`
3. Run setup.py script : `python setup.py install` 


## Usage
   
   
    import datetime as dt
   
    cache = FileMemCache(namespace="pycache1",filecache=r'z:\cache' )

    @cache.cache_it()
    def test(dateDt, a,b):
        if dateDt >= dt.date(2000,3,1):
           return (a+b) * dateDt.month
         else:
            return a*b  * dateDt.year


    test(datetime.date(2000,1,1), 10, 20)
    test(datetime.date(2005,1,1), 10, 20)
                
    cache.list_memory()
    cache.list_files()

    The code is influenced by vivek narayan's  https://github.com/vivekn/redis-simple-cache
    and ohanetz's https://github.com/ohanetz/redis-simple-cache-3k

