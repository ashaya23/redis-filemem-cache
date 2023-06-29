"""
Created on May 2, 2019

@author: ashaya


   Redis and File cache for storing functions indexed by datetime
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


"""

from functools import wraps
import pickle
import hashlib
import redis
import warnings
import inspect
import re
import os
import shutil
import glob

def to_unicode(obj, encoding='utf-8'):
    if not isinstance(obj, str):
        obj = str(obj, encoding)
    return obj

DEFAULT_EXPIRY = 60*60

class RedisConnect(object):
    """
    A simple object to store and pass database connection information.
    This makes the Simple Cache class a little more flexible, for cases
    where redis connection configuration needs customizing.
    """
    def __init__(self, host=None, port=None, db=None, password=None, decode_responses=True, encoding='iso-8859-1'):
        self.host = host if host else 'localhost'
        self.port = port if port else 6379
        self.db = db if db else 1
        self.password = password
        self.decode_responses = decode_responses
        self.encoding = encoding

    def connect(self):
        """
        We cannot assume that connection will succeed, as such we use a ping()
        method in the redis client library to validate ability to contact redis.
        RedisNoConnException is raised if we fail to ping.
        :return: redis.StrictRedis Connection Object
        """
        try:
            redis.StrictRedis(host=self.host, port=self.port, password=self.password).ping()
        except redis.ConnectionError as e:
            raise RedisNoConnException("Failed to create connection to redis",
                                       (self.host,
                                        self.port)
            )
        return redis.StrictRedis(host=self.host,
                                 port=self.port,
                                 db=self.db,
                                 password=self.password,
                                 decode_responses=self.decode_responses,
                                 encoding=self.encoding)


class CacheMissException(Exception):
    pass


class ExpiredKeyException(Exception):
    pass


class RedisNoConnException(Exception):
    pass


class DoNotCache(Exception):
    _result = None

    def __init__(self, result):
        super(DoNotCache, self).__init__()
        self._result = result

    @property
    def result(self):
        return self._result


class FileMemCache(object):
    def __init__(self,
                 limit=10000,
                 expire=DEFAULT_EXPIRY,
                 filecache = None,
                 donotfilecahe = False,
                 donotmemcache=False,
                 host=None,
                 port=None,
                 db=None,
                 password=None,
                 namespace="cache1",
                 decode_responses=True,
                 encoding='iso-8859-1'):
        # filecache     - is directory location for saving data in file cache.
        # expire        - Time to keys to expire in seconds. Files in filecache never expire
        # limit         - No of json encoded strings to cache. So such limit on file cache
        # donotfilecahe - as the name suggests this can be used with certain function that do not need to be cached in file
        # donotmemcache  - disable redis cache

        self.limit = limit
        self.expire = expire
        self.namespace = namespace
        self.donotfilecahe = donotfilecahe
        self.host = host
        self.port = port
        self.filecache = filecache
        self.donotmemcache = donotmemcache
        self.db = db

        if (not self.filecache) and (not self.donotfilecahe ):
            warnings.warn('Parameter filecahe is empty. Disabling file cache')
            self.donotfilecahe = True


        if not self.donotmemcache:
            try:
                self.connection = RedisConnect(host=self.host,
                                               port=self.port,
                                               db=self.db,
                                               password=password,
                                               decode_responses=decode_responses,
                                               encoding=encoding).connect()
            except RedisNoConnException:
                self.connection = None

                if not self.donotmemcache:
                    warnings.warn('Redis server unreachable. Diabling mem cache')
                    self.donotmemcache = True
        else:
            self.connection = None




    def __iter__(self):
        if not self.connection:
            return iter([])
        return iter(self.connection.keys(f"{self.namespace}:*") )


    def __contains__(self, key):
        return self.connection.sismember(self.get_set_name(key), key)


    def get_set_name(self,cache_key):
        return cache_key.rsplit(':',1)[0]

    def store(self, key, value, expire=None):
        """
        Method stores a value after checking for space constraints and
        freeing up space if required.
        :param key: key by which to reference datum being stored in Redis
        :param value: actual value being stored under this key
        :param expire: time-to-live (ttl) for this datum
        """
        key = to_unicode(key)
        #value = to_unicode(value)
        set_name = self.get_set_name(key)

        while self.connection.scard(set_name) >= self.limit:
            del_key = self.connection.spop(set_name)
            self.connection.delete(del_key)

        pipe = self.connection.pipeline()
        if expire is None:
            expire = self.expire

        if (isinstance(expire, int) and expire <= 0) or (expire is None):
            pipe.set(key, value)
        else:
            pipe.setex(key, expire, value)

        pipe.sadd(set_name, key)
        pipe.execute()


    def store_key(self, key, value, expire=None):
        self.store(key, pickle.dumps(value), expire)

    def store_key_file(self, key, value):
        full_file_name, file_dir, date_file = self.key_to_file(key)

        self.atomicwrite( file_dir, date_file, value)

    def key_to_file(self,key):

        namespace_dir, func, func_dir, date_file = key.split(':')

        func_dir = 'P' + func_dir
        date_file = date_file + '.pkl'
        file_dir = os.path.join(self.filecache, namespace_dir, func_dir)
        self.makeDirIfNotExist(file_dir)
        full_file_name = os.path.join(file_dir, date_file)

        return full_file_name, file_dir, date_file

    def atomicwrite(self, destination, filename, data):

        file_path = os.path.join(destination, filename)

        # store hash for
        check_sum = hashlib.md5(str(data).encode()).hexdigest()
        save_data = {'data' : data , 'check_sum' : check_sum}

        try:
            # Create a temporary file in the destination location
            temp_file_path = file_path + '.tmp'

            # Write the pickled object to the temporary file
            with open(temp_file_path, 'wb') as temp_file:
                pickle.dump(save_data, temp_file)

            # Perform atomic write by renaming the temporary file to the final destination
            shutil.move(temp_file_path, file_path)
        except Exception as e:
            # Handle any error that occurs during the write or rename process
            print(f"Error: writing {file_path}")
            # Clean up the temporary file if needed
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def atomicread(self,file_name):

        with open(file_name, 'rb') as file:
            unpickled_data = pickle.load(file)

        data = unpickled_data['data']
        check_sum = hashlib.md5(str(data).encode()).hexdigest()
        if check_sum == unpickled_data['check_sum']:
            return data
        else:
            # remove incorrect file
            print(f"Incorrect check sum for {file_name}. Removing!")
            os.remove(file_name)
            return None



    def get(self, key, encoding='iso-8859-1'):

        key = to_unicode(key)
        if key:  # No need to validate membership, which is an O(1) operation, but seems we can do without.
            value = self.connection.get(key)
            if value is None:  # expired key
                # check load it from file cache
                if not self.donotfilecahe:
                    file_name = self.key_to_file(key)[0]
                    if os.path.exists(file_name):
                        value = self.atomicread(file_name)
                        # save the file in memory
                        self.store_key(key,value)
                        # return the value
                        return value

                if not key in self:  # If key does not exist at all, it is a straight miss.
                    raise CacheMissException

                self.connection.srem(self.get_set_name(key), key)
                raise ExpiredKeyException
            else:
                value = pickle.loads(value.encode(encoding))
                # before returning value make sure the key in there in the file cache
                if not self.donotfilecahe:
                    file_name = self.key_to_file(key)[0]
                    if os.path.exists(file_name):
                        self.store_key_file(key, value)
                return value




    def makeDirIfNotExist(self,dirName):
        if not os.path.isdir(dirName):
            os.makedirs(dirName)

    def read_funcDef(self):
        cache_dir = os.path.join(self.filecache, self.namespace)
        funcDefDir = os.path.join(cache_dir, 'funcDefDir')

        all_keys = glob.glob(os.path.join(funcDefDir, 'P*'))

        funcDef_list = []
        for funcDefFile in all_keys:
            hash_str = os.path.split(funcDefFile)[1].replace('P', '').replace('.txt', '')

            with open(funcDefFile, 'r') as f:
                txt = f.read()
            f.close()

            funcDef_list.append(hash_str + ' | ' + txt)

        return funcDef_list

    def list_memory(self, func='',show = True):
        # set is a unique function/param conbination.
        # i.e. each unique function and param combination is saved like a set
        # List all set keys saved in memory

        if self.donotmemcache:
            print('Mem cache is disabled')
            return []

        set_str = []

        set_keys = self.connection.keys(f"{self.namespace}:{func}*")
        # Filter out only the set keys
        sets = [key for key in set_keys if self.connection.type(key) == 'set']
        for s in sets:
            k = self.connection.get(s + ':funcDef')
            if k:
                set_str.append(s + ' | ' + k)


        if show:
            for i in set_str:
                print(i)
            print(f"Total : {len(set_str)} keys found")
        else:
            return set_str



    def list_files(self, func='',  show = True):
        # set is a unique function/param conbination.
        # i.e. each unique function and param combination is saved like a set
        # List all set keys saved in files

        if self.donotfilecahe:
            print('File cache is disabled')
            return []


        set_str = self.read_funcDef()

        for key in set_str:
            if (len(func) > 0):
                if func not in key:
                    set_str.remove(key)

        if show:
            for i in set_str:
                print(i)
            print(f"Total : {len(set_str)} keys found")
        else:
            return set_str



    def get_hash(self,  funcname, bound_arguments):

        # the keys are stored as namespace:funcname:parameters hexcode:dateStr
        # to list all enteries for funcname, just supply funcname
        sorted_params = dict(sorted(bound_arguments.arguments.items(), key=lambda x: x[0]))
        try:
            dateDt = sorted_params.pop('dateDt')
            date_str = dateDt.strftime('%Y%m%d_%H%M')
        except:
            date_str = '0'

        st = (funcname + str(sorted_params)).encode()
        key = hashlib.sha512(st).hexdigest()
        # the cache also keeps a copy of function and paramters value for better diagnostics purposes.
        funcDef= st.decode()
        funcDefKey = f'{self.namespace}:{funcname}:{key}:funcDef'

        if not self.connection.keys(funcDefKey):
            # create funcDef entry is missing
            pipe = self.connection.pipeline()
            pipe.set(funcDefKey, funcDef)

            pipe.sadd(f'{self.namespace}:funcDef', funcDefKey)
            pipe.execute()

        if self.filecache:
            funcDef_dir = os.path.join(self.filecache, self.namespace,'funcDefDir')
            self.makeDirIfNotExist(funcDef_dir)
            funcDef_file = os.path.join(funcDef_dir, 'P' + key + '.txt')
            if not os.path.exists(funcDef_file):
                with open(funcDef_file,'w') as f:
                    f.write(funcDef)
                f.close()

        cache_key = f'{self.namespace}:{funcname}:{key}:{date_str}'

        return cache_key

    def clear_memory(self, func , param_str,start_str, end_str, hash_str, show = True):


        if self.donotmemcache:
            print('Mem cache is disabled')
            return


        if param_str:
            # this is a special case where the keys in list keys are further pared
            # down by likeness to function parameters to param_str e.g. we may want to delete getSf1 function which has
            # parameter 'pe' in the parameter dictionary.

            memory_list = self.list_memory(show=False)
            param_filter_list = []
            for m in memory_list:
                if param_str in m:
                    param_filter_list.append(m.split(' | ')[0])

        search_str = f"{self.namespace}"
        if func:
            search_str = f"{search_str}:{func}:"

        if hash_str:
            search_str = f"{search_str}*:{hash_str}"

        if search_str[-1] != '*' :
            search_str = search_str + '*'

        if start_str or end_str:
            # exact match for start_date when end_date is not applicable
            if start_str and not end_str:
                search_str = search_str + 'start_str'

        all_keys = []
        param_str_new = []
        if param_str:

            for p in param_filter_list:
                pattern = search_str.replace('*', '.*')
                match = re.match(pattern, p)
                if match:
                    param_str_new.append(p)

            for p in param_str_new:
                p = p + '*'
                all_keys = all_keys + self.connection.keys(p)

        else:
            all_keys = self.connection.keys(search_str)

        keys = []
        # case when start and end date is supplied
        if start_str and end_str:
            for k in all_keys:
                # only match the date
                datestr = re.findall(r'\d{8}_\d{4}$', k)
                if len(datestr) == 1:
                    datestr = datestr[0]
                    if (start_str <= datestr) and (datestr <= end_str):
                        keys.append(k)
        else:
            keys = all_keys



        if len(keys) > 0:
            with self.connection.pipeline() as pipe:
                pipe.delete(*keys)
                pipe.execute()

            if show:
                for i in keys:
                    print(f'Deleted : {i}')
                print(f"Total : {len(keys)} keys deleted from MEMORY")
        else:
            print('No key with matching fingerprint found in MEMORY. No key was deleted')

    def clear_files(self, func, param_str, start_str, end_str, hash_str, show=True):


        if self.donotfilecahe:
            print('File cache is disabled')
            return

        if param_str:
            # this is a special case where the keys in list keys are further pared
            # down by likeness to function parameters to param_str e.g. we may want to delete getSf1 function which has
            # parameter 'pe' in the parameter dictionary.

            file_list = self.list_files(show=False)
            param_filter_list = []
            for m in file_list:
                if param_str in m:
                    param_filter_list.append(m.split(' | ')[0])

        if len(end_str) == 0:
            if len(start_str) == 0:
                end_str = '99999999_9999'
            else:
                end_str = start_str

        if len(start_str) == 0:
            start_str = '0'

        cache_dir = os.path.join(self.filecache, self.namespace)
        funcDefDir = os.path.join(cache_dir, 'funcDefDir')

        all_keys = []
        if param_str:
            for p in param_filter_list:
                all_keys.append(os.path.join(cache_dir, 'P' + p))
        else:
            all_keys = glob.glob(os.path.join(cache_dir, 'P*'))

        count = 0
        for file_dir in all_keys:
            all_files = glob.glob(os.path.join(file_dir, '*.pkl'))
            key_dir = os.path.split(file_dir)[1]
            funcDefFile = os.path.join(funcDefDir, key_dir + '.txt')

            if func:
                with open(funcDefFile, 'r') as f:
                    txt = f.read()
                f.close()

                extracted_func = re.findall(r'(.*)({.*})', txt)[0][0]

                if not re.match(func.replace('*', '.*'), extracted_func):
                    # skip this iteration
                    continue

            if hash_str:
                if key_dir[1:] != hash_str:
                    # skip this iteration
                    continue

            for f in all_files:
                dateStr = os.path.split(f)[1].replace('.pkl', '')
                if (start_str <= dateStr) & (dateStr <= end_str):
                    full_file_name = os.path.join(file_dir, f)
                    try:
                        os.remove(full_file_name)
                        if show:
                            print(f'Deleted : {full_file_name}')
                        count += 1
                    except FileNotFoundError:
                        pass
                    except:
                        print(f"Cannot delete {full_file_name}")

            # check if the directory is empty
            if len(os.listdir(file_dir)) == 0:
                shutil.rmtree(file_dir)
                os.remove(funcDefFile)

            if show:
                print(f"Total : {count} FILES deleted")


    def clear(self, func = None, param_str = None,start_date= None, end_date = None, hash_str = None, show = True, memory=True, file=True):
        # func       - delete all enteries related to funcname, func* to delete all the functions starting with func
        # start_date - delete all enteries for start_date (just for funcname if supplied) this could be datetime object or YYYYMMDD_HHMM formatted string
        # end_date   - delete all enteries between start_date and end_date ( just for funcname if supplied) this could be datetime object or YYYYMMDD_HHMM formatted string
        # hash_str   - delete all enteries matching hash_str, and any of the above applicable conditions
        # memory     - clear in memory cached data
        # file       - clear cached files data

        if not start_date and end_date:
            raise ValueError('End date without a start date in cache.clear')

        if start_date:
            if type(start_date) is not str:
                start_str = start_date.strftime('%Y%m%d_%H%M')
            else:
                start_str = start_date
        else:
            start_str = ''

        if end_date:
            if type(start_date) is not str:
                end_str = end_date.strftime('%Y%m%d_%H%M')
            else:
                end_str = end_date
        else:
            end_str = ''


        if memory:
            self.clear_memory( func, param_str, start_str, end_str, hash_str, show)


        if file:
            self.clear_files( func, param_str, start_str, end_str, hash_str, show)



    def cache_it(self, expire= None):
        """
        This is a decorator factory
        Arguments and function result must be pickleable.
        :param cache: FileMemCache object, if created separately
        :return: decorated function
        """

        expire_ = expire
        def decorator(function):
            expire =  expire_

            @wraps(function)
            def func(*args, **kwargs):

                signature = inspect.signature(func)
                bound_arguments = signature.bind(*args, **kwargs)
                bound_arguments.apply_defaults()

                ## Handle cases where caching is down or otherwise not available.
                if self.connection is None:
                    result = function(*args, **kwargs)
                    return result

                ## key will be an hdf5 key in the form of namespace:func_name:hash for parameters:dateDt
                ## in the form of `function name`:`key`
                cache_key = self.get_hash( function.__name__, bound_arguments)


                try:
                    return self.get(cache_key,  encoding='iso-8859-1')
                except (ExpiredKeyException, CacheMissException) as e:
                    ## Add some sort of cache miss handing here.
                    pass
                except:
                    raise "Unknown redis-simple-cache error. Please check your Redis free space."


                try:
                    result = function(*args, **kwargs)
                except DoNotCache as e:
                    result = e.result
                else:
                    try:
                        # memory cache
                        self.store_key(cache_key, result, expire)
                        if not self.donotfilecahe:
                            # save it in file cache
                            self.store_key_file( cache_key, result)
                    except redis.ConnectionError as e:
                        raise e

                return result
            return func
        return decorator


