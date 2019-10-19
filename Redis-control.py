
import redis 
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
import json
from threading import Thread
import threading
import time

class Cliente(object):
    """ Prototipo de la clase modelo
        Copiar y pegar tantas veces como modelos se deseen crear (cambiando
        el nombre Model, por la entidad correspondiente), o bien crear tantas
        clases como modelos se deseen que hereden de esta clase. Este segundo 
        metodo puede resultar mas compleja
    """
    required_vars = []
    admissible_vars = []
    db = None
    r_cache = None
    

    def __init__(self, **kwargs):
        #TODO
        self.__dict__.update(kwargs)
        for key in self.required_vars:
            if(key not in kwargs):
                raise Exception("Error")
                
        
    def __str__(self):  
        string = ""
        for key,value in self.__dict__.items():
            string +=(key + ": " + str(value) + "\n")
        return string
      
    @classmethod
    def query(cls, _id):
        """ Devuelve un cursor de modelos        
        """ 
        key = _id
        if(cls.r_cache.existsKey(key)):
          print("Datos devuletos desde la cache")
          cls.r_cache.updateTimeValue(key)
          return cls.r_cache.getValue(key)
        else:
          obj = cls.db.find_one({'_id':ObjectId(_id)})
          if(obj is None):
            print("Objeto no encontrado")
            return None
          obj = Cliente(**obj)
          cls.r_cache.insertKeyValue(key, obj)
          print("Datos devuletos de la base de datos")
          return obj

    @classmethod
    def init_class(cls, db, cache, vars_path="model_name.vars"):
        """ Inicializa las variables de clase en la inicializacion del sistema.
        Argumentos:
            db (MongoClient) -- Conexion a la base de datos.
            vars_path (str) -- ruta al archivo con la definicion de variables
            del modelo.
        """
        cls.db = db
        cls.r_cache = cache
            
        with open(vars_path) as file:
            cls.required_vars = file.readline().split()
            cls.admissible_vars = file.readline().split()
            
class CacheRedis (object):
   
    redis_server = None
    time_exipre = None
    size = None
   
    def __init__(self, size):
        self.redis_server = redis.Redis("localhost", port = 6379)
        self.size = size +"mb" #150 bytes por 10ˆ6 para pasarlo a MB
        self.time_expire = 24 * 60 * 60 #24 horas, 84600 segundos
        self.redis_server.config_set('maxmemory',self.size)
        self.redis_server.config_set('maxmemory-policy',"volatile-ttl")
   
    def updateTimeValue(self, key):
        value = self.getValue(key)
        self.redis_server.set(key, value,ex=self.time_expire, xx = True)
       
    def insertKeyValue(self, key, cursorClient):
        value = str(cursorClient).replace("\n", " ")
        self.redis_server.set(key, value,ex=self.time_expire,nx = True)
        
    def getValue(self, key):
        return self.redis_server.get(key)
    
    def existsKey(self,key):
        return self.redis_server.exists(key)
      
     
class Sesion(object):
   
    #tiempo de sesion
    TDS = None
    nombre = None
    nombre_usuario = None
    password = None
    token = None
    privilegios = None
   
    def __init__(self, **kwargs):
        kwargs = kwargs['kwargs']
       
        #nombre, usuario, password
        self.redis_server = redis.Redis("localhost", port = 6379)
        self.TDS = 24 * 60 * 60 * 30 # 24 horas, lo pasamos a segundos y multiplicamos por 30 dias.
        self.nombre = kwargs[0]
        self.nombre_usuario = kwargs[1]
        self.password = kwargs[2]
        self.token = kwargs[2] #el token es el mismo que la password
        self.privilegios = 1
       
        
    def conexion_redis(self):
        self.redis_server.hset("usuarios", self.token, "{'nombre':" + self.nombre + ", 'nombre_usuario':" + self.nombre_usuario + ", 'password':" + self.password + ", 'privilegios':" + str(self.privilegios) + ", 'token':" + self.token + "}")
        self.redis_server.set(self.token, self.nombre_usuario, ex=self.TDS, xx = True)
       
       
    #metodo de acceso y actualizacion de los datos
    def update(self):
        #para actualizar los datos, los volvemos a pedir todos
        nuevos_datos = pedir_credenciales()
       
        self.nombre = nuevos_datos[0]
        self.nombre_usuario = nuevos_datos[1]
        self.password = nuevos_datos[2]
        self.token = nuevos_datos[2]
       
        self.conexion_redis()
       
   
    #un metodo para hacer login y generar una nueva sesion. Devolvera el valor
    #del campo privilegios junto al token asignado en caso de login satisfactorio
    #y -1 en caso contrario
    def login(self, **kwargs):
        kwargs = kwargs['kwargs']
       
        if (self.nombre_usuario == kwargs[1] and self.password == kwargs[2]):
            #hacemos login
            print("Bienvendio " + self.nombre + "!!")
            self.conexion_redis()
            return [self.privilegios, self.token]
       
        else:
            return -1
       
    #un metodo para hacer login mediante un token de sesion. Devolvera el valor del
    #campo privilegios asignados en caso de login satisfactorio. -1 en caso contrario
    def login_token(self, **kwargs):
        kwargs = kwargs['kwargs']
       
        existe_token = self.redis_server.hexists("usuarios", self.token)
           
       
        if(self.token == kwargs[2] and existe_token):
            #hacemos login
            print("Bienvendio " + self.nombre + "!!  connected from token")
            self.conexion_redis()
            return self.privilegios
        else:
            return -1
          
          
class MyThread(Thread):
    cache = None
 
    def __init__(self, actual_thread_id):
        ''' Constructor. '''
        Thread.__init__(self)
        self.time_to_sleep = 60
        self.cache = redis.Redis("localhost", port = 6379)
        self.main_thread = threading.Thread(target = self.run, args = (actual_thread_id,), daemon = True)
        self.lock = threading.Lock()
 
   
    def principal_service(self):
        self.main_thread.start()
           
 
    def run(self, actual_thread_id):
       
        if(actual_thread_id == 0):
            while(1):
                print('Hilo %d' % actual_thread_id)
                print("este thread es un demonio")
                self.lock.acquire()
                compra_demon = self.cache.brpop("cola_compras", 0)
                self.lock.release()
               
                new_thread = threading.Thread(target= self.run, args = (actual_thread_id + 1,), daemon = False)
                new_thread.start()
               
                self.unpacking(compra_demon)
                
                dt_object = datetime.now()
                service = {'id_service':(actual_thread_id), 'day':str(dt_object.day)}
                self.cache.lpush("services" + str(dt_object.month) + "-"  + str(dt_object.year),str(service))
           
        else:
            print('Hilo %d' % actual_thread_id)
            print("Este thread NO es un demonio")
            self.lock.acquire()
            compra = self.cache.brpop("cola_compras",self.time_to_sleep)
            self.lock.release()
            if(compra is not None):
                new_thread2 = threading.Thread(target= self.run, args = (actual_thread_id + 1,), daemon = False)
                new_thread2.start()
                self.unpacking(compra)
                
                dt_object = datetime.now()
                
                service = {'id_service':(actual_thread_id), 'day':str(dt_object.day)}
                self.cache.lpush("services" + str(dt_object.month) +"-" + str(dt_object.year), str(service))
                new_thread2.join()
       
     
    def unpacking(self, purchase):
       
        print('Empaquetando...')
        time.sleep(self.time_to_sleep)
        print('Empaquetado completo!!')
        
        
        
      
class Packaking(object):
  redis_server = None
  def __init__(self):
    self.redis_server = redis.Redis("localhost", port = 6379)
    self.month_list_services = {}
    
  def insert_purchase(self, purchase):
    self.redis_server.lpush("cola_compras", str(purchase))
    
  def get_month_analisis(self,month,year):
    tuplas = self.redis_server.lrange("services" + str(month) + "-" + str(year), 0, -1)
    
    for element in reversed(tuplas):
      element = json.loads(str(element)[2:-1].replace("'",'"'))

      if('id_service' + "-" + str(element['id_service']) not in self.month_list_services):
        self.month_list_services['id_service' + "-" + str(element['id_service'])] = 0
      self.month_list_services['id_service' + "-" + str(element['id_service'])] = self.month_list_services['id_service' + "-" + str(element['id_service'])] + 1 
    
    self.redis_server.hset('month-analisis',str(month) + "-" + str(year),str(self.month_list_services))
    return self.month_list_services
  
  def get_year_analisis(self,year):
    tuplas = self.redis_server.hmget('month-analisis',"1-"+str(year),"2-"+str(year),"3-"+str(year),"4-"+str(year),"5-"+str(year),"6-"+str(year),"7-"+str(year),"8-"+str(year),"9-"+str(year),"10-"+str(year),"11-"+str(year),"12-"+str(year))
    aux = json.loads(str(tuplas[0])[2:-1].replace("'",'"')) #primero cojemos la primera posicion, luego la utilizamos para compara en cada iteracion

    for i in range(1,len(tuplas)):
      element = json.loads(str(tuplas[i])[2:-1].replace("'",'"'))

      aux = self.compare_month_services(aux,element)
      
    self.redis_server.hset('year-analisis',str(year),str(aux))
    return aux
  
  def compare_month_services(self,first_month_service,second_month_service):
    result = None
    
    if(len(first_month_service) > len(second_month_service)):
      result = second_month_service
    else:
      result = first_month_service
      
    for key in result:

      max_value=0
      if(first_month_service[key] >= second_month_service[key]):
        max_value = first_month_service[key]
      else:
        max_value = second_month_service[key]
      
      result[key] = max_value
    
    return result
        
    
def pedir_credenciales():
   
    print("introduce tu nombre:")
    nombre = input()
   
    print("introduce tu usuario:")
    nombre_usuario = input()
   
    print("introduce tu password:")
    password = input()

    return[nombre, nombre_usuario, password]
      
if __name__ == '__main__':
   
   # cache_redis(mongo)
   id_thread = 0
   daemon_thread = MyThread(id_thread)
   daemon_thread.principal_service()
   
   database_json = "Database1.json"
   cache_size = "150"
   cache = CacheRedis(cache_size)
   client = MongoClient()
   
   db = client['Tienda']
   with open(database_json,'r') as insertItems:
        model_data = json.loads(insertItems.read())
   
   
   print("\nCache:")     
   Cliente.init_class(db['clientes'], cache,"ClienteVariables.txt")
   
   cursorClient = Cliente.query("5ca5f0bfdfa6bd07bfd92461")
   print(cursorClient)
   
   print("\nSesiones:")
   credenciales = pedir_credenciales()
   sesion = Sesion(kwargs=credenciales)
   
   sesion.login(kwargs=credenciales)
   sesion.login_token(kwargs=credenciales)
   #sesion.update()
   
   print("\nEmpaquetado:")
   empaquetado = Packaking()
   compra = { "_id" : ObjectId("5c9e0dc7dfa6bd3db4a19e6e"), "producto" : { "nombre" : "iphone 7", "proveedor" : [ { "nombre" : "Apple Inc.", "codigo" : "x000" }, { "nombre" : "Almacenes Ching Chong", "codigo" : "y111" }, { "nombre" : "Pccomponentes", "codigo" : "z222" }, { "nombre" : "Tienda de barrio cutre", "codigo" : "h333" } ], "precio_con_iva" : 700, "coste_de_envio" : 50, "descuento_por_rango_de_fechas" : { "from" : "2019-02-17T00:00:00.000Z", "to" : "2019-03-08T00:00:00.000Z" }, "dimensiones" : { "Altura" : 160, "Anchura" : 60, "Profundidad" : 10 }, "peso" : 100, "proveedores_almacen" : { "nombre" : "calle de Ferraz 31,Madrid", "coordinates" : { "type" : "Point", "coordinates" : [ -3.7193226, 40.4280601 ] } } }, "cliente" : { "nombre" : "Raul", "fecha_de_ultimo_acceso" : "2019-02-02T00:00:00.000Z" }, "precio_de_compra" : 700, "fecha_de_compra" : "2012-02-02T00:00:00.000Z", "direccion_de_envio" : { "nombre" : "calle del Buen Suceso 25,Madrid", "coordinates" : { "type" : "Point", "coordinates" : [ -3.7197467, 40.4278862 ] } } }
   
   '''
   empaquetado.insert_purchase(compra)
   empaquetado.insert_purchase(compra)
   empaquetado.insert_purchase(compra)
   empaquetado.insert_purchase(compra)
   '''
   
   month_service = empaquetado.get_month_analisis(4,2019)
   year_service = empaquetado.get_year_analisis(2019)
   
   print("ANALISIS-MENSUAL:") 
   print(month_service)
   
   print("ANALISIS-ANUAL:") 
   print(year_service)

