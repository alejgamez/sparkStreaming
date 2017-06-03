import os  
from pyspark import SparkContext  
from pyspark.streaming import StreamingContext  
from pyspark.streaming.kafka import KafkaUtils  
import json  

def createContext():  
    sc = SparkContext(appName="pySparkStreamingKafka")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)

    # Definir el consmidor de kafka indicando el servidor de zookeeper , el group id  y el topic
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})

    
    # Extraer los tweets
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))

    # Se cuentan los tweets en un bach
    count_this_batch = kafkaStream.count().map(lambda x:('Tweets en el batch: %s' % x))

    # Se cuentan por una ventana de tiempo
    count_windowed = kafkaStream.countByWindow(60,5).map(lambda x:('Tweets totales (agrupados en un minuto): %s' % x))

    # Obtenemos los autores
    authors_dstream = parsed.map(lambda tweet: tweet['user']['screen_name'])

    # 
    count_values_this_batch = authors_dstream.countByValue()\
                                .transform(lambda rdd:rdd\
                                  .sortBy(lambda x:-x[1]))\
                              .map(lambda x:"Autores contados en el batch:\tValue %s\tCount %s" % (x[0],x[1]))

   
    count_values_windowed = authors_dstream.countByValueAndWindow(60,5)\
                                .transform(lambda rdd:rdd\
                                  .sortBy(lambda x:-x[1]))\
                            .map(lambda x:"Autores contados por un minuto:\tValue %s\tCount %s" % (x[0],x[1]))

    count_this_batch.union(count_windowed).pprint()

    # Se muestra el resultado por pantalla
    count_values_this_batch.pprint(5)
    count_values_windowed.pprint(5)

    return ssc
# Se debe tener cuidado con el checkpoint ya que si existe un error puede probocar inconsistencia en futuras ejecuciones
ssc = StreamingContext.getOrCreate('/tmp/checkpoint_v01',lambda: createContext())  
ssc.start()  
ssc.awaitTermination() 