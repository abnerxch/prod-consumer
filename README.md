# Problema de sincronización de procesos

El problema consiste en implementar el modelo de productor y consumidor en un buffer de
tamaño limitado.

Se debe generar un conjunto de leads para la compra de vehículos, los hilos productores
comparten un archivo (personas.csv) que deben leer al azar hasta que se termine el archivo. Los productores no pueden enviar personas repetidas.

Cada vez que un productor lee exitosamente un registro de personas deberá generar un lead
para ser enviado a la cola de compradores donde un hilo consumidor vende un carro (los
vendedores de vehículos adquieren a las personas).

## Clonar repo

```bash
git clone https://github.com/abnerxch/prod-consumer.git

cd prod-consumer
```

## MySQL con Docker

En la terminal, hacer lo siguiente:

```bash
docker pull mysql:5.6

docker run --name devmysql -e MYSQL_ROOT_PASSWORD=test123 -p 3306:3306 -d mysql:5.6
```

Ingresar a modo CLI:

```bash
docker exec -it devmysql /bin/bash
```
Ingresar lo siguiente:

```mysql
mysql -p
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'test123';
```

Abrir una nueva terminal y ejecutar el proyecto

```python
python3 MAinn.py <tamaño_buffer> <cantidad_productores> <archivo_consumidores> <con_sin_alternancia>
```

Ejemplo:

```python
python3 MAinn.py 100 5 consumidores.csv 1
```