## Micro DB TCP/IP - Servidor y Cliente

### Requisitos
- C (GCC o compatible)
- Linux/Unix (usa sockets POSIX, pthreads y flock)

### Compilación
- Servidor:
```
gcc servidor.c -o servidor -pthread -lm
```
- Cliente:
```
gcc cliente.c -o cliente
```

### Archivo CSV (base de datos)
- Nombre por defecto: `registros_generados.csv`
- Formato: `ID;Producto;Cantidad;Precio` (separado por punto y coma)
- Debe existir en el mismo directorio que el ejecutable del servidor.

### Ejecución
- Servidor:
```
./servidor                           # Usa valores por defecto (127.0.0.1:8080, N=5, M=5)
./servidor N M                       # Configurar clientes concurrentes (N) y backlog (M)
./servidor IP PUERTO N M             # Configurar IP, puerto, clientes concurrentes y backlog
```
- Cliente:
```
./cliente                            # Conecta a servidor local (127.0.0.1:8080)
./cliente IP PUERTO                  # Conecta a servidor específico
```

**Nota:** Si se proporcionan parámetros incorrectos, ambos programas mostrarán ayuda automáticamente.

Al conectarse, el servidor asigna y muestra un identificador: "Usuario N".

### Protocolo de comandos (lado cliente)
Comandos soportados (escribir y presionar Enter):
- Consultas (sin transacción):
  - `SELECT ALL`
  - `SELECT WHERE CAMPO=VALOR`
    - CAMPO: `ID`, `Producto`, `Cantidad`, `Precio`
    - Ejemplos: `SELECT WHERE Producto=Tablet`, `SELECT WHERE ID=10`

- Transacciones y DML (requieren transacción activa):
  - `BEGIN TRANSACTION`
  - `INSERT id;producto;cantidad;precio`
    - Ej: `INSERT 100;Router;5;199.99`
  - `UPDATE ID=<id> SET Campo=Valor`
    - Ej: `UPDATE ID=10 SET Precio=15.50`, `UPDATE ID=20 SET Cantidad=42`, `UPDATE ID=30 SET Producto=Mouse`
  - `DELETE ID=<id>`
  - `COMMIT TRANSACTION`

- Control:
  - `HELP` (lista comandos detallados con ejemplos)
  - `EXIT` (cierra la conexión del cliente)

**Nota:** Si se ingresa un comando incorrecto, el servidor mostrará automáticamente la ayuda detallada.

### Reglas de concurrencia y bloqueo
- `BEGIN TRANSACTION` toma un lock exclusivo sobre `registros_generados.csv`.
- Mientras el lock esté activo:
  - Solo ese cliente puede ejecutar DML.
  - Otros clientes que intenten `SELECT` o DML reciben: `ERROR: Transaccion activa en curso. Reintente luego.`
- DML fuera de transacción responde: `ERROR: Las modificaciones requieren BEGIN TRANSACTION.`
- `COMMIT TRANSACTION` libera el lock y persiste cambios.

### Parámetros N y M
- `N`: cantidad de clientes concurrentes máximos. El servidor maneja cada cliente en un hilo hasta `N`.
- `M`: backlog de `listen` (clientes en espera de aceptación).

### Robustez y cierre controlado
- El servidor ignora `SIGPIPE` y maneja `SIGINT/SIGTERM` liberando lock y cerrando el socket de escucha.
- El cliente ignora `SIGPIPE` y cierra su socket en `SIGINT/SIGTERM`.
- Si un cliente cae durante una transacción, el servidor libera el lock y continúa atendiendo otros.

### Ejemplos rápidos
1) Consultar todo:
```
./servidor 3 10
./cliente
SELECT ALL
```
2) Transacción con modificaciones:
```
BEGIN TRANSACTION
INSERT 100;Router;5;199.99
UPDATE ID=10 SET Precio=15.50
DELETE ID=10
COMMIT TRANSACTION
```
3) Filtro:
```
SELECT WHERE Producto=Tablet
```

### Notas
- El servidor usa valores por defecto vía `load_config`; también podés pasar IP/PUERTO por parámetro.
- Si querés lectura de configuración desde archivo (p. ej., `server.conf`), se puede agregar fácilmente.