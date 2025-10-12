## Micro DB TCP/IP - Servidor y Cliente

### Requisitos
- C (GCC o compatible)
- Linux/Unix (usa sockets POSIX, pthreads y flock)
- Make (opcional, para usar el Makefile)

### Compilación

#### Opción 1: Usando Makefile (Recomendado)
```bash
# Compilar todo y crear archivo CSV de ejemplo
make setup

# Solo compilar servidor y cliente
make all

# Compilar individualmente
make servidor
make cliente

# Ver ayuda del Makefile
make help
```

#### Opción 2: Compilación manual
- Servidor:
```bash
gcc servidor.c -o servidor -pthread -lm -Wall -Wextra -std=c99 -O2
```
- Cliente:
```bash
gcc cliente.c -o cliente -Wall -Wextra -std=c99 -O2
```

### Archivo CSV (base de datos)
- Nombre por defecto: `registros_generados.csv`
- Formato: `ID;Producto;Cantidad;Precio` (separado por punto y coma)
- Se crea automáticamente con datos de ejemplo usando `make setup`
- Debe existir en el mismo directorio que el ejecutable del servidor

### Ejecución

#### Configuración inicial rápida
```bash
# Compilar y crear archivo CSV de ejemplo
make setup

# Ejecutar servidor en una terminal
./servidor

# Ejecutar cliente en otra terminal
./cliente
```

#### Parámetros del servidor
```bash
./servidor                           # Valores por defecto (127.0.0.1:8080, N=5, M=5)
./servidor N M                       # Configurar clientes concurrentes (N) y backlog (M)
./servidor IP PUERTO N M             # Configurar IP, puerto, clientes concurrentes y backlog
```

#### Parámetros del cliente
```bash
./cliente                            # Conecta a servidor local (127.0.0.1:8080)
./cliente IP PUERTO                  # Conecta a servidor específico
```

**Nota:** Si se proporcionan parámetros incorrectos, ambos programas mostrarán ayuda automáticamente.

Al conectarse, el servidor asigna y muestra un identificador único: "Usuario N".

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

#### 1) Configuración inicial y consulta básica
```bash
# Terminal 1: Compilar y ejecutar servidor
make setup
./servidor

# Terminal 2: Conectar cliente y consultar
./cliente
SELECT ALL
```

#### 2) Transacción con modificaciones
```bash
BEGIN TRANSACTION
INSERT 100;Router;5;199.99
UPDATE ID=10 SET Precio=15.50
DELETE ID=10
COMMIT TRANSACTION
```

#### 3) Consultas con filtros
```bash
SELECT WHERE Producto=Tablet
SELECT WHERE ID=5
SELECT WHERE Cantidad>20
SELECT WHERE Precio<100
```

### Características técnicas

#### Manejo de concurrencia
- El servidor maneja hasta N clientes concurrentes usando pthreads
- Cada cliente se ejecuta en un hilo independiente
- Sistema de bloqueo exclusivo para transacciones usando `flock()`
- Durante una transacción activa, otros clientes no pueden realizar operaciones

#### Robustez
- Manejo de señales (SIGINT, SIGTERM, SIGPIPE)
- Limpieza automática de recursos al cerrar
- Validación de parámetros con ayuda automática
- Manejo de desconexiones inesperadas

#### Optimizaciones
- Envío de datos grandes en chunks para evitar saturación de buffers
- Marcadores de fin de mensaje para contenido fragmentado
- Gestión eficiente de memoria con realloc dinámico

### Comandos del Makefile
```bash
make setup      # Compilar todo y crear CSV de ejemplo
make all        # Compilar servidor y cliente
make servidor   # Compilar solo el servidor
make cliente    # Compilar solo el cliente
make csv        # Crear archivo CSV de ejemplo
make clean      # Eliminar ejecutables
make clean-all  # Eliminar ejecutables y CSV
make help       # Mostrar ayuda del Makefile
make info       # Mostrar información del sistema
```

### Notas adicionales
- El servidor usa valores por defecto configurables vía `load_config()`
- Soporte para parámetros de línea de comandos para IP, puerto, N y M
- El archivo CSV se crea automáticamente con datos de ejemplo usando `make setup`
- Compatible con sistemas Linux/Unix que soporten POSIX sockets y pthreads