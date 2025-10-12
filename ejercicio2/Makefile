# Makefile para Micro DB TCP/IP
# Servidor y Cliente de Base de Datos Simple

# Compilador y flags
CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -O2
LDFLAGS_SERVIDOR = -pthread -lm
LDFLAGS_CLIENTE = 

# Nombres de archivos fuente
SERVIDOR_SRC = servidor.c
CLIENTE_SRC = cliente.c

# Nombres de ejecutables
SERVIDOR_EXE = servidor
CLIENTE_EXE = cliente

# Archivo CSV de ejemplo
CSV_FILE = registros_generados.csv

# Regla por defecto: compilar todo
all: $(SERVIDOR_EXE) $(CLIENTE_EXE)

# Compilar servidor
$(SERVIDOR_EXE): $(SERVIDOR_SRC)
	@echo "Compilando servidor..."
	$(CC) $(CFLAGS) $(SERVIDOR_SRC) -o $(SERVIDOR_EXE) $(LDFLAGS_SERVIDOR)
	@echo "Servidor compilado exitosamente: $(SERVIDOR_EXE)"

# Compilar cliente
$(CLIENTE_EXE): $(CLIENTE_SRC)
	@echo "Compilando cliente..."
	$(CC) $(CFLAGS) $(CLIENTE_SRC) -o $(CLIENTE_EXE) $(LDFLAGS_CLIENTE)
	@echo "Cliente compilado exitosamente: $(CLIENTE_EXE)"

# Crear archivo CSV de ejemplo
csv: $(CSV_FILE)

$(CSV_FILE):
	@echo "Creando archivo CSV de ejemplo..."
	@echo "ID;Producto;Cantidad;Precio" > $(CSV_FILE)
	@echo "1;Laptop;10;899.99" >> $(CSV_FILE)
	@echo "2;Mouse;50;25.50" >> $(CSV_FILE)
	@echo "3;Teclado;30;45.00" >> $(CSV_FILE)
	@echo "4;Monitor;15;299.99" >> $(CSV_FILE)
	@echo "5;Tablet;8;199.99" >> $(CSV_FILE)
	@echo "6;Router;12;79.99" >> $(CSV_FILE)
	@echo "7;Impresora;5;149.99" >> $(CSV_FILE)
	@echo "8;Webcam;20;35.00" >> $(CSV_FILE)
	@echo "9;Auriculares;25;59.99" >> $(CSV_FILE)
	@echo "10;Disco Duro;18;89.99" >> $(CSV_FILE)
	@echo "Archivo CSV creado: $(CSV_FILE)"

# Compilar todo y crear CSV
setup: all csv
	@echo ""
	@echo "=== CONFIGURACIÓN COMPLETA ==="
	@echo "Servidor: $(SERVIDOR_EXE)"
	@echo "Cliente: $(CLIENTE_EXE)"
	@echo "Base de datos: $(CSV_FILE)"
	@echo ""
	@echo "Para ejecutar:"
	@echo "  Servidor: ./$(SERVIDOR_EXE) [N M] [IP PUERTO N M]"
	@echo "  Cliente:  ./$(CLIENTE_EXE) [IP PUERTO]"
	@echo ""

# Limpiar archivos compilados
clean:
	@echo "Limpiando archivos compilados..."
	rm -f $(SERVIDOR_EXE) $(CLIENTE_EXE)
	@echo "Archivos compilados eliminados."

# Limpiar todo (incluyendo CSV)
clean-all: clean
	@echo "Limpiando archivo CSV..."
	rm -f $(CSV_FILE)
	@echo "Todos los archivos generados eliminados."

# Mostrar ayuda
help:
	@echo "=== MAKEFILE MICRO DB ==="
	@echo ""
	@echo "Comandos de compilación:"
	@echo "  make all        - Compilar servidor y cliente"
	@echo "  make servidor   - Compilar solo el servidor"
	@echo "  make cliente    - Compilar solo el cliente"
	@echo "  make csv        - Crear archivo CSV de ejemplo"
	@echo "  make setup      - Compilar todo y crear CSV"
	@echo "  make clean      - Eliminar ejecutables"
	@echo "  make clean-all  - Eliminar ejecutables y CSV"
	@echo ""
	@echo "Comandos de ejecución:"
	@echo "  make run-servidor        - Ejecutar servidor con parámetros por defecto"
	@echo "  make run-servidor-simple - Ejecutar servidor solo con N y M"
	@echo "  make run-cliente         - Ejecutar cliente con IP y puerto específicos"
	@echo "  make run-cliente-local   - Ejecutar cliente conectando a localhost"
	@echo ""
	@echo "Ejemplos de configuración:"
	@echo "  make run-servidor-3-10   - Servidor con 3 clientes concurrentes y 10 en espera"
	@echo "  make run-servidor-10-20 - Servidor con 10 clientes concurrentes y 20 en espera"
	@echo "  make run-servidor-20-50 - Servidor con 20 clientes concurrentes y 50 en espera"
	@echo ""
	@echo "Parámetros personalizados:"
	@echo "  make run-servidor N=15 M=30        - Servidor con N=15 y M=30"
	@echo "  make run-servidor IP=192.168.1.100 PUERTO=9090 N=8 M=20"
	@echo ""
	@echo "Ejemplos de uso:"
	@echo "  make setup              # Configuración inicial completa"
	@echo "  make run-servidor-3-10  # Ejecutar con configuración específica"
	@echo "  make clean              # Limpiar después de compilar"
	@echo ""

# Reglas adicionales para compilar individualmente
servidor: $(SERVIDOR_EXE)

cliente: $(CLIENTE_EXE)

# Regla para mostrar información del sistema
info:
	@echo "=== INFORMACIÓN DEL SISTEMA ==="
	@echo "Compilador: $(CC)"
	@echo "Flags: $(CFLAGS)"
	@echo "Servidor flags: $(LDFLAGS_SERVIDOR)"
	@echo "Cliente flags: $(LDFLAGS_CLIENTE)"
	@echo "Sistema operativo: $(shell uname -s)"
	@echo "Arquitectura: $(shell uname -m)"
	@echo ""

# Variables para parámetros del servidor (pueden ser sobrescritas)
SERVIDOR_N ?= 5
SERVIDOR_M ?= 5
SERVIDOR_IP ?= 127.0.0.1
SERVIDOR_PUERTO ?= 8080

# Ejecutar servidor con parámetros por defecto
run-servidor: $(SERVIDOR_EXE) csv
	@echo "Ejecutando servidor con parámetros por defecto..."
	@echo "IP: $(SERVIDOR_IP), Puerto: $(SERVIDOR_PUERTO), N: $(SERVIDOR_N), M: $(SERVIDOR_M)"
	./$(SERVIDOR_EXE) $(SERVIDOR_IP) $(SERVIDOR_PUERTO) $(SERVIDOR_N) $(SERVIDOR_M)

# Ejecutar servidor solo con N y M
run-servidor-simple: $(SERVIDOR_EXE) csv
	@echo "Ejecutando servidor con N=$(SERVIDOR_N) y M=$(SERVIDOR_M)..."
	./$(SERVIDOR_EXE) $(SERVIDOR_N) $(SERVIDOR_M)

# Ejecutar cliente
run-cliente: $(CLIENTE_EXE)
	@echo "Ejecutando cliente conectando a $(SERVIDOR_IP):$(SERVIDOR_PUERTO)..."
	./$(CLIENTE_EXE) $(SERVIDOR_IP) $(SERVIDOR_PUERTO)

# Ejecutar cliente con servidor local
run-cliente-local: $(CLIENTE_EXE)
	@echo "Ejecutando cliente conectando a servidor local..."
	./$(CLIENTE_EXE)

# Ejemplos de configuración
run-servidor-3-10: $(SERVIDOR_EXE) csv
	@echo "Ejecutando servidor con 3 clientes concurrentes y 10 en espera..."
	./$(SERVIDOR_EXE) 3 10

run-servidor-10-20: $(SERVIDOR_EXE) csv
	@echo "Ejecutando servidor con 10 clientes concurrentes y 20 en espera..."
	./$(SERVIDOR_EXE) 10 20

run-servidor-20-50: $(SERVIDOR_EXE) csv
	@echo "Ejecutando servidor con 20 clientes concurrentes y 50 en espera..."
	./$(SERVIDOR_EXE) 20 50

# Marcar reglas como no archivos
.PHONY: all clean clean-all help setup csv servidor cliente info run-servidor run-servidor-simple run-cliente run-cliente-local run-servidor-3-10 run-servidor-10-20 run-servidor-20-50
